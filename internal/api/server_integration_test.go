// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file" // Import file source driver
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/storage"
)

// TestAuthenticationIntegration tests the complete authentication flow with a real HTTP server and database.
func TestAuthenticationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("correlator_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("Failed to start postgres container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Errorf("Failed to terminate postgres container: %v", err)
		}
	})

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	defer func() {
		_ = db.Close()
	}()

	// Run migrations
	if err := runTestMigrations(db); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Create storage connection directly with config
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	defer func() {
		_ = conn.Close()
	}()

	// Wrap in storage.Connection
	storageConn := &storage.Connection{DB: conn}

	// Create key store
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	if err != nil {
		t.Fatalf("Failed to create key store: %v", err)
	}

	defer func() {
		_ = keyStore.Close()
	}()

	// Create test API key
	testAPIKey, err := storage.GenerateAPIKey("test-plugin")
	if err != nil {
		t.Fatalf("Failed to generate API key: %v", err)
	}

	apiKey := &storage.APIKey{
		ID:          "test-key-id",
		Key:         testAPIKey,
		PluginID:    "test-plugin",
		Name:        "Test Plugin",
		Permissions: []string{"lineage:write", "lineage:read"},
		CreatedAt:   time.Now(),
		ExpiresAt:   nil,
		Active:      true,
	}

	if err := keyStore.Add(ctx, apiKey); err != nil {
		t.Fatalf("Failed to add API key: %v", err)
	}

	// Create server config (pure configuration only)
	config := &ServerConfig{
		Port:               8080,
		Host:               "localhost",
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		LogLevel:           slog.LevelInfo,
		CORSAllowedOrigins: []string{"*"},
		CORSAllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		CORSAllowedHeaders: []string{"Content-Type", "Authorization", "X-Correlation-ID", "X-API-Key"},
		CORSMaxAge:         86400,
	}

	// Create server with dependency injection
	server := NewServer(config, keyStore, nil)

	t.Run("Successful Authentication with X-Api-Key Header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
		req.Header.Set("X-Api-Key", testAPIKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusOK, status, rr.Body.String())
		}

		// Verify correlation ID header is set
		if correlationID := rr.Header().Get("X-Correlation-ID"); correlationID == "" {
			t.Error("Expected X-Correlation-ID header to be set")
		}
	})

	t.Run("Successful Authentication with Authorization Bearer Header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
		req.Header.Set("Authorization", "Bearer "+testAPIKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusOK, status, rr.Body.String())
		}
	})

	t.Run("Missing API Key Returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusUnauthorized, status, rr.Body.String())
		}

		// Verify RFC 7807 error response
		var errorResp map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &errorResp); err != nil {
			t.Fatalf("Failed to parse error response: %v", err)
		}

		if errorResp["type"] == nil {
			t.Error("Expected RFC 7807 'type' field in error response")
		}

		if errorResp["title"] == nil {
			t.Error("Expected RFC 7807 'title' field in error response")
		}

		if errorResp["status"] == nil {
			t.Error("Expected RFC 7807 'status' field in error response")
		}

		if errorResp["detail"] == nil {
			t.Error("Expected RFC 7807 'detail' field in error response")
		}

		if errorResp["correlationId"] == nil {
			t.Error("Expected RFC 7807 'correlationId' field in error response")
		}
	})

	t.Run("Invalid API Key Returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
		req.Header.Set("X-Api-Key", "correlator_ak_"+string(make([]byte, 64)))

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusUnauthorized, status, rr.Body.String())
		}
	})

	t.Run("Inactive API Key Returns 403", func(t *testing.T) {
		// Create inactive API key
		inactiveKey, err := storage.GenerateAPIKey("inactive-plugin")
		if err != nil {
			t.Fatalf("Failed to generate inactive API key: %v", err)
		}

		inactiveAPIKey := &storage.APIKey{
			ID:          "inactive-key-id",
			Key:         inactiveKey,
			PluginID:    "inactive-plugin",
			Name:        "Inactive Plugin",
			Permissions: []string{"lineage:write"},
			CreatedAt:   time.Now(),
			ExpiresAt:   nil,
			Active:      false, // Inactive
		}

		if err := keyStore.Add(ctx, inactiveAPIKey); err != nil {
			t.Fatalf("Failed to add inactive API key: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
		req.Header.Set("X-Api-Key", inactiveKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusForbidden {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusForbidden, status, rr.Body.String())
		}
	})

	t.Run("Expired API Key Returns 401", func(t *testing.T) {
		// Create expired API key
		expiredKey, err := storage.GenerateAPIKey("expired-plugin")
		if err != nil {
			t.Fatalf("Failed to generate expired API key: %v", err)
		}

		expiredTime := time.Now().Add(-1 * time.Hour)
		expiredAPIKey := &storage.APIKey{
			ID:          "expired-key-id",
			Key:         expiredKey,
			PluginID:    "expired-plugin",
			Name:        "Expired Plugin",
			Permissions: []string{"lineage:write"},
			CreatedAt:   time.Now().Add(-2 * time.Hour),
			ExpiresAt:   &expiredTime, // Expired 1 hour ago
			Active:      true,
		}

		if err := keyStore.Add(ctx, expiredAPIKey); err != nil {
			t.Fatalf("Failed to add expired API key: %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/api/v1/version", nil)
		req.Header.Set("X-Api-Key", expiredKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusUnauthorized, status, rr.Body.String())
		}
	})

	// Enable after completing task 4
	// t.Run("Health Endpoints Work Without Authentication", func(t *testing.T) {
	//	endpoints := []string{"/ping", "/api/v1/health"}
	//
	//	for _, endpoint := range endpoints {
	//		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	//
	//		rr := httptest.NewRecorder()
	//		server.httpServer.Handler.ServeHTTP(rr, req)
	//
	//		if status := rr.Code; status != http.StatusOK {
	//			t.Errorf("Endpoint %s: Expected status %d, got %d. Body: %s",
	//				endpoint, http.StatusOK, status, rr.Body.String())
	//		}
	//	}
	// })
}

// TestRateLimitingIntegration tests the complete rate limiting flow with a real HTTP server and database.
func TestRateLimitingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("correlator_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("Failed to start postgres container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(pgContainer); err != nil {
			t.Errorf("Failed to terminate postgres container: %v", err)
		}
	})

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	defer func() {
		_ = db.Close()
	}()

	// Run migrations
	if err := runTestMigrations(db); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Create storage connection
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	defer func() {
		_ = conn.Close()
	}()

	// Wrap in storage.Connection
	storageConn := &storage.Connection{DB: conn}

	// Create key store
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	if err != nil {
		t.Fatalf("Failed to create key store: %v", err)
	}

	defer func() {
		_ = keyStore.Close()
	}()

	// Create test API keys for plugin-1 and plugin-2
	apiKey1, err := storage.GenerateAPIKey("plugin-1")
	if err != nil {
		t.Fatalf("Failed to generate API key for plugin-1: %v", err)
	}

	apiKeyObj1 := &storage.APIKey{
		ID:          "plugin-1-key-id",
		Key:         apiKey1,
		PluginID:    "plugin-1",
		Name:        "Plugin 1",
		Permissions: []string{"lineage:write", "lineage:read"},
		CreatedAt:   time.Now(),
		ExpiresAt:   nil,
		Active:      true,
	}

	if err := keyStore.Add(ctx, apiKeyObj1); err != nil {
		t.Fatalf("Failed to add API key for plugin-1: %v", err)
	}

	apiKey2, err := storage.GenerateAPIKey("plugin-2")
	if err != nil {
		t.Fatalf("Failed to generate API key for plugin-2: %v", err)
	}

	apiKeyObj2 := &storage.APIKey{
		ID:          "plugin-2-key-id",
		Key:         apiKey2,
		PluginID:    "plugin-2",
		Name:        "Plugin 2",
		Permissions: []string{"lineage:write", "lineage:read"},
		CreatedAt:   time.Now(),
		ExpiresAt:   nil,
		Active:      true,
	}

	if err := keyStore.Add(ctx, apiKeyObj2); err != nil {
		t.Fatalf("Failed to add API key for plugin-2: %v", err)
	}

	// Create server config
	serverConfig := &ServerConfig{
		Port:               8080,
		Host:               "localhost",
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		LogLevel:           slog.LevelInfo,
		CORSAllowedOrigins: []string{"*"},
		CORSAllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		CORSAllowedHeaders: []string{"Content-Type", "Authorization", "X-Correlation-ID", "X-API-Key"},
		CORSMaxAge:         86400,
	}

	// Test 1: Global Rate Limit Enforcement
	t.Run("Global Rate Limit Enforcement", func(t *testing.T) {
		// Create limiter: 5 RPS global, 50 RPS plugin (global is bottleneck)
		// Use 5 RPS to make limit easier to hit despite bcrypt latency (~50ms/request)
		rateLimiter := createTestRateLimiter(5, 50, 2)
		defer rateLimiter.Close()

		// Create server with rate limiter
		server := NewServer(serverConfig, keyStore, rateLimiter)

		// Send requests alternating between plugin-1 and plugin-2
		// With 5 RPS global limit and ~50ms bcrypt latency, we expect some rate limiting
		successCount := 0
		rateLimitedCount := 0

		// Send 15 requests rapidly
		for i := 0; i < 15; i++ {
			apiKey := apiKey1 // pragma: allowlist secret
			if i%2 == 1 {
				apiKey = apiKey2 // pragma: allowlist secret
			}

			response := makeAuthenticatedRequest(server, apiKey, "/api/v1/version")
			switch response.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
				// Verify RFC 7807 error format on first rate-limited response
				if rateLimitedCount == 1 {
					verifyRFC7807Error(t, response, http.StatusTooManyRequests)
				}
			}
		}

		// At 5 RPS global limit, some requests should be rate limited
		if rateLimitedCount == 0 {
			t.Errorf("Expected some requests to be rate limited (global limit), but all %d succeeded", successCount)
		}

		// Verify both plugins were affected (global limit applies to all)
		// We can't test this directly, but the fact that we have rate-limited requests
		// while alternating between plugins verifies global limiting
	})

	// Test 2: Per-Plugin Rate Limit Enforcement
	t.Run("Per-Plugin Rate Limit Enforcement", func(t *testing.T) {
		// Create limiter: 100 RPS global, 2 RPS plugin (plugin is bottleneck)
		// Use 2 RPS to make limit easier to hit despite bcrypt latency (~50ms/request)
		rateLimiter := createTestRateLimiter(100, 2, 1)
		defer rateLimiter.Close()

		// Create server with rate limiter
		server := NewServer(serverConfig, keyStore, rateLimiter)

		// Plugin 1: Send requests until rate limited
		// With 2 RPS limit and ~50ms bcrypt latency, we need more than 2 requests
		// to exhaust burst capacity (4 tokens = 2 RPS × 2 burst multiplier)
		successCount := 0
		rateLimitedCount := 0

		// Send 10 requests rapidly
		for i := 0; i < 10; i++ {
			response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/version")
			switch response.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
			}
		}

		// At 2 RPS with 50ms auth latency, some requests should be rate limited
		if rateLimitedCount == 0 {
			t.Errorf("Expected some requests to be rate limited, but all %d succeeded", successCount)
		}

		// Plugin 2: Should have independent limit
		// Reset counters
		successCount = 0
		rateLimitedCount = 0

		// Send 10 requests to plugin-2
		for i := 0; i < 10; i++ {
			response := makeAuthenticatedRequest(server, apiKey2, "/api/v1/version")
			switch response.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
				// Verify RFC 7807 error format on first rate-limited response
				if rateLimitedCount == 1 {
					verifyRFC7807Error(t, response, http.StatusTooManyRequests)
				}
			}
		}

		// Plugin 2 should also get some rate limited requests (independent limit)
		if rateLimitedCount == 0 {
			t.Errorf("Plugin-2 should have independent rate limit, but all %d requests succeeded", successCount)
		}
	})

	// Test 3: Unauthenticated Rate Limit Enforcement
	t.Run("Unauthenticated Rate Limit Enforcement", func(t *testing.T) {
		// Create limiter: 100 RPS global, 50 RPS plugin, 1 RPS unauth
		// Very low unauth limit (1 RPS) to test rate limiting of unauthenticated requests
		rateLimiter := createTestRateLimiter(100, 50, 1)
		defer rateLimiter.Close()

		// Create server with rate limiter
		server := NewServer(serverConfig, keyStore, rateLimiter)

		// IMPORTANT: Middleware order is Auth → RateLimit
		// Unauthenticated requests get rejected by Auth middleware (401)
		// BEFORE they reach the rate limiter
		// So we cannot directly test unauthenticated rate limiting in this configuration

		// Instead, verify that:
		// 1. Unauthenticated requests consistently return 401 (auth layer)
		// 2. Authenticated requests have independent rate limits

		// Send multiple unauthenticated requests - all should get 401
		for i := 0; i < 5; i++ {
			response := makeAuthenticatedRequest(server, "", "/api/v1/version")
			if response.Code != http.StatusUnauthorized {
				t.Errorf("Unauthenticated request %d should get 401 (auth fails), got %d", i+1, response.Code)
			}
		}

		// Verify authenticated requests work independently
		response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/version")
		if response.Code != http.StatusOK {
			t.Errorf("Authenticated request should succeed, got status %d", response.Code)
		}

		// Note: To truly test unauthenticated rate limiting, we would need
		// public endpoints (Task 4) where rate limiting happens before auth
	})

	// Test 4: Token Refill After Rate Limit
	t.Run("Token Refill After Rate Limit", func(t *testing.T) {
		// Create limiter: 100 RPS global, 2 RPS plugin (very restrictive)
		rateLimiter := createTestRateLimiter(100, 2, 1)
		defer rateLimiter.Close()

		// Create server with rate limiter
		server := NewServer(serverConfig, keyStore, rateLimiter)

		// Exhaust the rate limit by sending requests rapidly
		// With 2 RPS and burst=4, we should hit the limit quickly
		successCount := 0
		rateLimitedCount := 0

		for i := 0; i < 10; i++ {
			response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/version")
			switch response.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
				// Verify RFC 7807 on first rate-limited response
				if rateLimitedCount == 1 {
					verifyRFC7807Error(t, response, http.StatusTooManyRequests)
				}
			}
		}

		// Should have some rate limited requests
		if rateLimitedCount == 0 {
			t.Errorf("Expected some requests to be rate limited, but all %d succeeded", successCount)
		}

		// Wait for token refill (600ms = 1.2 tokens at 2 RPS)
		time.Sleep(600 * time.Millisecond)

		// After wait, at least 1 token should have refilled
		response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/version")
		if response.Code != http.StatusOK {
			t.Errorf("Expected request to succeed after token refill, got %d. Body: %s",
				response.Code, response.Body.String())
		}
	})
}

// runTestMigrations runs database migrations for testing.
// Uses golang-migrate for single source of truth.
func runTestMigrations(db *sql.DB) error {
	driver, err := migratepg.WithInstance(db, &migratepg.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://../../migrations",
		"postgres",
		driver,
	)
	if err != nil {
		return err
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

// Helper functions for rate limiting integration tests

// createTestRateLimiter creates a rate limiter with explicit configuration for testing.
//
// Parameters:
//   - globalRPS: Global rate limit (requests per second)
//   - pluginRPS: Per-plugin rate limit (requests per second)
//   - unauthRPS: Unauthenticated rate limit (requests per second)
//
// Burst capacity is automatically computed as 2 × rate for all tiers.
func createTestRateLimiter(globalRPS, pluginRPS, unauthRPS int) *middleware.InMemoryRateLimiter {
	config := &middleware.Config{
		GlobalRPS: globalRPS,
		PluginRPS: pluginRPS,
		UnAuthRPS: unauthRPS,
		// Burst values left as 0 to use auto-computed defaults (2 × rate)
		GlobalBurst: 0,
		PluginBurst: 0,
		UnAuthBurst: 0,
	}

	return middleware.NewInMemoryRateLimiter(config)
}

// makeAuthenticatedRequest creates and executes an HTTP request with API key authentication.
//
// Parameters:
//   - server: The server instance to test against
//   - apiKey: The API key to use for authentication (empty string for unauthenticated requests)
//   - path: The request path (e.g., "/api/v1/version")
//
// Returns:
//   - *httptest.ResponseRecorder containing the response
func makeAuthenticatedRequest(server *Server, apiKey, path string) *httptest.ResponseRecorder { //nolint:unparam
	req := httptest.NewRequest(http.MethodGet, path, nil)

	// Add API key header if provided (supports authenticated requests)
	if apiKey != "" {
		req.Header.Set("X-Api-Key", apiKey)
	}

	rr := httptest.NewRecorder()
	server.httpServer.Handler.ServeHTTP(rr, req)

	return rr
}

// verifyRFC7807Error validates that an HTTP response follows RFC 7807 Problem Details format.
//
// Checks for required fields:
//   - type: URI reference identifying the problem type
//   - title: Short, human-readable summary
//   - status: HTTP status code
//   - detail: Human-readable explanation
//   - instance: URI reference identifying the specific occurrence
//   - correlationId: Correlator-specific correlation ID for request tracing
//
// Parameters:
//   - t: Testing instance
//   - response: The HTTP response to validate
//   - expectedStatus: The expected HTTP status code
func verifyRFC7807Error(t *testing.T, response *httptest.ResponseRecorder, expectedStatus int) {
	t.Helper()

	// Verify HTTP status code
	if response.Code != expectedStatus {
		t.Errorf("Expected status %d, got %d. Body: %s", expectedStatus, response.Code, response.Body.String())
	}

	// Verify Content-Type header
	contentType := response.Header().Get("Content-Type")
	if contentType != "application/problem+json" {
		t.Errorf("Expected Content-Type 'application/problem+json', got '%s'", contentType)
	}

	// Parse JSON response
	var problem map[string]interface{}
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("Failed to parse RFC 7807 error response: %v", err)
	}

	// Verify required RFC 7807 fields
	requiredFields := []string{"type", "title", "status", "detail", "instance", "correlationId"}
	for _, field := range requiredFields {
		if problem[field] == nil {
			t.Errorf("Missing required RFC 7807 field: %s", field)
		}
	}

	// Verify status field matches HTTP status code
	if statusValue, ok := problem["status"].(float64); ok {
		if int(statusValue) != expectedStatus {
			t.Errorf("RFC 7807 'status' field (%d) does not match HTTP status code (%d)",
				int(statusValue), expectedStatus)
		}
	}
}
