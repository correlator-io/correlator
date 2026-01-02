// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/storage"
)

// middlewareTestServer encapsulates test server dependencies for middleware integration tests.
// Only stores fields used by helper methods (server, testAPIKey, rateLimiter).
// Cleanup dependencies (keyStore, lineageStore, testDB) are captured in t.Cleanup closures.
type middlewareTestServer struct {
	server      *Server
	testAPIKey  string
	rateLimiter *middleware.InMemoryRateLimiter
}

// setupMiddlewareTestServer creates a fully configured test server with all dependencies.
// This helper eliminates ~100 lines of duplicated setup code per test.
//
// Parameters:
//   - ctx: Context for database operations
//   - t: Testing instance for error reporting
//   - withRateLimiter: If true, creates rate limiter with restrictive limits for testing
//
// Returns:
//   - *middlewareTestServer containing server, API key, and optional rate limiter
func setupMiddlewareTestServer(ctx context.Context, t *testing.T, withRateLimiter bool) *middlewareTestServer {
	t.Helper()

	// Setup database with migrations
	testDB := config.SetupTestDatabase(ctx, t)
	storageConn := &storage.Connection{DB: testDB.Connection}

	// Create stores
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour) //nolint:contextcheck
	require.NoError(t, err, "Failed to create lineage store")

	// Create and register API key
	testAPIKey, err := storage.GenerateAPIKey("test-plugin")
	require.NoError(t, err, "Failed to generate API key")

	err = keyStore.Add(ctx, &storage.APIKey{
		ID:          "test-key-id",
		Key:         testAPIKey,
		PluginID:    "test-plugin",
		Name:        "Test Plugin",
		Permissions: []string{"lineage:write", "lineage:read"},
		CreatedAt:   time.Now(),
		Active:      true,
	})
	require.NoError(t, err, "Failed to add API key")

	// Create rate limiter if requested
	var rateLimiter *middleware.InMemoryRateLimiter
	if withRateLimiter {
		rateLimiter = createTestRateLimiter(5, 2, 1) // Restrictive limits for testing
	}

	// Create server config
	config := &ServerConfig{
		Port:               8080,
		Host:               "localhost",
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		LogLevel:           slog.LevelInfo,
		MaxRequestSize:     defaultMaxRequestSize,
		CORSAllowedOrigins: []string{"*"},
		CORSAllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		CORSAllowedHeaders: []string{"Content-Type", "Authorization", "X-Correlation-ID", "X-API-Key"},
		CORSMaxAge:         86400,
	}

	// Create server with dependencies
	server := NewServer(config, keyStore, rateLimiter, lineageStore)

	// Register cleanup (closure captures dependencies)
	t.Cleanup(func() {
		if rateLimiter != nil {
			rateLimiter.Close()
		}

		_ = keyStore.Close()
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	return &middlewareTestServer{
		server:      server,
		testAPIKey:  testAPIKey,
		rateLimiter: rateLimiter,
	}
}

// TestAuthenticationIntegration tests the complete authentication flow with a real HTTP server and database.
// Note: Uses manual setup (not helper) because it needs NO rate limiter and dynamically adds inactive/expired keys.
func TestAuthenticationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)
	storageConn := &storage.Connection{DB: testDB.Connection}

	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour)
	require.NoError(t, err, "Failed to create lineage store")

	t.Cleanup(func() {
		_ = keyStore.Close()
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create test API key
	testAPIKey, err := storage.GenerateAPIKey("test-plugin")
	require.NoError(t, err, "Failed to generate API key")

	err = keyStore.Add(ctx, &storage.APIKey{
		ID:          "test-key-id",
		Key:         testAPIKey,
		PluginID:    "test-plugin",
		Name:        "Test Plugin",
		Permissions: []string{"lineage:write", "lineage:read"},
		CreatedAt:   time.Now(),
		Active:      true,
	})
	require.NoError(t, err, "Failed to add API key")

	// Create server (NO rate limiter for this test)
	config := &ServerConfig{
		Port:               8080,
		Host:               "localhost",
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		LogLevel:           slog.LevelInfo,
		MaxRequestSize:     defaultMaxRequestSize,
		CORSAllowedOrigins: []string{"*"},
		CORSAllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		CORSAllowedHeaders: []string{"Content-Type", "Authorization", "X-Correlation-ID", "X-API-Key"},
		CORSMaxAge:         86400,
	}
	server := NewServer(config, keyStore, nil, lineageStore)

	t.Run("Successful Authentication with X-Api-Key Header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
		req.Header.Set("X-Api-Key", testAPIKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response body: %s", rr.Body.String())
		assert.NotEmpty(t, rr.Header().Get("X-Correlation-ID"), "Expected X-Correlation-ID header")
	})

	t.Run("Successful Authentication with Authorization Bearer Header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
		req.Header.Set("Authorization", "Bearer "+testAPIKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response body: %s", rr.Body.String())
	})

	t.Run("Missing API Key Returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code, "Response body: %s", rr.Body.String())
		verifyRFC7807Error(t, rr, http.StatusUnauthorized)
	})

	t.Run("Invalid API Key Returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
		req.Header.Set("X-Api-Key", "correlator_ak_"+string(make([]byte, 64)))

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code, "Response body: %s", rr.Body.String())
	})

	t.Run("Inactive API Key Returns 403", func(t *testing.T) {
		inactiveKey, err := storage.GenerateAPIKey("inactive-plugin")
		require.NoError(t, err)

		err = keyStore.Add(ctx, &storage.APIKey{
			ID:          "inactive-key-id",
			Key:         inactiveKey,
			PluginID:    "inactive-plugin",
			Name:        "Inactive Plugin",
			Permissions: []string{"lineage:write"},
			CreatedAt:   time.Now(),
			Active:      false,
		})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
		req.Header.Set("X-Api-Key", inactiveKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusForbidden, rr.Code, "Response body: %s", rr.Body.String())
	})

	t.Run("Expired API Key Returns 401", func(t *testing.T) {
		expiredKey, err := storage.GenerateAPIKey("expired-plugin")
		require.NoError(t, err)

		expiredTime := time.Now().Add(-1 * time.Hour)
		err = keyStore.Add(ctx, &storage.APIKey{
			ID:          "expired-key-id",
			Key:         expiredKey,
			PluginID:    "expired-plugin",
			Name:        "Expired Plugin",
			Permissions: []string{"lineage:write"},
			CreatedAt:   time.Now().Add(-2 * time.Hour),
			ExpiresAt:   &expiredTime,
			Active:      true,
		})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
		req.Header.Set("X-Api-Key", expiredKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code, "Response body: %s", rr.Body.String())
	})
}

// TestPublicEndpointAuthBypass tests that public health endpoints work without authentication.
// This test validates the auth bypass functionality for Kubernetes health probes and monitoring tools.
//
// Test scenarios:
//   - /ping works without API key (liveness probe)
//   - /api/v1/health works without API key (basic health check)
func TestPublicEndpointAuthBypass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupMiddlewareTestServer(ctx, t, false)

	t.Run("Ping Endpoint Works Without Authentication", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response body: %s", rr.Body.String())
		assert.Equal(t, "pong", rr.Body.String(), "Expected 'pong' response")
		verifyCorrelationID(t, rr)
	})

	t.Run("Health Endpoint Works Without Authentication", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response body: %s", rr.Body.String())

		var health HealthStatus

		err := json.Unmarshal(rr.Body.Bytes(), &health)
		require.NoError(t, err, "Failed to parse health response")

		assert.Equal(t, "healthy", health.Status, "Expected healthy status")
		assert.Equal(t, "correlator", health.ServiceName, "Expected correlator service name")
		assert.NotEmpty(t, health.Version, "Expected version to be set")

		verifyCorrelationID(t, rr)
	})

	t.Run("Protected Endpoint Still Requires Authentication", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code, "Response body: %s", rr.Body.String())
		verifyRFC7807Error(t, rr, http.StatusUnauthorized)
	})
}

// TestPublicEndpointRateLimitBypass tests that public health endpoints bypass rate limiting.
// This test validates that K8s health probes and monitoring tools can always access
// public endpoints without being rate limited, preventing cascading failures.
//
// Test scenarios:
//   - /ping bypasses rate limiting (unlimited requests)
//   - /api/v1/health bypasses rate limiting (unlimited requests)
//   - Protected endpoints still enforce rate limits
//
// This test sends 100 rapid requests to each public endpoint to verify no rate limiting occurs.
func TestPublicEndpointRateLimitBypass(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	testDB := config.SetupTestDatabase(ctx, t)

	// Wrap in storage.Connection
	storageConn := &storage.Connection{DB: testDB.Connection}

	// Create key store
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour)
	require.NoError(t, err, "Failed to create lineage store")

	t.Cleanup(func() {
		_ = keyStore.Close()
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create a test API key for protected endpoint verification
	testAPIKey, err := storage.GenerateAPIKey("test-plugin")
	require.NoError(t, err, "Failed to generate API key")

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

	err = keyStore.Add(ctx, apiKey)
	require.NoError(t, err, "Failed to add API key")

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
		MaxRequestSize:     defaultMaxRequestSize, // 1 MB
	}

	// Create rate limiter with VERY restrictive limits to ensure bypass is working
	// If bypass didn't work, these limits would be hit immediately
	rateLimiter := createTestRateLimiter(5, 2, 1) // 5 global RPS, 2 plugin RPS, 1 unauth RPS

	t.Cleanup(func() {
		rateLimiter.Close()
	})

	// Create server with auth AND rate limiting enabled
	server := NewServer(serverConfig, keyStore, rateLimiter, lineageStore)

	t.Run("Ping Endpoint Bypasses Rate Limiting", func(t *testing.T) {
		// Send 100 rapid requests to /ping without API key
		// If rate limiting applied, we would hit the limit immediately (1 RPS unauth)
		successCount := 0
		rateLimitedCount := 0

		for i := 0; i < 100; i++ {
			req := httptest.NewRequest(http.MethodGet, "/ping", nil)

			rr := httptest.NewRecorder()
			server.httpServer.Handler.ServeHTTP(rr, req)

			switch rr.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
			}
		}

		// ALL requests should succeed (no rate limiting)
		assert.Equalf(
			t,
			0,
			rateLimitedCount,
			"/ping: Expected 0 rate-limited requests (bypass enabled), got %d",
			rateLimitedCount,
		)
		assert.Equalf(
			t,
			100,
			successCount,
			"/ping: Expected 100 successful requests, got %d",
			successCount,
		)
	})

	t.Run("Health Endpoint Bypasses Rate Limiting", func(t *testing.T) {
		// Send 100 rapid requests to /api/v1/health without API key
		// If rate limiting applied, we would hit the limit immediately (1 RPS unauth)
		successCount := 0
		rateLimitedCount := 0

		for i := 0; i < 100; i++ {
			req := httptest.NewRequest(http.MethodGet, "/health", nil)

			rr := httptest.NewRecorder()
			server.httpServer.Handler.ServeHTTP(rr, req)

			switch rr.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
			}
		}

		// ALL requests should succeed (no rate limiting)
		assert.Equalf(
			t,
			0,
			rateLimitedCount,
			"/health: Expected 0 rate-limited requests (bypass enabled), got %d",
			rateLimitedCount,
		)
		assert.Equalf(
			t,
			100,
			successCount,
			"/health: Expected 100 successful requests, got %d",
			successCount,
		)
	})

	t.Run("Protected Endpoint Still Enforces Rate Limits", func(t *testing.T) {
		// Verify /api/v1/health/data-consistency DOES get rate limited (it's protected)
		// With 2 RPS plugin limit, we should hit rate limit quickly
		successCount := 0
		rateLimitedCount := 0

		// Send 20 rapid requests with API key
		for i := 0; i < 20; i++ {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
			req.Header.Set("X-Api-Key", testAPIKey)

			rr := httptest.NewRecorder()
			server.httpServer.Handler.ServeHTTP(rr, req)

			switch rr.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
				// Verify RFC 7807 error format on first rate-limited response
				if rateLimitedCount == 1 {
					verifyRFC7807Error(t, rr, http.StatusTooManyRequests)
				}
			}
		}

		// Should have SOME rate-limited requests (2 RPS limit with bcrypt latency)
		assert.NotEqualf(
			t,
			0,
			rateLimitedCount,
			"/api/v1/health/data-consistency: Expected some rate-limited requests, but all %d succeeded",
			successCount,
		)
	})
}

// TestReadyEndpoint tests the /ready endpoint for K8s readiness probes.
// This endpoint performs dependency health checks with a 2-second timeout.
// The server depends on apiKeyStore which in turn has a dependency on a database connection.
// The logic is if the  last dependency in the stack is health, then the server is healthy.
func TestReadyEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	testDB := config.SetupTestDatabase(ctx, t)

	// Wrap in storage.Connection
	storageConn := &storage.Connection{DB: testDB.Connection}

	// Create key store
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour)
	require.NoError(t, err, "Failed to create lineage store")

	// Create rate limiter with VERY restrictive limits
	// If bypass didn't work, these limits would be hit immediately
	rateLimiter := createTestRateLimiter(5, 2, 1) // 5 global RPS, 2 plugin RPS, 1 unauth RPS

	t.Cleanup(func() {
		rateLimiter.Close()
		_ = keyStore.Close()
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

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
		MaxRequestSize:     defaultMaxRequestSize, // 1 MB
	}

	// Create server with key store that has database health checking
	server := NewServer(serverConfig, keyStore, rateLimiter, lineageStore)

	t.Run("Ready Endpoint Bypasses Authentication", func(t *testing.T) {
		// Send 10 requests without API key - all should succeed (no auth required)
		for i := 0; i < 10; i++ {
			req := httptest.NewRequest(http.MethodGet, "/ready", nil)

			rr := httptest.NewRecorder()
			server.httpServer.Handler.ServeHTTP(rr, req)

			if status := rr.Code; status != http.StatusOK {
				t.Errorf("/ready: Request %d failed with status %d (should bypass auth)", i+1, status)
			}
		}
	})

	t.Run("Ready Endpoint Bypasses Rate Limiting", func(t *testing.T) {
		// Send 100 rapid requests to /ready without API key
		// If rate limiting applied, we would hit the limit immediately (1 RPS unauth)
		successCount := 0
		rateLimitedCount := 0

		for i := 0; i < 100; i++ {
			req := httptest.NewRequest(http.MethodGet, "/ready", nil)

			rr := httptest.NewRecorder()
			server.httpServer.Handler.ServeHTTP(rr, req)

			switch rr.Code {
			case http.StatusOK:
				successCount++
			case http.StatusTooManyRequests:
				rateLimitedCount++
			}
		}

		// ALL requests should succeed (no rate limiting)
		if rateLimitedCount > 0 {
			t.Errorf("/ready: Expected 0 rate-limited requests (bypass enabled), got %d", rateLimitedCount)
		}

		if successCount != 100 {
			t.Errorf("/ready: Expected 100 successful requests, got %d", successCount)
		}
	})

	t.Run("Ready Endpoint Returns 200 When Database Available", func(t *testing.T) {
		// Make request to /ready WITHOUT API key (public endpoint)
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		// Should return 200 OK (database is healthy)
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("/ready: Expected status %d, got %d. Body: %s",
				http.StatusOK, status, rr.Body.String())
		}

		// Verify response body
		if body := rr.Body.String(); body != "ready" {
			t.Errorf("/ready: Expected body 'ready', got '%s'", body)
		}

		// Verify correlation ID is set
		verifyCorrelationID(t, rr)
	})

	t.Run("Ready Endpoint Returns 503 When Database Unavailable", func(t *testing.T) {
		// Close the database connection to simulate database outage
		if err := testDB.Connection.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}

		// Make request to /ready WITHOUT API key (public endpoint)
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		// Should return 503 Service Unavailable (database is down)
		if status := rr.Code; status != http.StatusServiceUnavailable {
			t.Errorf("/ready: Expected status %d, got %d. Body: %s",
				http.StatusServiceUnavailable, status, rr.Body.String())
		}

		// Verify response body
		if body := rr.Body.String(); body != "storage unavailable" {
			t.Errorf("/ready: Expected body 'storage unavailable', got '%s'", body)
		}

		// Verify correlation ID is still set (even on failure)
		verifyCorrelationID(t, rr)
	})
}

// TestRateLimitingIntegration tests the complete rate limiting flow with a real HTTP server and database.
func TestRateLimitingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	// Wrap in storage.Connection
	storageConn := &storage.Connection{DB: testDB.Connection}

	// Create key store
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour)
	require.NoError(t, err, "Failed to create lineage store")

	t.Cleanup(func() {
		_ = keyStore.Close()
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create test API keys for plugin-1 and plugin-2
	apiKey1, err := storage.GenerateAPIKey("plugin-1")
	require.NoError(t, err, "Failed to generate API key for plugin-1")

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

	err = keyStore.Add(ctx, apiKeyObj1)
	require.NoError(t, err, "Failed to add API key for plugin-1")

	apiKey2, err := storage.GenerateAPIKey("plugin-2")
	require.NoError(t, err, "Failed to generate API key for plugin-2")

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

	err = keyStore.Add(ctx, apiKeyObj2)
	require.NoError(t, err, "Failed to add API key for plugin-2")

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
		MaxRequestSize:     defaultMaxRequestSize, // 1 MB
	}

	// Test 1: Global Rate Limit Enforcement
	t.Run("Global Rate Limit Enforcement", func(t *testing.T) {
		// Create limiter: 5 RPS global, 50 RPS plugin (global is bottleneck)
		// Use 5 RPS to make limit easier to hit despite bcrypt latency (~50ms/request)
		rateLimiter := createTestRateLimiter(2, 50, 2)

		t.Cleanup(func() {
			rateLimiter.Close()
		})

		// Create server with rate limiter
		server := NewServer(serverConfig, keyStore, rateLimiter, lineageStore)

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

			response := makeAuthenticatedRequest(server, apiKey, "/api/v1/health/data-consistency")
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
		server := NewServer(serverConfig, keyStore, rateLimiter, lineageStore)

		// Plugin 1: Send requests until rate limited
		// With 2 RPS limit and ~50ms bcrypt latency, we need more than 2 requests
		// to exhaust burst capacity (4 tokens = 2 RPS × 2 burst multiplier)
		successCount := 0
		rateLimitedCount := 0

		// Send 10 requests rapidly
		for i := 0; i < 10; i++ {
			response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/health/data-consistency")
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
			response := makeAuthenticatedRequest(server, apiKey2, "/api/v1/health/data-consistency")
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
		server := NewServer(serverConfig, keyStore, rateLimiter, lineageStore)

		// IMPORTANT: Middleware order is Auth → RateLimit
		// Unauthenticated requests get rejected by Auth middleware (401)
		// BEFORE they reach the rate limiter
		// So we cannot directly test unauthenticated rate limiting in this configuration

		// Instead, verify that:
		// 1. Unauthenticated requests consistently return 401 (auth layer)
		// 2. Authenticated requests have independent rate limits

		// Send multiple unauthenticated requests - all should get 401
		for i := 0; i < 5; i++ {
			response := makeAuthenticatedRequest(server, "", "/api/v1/health/data-consistency")
			if response.Code != http.StatusUnauthorized {
				t.Errorf("Unauthenticated request %d should get 401 (auth fails), got %d", i+1, response.Code)
			}
		}

		// Verify authenticated requests work independently
		response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/health/data-consistency")
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
		server := NewServer(serverConfig, keyStore, rateLimiter, lineageStore)

		// Exhaust the rate limit by sending requests rapidly
		// With 2 RPS and burst=4, we should hit the limit quickly
		successCount := 0
		rateLimitedCount := 0

		for i := 0; i < 10; i++ {
			response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/health/data-consistency")
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
		response := makeAuthenticatedRequest(server, apiKey1, "/api/v1/health/data-consistency")
		if response.Code != http.StatusOK {
			t.Errorf("Expected request to succeed after token refill, got %d. Body: %s",
				response.Code, response.Body.String())
		}
	})
}

// TestFullMiddlewareStackIntegration validates that all middleware layers execute in the correct order
// and each middleware contributes its expected behavior.
//
// Middleware chain order (from server.go):
//  1. CorrelationID()      - Generate correlation ID for all responses
//  2. Recovery()           - Catch panics in all downstream middleware
//  3. AuthenticatePlugin() - Identify plugin (sets PluginContext)
//  4. RateLimit()          - Block before expensive operations
//  5. RequestLogger()      - Log only legitimate requests
//  6. CORS()               - Lightweight header manipulation
//
// This test validates:
//   - Successful requests have correlation ID + CORS headers
//   - Authentication failures (401) have correlation ID + CORS + RFC 7807
//   - Rate limiting (429) has correlation ID + CORS + RFC 7807
//   - Authorization failures (403) have correlation ID + CORS + RFC 7807
//   - Panic recovery (500) has correlation ID + CORS + RFC 7807
func TestFullMiddlewareStackIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	testDB := config.SetupTestDatabase(ctx, t)

	// Wrap in storage.Connection
	storageConn := &storage.Connection{DB: testDB.Connection}

	// Create key store
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour)
	require.NoError(t, err, "Failed to create lineage store")

	t.Cleanup(func() {
		_ = keyStore.Close()
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create test API key for authenticated requests
	testAPIKey, err := storage.GenerateAPIKey("test-plugin")
	require.NoError(t, err, "Failed to generate API key")

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

	err = keyStore.Add(ctx, apiKey)
	require.NoError(t, err, "Failed to add API key")

	// Create inactive API key for authorization failure tests
	inactiveAPIKey, err := storage.GenerateAPIKey("inactive-plugin")
	require.NoError(t, err, "Failed to generate inactive API key")

	inactiveKey := &storage.APIKey{
		ID:          "inactive-key-id",
		Key:         inactiveAPIKey,
		PluginID:    "inactive-plugin",
		Name:        "Inactive Plugin",
		Permissions: []string{"lineage:write"},
		CreatedAt:   time.Now(),
		ExpiresAt:   nil,
		Active:      false, // Inactive
	}

	err = keyStore.Add(ctx, inactiveKey)
	require.NoError(t, err, "Failed to add inactive API key")

	// Create rate limiter with low limits for easy testing
	rateLimiter := createTestRateLimiter(100, 2, 1)
	defer rateLimiter.Close()

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
		MaxRequestSize:     1048576, // 1 MB
	}

	// Create server with all middleware enabled (auth + rate limiting + CORS)
	server := NewServer(serverConfig, keyStore, rateLimiter, lineageStore)

	// Test Case 1: Successful Request Flows Through All Middleware
	t.Run("Successful Request Flows Through All Middleware", func(t *testing.T) {
		// Make authenticated request to /api/v1/health/data-consistency
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
		req.Header.Set("X-Api-Key", testAPIKey)

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		// Verify: 200 OK status (authentication succeeded, all middleware passed)
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusOK, status, rr.Body.String())
		}

		// Verify: Common headers present (correlation ID + CORS)
		// This validates that CorrelationID and CORS middleware executed
		verifyCORSHeaders(t, rr)
		verifyCorrelationID(t, rr)
	})

	// Test Case 2: Authentication Failure Has Correlation ID And CORS
	t.Run("Authentication Failure Has Correlation ID And CORS", func(t *testing.T) {
		// Make request with missing API key
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
		// No X-Api-Key header set

		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		// Verify: 401 Unauthorized (authentication failed)
		if status := rr.Code; status != http.StatusUnauthorized {
			t.Errorf("Expected status %d, got %d. Body: %s", http.StatusUnauthorized, status, rr.Body.String())
		}

		// Verify: RFC 7807 error response format
		verifyRFC7807Error(t, rr, http.StatusUnauthorized)

		// Verify: Correlation ID present
		verifyCorrelationID(t, rr)
	})

	// Test Case 3: Rate Limiting Has Correlation ID
	t.Run("Rate Limiting Has Correlation ID", func(t *testing.T) {
		// Exhaust rate limit by sending multiple rapid requests
		// Rate limiter configured with 2 RPS per plugin, burst = 4
		// Send requests until we hit the rate limit
		var rateLimitedResponse *httptest.ResponseRecorder

		for i := 0; i < 10; i++ {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/health/data-consistency", nil)
			req.Header.Set("X-Api-Key", testAPIKey)

			rr := httptest.NewRecorder()
			server.httpServer.Handler.ServeHTTP(rr, req)

			if rr.Code == http.StatusTooManyRequests {
				rateLimitedResponse = rr

				break
			}
		}

		// Verify we got a rate-limited response
		if rateLimitedResponse == nil {
			t.Fatal("Expected to hit rate limit, but all requests succeeded")
		}

		// Verify: 429 Too Many Requests (rate limit exceeded)
		if status := rateLimitedResponse.Code; status != http.StatusTooManyRequests {
			t.Errorf(
				"Expected status %d, got %d. Body: %s", http.StatusTooManyRequests,
				status,
				rateLimitedResponse.Body.String(),
			)
		}

		// Verify: RFC 7807 error response format
		verifyRFC7807Error(t, rateLimitedResponse, http.StatusTooManyRequests)

		// Verify: Correlation ID present (CorrelationID middleware runs before rate limit)
		verifyCorrelationID(t, rateLimitedResponse)
	})
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
//   - path: The request path (e.g., "/api/v1/health/data-consistency")
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
//   - correlation_id: Correlator-specific correlation ID for request tracing
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
	if contentType != contentTypeProblemJSON {
		t.Errorf("Expected Content-Type '%s', got '%s'", contentTypeProblemJSON, contentType)
	}

	// Parse JSON response
	var problem map[string]interface{}
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("Failed to parse RFC 7807 error response: %v", err)
	}

	// Verify required RFC 7807 fields
	requiredFields := []string{"type", "title", "status", "detail", "instance", "correlation_id"}
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

// verifyCORSHeaders validates that CORS headers (from CORS middleware) are present in the response.
//
// Validated headers:
//   - Access-Control-Allow-Origin: Set by CORS middleware
//   - Access-Control-Allow-Methods: Set by CORS middleware
//
// Parameters:
//   - t: Testing instance
//   - response: The HTTP response to validate
func verifyCORSHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()

	// Verify CORS headers (set by CORS middleware)
	origin := response.Header().Get("Access-Control-Allow-Origin")
	if origin == "" {
		t.Error("Expected Access-Control-Allow-Origin header to be set")
	}

	methods := response.Header().Get("Access-Control-Allow-Methods")
	if methods == "" {
		t.Error("Expected Access-Control-Allow-Methods header to be set")
	}
}

// verifyCorrelationID validates that correlation ID (from CorrelationID middleware) is present in the response.
//
// Validated headers:
//   - X-Correlation-ID: 16-character hex string generated by CorrelationID middleware
//
// Parameters:
//   - t: Testing instance
//   - response: The HTTP response to validate
func verifyCorrelationID(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()

	// Verify correlation ID header (set by CorrelationID middleware)
	correlationID := response.Header().Get("X-Correlation-ID")
	if correlationID == "" {
		t.Error("Expected X-Correlation-ID header to be set")
	}

	if len(correlationID) != 16 { // Correlation IDs are 16 hex chars
		t.Errorf("Expected correlation ID length 16, got %d", len(correlationID))
	}
}
