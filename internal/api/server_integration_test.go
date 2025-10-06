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

	"github.com/correlator-io/correlator/internal/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file" // Import file source driver
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

	// Create server config with authentication
	config := ServerConfig{
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
		APIKeyStore:        keyStore,
	}

	// Create server
	server := NewServer(config)

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
