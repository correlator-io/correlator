// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/correlator-io/correlator/internal/storage"
)

const testKey = "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

// TestExtractAPIKey_XAPIKeyHeader verifies that extractAPIKey correctly extracts.
// API key from the X-Api-Key header (primary header).
func TestExtractAPIKey_XAPIKeyHeader(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Api-Key", "correlator_ak_test123456789")

	apiKey, found := extractAPIKey(req)

	if !found {
		t.Fatal("extractAPIKey should return true when X-Api-Key header is present")
	}

	expected := "correlator_ak_test123456789"
	if apiKey != expected { // pragma: allowlist secret
		t.Errorf("Expected API key %q, got %q", expected, apiKey)
	}
}

// TestExtractAPIKey_AuthorizationHeader verifies that extractAPIKey correctly extracts.
// API key from the Authorization: Bearer header (secondary/fallback header).
func TestExtractAPIKey_AuthorizationHeader(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer correlator_ak_test123456789")

	apiKey, found := extractAPIKey(req)

	if !found {
		t.Fatal("extractAPIKey should return true when Authorization header is present")
	}

	expected := "correlator_ak_test123456789"
	if apiKey != expected { // pragma: allowlist secret
		t.Errorf("Expected API key %q, got %q", expected, apiKey)
	}
}

// TestExtractAPIKey_BothHeaders verifies that X-Api-Key takes precedence.
// when both X-Api-Key and Authorization headers are present.
func TestExtractAPIKey_BothHeaders(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Api-Key", "correlator_ak_primary")
	req.Header.Set("Authorization", "Bearer correlator_ak_secondary")

	apiKey, found := extractAPIKey(req)

	if !found {
		t.Fatal("extractAPIKey should return true when headers are present")
	}

	// X-Api-Key should take precedence
	expected := "correlator_ak_primary"
	if apiKey != expected { // pragma: allowlist secret
		t.Errorf("X-Api-Key should take precedence. Expected %q, got %q", expected, apiKey)
	}
}

// TestExtractAPIKey_NoHeaders verifies that extractAPIKey returns false.
// when neither X-Api-Key nor Authorization header is present.
func TestExtractAPIKey_NoHeaders(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	req := httptest.NewRequest(http.MethodGet, "/test", nil)

	apiKey, found := extractAPIKey(req)

	if found {
		t.Error("extractAPIKey should return false when no headers are present")
	}

	if apiKey != "" {
		t.Errorf("Expected empty API key, got %q", apiKey)
	}
}

// TestExtractAPIKey_InvalidBearerFormat verifies that extractAPIKey returns false.
// when Authorization header doesn't have "Bearer " prefix.
func TestExtractAPIKey_InvalidBearerFormat(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name   string
		header string
	}{
		{
			name:   "Missing Bearer prefix",
			header: "correlator_ak_test123456789",
		},
		{
			name:   "Basic auth format",
			header: "Basic dXNlcjpwYXNz",
		},
		{
			name:   "Lowercase bearer",
			header: "bearer correlator_ak_test123456789",
		},
		{
			name:   "Empty value after Bearer",
			header: "Bearer ",
		},
		{
			name:   "Just Bearer",
			header: "Bearer",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set("Authorization", tc.header)

			apiKey, found := extractAPIKey(req)

			if found {
				t.Errorf("extractAPIKey should return false for invalid Bearer format: %q", tc.header)
			}

			if apiKey != "" {
				t.Errorf("Expected empty API key, got %q", apiKey)
			}
		})
	}
}

// TestExtractAPIKey_HeaderInjection verifies that extractAPIKey rejects
// API keys containing newlines (header injection prevention).
func TestExtractAPIKey_HeaderInjection(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name   string
		header string
	}{
		{
			name:   "Newline in X-Api-Key",
			header: "correlator_ak_test\nInjected-Header: malicious",
		},
		{
			name:   "Carriage return in X-Api-Key",
			header: "correlator_ak_test\rInjected-Header: malicious",
		},
		{
			name:   "CRLF in X-Api-Key",
			header: "correlator_ak_test\r\nInjected-Header: malicious",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set("X-Api-Key", tc.header)

			apiKey, found := extractAPIKey(req)

			if found {
				t.Errorf("extractAPIKey should return false for header injection attempt: %q", tc.header)
			}

			if apiKey != "" {
				t.Errorf("Expected empty API key for injection attempt, got %q", apiKey)
			}
		})
	}
}

// TestExtractAPIKey_WhitespaceHandling verifies that extractAPIKey properly
// handles API keys with leading/trailing whitespace.
func TestExtractAPIKey_WhitespaceHandling(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name     string
		header   string
		expected string
		found    bool
	}{
		{
			name:     "Leading whitespace in X-Api-Key",
			header:   "  correlator_ak_test123456789",
			expected: "correlator_ak_test123456789",
			found:    true,
		},
		{
			name:     "Trailing whitespace in X-Api-Key",
			header:   "correlator_ak_test123456789  ",
			expected: "correlator_ak_test123456789",
			found:    true,
		},
		{
			name:     "Leading and trailing whitespace",
			header:   "  correlator_ak_test123456789  ",
			expected: "correlator_ak_test123456789",
			found:    true,
		},
		{
			name:     "Only whitespace",
			header:   "   ",
			expected: "",
			found:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set("X-Api-Key", tc.header)

			apiKey, found := extractAPIKey(req)

			if found != tc.found {
				t.Errorf("Expected found=%v, got found=%v", tc.found, found)
			}

			if apiKey != tc.expected { // pragma: allowlist secret
				t.Errorf("Expected API key %q, got %q", tc.expected, apiKey)
			}
		})
	}
}

// TestExtractAPIKey_EmptyHeaders verifies that extractAPIKey returns false
// when headers are present but empty.
func TestExtractAPIKey_EmptyHeaders(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name        string
		headerName  string
		headerValue string
	}{
		{
			name:        "Empty X-Api-Key",
			headerName:  "X-Api-Key",
			headerValue: "",
		},
		{
			name:        "Empty Authorization",
			headerName:  "Authorization",
			headerValue: "",
		},
		{
			name:        "Authorization with just Bearer",
			headerName:  "Authorization",
			headerValue: "Bearer",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set(tc.headerName, tc.headerValue)

			apiKey, found := extractAPIKey(req)

			if found {
				t.Error("extractAPIKey should return false for empty header")
			}

			if apiKey != "" {
				t.Errorf("Expected empty API key, got %q", apiKey)
			}
		})
	}
}

// TestExtractAPIKey_AuthorizationBearerWithWhitespace verifies proper handling
// of whitespace in Authorization: Bearer header.
func TestExtractAPIKey_AuthorizationBearerWithWhitespace(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name     string
		header   string
		expected string
		found    bool
	}{
		{
			name:     "Extra spaces after Bearer",
			header:   "Bearer   correlator_ak_test123456789",
			expected: "correlator_ak_test123456789",
			found:    true,
		},
		{
			name:     "Trailing space after token",
			header:   "Bearer correlator_ak_test123456789 ",
			expected: "correlator_ak_test123456789",
			found:    true,
		},
		{
			name:     "Multiple spaces",
			header:   "Bearer    correlator_ak_test123456789   ",
			expected: "correlator_ak_test123456789",
			found:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set("Authorization", tc.header)

			apiKey, found := extractAPIKey(req)

			if found != tc.found {
				t.Errorf("Expected found=%v, got found=%v", tc.found, found)
			}

			if apiKey != tc.expected { // pragma: allowlist secret
				t.Errorf("Expected API key %q, got %q", tc.expected, apiKey)
			}
		})
	}
}

// TestAuthenticateRequest_ValidKey verifies successful authentication with a valid API key.
func TestAuthenticateRequest_ValidKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	store := storage.NewInMemoryKeyStore()

	// Parse the key to get the correct format
	parsedKey, err := storage.ParseAPIKey(testKey)
	if err != nil {
		t.Fatalf("Failed to parse test key: %v", err)
	}

	testAPIKey := &storage.APIKey{
		ID:          "test-key-123",
		Key:         parsedKey,
		PluginID:    "dbt-plugin-v1",
		Name:        "dbt Core Plugin",
		Permissions: []string{"lineage:write", "metrics:read"},
		Active:      true,
		ExpiresAt:   nil,
	}

	err = store.Add(ctx, testAPIKey)
	if err != nil {
		t.Fatalf("Failed to create test API key: %v", err)
	}

	logger := slog.Default()

	apiKey, err := authenticateRequest(ctx, store, testKey, logger)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if apiKey == nil { // pragma: allowlist secret
		t.Fatal("Expected API key to be returned")
	}

	if apiKey.ID != testAPIKey.ID {
		t.Errorf("Expected ID %q, got %q", testAPIKey.ID, apiKey.ID)
	}

	if apiKey.PluginID != testAPIKey.PluginID {
		t.Errorf("Expected PluginID %q, got %q", testAPIKey.PluginID, apiKey.PluginID)
	}
}

// TestAuthenticateRequest_InvalidFormat verifies that authentication fails
// for API keys with invalid format.
func TestAuthenticateRequest_InvalidFormat(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	store := storage.NewInMemoryKeyStore()

	testCases := []struct {
		name   string
		apiKey string
	}{
		{
			name:   "Missing prefix",
			apiKey: "invalid_key_format",
		},
		{
			name:   "Wrong prefix",
			apiKey: "wrong_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		},
		{
			name:   "Too short",
			apiKey: "correlator_ak_short",
		},
		{
			name:   "Too long",
			apiKey: "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefextra",
		},
		{
			name:   "Empty string",
			apiKey: "",
		},
	}

	logger := slog.Default()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			apiKey, err := authenticateRequest(ctx, store, tc.apiKey, logger)
			if err == nil {
				t.Error("Expected error for invalid format, got nil")
			}

			if !errors.Is(err, ErrInvalidAPIKey) {
				t.Errorf("Expected ErrInvalidAPIKey, got %v", err)
			}

			if apiKey != nil { // pragma: allowlist secret
				t.Error("Expected nil API key for invalid format")
			}
		})
	}
}

// TestAuthenticateRequest_KeyNotFound verifies that authentication fails
// when the API key is not found in the store.
func TestAuthenticateRequest_KeyNotFound(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	validKey := testKey

	// Use real in-memory store (empty, so key won't be found)
	store := storage.NewInMemoryKeyStore()

	logger := slog.Default()

	apiKey, err := authenticateRequest(ctx, store, validKey, logger)
	if err == nil {
		t.Fatal("Expected error for key not found, got nil")
	}

	if !errors.Is(err, ErrInvalidAPIKey) {
		t.Errorf("Expected ErrInvalidAPIKey for not found, got %v", err)
	}

	if apiKey != nil { // pragma: allowlist secret
		t.Error("Expected nil API key when not found")
	}
}

// TestAuthenticateRequest_InactiveKey verifies that authentication fails
// for inactive API keys (soft-deleted).
func TestAuthenticateRequest_InactiveKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	store := storage.NewInMemoryKeyStore()

	inactiveKeyID := "inactive-key-456"
	inactiveTestKey := "correlator_ak_inact67890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	testAPIKey := &storage.APIKey{
		ID:          inactiveKeyID,
		Key:         inactiveTestKey,
		PluginID:    "inactive-plugin",
		Name:        "Inactive Plugin",
		Active:      true,
		Permissions: []string{},
	}

	err := store.Add(ctx, testAPIKey)
	if err != nil {
		t.Fatalf("Failed to create test API key: %v", err)
	}

	// Delete the key (soft delete - sets active=false)
	if err := store.Delete(ctx, inactiveKeyID); err != nil {
		t.Fatalf("Failed to delete API key: %v", err)
	}

	logger := slog.Default()

	// Try to authenticate with the inactive key
	apiKey, err := authenticateRequest(ctx, store, inactiveTestKey, logger)
	if err == nil {
		t.Fatal("Expected error for inactive key, got nil")
	}

	if !errors.Is(err, ErrAPIKeyInactive) {
		t.Errorf("Expected ErrAPIKeyInactive, got %v", err)
	}

	if apiKey != nil { // pragma: allowlist secret
		t.Error("Expected nil API key for inactive key")
	}
}

// TestAuthenticateRequest_ExpiredKey verifies that authentication fails
// for expired API keys.
func TestAuthenticateRequest_ExpiredKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	store := storage.NewInMemoryKeyStore()

	// Create a key with expiration in the past (must be 78 chars total including prefix)
	pastTime := time.Now().Add(-24 * time.Hour) // Expired yesterday
	expiredKeyID := "expired-key-789"
	expiredTestKey := "correlator_ak_expire7890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	testAPIKey := &storage.APIKey{
		ID:          expiredKeyID,
		Key:         expiredTestKey,
		PluginID:    "expired-plugin",
		Name:        "Expired Plugin",
		Active:      true,
		Permissions: []string{},
		ExpiresAt:   &pastTime, // Key has expired
	}

	err := store.Add(ctx, testAPIKey)
	if err != nil {
		t.Fatalf("Failed to create test API key: %v", err)
	}

	logger := slog.Default()

	// Try to authenticate with the expired key
	apiKey, err := authenticateRequest(ctx, store, expiredTestKey, logger)
	if err == nil {
		t.Fatal("Expected error for expired key, got nil")
	}

	if !errors.Is(err, ErrAPIKeyExpired) {
		t.Errorf("Expected ErrAPIKeyExpired, got %v", err)
	}

	if apiKey != nil { // pragma: allowlist secret
		t.Error("Expected nil API key for expired key")
	}
}

// TestPluginAuthenticationMiddleware_HappyPath verifies successful authentication flow through middleware.
func TestPluginAuthenticationMiddleware_HappyPath(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	validKey := testKey

	// Parse the key to get the correct format
	parsedKey, err := storage.ParseAPIKey(validKey)
	if err != nil {
		t.Fatalf("Failed to parse test key: %v", err)
	}

	expectedAPIKey := &storage.APIKey{
		ID:          "key-123",
		Key:         parsedKey,
		PluginID:    "dbt-plugin-v1",
		Name:        "dbt Core Plugin",
		Permissions: []string{"lineage:write", "metrics:read"},
		Active:      true,
		ExpiresAt:   nil,
	}

	store := storage.NewInMemoryKeyStore()

	// Add the key to the store
	err = store.Add(ctx, expectedAPIKey)
	if err != nil {
		t.Fatalf("Failed to add API key: %v", err)
	}

	logger := slog.New(slog.DiscardHandler)

	// Handler that checks plugin context
	var capturedContext PluginContext

	var contextFound bool

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContext, contextFound = GetPluginContext(r.Context())

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("authenticated"))
	})

	// Create middleware
	middleware := AuthenticatePlugin(store, logger)
	wrappedHandler := middleware(handler)

	// Create request with valid API key
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Api-Key", validKey)

	rec := httptest.NewRecorder()

	// Execute request
	wrappedHandler.ServeHTTP(rec, req)

	// Verify response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	// Verify plugin context was set
	if !contextFound {
		t.Fatal("Plugin context was not set in request context")
	}

	if capturedContext.PluginID != expectedAPIKey.PluginID {
		t.Errorf("Expected PluginID %q, got %q", expectedAPIKey.PluginID, capturedContext.PluginID)
	}

	if capturedContext.Name != expectedAPIKey.Name {
		t.Errorf("Expected Name %q, got %q", expectedAPIKey.Name, capturedContext.Name)
	}

	if capturedContext.KeyID != expectedAPIKey.ID {
		t.Errorf("Expected KeyID %q, got %q", expectedAPIKey.ID, capturedContext.KeyID)
	}

	if len(capturedContext.Permissions) != len(expectedAPIKey.Permissions) {
		t.Errorf("Expected %d permissions, got %d", len(expectedAPIKey.Permissions), len(capturedContext.Permissions))
	}

	if capturedContext.AuthTime.IsZero() {
		t.Error("Expected AuthTime to be set, got zero value")
	}
}

// TestPluginAuthenticationMiddleware_MissingAPIKey verifies 401 response when API key is missing.
func TestPluginAuthenticationMiddleware_MissingAPIKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	store := storage.NewInMemoryKeyStore()
	logger := slog.New(slog.DiscardHandler)

	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("Handler should not be called when API key is missing")
	})

	middleware := AuthenticatePlugin(store, logger)
	wrappedHandler := middleware(handler)

	// testKey is not added to the request headers
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", rec.Code)
	}

	// Verify RFC 7807 response
	var problem map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if problem["status"] != float64(http.StatusUnauthorized) {
		t.Errorf("Expected status 401 in problem detail, got %v", problem["status"])
	}

	if problem["type"] == nil {
		t.Error("Expected type field in problem detail")
	}
}

// TestPluginAuthenticationMiddleware_InvalidAPIKey verifies 401 response for invalid API key.
func TestPluginAuthenticationMiddleware_InvalidAPIKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// testKey is not added to the store
	store := storage.NewInMemoryKeyStore()

	logger := slog.New(slog.DiscardHandler)

	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("Handler should not be called for invalid API key")
	})

	middleware := AuthenticatePlugin(store, logger)
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// testKey is added to the request headers, but does not exist in the store
	req.Header.Set("X-Api-Key", testKey)

	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", rec.Code)
	}
}

// TestPluginAuthenticationMiddleware_InactiveKey verifies 403 response for inactive API key.
func TestPluginAuthenticationMiddleware_InactiveKey(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	store := storage.NewInMemoryKeyStore()

	inactiveKey := &storage.APIKey{
		ID:          "key-inactive",
		Key:         testKey,
		PluginID:    "inactive-plugin",
		Name:        "Inactive Plugin",
		Active:      true,
		Permissions: []string{},
	}

	// Add the key to the store
	err := store.Add(ctx, inactiveKey)
	if err != nil {
		t.Fatalf("Failed to add inactive key: %v", err)
	}

	// Mark it as inactive by deleting it (soft delete)
	err = store.Delete(ctx, inactiveKey.ID)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	logger := slog.New(slog.DiscardHandler)

	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("Handler should not be called for inactive API key")
	})

	middleware := AuthenticatePlugin(store, logger)
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Api-Key", testKey)

	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("Expected status 403, got %d", rec.Code)
	}
}

// TestPluginAuthenticationMiddleware_CorrelationIDInError verifies correlation ID is included in error responses.
func TestPluginAuthenticationMiddleware_CorrelationIDInError(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	store := storage.NewInMemoryKeyStore()
	logger := slog.New(slog.DiscardHandler)

	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("Handler should not be called")
	})

	middleware := AuthenticatePlugin(store, logger)
	wrappedHandler := middleware(handler)

	// Add correlation ID middleware first
	correlationMiddleware := CorrelationID()
	wrappedHandler = correlationMiddleware(wrappedHandler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	// Verify correlation ID in response
	var problem map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if problem["correlationId"] == nil || problem["correlationId"] == "" {
		t.Error("Expected correlationId in problem detail")
	}
}
