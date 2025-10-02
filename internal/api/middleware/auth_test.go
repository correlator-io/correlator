// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
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
	validKey := testKey

	expectedAPIKey := &storage.APIKey{
		ID:          "key-123",
		Key:         validKey,
		PluginID:    "dbt-plugin-v1",
		Name:        "dbt Core Plugin",
		Permissions: []string{"lineage:write", "metrics:read"},
		Active:      true,
		ExpiresAt:   nil,
	}

	store := &MockAPIKeyStore{
		FindByKeyFunc: func(_ context.Context, key string) (*storage.APIKey, bool) {
			if key == validKey {
				return expectedAPIKey, true
			}

			return nil, false
		},
	}

	apiKey, err := authenticateRequest(ctx, store, validKey)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if apiKey == nil { // pragma: allowlist secret
		t.Fatal("Expected API key to be returned")
	}

	if apiKey.ID != expectedAPIKey.ID {
		t.Errorf("Expected ID %q, got %q", expectedAPIKey.ID, apiKey.ID)
	}

	if apiKey.PluginID != expectedAPIKey.PluginID {
		t.Errorf("Expected PluginID %q, got %q", expectedAPIKey.PluginID, apiKey.PluginID)
	}
}

// TestAuthenticateRequest_InvalidFormat verifies that authentication fails
// for API keys with invalid format.
func TestAuthenticateRequest_InvalidFormat(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	store := &MockAPIKeyStore{}

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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			apiKey, err := authenticateRequest(ctx, store, tc.apiKey)
			if err == nil {
				t.Error("Expected error for invalid format, got nil")
			}

			if !IsAuthError(err, ErrInvalidAPIKey) {
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

	store := &MockAPIKeyStore{
		FindByKeyFunc: func(_ context.Context, _ string) (*storage.APIKey, bool) {
			return nil, false // Key not found
		},
	}

	apiKey, err := authenticateRequest(ctx, store, validKey)
	if err == nil {
		t.Fatal("Expected error for key not found, got nil")
	}

	if !IsAuthError(err, ErrInvalidAPIKey) {
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
	validKey := testKey

	inactiveKey := &storage.APIKey{
		ID:       "key-456",
		Key:      validKey,
		PluginID: "inactive-plugin",
		Name:     "Inactive Plugin",
		Active:   false, // Key is inactive
	}

	store := &MockAPIKeyStore{
		FindByKeyFunc: func(_ context.Context, _ string) (*storage.APIKey, bool) {
			return inactiveKey, true
		},
	}

	apiKey, err := authenticateRequest(ctx, store, validKey)
	if err == nil {
		t.Fatal("Expected error for inactive key, got nil")
	}

	if !IsAuthError(err, ErrAPIKeyInactive) {
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
	validKey := testKey

	pastTime := time.Now().Add(-24 * time.Hour) // Expired yesterday
	expiredKey := &storage.APIKey{
		ID:        "key-789",
		Key:       validKey,
		PluginID:  "expired-plugin",
		Name:      "Expired Plugin",
		Active:    true,
		ExpiresAt: &pastTime, // Key has expired
	}

	store := &MockAPIKeyStore{
		FindByKeyFunc: func(_ context.Context, _ string) (*storage.APIKey, bool) {
			return expiredKey, true
		},
	}

	apiKey, err := authenticateRequest(ctx, store, validKey)
	if err == nil {
		t.Fatal("Expected error for expired key, got nil")
	}

	if !IsAuthError(err, ErrAPIKeyExpired) {
		t.Errorf("Expected ErrAPIKeyExpired, got %v", err)
	}

	if apiKey != nil { // pragma: allowlist secret
		t.Error("Expected nil API key for expired key")
	}
}
