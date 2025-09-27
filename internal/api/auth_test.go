package api

import (
	"testing"
	"time"
)

func TestAPIKeyValidation(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	apiKey := &Key{
		ID:          "api-key-1",
		Key:         "test-key-123",
		PluginID:    "dbt-plugin",
		Name:        "DBT Production Plugin",
		Permissions: []string{"lineage:write", "health:read"},
		CreatedAt:   time.Now(),
		ExpiresAt:   nil, // No expiration for MVP
		Active:      true,
	}

	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{
			name:     "valid API key matches",
			key:      "test-key-123",
			expected: true,
		},
		{
			name:     "invalid API key does not match",
			key:      "wrong-key",
			expected: false,
		},
		{
			name:     "empty key fails validation",
			key:      "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := apiKey.ValidateKey(tt.key)
			if result != tt.expected {
				t.Errorf("ValidateKey(%q) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}

	// Test inactive API key
	t.Run("inactive API key fails validation", func(t *testing.T) {
		inactiveKey := &Key{
			ID:       "api-key-2",
			Key:      "inactive-key",
			PluginID: "test-plugin",
			Active:   false,
		}

		result := inactiveKey.ValidateKey("inactive-key")
		if result != false {
			t.Errorf("ValidateKey on inactive key = %v, want false", result)
		}
	})

	// Test expired API key
	t.Run("expired API key fails validation", func(t *testing.T) {
		pastTime := time.Now().Add(-time.Hour)
		expiredKey := &Key{
			ID:        "api-key-3",
			Key:       "expired-key",
			PluginID:  "test-plugin",
			Active:    true,
			ExpiresAt: &pastTime,
		}

		result := expiredKey.ValidateKey("expired-key")
		if result != false {
			t.Errorf("ValidateKey on expired key = %v, want false", result)
		}
	})
}

func TestAPIKeyPermissions(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	apiKey := &Key{
		ID:          "api-key-1",
		Key:         "test-key-123",
		PluginID:    "dbt-plugin",
		Name:        "DBT Production Plugin",
		Permissions: []string{"lineage:write", "health:read", "metrics:read"},
		Active:      true,
	}

	tests := []struct {
		name       string
		permission string
		expected   bool
	}{
		{
			name:       "has lineage write permission",
			permission: "lineage:write",
			expected:   true,
		},
		{
			name:       "has health read permission",
			permission: "health:read",
			expected:   true,
		},
		{
			name:       "does not have admin permission",
			permission: "admin:write",
			expected:   false,
		},
		{
			name:       "empty permission string",
			permission: "",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := apiKey.HasPermission(tt.permission)
			if result != tt.expected {
				t.Errorf("HasPermission(%q) = %v, want %v", tt.permission, result, tt.expected)
			}
		})
	}
}

func TestAPIKeyStore(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Skip this test for now - KeyStore interface will be tested in Task 2.2 with actual implementation
	t.Skip("KeyStore interface tests will be implemented in Task 2.2 with InMemoryAPIKeyStore")
}

func TestAPIKeyGeneration(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		pluginID string
		wantErr  bool
	}{
		{
			name:     "valid plugin ID generates key",
			pluginID: "dbt-plugin",
			wantErr:  false,
		},
		{
			name:     "empty plugin ID fails",
			pluginID: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This will fail - GenerateAPIKey function doesn't exist yet
			key, err := GenerateAPIKey(tt.pluginID)

			if tt.wantErr {
				if err == nil {
					t.Errorf("GenerateAPIKey(%q) expected error, got nil", tt.pluginID)
				}

				return
			}

			if err != nil {
				t.Errorf("GenerateAPIKey(%q) unexpected error: %v", tt.pluginID, err)

				return
			}

			if key == "" {
				t.Errorf("GenerateAPIKey(%q) returned empty key", tt.pluginID)
			}

			// Key should be at least 32 characters for security
			if len(key) < 32 {
				t.Errorf("GenerateAPIKey(%q) key too short: %d characters", tt.pluginID, len(key))
			}
		})
	}
}

func TestAPIKeyParsing(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name      string
		keyString string
		expected  string
		wantErr   bool
	}{
		{
			name:      "valid API key format",
			keyString: "Bearer correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			expected:  "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			wantErr:   false,
		},
		{
			name:      "API key without Bearer prefix",
			keyString: "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			expected:  "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			wantErr:   false,
		},
		{
			name:      "invalid key format",
			keyString: "invalid-key-format",
			expected:  "",
			wantErr:   true,
		},
		{
			name:      "empty key string",
			keyString: "",
			expected:  "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This will fail - ParseAPIKey function doesn't exist yet
			key, err := ParseAPIKey(tt.keyString)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseAPIKey(%q) expected error, got nil", tt.keyString)
				}

				return
			}

			if err != nil {
				t.Errorf("ParseAPIKey(%q) unexpected error: %v", tt.keyString, err)

				return
			}

			if key != tt.expected {
				t.Errorf("ParseAPIKey(%q) = %q, want %q", tt.keyString, key, tt.expected)
			}
		})
	}
}

func TestSecureCompare(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		key1     string
		key2     string
		expected bool
	}{
		{
			name:     "identical keys match",
			key1:     "correlator_ak_1234567890abcdef",
			key2:     "correlator_ak_1234567890abcdef",
			expected: true,
		},
		{
			name:     "different keys don't match",
			key1:     "correlator_ak_1234567890abcdef",
			key2:     "correlator_ak_abcdef1234567890",
			expected: false,
		},
		{
			name:     "different length keys don't match",
			key1:     "correlator_ak_1234567890abcdef",
			key2:     "correlator_ak_1234",
			expected: false,
		},
		{
			name:     "empty keys match",
			key1:     "",
			key2:     "",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This will fail - SecureCompare function doesn't exist yet
			result := SecureCompare(tt.key1, tt.key2)
			if result != tt.expected {
				t.Errorf("SecureCompare(%q, %q) = %v, want %v", tt.key1, tt.key2, result, tt.expected)
			}
		})
	}
}
