package main

import (
	"os"
	"strings"
	"testing"
)

// TestLoadConfig tests the LoadConfig function with various scenarios
func TestLoadConfig(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name        string
		envVars     map[string]string
		wantErr     bool
		errContains string
		validate    func(t *testing.T, config *Config)
	}{
		{
			name: "default values when DATABASE_URL provided",
			envVars: map[string]string{
				"DATABASE_URL":    "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret`
				"MIGRATION_TABLE": "",
			},
			wantErr: false,
			validate: func(t *testing.T, config *Config) {
				if config.DatabaseURL != "postgres://user:pass@localhost:5432/testdb" { // pragma: allowlist secret`
					t.Errorf("Expected DATABASE_URL from env var, got %s", config.DatabaseURL)
				}
				if config.MigrationTable != "schema_migrations" {
					t.Errorf("Expected default MIGRATION_TABLE, got %s", config.MigrationTable)
				}
			},
		},
		{
			name: "custom migration table",
			envVars: map[string]string{
				"DATABASE_URL":    "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret`
				"MIGRATION_TABLE": "custom_migrations",
			},
			wantErr: false,
			validate: func(t *testing.T, config *Config) {
				if config.DatabaseURL != "postgres://user:pass@localhost:5432/testdb" { // pragma: allowlist secret`
					t.Errorf("Expected custom DATABASE_URL, got %s", config.DatabaseURL)
				}
				if config.MigrationTable != "custom_migrations" {
					t.Errorf("Expected custom MIGRATION_TABLE, got %s", config.MigrationTable)
				}
			},
		},
		{
			name: "validation fails with empty DATABASE_URL",
			envVars: map[string]string{
				"DATABASE_URL":    "",
				"MIGRATION_TABLE": "migrations",
			},
			wantErr:     true,
			errContains: "DATABASE_URL cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup environment variables
			originalEnv := make(map[string]string)
			for key, value := range tt.envVars {
				originalEnv[key] = os.Getenv(key)
				if value == "" {
					os.Unsetenv(key)
				} else {
					os.Setenv(key, value)
				}
			}

			// Cleanup function
			defer func() {
				// Restore original environment
				for key, originalValue := range originalEnv {
					if originalValue == "" {
						os.Unsetenv(key)
					} else {
						os.Setenv(key, originalValue)
					}
				}
			}()

			// Test LoadConfig
			config, err := LoadConfig()

			// Validate error expectations
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errContains, err)
				}
				return
			}

			// Validate success case
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if config == nil {
				t.Error("Expected config but got nil")
				return
			}

			// Run custom validation if provided
			if tt.validate != nil {
				tt.validate(t, config)
			}
		})
	}
}

// TestConfigValidate tests the Validate method with various configurations
func TestConfigValidate(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name        string
		config      *Config
		wantErr     bool
		errContains string
	}{
		{
			name: "valid configuration",
			config: &Config{
				DatabaseURL:    "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret`
				MigrationTable: "migrations",
			},
			wantErr: false,
		},
		{
			name: "empty DATABASE_URL",
			config: &Config{
				DatabaseURL:    "",
				MigrationTable: "migrations",
			},
			wantErr:     true,
			errContains: "DATABASE_URL cannot be empty",
		},
		{
			name: "empty MIGRATION_TABLE",
			config: &Config{
				DatabaseURL:    "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret`
				MigrationTable: "",
			},
			wantErr:     true,
			errContains: "MIGRATION_TABLE cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test validation
			err := tt.config.Validate()

			// Check error expectations
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errContains, err)
				}
				return
			}

			// Check success case
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
		})
	}
}

// TestConfigString tests the String method
func TestConfigString(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name        string
		config      *Config
		contains    []string
		notContains []string
	}{
		{
			name: "normal configuration",
			config: &Config{
				DatabaseURL:    "postgres://user:password@localhost:5432/testdb", // pragma: allowlist secret`
				MigrationTable: "migrations",
			},
			contains: []string{
				"Config{",
				"DatabaseURL:",
				"MigrationTable: migrations",
			},
			notContains: []string{
				"password", // should be masked
			},
		},
		{
			name: "empty database URL",
			config: &Config{
				DatabaseURL:    "",
				MigrationTable: "migrations",
			},
			contains: []string{
				"Config{",
				"DatabaseURL:",
				"MigrationTable: migrations",
			},
		},
		{
			name: "database URL without password",
			config: &Config{
				DatabaseURL:    "postgres://user@localhost:5432/testdb",
				MigrationTable: "migrations",
			},
			contains: []string{
				"postgres://user@localhost:5432/testdb",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.String()

			for _, substr := range tt.contains {
				if !strings.Contains(result, substr) {
					t.Errorf("Expected result to contain '%s', got: %s", substr, result)
				}
			}

			for _, substr := range tt.notContains {
				if strings.Contains(result, substr) {
					t.Errorf("Expected result to NOT contain '%s', got: %s", substr, result)
				}
			}
		})
	}
}

// TestGetEnvOrDefault tests the getEnvOrDefault function
func TestGetEnvOrDefault(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name         string
		key          string
		defaultValue string
		envValue     string
		setEnv       bool
		expected     string
	}{
		{
			name:         "environment variable exists",
			key:          "TEST_ENV_VAR",
			defaultValue: "default",
			envValue:     "custom_value",
			setEnv:       true,
			expected:     "custom_value",
		},
		{
			name:         "environment variable not set",
			key:          "UNSET_ENV_VAR",
			defaultValue: "default_value",
			setEnv:       false,
			expected:     "default_value",
		},
		{
			name:         "environment variable set to empty string",
			key:          "EMPTY_ENV_VAR",
			defaultValue: "default_value",
			envValue:     "",
			setEnv:       true,
			expected:     "default_value",
		},
		{
			name:         "environment variable with whitespace",
			key:          "WHITESPACE_ENV_VAR",
			defaultValue: "default",
			envValue:     "  value_with_spaces  ",
			setEnv:       true,
			expected:     "  value_with_spaces  ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Store original value
			originalValue := os.Getenv(tt.key)
			defer func() {
				if originalValue == "" {
					os.Unsetenv(tt.key)
				} else {
					os.Setenv(tt.key, originalValue)
				}
			}()

			// Set up environment
			if tt.setEnv {
				os.Setenv(tt.key, tt.envValue)
			} else {
				os.Unsetenv(tt.key)
			}

			// Test function
			result := getEnvOrDefault(tt.key, tt.defaultValue)

			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// TestMaskDatabaseURL tests the maskDatabaseURL function
func TestMaskDatabaseURL(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "postgres URL with password",
			input:    "postgres://user:password@localhost:5432/dbname", // pragma: allowlist secret`
			expected: "postgres://user:***@localhost:5432/dbname",
		},
		{
			name:     "postgres URL without password",
			input:    "postgres://user@localhost:5432/dbname",
			expected: "postgres://user@localhost:5432/dbname",
		},
		{
			name:     "empty URL",
			input:    "",
			expected: "",
		},
		{
			name:     "URL with complex password",
			input:    "postgres://admin:p@ssw0rd!@localhost:5432/correlator",
			expected: "postgres://admin:***@localhost:5432/correlator",
		},
		{
			name:     "URL with no @ symbol",
			input:    "postgres://localhost:5432/dbname",
			expected: "postgres://localhost:5432/dbname",
		},
		{
			name:     "URL with multiple colons",
			input:    "postgres://user:pass:word@localhost:5432/dbname",
			expected: "postgres://user:***@localhost:5432/dbname",
		},
		{
			name:     "malformed URL",
			input:    "not-a-url",
			expected: "not-a-url",
		},
		{
			name:     "URL with empty password",
			input:    "postgres://user:@localhost:5432/dbname",
			expected: "postgres://user:@localhost:5432/dbname",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := maskDatabaseURL(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// TestConfigIntegration tests the full integration flow for embedded mode
func TestConfigIntegration(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	t.Run("embedded configuration workflow", func(t *testing.T) {
		// Set environment variables for embedded mode
		originalDB := os.Getenv("DATABASE_URL")
		originalTable := os.Getenv("MIGRATION_TABLE")

		os.Setenv(
			"DATABASE_URL",
			"postgres://testuser:testpass@localhost:5432/testdb", // pragma: allowlist secret`
		) // pragma: allowlist secret`
		os.Setenv("MIGRATION_TABLE", "test_migrations")

		defer func() {
			// Restore environment
			if originalDB == "" {
				os.Unsetenv("DATABASE_URL")
			} else {
				os.Setenv("DATABASE_URL", originalDB)
			}
			if originalTable == "" {
				os.Unsetenv("MIGRATION_TABLE")
			} else {
				os.Setenv("MIGRATION_TABLE", originalTable)
			}
		}()

		// Load configuration
		config, err := LoadConfig()
		if err != nil {
			t.Fatalf("Unexpected error loading config: %v", err)
		}

		// Validate configuration content
		if config.DatabaseURL != "postgres://testuser:testpass@localhost:5432/testdb" { // pragma: allowlist secret`
			t.Errorf("Expected custom DATABASE_URL, got %s", config.DatabaseURL)
		}
		if config.MigrationTable != "test_migrations" {
			t.Errorf("Expected custom MIGRATION_TABLE, got %s", config.MigrationTable)
		}

		// Test string representation
		configStr := config.String()
		if !strings.Contains(configStr, "testuser:***@localhost:5432") {
			t.Errorf("Expected masked password in config string, got: %s", configStr)
		}
		if strings.Contains(configStr, "testpass") {
			t.Errorf("Password should be masked in config string, got: %s", configStr)
		}
	})
}
