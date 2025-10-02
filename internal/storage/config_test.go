package storage

import (
	"errors"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		envVars  map[string]string
		expected *Config
	}{
		{
			name: "loads config with all environment variables set",
			envVars: map[string]string{
				"DATABASE_URL":                "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				"DATABASE_MAX_OPEN_CONNS":     "25",
				"DATABASE_MAX_IDLE_CONNS":     "5",
				"DATABASE_CONN_MAX_LIFETIME":  "30m",
				"DATABASE_CONN_MAX_IDLE_TIME": "10m",
			},
			expected: &Config{
				databaseURL:     "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
		},
		{
			name: "loads config with defaults when environment variables not set",
			envVars: map[string]string{
				"DATABASE_URL": "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
			},
			expected: &Config{
				databaseURL:     "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
		},
		{
			name: "uses defaults for invalid integer environment variables",
			envVars: map[string]string{
				"DATABASE_URL":            "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				"DATABASE_MAX_OPEN_CONNS": "invalid",
				"DATABASE_MAX_IDLE_CONNS": "also-invalid",
			},
			expected: &Config{
				databaseURL:     "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
		},
		{
			name: "uses defaults for invalid duration environment variables",
			envVars: map[string]string{
				"DATABASE_URL":                "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				"DATABASE_CONN_MAX_LIFETIME":  "not-a-duration",
				"DATABASE_CONN_MAX_IDLE_TIME": "also-not-duration",
			},
			expected: &Config{
				databaseURL:     "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
		},
		{
			name: "returns config with empty database URL when not set",
			envVars: map[string]string{
				"DATABASE_URL": "",
			},
			expected: &Config{
				databaseURL:     "",
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set test environment variables using t.Setenv (automatically cleaned up)
			for key, value := range tt.envVars {
				t.Setenv(key, value)
			}

			// Load config
			config := LoadConfig()

			// Verify all fields
			if config.databaseURL != tt.expected.databaseURL {
				t.Errorf("databaseURL = %q, want %q", config.databaseURL, tt.expected.databaseURL)
			}

			if config.MaxOpenConns != tt.expected.MaxOpenConns {
				t.Errorf("MaxOpenConns = %d, want %d", config.MaxOpenConns, tt.expected.MaxOpenConns)
			}

			if config.MaxIdleConns != tt.expected.MaxIdleConns {
				t.Errorf("MaxIdleConns = %d, want %d", config.MaxIdleConns, tt.expected.MaxIdleConns)
			}

			if config.ConnMaxLifetime != tt.expected.ConnMaxLifetime {
				t.Errorf("ConnMaxLifetime = %v, want %v", config.ConnMaxLifetime, tt.expected.ConnMaxLifetime)
			}

			if config.ConnMaxIdleTime != tt.expected.ConnMaxIdleTime {
				t.Errorf("ConnMaxIdleTime = %v, want %v", config.ConnMaxIdleTime, tt.expected.ConnMaxIdleTime)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name      string
		config    *Config
		expectErr error
	}{
		{
			name: "validation passes with valid database URL",
			config: &Config{
				databaseURL:     "postgres://user:pass@localhost:5432/testdb", // pragma: allowlist secret
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
			expectErr: nil,
		},
		{
			name: "validation fails with empty database URL",
			config: &Config{
				databaseURL:     "",
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
			expectErr: ErrDatabaseURLEmpty,
		},
		{
			name: "validation fails with whitespace-only database URL",
			config: &Config{
				databaseURL:     "   ",
				MaxOpenConns:    defaultMaxOpenConns,
				MaxIdleConns:    defaultMaxIdleConns,
				ConnMaxLifetime: defaultConnMaxLifetime,
				ConnMaxIdleTime: defaultConnMaxIdleTime,
			},
			expectErr: ErrDatabaseURLEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.expectErr != nil {
				if err == nil {
					t.Errorf("Validate() expected error %v, got nil", tt.expectErr)
				} else if !errors.Is(err, tt.expectErr) {
					t.Errorf("Validate() error = %v, want %v", err, tt.expectErr)
				}
			} else {
				if err != nil {
					t.Errorf("Validate() unexpected error: %v", err)
				}
			}
		})
	}
}

func TestMaskDatabaseURL(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	pwd := "postgres://user:secret@localhost:5432/db?sslmode=require&connect_timeout=10" // pragma: allowlist secret

	tests := []struct {
		name     string
		config   *Config
		expected string
	}{
		{
			name: "masks password in standard PostgreSQL URL",
			config: &Config{
				databaseURL: "postgres://myuser:mysecretpassword@localhost:5432/mydb", // pragma: allowlist secret
			},
			expected: "postgres://myuser:***@localhost:5432/mydb",
		},
		{
			name: "masks complex password with special characters",
			config: &Config{
				databaseURL: "postgres://user:p@ssw0rd!#$%@localhost:5432/db",
			},
			expected: "postgres://user:***@localhost:5432/db",
		},
		{
			name: "returns original URL when no password present",
			config: &Config{
				databaseURL: "postgres://localhost:5432/mydb",
			},
			expected: "postgres://localhost:5432/mydb",
		},
		{
			name: "returns original URL when username only (no password)",
			config: &Config{
				databaseURL: "postgres://myuser@localhost:5432/mydb",
			},
			expected: "postgres://myuser@localhost:5432/mydb",
		},
		{
			name: "returns empty string for empty database URL",
			config: &Config{
				databaseURL: "",
			},
			expected: "",
		},
		{
			name: "returns original URL for malformed URL",
			config: &Config{
				databaseURL: "not-a-valid-url",
			},
			expected: "not-a-valid-url",
		},
		{
			name: "masks password when password is empty string",
			config: &Config{
				databaseURL: "postgres://user:@localhost:5432/db",
			},
			expected: "postgres://user:@localhost:5432/db",
		},
		{
			name: "masks password in URL with query parameters",
			config: &Config{
				databaseURL: pwd,
			},
			expected: "postgres://user:***@localhost:5432/db?sslmode=require&connect_timeout=10", // pragma: allowlist secret
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			masked := tt.config.MaskDatabaseURL()

			if masked != tt.expected {
				t.Errorf("MaskDatabaseURL() = %q, want %q", masked, tt.expected)
			}
		})
	}
}
