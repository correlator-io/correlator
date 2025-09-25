package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// Static errors for validation.
var (
	ErrDatabaseURLEmpty    = errors.New("DATABASE_URL cannot be empty")
	ErrMigrationTableEmpty = errors.New("MIGRATION_TABLE cannot be empty")
)

// Config holds all configuration for the migration tool.
type Config struct {
	// DatabaseURL is the PostgreSQL connection string
	DatabaseURL string

	// MigrationTable is the name of the table to track migrations
	MigrationTable string
}

// LoadConfig loads configuration from environment variables with sensible defaults.
func LoadConfig() (*Config, error) {
	config := &Config{
		DatabaseURL:    getEnvOrDefault("DATABASE_URL", ""),
		MigrationTable: getEnvOrDefault("MIGRATION_TABLE", "schema_migrations"),
	}

	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return config, nil
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	if c.DatabaseURL == "" {
		return ErrDatabaseURLEmpty
	}

	if c.MigrationTable == "" {
		return ErrMigrationTableEmpty
	}

	return nil
}

// String returns a string representation of the configuration (safe for logging).
func (c *Config) String() string {
	maskedURL := maskDatabaseURL(c.DatabaseURL)

	return fmt.Sprintf("Config{DatabaseURL: %s, MigrationTable: %s}",
		maskedURL, c.MigrationTable)
}

// getEnvOrDefault returns the environment variable value or a default if not set.
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

// maskDatabaseURL masks sensitive information in database URLs for logging.
func maskDatabaseURL(urlStr string) string {
	if urlStr == "" {
		return ""
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		// If parsing fails, return the original URL as-is
		// This maintains backwards compatibility with malformed URLs
		return urlStr
	}

	if u.User == nil {
		return urlStr
	}

	// Check if there's a password to mask
	if password, hasPassword := u.User.Password(); hasPassword {
		if password != "" {
			// Create new user info with masked password
			u.User = url.UserPassword(u.User.Username(), "***")
			// Convert back to string and manually fix the URL encoding issue
			// net/url encodes *** as %2A%2A%2A, but we want literal ***
			result := u.String()

			return strings.Replace(result, "%2A%2A%2A", "***", 1)
		}
	}

	return urlStr
}
