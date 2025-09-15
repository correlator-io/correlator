package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// Config holds all configuration for the migration tool
type Config struct {
	// DatabaseURL is the PostgreSQL connection string
	DatabaseURL string

	// MigrationsPath is the path to migration files
	MigrationsPath string

	// MigrationTable is the name of the table to track migrations
	MigrationTable string
}

// LoadConfig loads configuration from environment variables with sensible defaults
func LoadConfig() (*Config, error) {
	config := &Config{
		DatabaseURL:    getEnvOrDefault("DATABASE_URL", ""),
		MigrationsPath: getEnvOrDefault("MIGRATIONS_PATH", "./migrations"),
		MigrationTable: getEnvOrDefault("MIGRATION_TABLE", "schema_migrations"),
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return config, nil
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL cannot be empty")
	}

	if c.MigrationTable == "" {
		return fmt.Errorf("MIGRATION_TABLE cannot be empty")
	}

	if c.MigrationsPath == "" {
		return fmt.Errorf("MIGRATIONS_PATH cannot be empty")
	}

	absPath, err := filepath.Abs(c.MigrationsPath)
	if err != nil {
		return fmt.Errorf("failed to resolve migrations path: %w", err)
	}
	c.MigrationsPath = absPath

	// Check if directory exists
	if _, err := os.Stat(c.MigrationsPath); os.IsNotExist(err) {
		return fmt.Errorf("migrations directory does not exist: %s", c.MigrationsPath)
	}

	return nil
}

// String returns a string representation of the configuration (safe for logging)
func (c *Config) String() string {
	maskedURL := maskDatabaseURL(c.DatabaseURL)

	return fmt.Sprintf("Config{DatabaseURL: %s, MigrationsPath: %s, MigrationTable: %s}",
		maskedURL, c.MigrationsPath, c.MigrationTable)
}

// getEnvOrDefault returns the environment variable value or a default if not set
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// maskDatabaseURL masks sensitive information in database URLs for logging
func maskDatabaseURL(url string) string {
	if url == "" {
		return ""
	}

	// Find the "//" that indicates the start of the authority section
	authStart := -1
	for i := 0; i < len(url)-1; i++ {
		if url[i] == '/' && url[i+1] == '/' {
			authStart = i + 2
			break
		}
	}

	// If no "//" found, return original URL (no authority section)
	if authStart == -1 {
		return url
	}

	// Find the "@" symbol which separates user info from host
	// We need to find the LAST "@" in the authority section in case the password contains "@"
	atPos := -1
	for i := authStart; i < len(url); i++ {
		if url[i] == '@' {
			atPos = i
			// Don't break - keep looking for the last "@"
		}
		// Stop at path, query, or fragment
		if url[i] == '/' || url[i] == '?' || url[i] == '#' {
			break
		}
	}

	// If no "@" found, there's no user info to mask
	if atPos == -1 {
		return url
	}

	// Find the ":" in the user info section (between authStart and atPos)
	colonPos := -1
	for i := authStart; i < atPos; i++ {
		if url[i] == ':' {
			colonPos = i
			break
		}
	}

	// If no ":" found in user info, there's no password to mask
	if colonPos == -1 {
		return url
	}

	// Calculate password length
	passwordLen := atPos - (colonPos + 1)

	// If password is empty, don't mask anything
	if passwordLen == 0 {
		return url
	}

	// Replace password with asterisks
	return url[:colonPos+1] + "***" + url[atPos:]
}
