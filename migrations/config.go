package main

import (
	"errors"
	"fmt"
	"os"
)

// Constants for URL parsing.
const (
	authorityStartOffset = 2 // Offset after "//" to get to the authority section
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
func maskDatabaseURL(url string) string {
	if url == "" {
		return ""
	}

	authStart := findAuthorityStart(url)
	if authStart == -1 {
		return url
	}

	atPos := findLastAtSymbol(url, authStart)
	if atPos == -1 {
		return url
	}

	colonPos := findColonInUserInfo(url, authStart, atPos)
	if colonPos == -1 {
		return url
	}

	passwordLen := atPos - (colonPos + 1) // pragma: allowlist secret
	if passwordLen == 0 {
		return url
	}

	// Replace password with asterisks
	return url[:colonPos+1] + "***" + url[atPos:]
}

// findAuthorityStart finds the "//" that indicates the start of the authority section.
func findAuthorityStart(url string) int {
	for i := range len(url) - 1 {
		if url[i] == '/' && url[i+1] == '/' {
			return i + authorityStartOffset
		}
	}
	return -1
}

// findLastAtSymbol finds the last "@" symbol in the authority section.
func findLastAtSymbol(url string, authStart int) int {
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
	return atPos
}

// findColonInUserInfo finds the ":" in the user info section.
func findColonInUserInfo(url string, authStart, atPos int) int {
	for i := authStart; i < atPos; i++ {
		if url[i] == ':' {
			return i
		}
	}
	return -1
}
