package storage

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 30 * time.Minute
	defaultConnMaxIdleTime = 10 * time.Minute
)

var (
	// ErrDatabaseURLEmpty is returned when the database url is an empty string.
	ErrDatabaseURLEmpty = errors.New("database URL cannot be empty")
)

// Config holds PostgreSQL connection configuration with production-ready defaults.
type Config struct {
	databaseURL     string
	MaxOpenConns    int           // Maximum number of open connections
	MaxIdleConns    int           // Maximum number of idle connections
	ConnMaxLifetime time.Duration // Maximum lifetime of connections
	ConnMaxIdleTime time.Duration // Maximum idle time for connections
}

// LoadConfig loads PostgreSQL configuration from environment variables with fallback to defaults.
func LoadConfig() *Config {
	config := &Config{
		databaseURL:     getEnvStr("DATABASE_URL", ""), // DatabaseURL is private for obvious reasons.
		MaxOpenConns:    getEnvInt("DATABASE_MAX_OPEN_CONNS", defaultMaxOpenConns),
		MaxIdleConns:    getEnvInt("DATABASE_MAX_IDLE_CONNS", defaultMaxIdleConns),
		ConnMaxLifetime: getEnvDuration("DATABASE_CONN_MAX_LIFETIME", defaultConnMaxLifetime),
		ConnMaxIdleTime: getEnvDuration("DATABASE_CONN_MAX_IDLE_TIME", defaultConnMaxIdleTime),
	}

	return config
}

// Validate checks if the PostgreSQL configuration is valid.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.databaseURL) == "" {
		return ErrDatabaseURLEmpty
	}

	return nil
}

// MaskDatabaseURL returns a masked databaseURL safe for logging.
func (c *Config) MaskDatabaseURL() string {
	if c.databaseURL == "" {
		return ""
	}

	// Find the scheme separator
	schemeEnd := strings.Index(c.databaseURL, "://")
	if schemeEnd == -1 {
		return c.databaseURL
	}

	// Find the last @ which separates userinfo from host
	afterScheme := c.databaseURL[schemeEnd+3:]

	lastAtIndex := strings.LastIndex(afterScheme, "@")
	if lastAtIndex == -1 {
		// No @ found, no userinfo
		return c.databaseURL
	}

	// Extract userinfo
	userInfo := afterScheme[:lastAtIndex]

	colonIndex := strings.Index(userInfo, ":")
	if colonIndex == -1 {
		// No password
		return c.databaseURL
	}

	// Found username:password
	username := userInfo[:colonIndex]
	password := userInfo[colonIndex+1:]

	if password == "" {
		// Empty password, don't mask
		return c.databaseURL
	}

	// Build masked URL
	scheme := c.databaseURL[:schemeEnd]
	hostAndRest := afterScheme[lastAtIndex:]

	return scheme + "://" + username + ":***" + hostAndRest
}

// getEnvStr returns the environment variable value or a default if not set.
func getEnvStr(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

// getEnvInt returns the environment variable value or a default if not set.
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}

	return defaultValue
}

// getEnvInt returns the environment variable value or a default if not set.
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}

	return defaultValue
}

// getEnvLogLevel returns the environment variable value or a default if not set.
func getEnvLogLevel(key string, defaultValue slog.Level) slog.Level {
	if value := os.Getenv(key); value != "" {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "debug":
			return slog.LevelDebug
		case "info":
			return slog.LevelInfo
		case "warn", "warning":
			return slog.LevelWarn
		case "error":
			return slog.LevelError
		}
	}

	return defaultValue
}
