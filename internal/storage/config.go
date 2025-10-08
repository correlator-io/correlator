package storage

import (
	"errors"
	"strings"
	"time"

	"github.com/correlator-io/correlator/internal/config"
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
	return &Config{
		databaseURL:     config.GetEnvStr("DATABASE_URL", ""), // DatabaseURL is private for obvious reasons.
		MaxOpenConns:    config.GetEnvInt("DATABASE_MAX_OPEN_CONNS", defaultMaxOpenConns),
		MaxIdleConns:    config.GetEnvInt("DATABASE_MAX_IDLE_CONNS", defaultMaxIdleConns),
		ConnMaxLifetime: config.GetEnvDuration("DATABASE_CONN_MAX_LIFETIME", defaultConnMaxLifetime),
		ConnMaxIdleTime: config.GetEnvDuration("DATABASE_CONN_MAX_IDLE_TIME", defaultConnMaxIdleTime),
	}
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
