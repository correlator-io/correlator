// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/correlator-io/correlator/internal/config"
)

const (
	defaultPort           int    = 8080
	maxPort               int    = 65535
	defaultHost           string = "0.0.0.0"
	defaultCORSMaxAge     int    = 86400
	defaultTimeout               = 30 * time.Second
	defaultLogLevel              = slog.LevelInfo
	defaultMaxRequestSize int64  = 1048576 // 1 MB (1024 * 1024 bytes)
)

var (
	// ErrInvalidPort indicates the port number is outside valid range (1-65535).
	ErrInvalidPort = errors.New("invalid port")

	// ErrEmptyHost indicates the server host address is empty.
	ErrEmptyHost = errors.New("host cannot be empty")

	// ErrInvalidReadTimeout indicates the read timeout is zero or negative.
	ErrInvalidReadTimeout = errors.New("read timeout must be positive")

	// ErrInvalidWriteTimeout indicates the write timeout is zero or negative.
	ErrInvalidWriteTimeout = errors.New("write timeout must be positive")

	// ErrInvalidShutdownTimeout indicates the shutdown timeout is zero or negative.
	ErrInvalidShutdownTimeout = errors.New("shutdown timeout must be positive")

	// ErrInvalidMaxRequestSize indicates the max request size is zero or negative.
	ErrInvalidMaxRequestSize = errors.New("max request size must be positive")
)

type (
	// ServerConfig holds HTTP server configuration.
	// Pure configuration only - no runtime dependencies.
	ServerConfig struct {
		Port               int
		Host               string
		ReadTimeout        time.Duration
		WriteTimeout       time.Duration
		ShutdownTimeout    time.Duration
		LogLevel           slog.Level
		MaxRequestSize     int64
		CORSAllowedOrigins []string
		CORSAllowedMethods []string
		CORSAllowedHeaders []string
		CORSMaxAge         int
	}

	// CORSConfig holds CORS configuration options.
	// This is defined here to keep CORS configuration centralized.
	CORSConfig struct {
		AllowedOrigins []string
		AllowedMethods []string
		AllowedHeaders []string
		MaxAge         int
	}
)

// LoadServerConfig loads server configuration from environment variables with sensible defaults.
func LoadServerConfig() *ServerConfig {
	return &ServerConfig{
		Port:            config.GetEnvInt("CORRELATOR_SERVER_PORT", defaultPort),
		Host:            config.GetEnvStr("CORRELATOR_SERVER_HOST", defaultHost),
		ReadTimeout:     config.GetEnvDuration("CORRELATOR_SERVER_READ_TIMEOUT", defaultTimeout),
		WriteTimeout:    config.GetEnvDuration("CORRELATOR_SERVER_WRITE_TIMEOUT", defaultTimeout),
		ShutdownTimeout: config.GetEnvDuration("CORRELATOR_SERVER_TIMEOUT", defaultTimeout),
		LogLevel:        config.GetEnvLogLevel("CORRELATOR_SERVER_LOG_LEVEL", defaultLogLevel),
		MaxRequestSize:  config.GetEnvInt64("CORRELATOR_MAX_REQUEST_SIZE", defaultMaxRequestSize),
		CORSAllowedOrigins: config.ParseCommaSeparatedList(
			config.GetEnvStr("CORRELATOR_CORS_ALLOWED_ORIGINS", "*"),
		), // "*" is Development default - should be restricted in production
		CORSAllowedMethods: config.ParseCommaSeparatedList(
			config.GetEnvStr("CORRELATOR_CORS_ALLOWED_METHODS", "GET,POST,PUT,DELETE,OPTIONS"),
		),
		CORSAllowedHeaders: config.ParseCommaSeparatedList(
			config.GetEnvStr(
				"CORRELATOR_CORS_ALLOWED_HEADERS",
				"Content-Type,Authorization,X-Correlation-ID,X-API-Key",
			),
		),
		CORSMaxAge: config.GetEnvInt("CORRELATOR_CORS_MAX_AGE", defaultCORSMaxAge),
	}
}

// Address returns the server address in host:port format.
func (c *ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// ToCORSConfig converts ServerConfig CORS fields to middleware.CORSConfigProvider.
func (c *ServerConfig) ToCORSConfig() *CORSConfig {
	return &CORSConfig{
		AllowedOrigins: c.CORSAllowedOrigins,
		AllowedMethods: c.CORSAllowedMethods,
		AllowedHeaders: c.CORSAllowedHeaders,
		MaxAge:         c.CORSMaxAge,
	}
}

// GetAllowedOrigins returns the allowed origins for CORS.
func (c *CORSConfig) GetAllowedOrigins() []string {
	return c.AllowedOrigins
}

// GetAllowedMethods returns the allowed methods for CORS.
func (c *CORSConfig) GetAllowedMethods() []string {
	return c.AllowedMethods
}

// GetAllowedHeaders returns the allowed headers for CORS.
func (c *CORSConfig) GetAllowedHeaders() []string {
	return c.AllowedHeaders
}

// GetMaxAge returns the max age for CORS preflight cache.
func (c *CORSConfig) GetMaxAge() int {
	return c.MaxAge
}

// Validate validates the server configuration.
func (c *ServerConfig) Validate() error {
	if c.Port <= 0 || c.Port > maxPort {
		return fmt.Errorf("%w: %d, must be between 1 and %d", ErrInvalidPort, c.Port, maxPort)
	}

	if c.Host == "" {
		return ErrEmptyHost
	}

	if c.ReadTimeout <= 0 {
		return fmt.Errorf("%w: got %v", ErrInvalidReadTimeout, c.ReadTimeout)
	}

	if c.WriteTimeout <= 0 {
		return fmt.Errorf("%w: got %v", ErrInvalidWriteTimeout, c.WriteTimeout)
	}

	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("%w: got %v", ErrInvalidShutdownTimeout, c.ShutdownTimeout)
	}

	if c.MaxRequestSize <= 0 {
		return fmt.Errorf("%w: got %d bytes", ErrInvalidMaxRequestSize, c.MaxRequestSize)
	}

	return nil
}
