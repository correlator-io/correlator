// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/storage"
)

const (
	// DefaultPort is the default HTTP server port.
	DefaultPort = 8080
	// MaxPort is the maximum valid port number.
	MaxPort = 65535
	// DefaultHost is the default server host.
	DefaultHost = "0.0.0.0"
	// DefaultTimeout is the default timeout for HTTP operations.
	DefaultTimeout = 30 * time.Second
	// DefaultLogLevel is the default log level.
	DefaultLogLevel = slog.LevelInfo
	// DefaultCORSMaxAge is the default CORS max age (24 hours).
	DefaultCORSMaxAge = 86400
)

// Static validation errors.
var (
	ErrInvalidPort            = errors.New("invalid port")
	ErrEmptyHost              = errors.New("host cannot be empty")
	ErrInvalidReadTimeout     = errors.New("read timeout must be positive")
	ErrInvalidWriteTimeout    = errors.New("write timeout must be positive")
	ErrInvalidShutdownTimeout = errors.New("shutdown timeout must be positive")
)

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Port               int
	Host               string
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	ShutdownTimeout    time.Duration
	LogLevel           slog.Level
	CORSAllowedOrigins []string
	CORSAllowedMethods []string
	CORSAllowedHeaders []string
	CORSMaxAge         int
	APIKeyStore        storage.APIKeyStore
	RateLimiter        middleware.RateLimiter
}

// LoadServerConfig loads server configuration from environment variables with sensible defaults.
func LoadServerConfig() ServerConfig {
	config := ServerConfig{
		Port:               DefaultPort,
		Host:               DefaultHost,
		ReadTimeout:        DefaultTimeout,
		WriteTimeout:       DefaultTimeout,
		ShutdownTimeout:    DefaultTimeout,
		LogLevel:           DefaultLogLevel,
		CORSAllowedOrigins: []string{"*"}, // Development default - should be restricted in production
		CORSAllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		CORSAllowedHeaders: []string{"Content-Type", "Authorization", "X-Correlation-ID", "X-API-Key"},
		CORSMaxAge:         DefaultCORSMaxAge,
	}

	// Load configuration from environment variables
	loadServerAddress(&config)
	loadTimeouts(&config)
	loadLogLevel(&config)
	loadCORSConfig(&config)

	return config
}

// Address returns the server address in host:port format.
func (c ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// ToCORSConfig converts ServerConfig CORS fields to middleware.CORSConfigProvider.
func (c ServerConfig) ToCORSConfig() CORSConfig {
	return CORSConfig{
		AllowedOrigins: c.CORSAllowedOrigins,
		AllowedMethods: c.CORSAllowedMethods,
		AllowedHeaders: c.CORSAllowedHeaders,
		MaxAge:         c.CORSMaxAge,
	}
}

// CORSConfig holds CORS configuration options.
// This is defined here to keep CORS configuration centralized.
type CORSConfig struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
	MaxAge         int
}

// GetAllowedOrigins returns the allowed origins for CORS.
func (c CORSConfig) GetAllowedOrigins() []string {
	return c.AllowedOrigins
}

// GetAllowedMethods returns the allowed methods for CORS.
func (c CORSConfig) GetAllowedMethods() []string {
	return c.AllowedMethods
}

// GetAllowedHeaders returns the allowed headers for CORS.
func (c CORSConfig) GetAllowedHeaders() []string {
	return c.AllowedHeaders
}

// GetMaxAge returns the max age for CORS preflight cache.
func (c CORSConfig) GetMaxAge() int {
	return c.MaxAge
}

// Validate validates the server configuration.
func (c ServerConfig) Validate() error {
	if c.Port <= 0 || c.Port > MaxPort {
		return fmt.Errorf("%w: %d, must be between 1 and %d", ErrInvalidPort, c.Port, MaxPort)
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

	return nil
}

// loadServerAddress loads server address configuration from environment variables.
func loadServerAddress(config *ServerConfig) {
	if portStr := os.Getenv("CORRELATOR_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil && port > 0 && port <= MaxPort {
			config.Port = port
		}
	}

	if host := os.Getenv("CORRELATOR_HOST"); host != "" {
		config.Host = host
	}
}

// loadTimeouts loads timeout configuration from environment variables.
func loadTimeouts(config *ServerConfig) {
	if timeoutStr := os.Getenv("CORRELATOR_READ_TIMEOUT"); timeoutStr != "" {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			config.ReadTimeout = timeout
		}
	}

	if timeoutStr := os.Getenv("CORRELATOR_WRITE_TIMEOUT"); timeoutStr != "" {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			config.WriteTimeout = timeout
		}
	}

	if timeoutStr := os.Getenv("CORRELATOR_SHUTDOWN_TIMEOUT"); timeoutStr != "" {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			config.ShutdownTimeout = timeout
		}
	}
}

// loadLogLevel loads log level configuration from environment variables.
func loadLogLevel(config *ServerConfig) {
	if logLevelStr := os.Getenv("CORRELATOR_LOG_LEVEL"); logLevelStr != "" {
		config.LogLevel = parseLogLevel(logLevelStr)
	}
}

// loadCORSConfig loads CORS configuration from environment variables.
func loadCORSConfig(config *ServerConfig) {
	if originsStr := os.Getenv("CORRELATOR_CORS_ALLOWED_ORIGINS"); originsStr != "" {
		config.CORSAllowedOrigins = parseCommaSeparatedList(originsStr)
	}

	if methodsStr := os.Getenv("CORRELATOR_CORS_ALLOWED_METHODS"); methodsStr != "" {
		config.CORSAllowedMethods = parseCommaSeparatedList(methodsStr)
	}

	if headersStr := os.Getenv("CORRELATOR_CORS_ALLOWED_HEADERS"); headersStr != "" {
		config.CORSAllowedHeaders = parseCommaSeparatedList(headersStr)
	}

	if maxAgeStr := os.Getenv("CORRELATOR_CORS_MAX_AGE"); maxAgeStr != "" {
		if maxAge, err := strconv.Atoi(maxAgeStr); err == nil && maxAge >= 0 {
			config.CORSMaxAge = maxAge
		}
	}
}

// parseLogLevel parses a log level string into slog.Level.
// Supports: "debug", "info", "warn", "error" (case insensitive).
func parseLogLevel(levelStr string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// parseCommaSeparatedList parses a comma-separated string into a slice of trimmed strings.
// Empty values are filtered out.
func parseCommaSeparatedList(input string) []string {
	if input == "" {
		return []string{}
	}

	parts := strings.Split(input, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
