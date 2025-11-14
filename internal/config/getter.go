// Package config provides functions for reading config settings from ENV.
package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// GetEnvStr returns a string environment variable value or a default if not set.
//
// Parameters:
//   - key[string]: Name of the environment variable as a string
//   - defaultValue[string]: The default value to return in-case no environment variable is set
//
// Example:
//
//	s := GetEnvStr("HOST", "localhost")
func GetEnvStr(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return defaultValue
}

// GetEnvInt returns an int environment variable value or a default if not set.
//
// Parameters:
//   - key[string]: Name of the environment variable as a string
//   - defaultValue[int]: The default value to return in-case no environment variable is set
//
// Example:
//
//	i := GetEnvStr("PORT", "8000")
func GetEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}

	return defaultValue
}

// GetEnvInt64 returns an int64 environment variable value or a default if not set.
//
// Parameters:
//   - key[string]: Name of the environment variable as a string
//   - defaultValue[int64]: The default value to return in-case no environment variable is set
//
// Example:
//
//	i := GetEnvInt64("MAX_REQUEST_SIZE", 1048576)
func GetEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if int64Value, err := strconv.ParseInt(value, 10, 64); err == nil {
			return int64Value
		}
	}

	return defaultValue
}

// GetEnvBool returns a bool environment variable value or a default if not set.
// Accepts: "true", "1", "yes" as true; "false", "0", "no" as false (case-insensitive).
//
// Parameters:
//   - key[string]: Name of the environment variable as a string
//   - defaultValue[bool]: The default value to return in-case no environment variable is set
//
// Example:
//
//	b := GetEnvBool("CORRELATOR_AUTH_ENABLED", false)
func GetEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true", "1", "yes":
			return true
		case "false", "0", "no":
			return false
		}
	}

	return defaultValue
}

// GetEnvDuration returns the environment variable value or a default if not set.
//
// Parameters:
//   - key[string]: Name of the environment variable as a string
//   - defaultValue[string]: The default value to return in-case no environment variable is set
//
// Example:
//
//	d := GetEnvStr("TIMEOUT", "5m")
func GetEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}

	return defaultValue
}

// GetEnvLogLevel returns the environment variable value or a default if not set.
//
// Parameters:
//   - key[string]: Name of the environment variable as a string
//   - defaultValue[slog.Level]: The default value to return in-case no environment variable is set
//
// Example:
//
//	l := GetEnvStr("LOG_LEVEL", "debug")
func GetEnvLogLevel(key string, defaultValue slog.Level) slog.Level {
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

// ParseCommaSeparatedList parses a comma-separated string into a slice of trimmed strings.
// Empty values are filtered out.
func ParseCommaSeparatedList(input string) []string {
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
