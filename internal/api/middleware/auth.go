// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/correlator-io/correlator/internal/storage"
	"golang.org/x/crypto/bcrypt"
)

type (
	// AuthError represents an authentication error with a specific type.
	AuthError struct {
		Type    error
		Message string
	}
)

// Authentication error types for granular error handling.
var (
	// ErrMissingAPIKey is returned when no API key is provided in headers.
	ErrMissingAPIKey = errors.New("missing API key")

	// ErrInvalidAPIKey is returned for invalid API key format or not found.
	// Generic error prevents enumeration attacks.
	ErrInvalidAPIKey = errors.New("invalid API key")

	// ErrAPIKeyExpired is returned when the API key has expired.
	ErrAPIKeyExpired = errors.New("API key expired")

	// ErrAPIKeyInactive is returned when the API key is inactive (soft-deleted).
	ErrAPIKeyInactive = errors.New("API key inactive")
)

// extractAPIKey extracts the API key from request headers.
// It checks the X-Api-Key header first (primary), then falls back to
// Authorization: Bearer header (secondary).
//
// Returns (key, true) if found and valid, ("", false) otherwise.
//
// Security considerations:
// - Rejects keys containing newlines (header injection prevention)
// - Trims whitespace from keys
// - Case-sensitive "Bearer " prefix check
// - X-Api-Key takes precedence over Authorization header.
func extractAPIKey(r *http.Request) (string, bool) {
	// Primary: Check X-Api-Key header
	if apiKey := r.Header.Get("X-Api-Key"); apiKey != "" {
		return validateAPIKey(apiKey)
	}

	// Secondary: Check Authorization: Bearer header
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		// Check for "Bearer " prefix (note the space)
		if strings.HasPrefix(authHeader, "Bearer ") {
			// Extract token after "Bearer "
			token := strings.TrimPrefix(authHeader, "Bearer ")

			return validateAPIKey(token)
		}
	}

	return "", false
}

// validateAPIKey validates and cleans an API key value.
// Returns (cleanedKey, true) if valid, ("", false) otherwise.
//
// Validation rules:
// - Rejects keys containing newlines (\r or \n) for header injection prevention
// - Trims leading/trailing whitespace
// - Rejects empty keys after trimming.
func validateAPIKey(key string) (string, bool) {
	// Security: Reject keys containing newlines (header injection prevention)
	if strings.ContainsAny(key, "\r\n") {
		return "", false
	}

	// Trim whitespace
	key = strings.TrimSpace(key)

	// Reject empty keys
	if key == "" {
		return "", false
	}

	return key, true
}

// Error implements the error interface for AuthError.
func (e *AuthError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("authentication failed: %s: %s", e.Type.Error(), e.Message)
	}

	return "authentication failed: " + e.Type.Error()
}

// Unwrap returns the wrapped error type, enabling standard errors.Is() and errors.As() behavior.
func (e *AuthError) Unwrap() error {
	return e.Type
}

// Timing attack prevention: Perform dummy bcrypt comparison
// to maintain constant time.
func performDummyBcryptComparison() {
	_ = bcrypt.CompareHashAndPassword([]byte("dummy"), []byte("dummy"))
}

// authenticateRequest performs API key authentication and validation.
// Returns the authenticated API key or an AuthError.
//
// Security considerations:
// - Timing attack prevention: Always performs full validation even if format is invalid
// - Constant-time comparison via storage.SecureCompare
// - Generic error messages to prevent enumeration
//
// Error handling:
// - Invalid format → ErrInvalidAPIKey (generic)
// - Key not found → ErrInvalidAPIKey (generic)
// - Inactive key → ErrAPIKeyInactive (specific)
// - Expired key → ErrAPIKeyExpired (specific).
func authenticateRequest(
	ctx context.Context,
	store storage.APIKeyStore,
	apiKey string,
) (*storage.APIKey, error) {
	// Validate API key format
	parsedKey, err := storage.ParseAPIKey(apiKey)
	if err != nil {
		performDummyBcryptComparison()

		return nil, &AuthError{
			Type:    ErrInvalidAPIKey,
			Message: "Invalid or missing API key",
		}
	}

	// Lookup Parsed API key in store
	foundKey, exists := store.FindByKey(ctx, parsedKey)
	if !exists {
		performDummyBcryptComparison()

		return nil, &AuthError{
			Type:    ErrInvalidAPIKey,
			Message: "Invalid or missing API key",
		}
	}

	// Check if key is active
	if !foundKey.Active {
		return nil, &AuthError{
			Type:    ErrAPIKeyInactive,
			Message: "API key is inactive",
		}
	}

	// Check if key has expired
	if foundKey.ExpiresAt != nil && time.Now().After(*foundKey.ExpiresAt) {
		return nil, &AuthError{
			Type:    ErrAPIKeyExpired,
			Message: "API key has expired",
		}
	}

	return foundKey, nil
}

// AuthenticatePlugin creates an authentication middleware that validates API keys
// and enriches request context with plugin information.
//
// The middleware:
// - Extracts API keys from X-Api-Key (primary) or Authorization: Bearer (fallback) headers
// - Validates API key format and authenticity
// - Checks active status and expiration
// - Enriches request context with PluginContext
// - Returns RFC 7807 compliant error responses on failure
//
// Example usage:
//
//	store := storage.NewPersistentKeyStore(db)
//	logger := slog.Default()
//	authMiddleware := middleware.AuthenticatePlugin(store, logger)
//	handler = authMiddleware(handler)
func AuthenticatePlugin(store storage.APIKeyStore, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authStart := time.Now()

			// Extract API key from headers
			apiKey, found := extractAPIKey(r)
			if !found {
				writeAuthError(w, r, logger, &AuthError{
					Type:    ErrMissingAPIKey,
					Message: "Missing API key",
				})

				return
			}

			// Authenticate request
			authenticated, err := authenticateRequest(r.Context(), store, apiKey)
			if err != nil {
				writeAuthError(w, r, logger, err)

				return
			}

			// Enrich context with plugin information
			pluginCtx := PluginContext{
				PluginID:    authenticated.PluginID,
				Name:        authenticated.Name,
				Permissions: authenticated.Permissions,
				KeyID:       authenticated.ID,
				AuthTime:    time.Now(),
			}
			ctx := SetPluginContext(r.Context(), pluginCtx)

			// Log successful authentication
			logger.Info("API key authenticated",
				slog.String("plugin_id", pluginCtx.PluginID),
				slog.String("key_id", pluginCtx.KeyID),
				slog.String("key", storage.MaskKey(authenticated.Key)),
				slog.Duration("auth_latency", time.Since(authStart)),
				slog.String("correlation_id", GetCorrelationID(r.Context())),
				slog.String("endpoint", r.URL.Path),
			)

			// Continue to next handler with enriched context
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// writeAuthError writes an RFC 7807 compliant error response for authentication failures.
// It maps authentication errors to appropriate HTTP status codes and logs the failure.
func writeAuthError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	correlationID := GetCorrelationID(r.Context())

	// Map authentication error to HTTP status code
	var statusCode int

	var authErr *AuthError
	if errors.As(err, &authErr) {
		switch {
		case errors.Is(authErr.Type, ErrMissingAPIKey):
			statusCode = http.StatusUnauthorized
		case errors.Is(authErr.Type, ErrInvalidAPIKey):
			statusCode = http.StatusUnauthorized
		case errors.Is(authErr.Type, ErrAPIKeyExpired):
			statusCode = http.StatusUnauthorized
		case errors.Is(authErr.Type, ErrAPIKeyInactive):
			statusCode = http.StatusForbidden
		default:
			statusCode = http.StatusUnauthorized
		}
	} else {
		// Fallback for unexpected errors
		statusCode = http.StatusUnauthorized
	}

	// Log authentication failure (no sensitive data)
	logger.Warn("Authentication failed",
		slog.String("reason", err.Error()),
		slog.String("correlation_id", correlationID),
		slog.String("endpoint", r.URL.Path),
		slog.String("remote_addr", r.RemoteAddr),
		slog.String("user_agent", r.UserAgent()),
	)

	// Write RFC 7807 compliant error response
	if err := writeRFC7807Error(w, r, statusCode, err.Error(), correlationID); err != nil {
		logger.Error("Failed to encode authentication error response",
			slog.String("correlation_id", correlationID),
			slog.String("path", r.URL.Path),
			slog.Any("encode_error", err),
		)
	}
}

// writeRFC7807Error writes an RFC 7807 compliant error response without importing the api package.
func writeRFC7807Error(
	w http.ResponseWriter,
	r *http.Request,
	statusCode int,
	detail,
	correlationID string,
) error {
	// Map status code to title
	var title string

	switch statusCode {
	case http.StatusUnauthorized:
		title = "Unauthorized"
	case http.StatusForbidden:
		title = "Forbidden"
	default:
		title = "Authentication Failed"
	}

	// Create RFC 7807 problem detail
	problem := map[string]interface{}{
		"type":          fmt.Sprintf("https://correlator.io/problems/%d", statusCode),
		"title":         title,
		"status":        statusCode,
		"detail":        detail,
		"instance":      r.URL.Path,
		"correlationId": correlationID,
	}

	// Set proper content type and status code
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(statusCode)

	// Write response
	return json.NewEncoder(w).Encode(problem)
}
