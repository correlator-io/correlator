// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"errors"
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
	return e.Message
}

// Is implements error unwrapping for errors.Is() compatibility.
func (e *AuthError) Is(target error) bool {
	// First check if target is comparing against AuthError itself
	var authError *AuthError
	if errors.As(target, &authError) {
		return false // AuthErrors are unique, not equal to other AuthErrors
	}

	return errors.Is(e.Type, target)
}

// IsAuthError checks if an error is an AuthError of a specific type.
func IsAuthError(err error, target error) bool {
	var authErr *AuthError
	if errors.As(err, &authErr) {
		return errors.Is(authErr.Type, target)
	}

	return false
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
