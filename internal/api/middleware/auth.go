// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"net/http"
	"strings"
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
