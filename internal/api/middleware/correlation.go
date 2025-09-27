// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"
	"unsafe"
)

const (
	correlationIDSize = 8
	// correlationIDLength is the expected output length in hex characters (8 bytes = 16 hex chars).
	correlationIDLength = 16
)

// correlationIDKey is the context key for correlation ID.
type correlationIDKey struct{}

// CorrelationID creates a middleware that adds a correlation ID to each request.
// If the request already has a X-Correlation-ID header, it uses that value.
// Otherwise, it generates a new correlation ID.
func CorrelationID() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			correlationID := r.Header.Get("X-Correlation-ID")

			// Generate new correlation ID if not provided
			if correlationID == "" {
				correlationID = generateCorrelationID()
			}

			// Add correlation ID to response headers
			w.Header().Set("X-Correlation-ID", correlationID)

			// Add correlation ID to request context
			ctx := context.WithValue(r.Context(), correlationIDKey{}, correlationID)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetCorrelationID extracts the correlation ID from the request context.
func GetCorrelationID(ctx context.Context) string {
	if correlationID, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return correlationID
	}

	return "unknown"
}

// generateCorrelationID generates a new correlation ID with proper fallback.
// Uses crypto/rand for primary generation, time+process-based entropy for fallback.
func generateCorrelationID() string {
	bytes := make([]byte, correlationIDSize)
	if _, err := rand.Read(bytes); err != nil {
		// Enhanced fallback: timestamp + process-based entropy
		timestamp := time.Now().UnixNano()
		// Add process-based entropy using timestamp address (safer than unsafe)
		ptr := &timestamp
		//nolint:gosec // G103: Using pointer address for entropy in fallback case only
		entropy := uintptr(unsafe.Pointer(ptr))

		// Combine timestamp and memory address for better uniqueness
		combined := fmt.Sprintf("%x%x", timestamp, entropy)

		// Ensure we return exactly correlationIDLength characters (same as crypto version)
		if len(combined) > correlationIDLength {
			return combined[:correlationIDLength]
		}

		// Pad with process-specific data if needed
		return fmt.Sprintf("%-*s", correlationIDLength, combined)
	}

	return hex.EncodeToString(bytes)
}
