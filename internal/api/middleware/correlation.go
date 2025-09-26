// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

const correlationIDSize = 8

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

// generateCorrelationID generates a new correlation ID.
func generateCorrelationID() string {
	bytes := make([]byte, correlationIDSize)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based ID if random generation fails
		return hex.EncodeToString([]byte("fallback"))
	}

	return hex.EncodeToString(bytes)
}
