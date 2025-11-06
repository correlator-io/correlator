// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// Recovery creates a middleware that recovers from panics and logs them.
func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func(ctx context.Context) {
				if err := recover(); err != nil {
					correlationID := GetCorrelationID(ctx)

					logger.Error("HTTP request panic recovered",
						slog.String("method", r.Method),
						slog.String("path", r.URL.Path),
						slog.String("correlation_id", correlationID),
						slog.Any("panic", err),
						slog.String("stack_trace", string(debug.Stack())),
					)

					// Return RFC 7807 compliant error response
					problemDetail := struct {
						Type          string `json:"type"`
						Title         string `json:"title"`
						Status        int    `json:"status"`
						Detail        string `json:"detail"`
						Instance      string `json:"instance"`
						CorrelationID string `json:"correlation_id"` //nolint: tagliatelle
					}{
						Type:          fmt.Sprintf("https://correlator.io/problems/%d", http.StatusInternalServerError),
						Title:         "Internal Server Error",
						Status:        http.StatusInternalServerError,
						Detail:        "An unexpected error occurred while processing the request",
						Instance:      r.URL.Path,
						CorrelationID: correlationID,
					}

					w.Header().Set("Content-Type", "application/problem+json")
					w.WriteHeader(http.StatusInternalServerError)

					if err := json.NewEncoder(w).Encode(problemDetail); err != nil {
						logger.Error(
							"Failed to encode error response",
							slog.Any("error", err),
							slog.String("correlation_id", correlationID),
						)
					}
				}
			}(r.Context())

			next.ServeHTTP(w, r)
		})
	}
}
