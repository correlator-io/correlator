// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
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

					// Return 500 Internal Server Error
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)

					_, err = w.Write(
						[]byte(`{"error": "Internal server error", "correlation_id": "` + correlationID + `"}`),
					)
					if err != nil {
						logger.Error(
							"Failed to write response",
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
