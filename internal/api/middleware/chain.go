// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"log/slog"
	"net/http"

	"github.com/correlator-io/correlator/internal/storage"
)

type (
	// Option is a function that applies middleware to a handler.
	Option func(http.Handler) http.Handler
)

// Apply applies a chain of middleware options to a base handler.
// Middleware is applied in the order provided (first option wraps handler first).
//
// Example:
//
//	handler := middleware.Apply(mux,
//	    middleware.WithCorrelationID(),
//	    middleware.WithRecovery(logger),
//	    middleware.WithAuthPlugin(store, logger),
//	    middleware.WithRateLimit(limiter, logger),
//	    middleware.WithRequestLogger(logger),
//	    middleware.WithCORS(corsConfig),
//	)
func Apply(handler http.Handler, options ...Option) http.Handler {
	// Apply middleware in reverse order so that the first option
	// becomes the outermost middleware in the chain
	for i := len(options) - 1; i >= 0; i-- {
		handler = options[i](handler)
	}

	return handler
}

// WithCorrelationID returns an option that adds correlation ID middleware.
func WithCorrelationID() Option {
	return func(next http.Handler) http.Handler {
		return CorrelationID()(next)
	}
}

// WithRecovery returns an option that adds panic recovery middleware.
func WithRecovery(logger *slog.Logger) Option {
	return func(next http.Handler) http.Handler {
		return Recovery(logger)(next)
	}
}

// WithAuthPlugin returns an option that adds plugin authentication middleware.
// If store is nil, this option is skipped (no middleware applied).
func WithAuthPlugin(store storage.APIKeyStore, logger *slog.Logger) Option {
	if store == nil {
		return func(next http.Handler) http.Handler {
			return next // No-op if store not configured
		}
	}

	return func(next http.Handler) http.Handler {
		return AuthenticatePlugin(store, logger)(next)
	}
}

// WithRateLimit returns an option that adds rate limiting middleware.
// If limiter is nil, this option is skipped (no middleware applied).
func WithRateLimit(limiter RateLimiter, logger *slog.Logger) Option {
	if limiter == nil {
		return func(next http.Handler) http.Handler {
			return next // No-op if limiter not configured
		}
	}

	return func(next http.Handler) http.Handler {
		return RateLimit(limiter, logger)(next)
	}
}

// WithRequestLogger returns an option that adds request logging middleware.
func WithRequestLogger(logger *slog.Logger) Option {
	return func(next http.Handler) http.Handler {
		return RequestLogger(logger)(next)
	}
}

// WithCORS returns an option that adds CORS middleware.
func WithCORS(config CORSConfigProvider) Option {
	return func(next http.Handler) http.Handler {
		return CORS(config)(next)
	}
}
