// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"net/http"
	"strconv"
	"strings"
)

// CORSConfig is imported from the api package to avoid duplication.
// This type is defined in internal/api/config.go.
type CORSConfig interface {
	GetAllowedOrigins() []string
	GetAllowedMethods() []string
	GetAllowedHeaders() []string
	GetMaxAge() int
}

// CORS creates a middleware that handles Cross-Origin Resource Sharing (CORS).
func CORS(config CORSConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Set all CORS headers
			setCORSOriginHeader(w, r, config.GetAllowedOrigins())
			setCORSMethodsHeader(w, config.GetAllowedMethods())
			setCORSHeadersHeader(w, config.GetAllowedHeaders())
			setCORSMaxAgeHeader(w, config.GetMaxAge())

			// Handle preflight requests
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)

				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// setCORSOriginHeader sets the Access-Control-Allow-Origin header based on allowed origins.
func setCORSOriginHeader(w http.ResponseWriter, r *http.Request, allowedOrigins []string) {
	if len(allowedOrigins) == 0 {
		return
	}

	if len(allowedOrigins) == 1 && allowedOrigins[0] == "*" {
		w.Header().Set("Access-Control-Allow-Origin", "*")

		return
	}

	origin := r.Header.Get("Origin")
	for _, allowedOrigin := range allowedOrigins {
		if origin == allowedOrigin {
			w.Header().Set("Access-Control-Allow-Origin", origin)

			break
		}
	}
}

// setCORSMethodsHeader sets the Access-Control-Allow-Methods header.
func setCORSMethodsHeader(w http.ResponseWriter, allowedMethods []string) {
	if len(allowedMethods) > 0 {
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(allowedMethods, ", "))
	}
}

// setCORSHeadersHeader sets the Access-Control-Allow-Headers header.
func setCORSHeadersHeader(w http.ResponseWriter, allowedHeaders []string) {
	if len(allowedHeaders) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(allowedHeaders, ", "))
	}
}

// setCORSMaxAgeHeader sets the Access-Control-Max-Age header.
func setCORSMaxAgeHeader(w http.ResponseWriter, maxAge int) {
	if maxAge > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(maxAge))
	}
}
