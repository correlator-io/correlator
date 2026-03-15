// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
)

const (
	healthCheckTimeout     = 2 * time.Second
	expectedURLParts       = 2
	contentTypeProblemJSON = "application/problem+json"
)

type (
	// Version represents the API version response structure.
	Version struct {
		Version     string `json:"version"`
		ServiceName string `json:"serviceName"`
		BuildInfo   string `json:"buildInfo,omitempty"`
	}

	// Route represents an HTTP route configuration with a path and handler.
	// Used for declarative route registration with middleware bypass support.
	Route struct {
		Path    string           // The URL path for this route (e.g., "/ping", "/api/v1/health")
		Handler http.HandlerFunc // The HTTP handler function for this route
	}
)

// Routes sets up all HTTP routes for the API server.
func (s *Server) setupRoutes(mux *http.ServeMux) {
	// Public health endpoints
	s.registerPublicRoutes(
		mux,
		Route{"GET /ping", s.handlePing},     // K8s liveness probe
		Route{"GET /ready", s.handleReady},   // K8s readiness probe
		Route{"GET /health", s.handleHealth}, // Basic health check - status, uptime, version
		Route{"/", s.handleNotFound},         // Catch-all handler for 404 responses
	)

	// Lineage endpoints
	mux.HandleFunc("POST /api/v1/lineage", s.handleLineageEvent)        // Single event (standard OL API)
	mux.HandleFunc("POST /api/v1/lineage/batch", s.handleLineageEvents) // Batch events

	// Correlation endpoints (UI)
	if s.correlationStore != nil {
		mux.HandleFunc("GET /api/v1/incidents", s.handleListIncidents)
		mux.HandleFunc("GET /api/v1/incidents/counts", s.handleGetIncidentCounts)
		mux.HandleFunc("GET /api/v1/incidents/{id}", s.handleGetIncidentDetails)
		mux.HandleFunc("GET /api/v1/health/correlation", s.handleGetCorrelationHealth)
	}

	// Resolution endpoints (write operations)
	if s.resolutionStore != nil {
		mux.HandleFunc("PATCH /api/v1/incidents/{id}/status", s.handleUpdateIncidentStatus)
	}
}

// registerPublicRoutes registers HTTP routes that bypass authentication and rate limiting.
// This is a convenience method that:
//  1. Registers the route handler with the HTTP mux
//  2. Automatically registers the path as a public endpoint (bypasses auth middleware)
//
// Public routes should only be used for health check endpoints that need to be accessible
// without authentication (e.g., K8s liveness/readiness probes, monitoring tools).
//
// Security Warning: Never register business logic endpoints as public routes.
//
// Example:
//
//	s.registerPublicRoutes(
//	    mux,
//	    Route{"/ping", s.handlePing},
//	    Route{"/health", s.handleHealth},
//	)
func (s *Server) registerPublicRoutes(mux *http.ServeMux, routes ...Route) {
	validHTTPMethods := map[string]bool{
		"GET":    true,
		"POST":   true,
		"PUT":    true,
		"PATCH":  true,
		"DELETE": true,
	}

	for _, route := range routes {
		mux.Handle(route.Path, route.Handler)

		// Strip method prefix for public endpoint bypass registration
		// Go 1.22+ method-based routing uses "GET /path" format
		// But r.URL.Path is just "/path" (no method prefix)
		path := route.Path

		parts := strings.Fields(path)
		// If the route path contains a method prefix (e.g., "GET /ping"), extract the path part.
		if len(parts) == expectedURLParts && validHTTPMethods[parts[0]] {
			path = strings.TrimSpace(parts[1]) // Extract path after method (e.g., "GET /ping" → "/ping")
		}

		// Skip registering an empty path as a public
		if path == "" {
			s.logger.Warn("Malformed route path detected, ignoring route", slog.String("path", path))

			continue
		}

		// Always register (handles both "GET /ping" and "/" formats)
		middleware.RegisterPublicEndpoint(path)
	}
}

// handlePing responds to ping requests for basic server validation.
func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-Correlator-Version", s.buildInfo.Version)
	w.WriteHeader(http.StatusOK)

	_, err := w.Write([]byte("pong"))
	if err != nil {
		s.logger.Error("Failed to write ping response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)
	}
}

// handleReady responds to Kubernetes readiness probes with storage backend health checks.
//
// Checks the ingestion store, which is the common dependency between
// the HTTP and Kafka ingestion paths. If the ingestion store is unhealthy, neither
// ingestion path can function.
//
// Response codes:
//   - 200 OK: Storage backend is healthy and ready to accept traffic
//   - 503 Service Unavailable: Storage backend is unhealthy or unreachable
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
	defer cancel()

	if err := s.ingestionStore.HealthCheck(ctx); err != nil {
		s.logger.Error("Readiness check failed",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)

		_, writeErr := w.Write([]byte("storage unavailable"))
		if writeErr != nil {
			s.logger.Error("Failed to write unavailable response",
				slog.String("correlation_id", correlationID),
				slog.String("error", writeErr.Error()),
			)
		}

		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)

	_, err := w.Write([]byte("ready"))
	if err != nil {
		s.logger.Error("Failed to write ready response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)
	}
}

// handleHealth returns dependency-aware health status for operator diagnostics.
// Unlike /ready (binary K8s probe), this endpoint reports per-component status
// so operators can pinpoint which dependency is failing during initial setup
// or incident investigation.
//
// Response codes:
//   - 200 OK: All dependencies healthy, or degraded (some non-critical dependency down)
//   - 503 Service Unavailable: Critical dependency down (PostgreSQL unreachable)
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
	defer cancel()

	health := s.healthChecker.Check(ctx)

	health.Version = s.buildInfo.Version
	if !s.startTime.IsZero() {
		health.Uptime = time.Since(s.startTime).Round(time.Second).String()
	}

	data, err := json.Marshal(health.toResponse())
	if err != nil {
		s.logger.Error("Failed to encode health response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode health response"))

		return
	}

	httpStatus := http.StatusOK
	if health.Status == statusUnhealthy {
		httpStatus = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)

	if _, err := w.Write(data); err != nil {
		s.logger.Error("Failed to write health response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)
	}
}

// handleNotFound returns RFC 7807 compliant 404 responses for unknown endpoints.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	WriteErrorResponse(w, r, s.logger, NotFound("The requested resource was not found"))
}

// hasJSONContentType checks if Content-Type header starts with "application/json".
// This allows charset parameters (e.g., "application/json; charset=utf-8").
func hasJSONContentType(contentType string) bool {
	return strings.HasPrefix(strings.TrimSpace(contentType), "application/json")
}
