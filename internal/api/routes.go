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
	// HealthStatus represents the health check response structure.
	HealthStatus struct {
		Status      string `json:"status"`
		ServiceName string `json:"serviceName"`
		Version     string `json:"version"`
		Uptime      string `json:"uptime,omitempty"`
	}

	// LineageResponse represents OpenLineage-compliant batch response.
	// Spec: https://openlineage.io/apidocs/openapi/#tag/OpenLineage/operation/postEventBatch
	//
	// The response includes only failed events (OpenLineage spec) plus Correlator extensions
	// for observability (correlation_id, timestamp).
	//
	// Correlator Extensions (not in OpenLineage spec):
	//   - correlation_id: Request correlation ID for tracing
	//   - timestamp: Response generation time (ISO8601)
	LineageResponse struct {
		Status        string          `json:"status"`         // "success" or "error" (OpenLineage spec)
		Summary       ResponseSummary `json:"summary"`        // Event counts (received, successful, failed, retriable)
		FailedEvents  []FailedEvent   `json:"failed_events"`  //nolint: tagliatelle // Only failed events
		CorrelationID string          `json:"correlation_id"` //nolint: tagliatelle // Correlator extension
		Timestamp     string          `json:"timestamp"`      // Correlator extension
	}

	// ResponseSummary provides aggregate counts for batch processing.
	// This matches the OpenLineage spec format.
	ResponseSummary struct {
		Received     int `json:"received"`      // Total events in batch
		Successful   int `json:"successful"`    // Stored + duplicates (idempotent = success)
		Failed       int `json:"failed"`        // Events that failed validation or storage
		Retriable    int `json:"retriable"`     // Transient failures (network, timeout)
		NonRetriable int `json:"non_retriable"` //nolint: tagliatelle // Permanent failures (validation, missing fields)
	}

	// FailedEvent describes a single failed event in the batch.
	// OpenLineage spec only includes failed events in response (not successful).
	FailedEvent struct {
		Index     int    `json:"index"`     // Event index in original batch (0-based)
		Reason    string `json:"reason"`    // Human-readable failure reason
		Retriable bool   `json:"retriable"` // True if transient failure (can retry)
	}

	// LineageEvent model represents an event in the payload of an API request to ingest OpenLineage events.
	// This is separate from the domain model (ingestion.RunEvent) to decouple
	// the API contract from internal domain types.
	//
	// MVP Scope:
	//   - Producer URL format validation is a nice-to-have for post-MVP
	//   - SchemaURL validation is a nice-to-have for post-MVP
	//   - Current implementation prioritizes flexibility over strict validation
	LineageEvent struct {
		EventTime time.Time `json:"eventTime"`
		EventType string    `json:"eventType"`
		Producer  string    `json:"producer"`  // Optional, URL format validation deferred to post-MVP
		SchemaURL string    `json:"schemaURL"` //nolint: tagliatelle // Optional, validation deferred to post-MVP
		Run       Run       `json:"run"`
		Job       Job       `json:"job"`
		Inputs    []Dataset `json:"inputs,omitempty"`
		Outputs   []Dataset `json:"outputs,omitempty"`
	}

	// Run represents the run section of a LineageEvent.
	Run struct {
		ID     string                 `json:"runId"`
		Facets map[string]interface{} `json:"facets,omitempty"`
	}

	// Job represents the job section of a LineageEvent.
	Job struct {
		Namespace string                 `json:"namespace"`
		Name      string                 `json:"name"`
		Facets    map[string]interface{} `json:"facets,omitempty"`
	}

	// Dataset represents a dataset (input or output) in a LineageEvent.
	Dataset struct {
		Namespace    string                 `json:"namespace"`
		Name         string                 `json:"name"`
		Facets       map[string]interface{} `json:"facets,omitempty"`
		InputFacets  map[string]interface{} `json:"inputFacets,omitempty"`
		OutputFacets map[string]interface{} `json:"outputFacets,omitempty"`
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

	// Protected endpoints
	mux.HandleFunc("GET /api/v1/health/data-consistency", s.handleDataConsistency)

	// Lineage endpoints
	mux.HandleFunc("POST /api/v1/lineage/events", s.handleLineageEvents)
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
	w.Header().Set("X-Correlator-Version", "v1.0.0") // TODO: inject version at build time at the end of week 2
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
// This endpoint verifies that all storage dependencies are healthy and ready to serve requests.
//
// Response codes:
//   - 200 OK: All storage backends are healthy and ready to accept traffic
//   - 503 Service Unavailable: Storage backend is unhealthy or unreachable
//
// K8s readiness probes use this endpoint to determine if the pod should receive traffic.
// If this endpoint returns 503, K8s will stop routing requests to the pod until it recovers.
//
// The health check delegates to the APIKeyStore's HealthCheck method, which verifies
// the underlying storage backend (database, cache, etc.) is operational.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	// If API key store not configured, return ready (degraded mode - no auth required)
	if s.apiKeyStore == nil { // pragma: allowlist secret
		s.logger.Warn("API key store not configured - readiness check disabled",
			slog.String("correlation_id", correlationID),
		)

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		_, err := w.Write([]byte("ready"))
		if err != nil {
			s.logger.Error("Failed to write ready response",
				slog.String("correlation_id", correlationID),
				slog.String("error", err.Error()),
			)
		}

		return
	}

	// Create context with 2-second timeout for storage health check
	ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
	defer cancel()

	if err := s.apiKeyStore.HealthCheck(ctx); err != nil {
		s.logger.Error("Storage health check failed",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)

		// Return 503 Service Unavailable if storage backend is unhealthy
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

// handleDataConsistency returns correlator health check.
// TODO: Implement full data consistency check by the end of week 2 or week 4.
func (s *Server) handleDataConsistency(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	// Dummy response for now
	health := map[string]interface{}{
		"missing_correlations": 23, //nolint: mnd
		"stale_events":         5,  //nolint: mnd
		"plugin_failures":      map[string]interface{}{},
	}

	data, err := json.Marshal(health)
	if err != nil {
		s.logger.Error("Failed to marshal data consistency response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)
		WriteErrorResponse(w, r, s.logger, InternalServerError("..."))

		return
	}

	// Only write headers after successful marshaling
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(data); err != nil {
		// At this point headers already sent, log only
		correlationID := middleware.GetCorrelationID(r.Context())
		s.logger.Error("Failed to write data consistency response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)
	}
}

// handleHealth returns detailed health status information.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	// Calculate uptime if server has started
	var uptime string

	if !s.startTime.IsZero() {
		duration := time.Since(s.startTime)
		uptime = duration.Round(time.Second).String()
	}

	health := HealthStatus{
		Status:      "healthy",
		ServiceName: "correlator",
		Version:     "v1.0.0", // TODO: inject version at build time at the end of week 2
		Uptime:      uptime,
	}

	data, err := json.Marshal(health)
	if err != nil {
		s.logger.Error("Failed to encode health response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode health response"))

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Correlator-Version", "v1.0.0") // TODO: inject version at build time at the end of week 2
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(data); err != nil {
		correlationID := middleware.GetCorrelationID(r.Context())
		s.logger.Error("Failed to write data consistency response",
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
