// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/ingestion"
)

const (
	healthCheckTimeout = 2 * time.Second
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

	// LineageResponse represents the response from the lineage events endpoint.
	LineageResponse struct {
		CorrelationID string        `json:"correlation_id"` //nolint: tagliatelle
		Timestamp     string        `json:"timestamp"`
		Stored        int           `json:"stored"`
		Duplicates    int           `json:"duplicates"`
		Failed        int           `json:"failed"`
		Results       []EventResult `json:"results"`
	}

	// EventResult represents the result of processing a single event.
	EventResult struct {
		Index   int    `json:"index"`
		Status  int    `json:"status"`
		Message string `json:"message,omitempty"` // "stored", "duplicate"
		Error   string `json:"error,omitempty"`   // Validation/storage error
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
		Route{"/ping", s.handlePing},     // K8s liveness probe
		Route{"/ready", s.handleReady},   // K8s readiness probe
		Route{"/health", s.handleHealth}, // Basic health check - status, uptime, version
		Route{"/", s.handleNotFound},     // Catch-all handler for 404 responses
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
	for _, route := range routes {
		mux.Handle(route.Path, route.Handler)
		middleware.RegisterPublicEndpoint(route.Path)
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

// handleLineageEvents handles OpenLineage event ingestion.
// POST /api/v1/lineage/events - Ingest single or batch OpenLineage events
//
// Request validation (returns 4xx):
//   - 405 Method Not Allowed: Only POST is allowed (handled by route pattern)
//   - 415 Unsupported Media Type: Content-Type must be application/json
//   - 413 Payload Too Large: Request body exceeds MaxRequestSize
//   - 400 Bad Request: Empty body, invalid JSON, or empty event array
//   - 422 Unprocessable Entity: Invalid event sequence or all events fail validation
//
// Success responses:
//   - 200 OK: All events stored or duplicates (idempotency)
//   - 207 Multi-Status: Partial success (some stored, some failed)
func (s *Server) handleLineageEvents(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	correlationID := middleware.GetCorrelationID(r.Context())

	// Content-Type validation
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		WriteErrorResponse(w, r, s.logger, UnsupportedMediaType("Content-Type must be application/json"))

		return
	}

	// Parse and validate request
	events, problem := s.parseLineageRequest(r)
	if problem != nil {
		WriteErrorResponse(w, r, s.logger, problem)

		return
	}

	// Validate and sort events
	sortedEvents, validationErrors, problem := s.validateEvents(events)
	if problem != nil {
		WriteErrorResponse(w, r, s.logger, problem)

		return
	}

	// Store only valid events (filter out invalid events)
	storeResults, problem := s.storeValidEvents(r.Context(), sortedEvents, validationErrors)
	if problem != nil {
		WriteErrorResponse(w, r, s.logger, problem)

		return
	}

	// Build response
	response := s.buildLineageResponse(correlationID, sortedEvents, validationErrors, storeResults)

	// Send response (returns status code for logging)
	statusCode := s.sendLineageResponse(w, r, response)

	// Log success with duration
	duration := time.Since(startTime)
	s.logger.Info("Lineage events processed",
		slog.String("correlation_id", response.CorrelationID),
		slog.Int("total", len(response.Results)),
		slog.Int("stored", response.Stored),
		slog.Int("duplicates", response.Duplicates),
		slog.Int("failed", response.Failed),
		slog.Int("status_code", statusCode),
		slog.Duration("duration", duration),
	)
}

// buildLineageResponse aggregates per-event results into a LineageResponse.
// This helper method combines validation errors and storage results to build
// the final HTTP response with detailed per-event status.
//
// Uses early returns (continue) to reduce nesting and improve readability.
func (s *Server) buildLineageResponse(
	correlationID string,
	events []*ingestion.RunEvent,
	validationErrors []error,
	storeResults []*ingestion.EventStoreResult,
) *LineageResponse {
	results := make([]EventResult, len(events))
	stored, duplicates, failed := 0, 0, 0

	for i := range events {
		// Check validation error first (early return)
		if validationErrors[i] != nil {
			results[i] = EventResult{
				Index:  i,
				Status: http.StatusUnprocessableEntity,
				Error:  validationErrors[i].Error(),
			}
			failed++

			continue
		}

		// Storage result should exist if validation passed
		storeResult := storeResults[i]
		if storeResult == nil {
			// Shouldn't happen, but handle gracefully
			results[i] = EventResult{
				Index:  i,
				Status: http.StatusUnprocessableEntity,
				Error:  "storage result missing",
			}
			failed++

			continue
		}

		// Check storage error (early return)
		if storeResult.Error != nil {
			results[i] = EventResult{
				Index:  i,
				Status: http.StatusUnprocessableEntity,
				Error:  storeResult.Error.Error(),
			}
			failed++

			continue
		}

		// Check duplicate (early return)
		if storeResult.Duplicate {
			results[i] = EventResult{
				Index:   i,
				Status:  http.StatusOK,
				Message: "duplicate",
			}
			duplicates++

			continue
		}

		// Must be stored successfully (happy path at the end)
		results[i] = EventResult{
			Index:   i,
			Status:  http.StatusOK,
			Message: "stored",
		}
		stored++
	}

	return &LineageResponse{
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Stored:        stored,
		Duplicates:    duplicates,
		Failed:        failed,
		Results:       results,
	}
}

// determineStatusCode determines the HTTP status code based on the response.
//
// Status code logic:
//   - 200 OK: All events succeeded (stored or duplicate)
//   - 207 Multi-Status: Partial success (some stored/duplicate, some failed)
//   - 422 Unprocessable Entity: All events failed validation
func determineStatusCode(response *LineageResponse) int {
	if response.Failed == 0 {
		// All success (stored or duplicates)
		return http.StatusOK
	} else if response.Stored > 0 || response.Duplicates > 0 {
		// Partial success
		return http.StatusMultiStatus
	}

	// All failed
	return http.StatusUnprocessableEntity
}

// hasJSONContentType checks if Content-Type header starts with "application/json".
// This allows charset parameters (e.g., "application/json; charset=utf-8").
func hasJSONContentType(contentType string) bool {
	return strings.HasPrefix(strings.TrimSpace(contentType), "application/json")
}

// isSingleRunBatch checks if all events in the batch belong to the same run.
// ValidateEventSequence is designed for single-run batches only.
func isSingleRunBatch(events []*ingestion.RunEvent) bool {
	if len(events) == 0 {
		return false
	}

	firstRunID := events[0].Run.ID
	for i := 1; i < len(events); i++ {
		if events[i].Run.ID != firstRunID {
			return false
		}
	}

	return true
}

// parseLineageRequest parses and validates the HTTP request body.
// Returns parsed events or a ProblemDetail if validation fails.
//
// Validates:
//   - Request size (optimization for known oversized requests)
//   - Empty body check (better UX than JSON decode error)
//   - JSON parsing
//   - Empty array check
func (s *Server) parseLineageRequest(r *http.Request) ([]*ingestion.RunEvent, *ProblemDetail) {
	// 2. Request size check (optimization: fail fast for known oversized requests)
	// Allow unknown sizes (-1) or 0 (empty, caught later)
	if r.ContentLength > 0 && r.ContentLength > s.config.MaxRequestSize {
		return nil, PayloadTooLarge(
			fmt.Sprintf("Request body exceeds maximum size of %d bytes", s.config.MaxRequestSize),
		)
	}

	// 3. Empty body check (better UX: specific error message)
	if r.ContentLength == 0 {
		return nil, BadRequest("Request body cannot be empty")
	}

	// 4. Parse JSON array (with size limit - ultimate protection)
	var events []ingestion.RunEvent

	decoder := json.NewDecoder(io.LimitReader(r.Body, s.config.MaxRequestSize))
	if err := decoder.Decode(&events); err != nil {
		return nil, BadRequest("Invalid JSON: " + err.Error())
	}

	// 5. Empty array check
	if len(events) == 0 {
		return nil, BadRequest("Event array cannot be empty")
	}

	// 6. Convert to pointers and normalize nil slices (JSON decoding quirk)
	// Storage layer expects non-nil slices for Inputs/Outputs
	eventPointers := make([]*ingestion.RunEvent, len(events))
	for i := range events {
		if events[i].Inputs == nil {
			events[i].Inputs = []ingestion.Dataset{}
		}

		if events[i].Outputs == nil {
			events[i].Outputs = []ingestion.Dataset{}
		}

		eventPointers[i] = &events[i]
	}

	return eventPointers, nil
}

// validateEvents validates event sequence and individual events.
// Returns sorted events, validation errors per event, or a ProblemDetail if sequence validation fails.
//
// Performs:
//   - Event sequence validation (for single-run batches only)
//   - Sorting by eventTime
//   - Individual event validation
func (s *Server) validateEvents(
	events []*ingestion.RunEvent,
) ([]*ingestion.RunEvent, []error, *ProblemDetail) {
	// Validate event sequence (for single-run batches only)
	// ValidateEventSequence is designed for events from a SINGLE run.
	// For multi-run batches, we skip sequence validation (each run is independent).
	var sortedEvents []*ingestion.RunEvent

	if len(events) > 1 && isSingleRunBatch(events) {
		var err error

		sortedEvents, _, err = ingestion.ValidateEventSequence(events)
		if err != nil {
			return nil, nil, UnprocessableEntity("Invalid event sequence: " + err.Error())
		}
	} else {
		sortedEvents = events
	}

	// 8. Validate individual events using shared validator (created once in constructor)
	validationErrors := make([]error, len(sortedEvents))

	for i := range sortedEvents {
		if err := s.validator.ValidateRunEvent(sortedEvents[i]); err != nil {
			validationErrors[i] = err
		}
	}

	return sortedEvents, validationErrors, nil
}

// storeValidEvents filters valid events and stores them in the database.
// Returns store results (sparse array with nil for invalid events) or a ProblemDetail on catastrophic failure.
//
// This function implements the critical bug fix: filters out invalid events before passing to storage,
// preventing nil pointer panics in the storage layer.
func (s *Server) storeValidEvents(
	ctx context.Context,
	events []*ingestion.RunEvent,
	validationErrors []error,
) ([]*ingestion.EventStoreResult, *ProblemDetail) {
	correlationID := middleware.GetCorrelationID(ctx)

	// Filter out invalid events (don't send nil pointers to storage)
	validEvents := make([]*ingestion.RunEvent, 0, len(events))
	validIndexes := make([]int, 0, len(events))

	for i := range events {
		if validationErrors[i] == nil {
			validEvents = append(validEvents, events[i])
			validIndexes = append(validIndexes, i)
		}
	}

	// Store only valid events
	storeResults := make([]*ingestion.EventStoreResult, len(events))

	if len(validEvents) > 0 {
		validResults, err := s.lineageStore.StoreEvents(ctx, validEvents)
		if err != nil {
			s.logger.Error("Failed to store events",
				slog.String("correlation_id", correlationID),
				slog.String("error", err.Error()),
			)

			return nil, InternalServerError("Failed to store events")
		}

		// Map results back to original indexes (sparse array)
		for i, validIdx := range validIndexes {
			storeResults[validIdx] = validResults[i]
		}
	}

	return storeResults, nil
}

// sendLineageResponse marshals and sends the lineage response to the client.
// Returns the HTTP status code for logging purposes.
//
// The response parameter should be pre-built using buildLineageResponse().
// This function focuses solely on HTTP transmission: marshaling, setting headers, and writing the response.
func (s *Server) sendLineageResponse(
	w http.ResponseWriter,
	r *http.Request,
	response *LineageResponse,
) int {
	// Determine status code
	statusCode := determineStatusCode(response)

	// Marshal response (fail fast before headers)
	data, err := json.Marshal(response)
	if err != nil {
		s.logger.Error("Failed to marshal lineage response",
			slog.String("correlation_id", response.CorrelationID),
			slog.String("error", err.Error()),
		)
		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode response"))

		return http.StatusInternalServerError
	}

	// Write headers and response body
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if _, err := w.Write(data); err != nil {
		s.logger.Error("Failed to write lineage response",
			slog.String("correlation_id", response.CorrelationID),
			slog.String("error", err.Error()),
		)

		return statusCode
	}

	return statusCode
}
