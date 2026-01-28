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
	"github.com/correlator-io/correlator/internal/canonicalization"
	"github.com/correlator-io/correlator/internal/ingestion"
)

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
		slog.String("status", response.Status),
		slog.Int("received", response.Summary.Received),
		slog.Int("successful", response.Summary.Successful),
		slog.Int("failed", response.Summary.Failed),
		slog.Int("retriable", response.Summary.Retriable),
		slog.Int("non_retriable", response.Summary.NonRetriable),
		slog.Int("status_code", statusCode),
		slog.Duration("duration", duration),
	)
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
// Decodes API request types and maps them to domain models.
// Returns parsed events or a ProblemDetail if parsing fails.
//
// Validates:
//   - Request size (optimization for known oversized requests)
//   - Empty body check (better UX than JSON decode error)
//   - JSON parsing
//   - Empty array check
func (s *Server) parseLineageRequest(r *http.Request) ([]*ingestion.RunEvent, *ProblemDetail) {
	// Request size check (optimization: fail fast for known oversized requests)
	// Allow unknown sizes (-1) or 0 (empty, caught later)
	if r.ContentLength > 0 && r.ContentLength > s.config.MaxRequestSize {
		return nil, PayloadTooLarge(
			fmt.Sprintf("Request body exceeds maximum size of %d bytes", s.config.MaxRequestSize),
		)
	}

	// Empty body check (better UX: specific error message)
	if r.ContentLength == 0 {
		return nil, BadRequest("Request body cannot be empty")
	}

	var events []LineageEvent

	decoder := json.NewDecoder(io.LimitReader(r.Body, s.config.MaxRequestSize))
	if err := decoder.Decode(&events); err != nil {
		return nil, BadRequest("Invalid JSON: " + err.Error())
	}

	// Empty request check
	if len(events) == 0 {
		return nil, BadRequest("Event array cannot be empty")
	}

	// Map API requests to domain models
	runEvents := make([]*ingestion.RunEvent, len(events))

	for i := range events {
		runEvents[i] = mapLineageRequest(&events[i])
	}

	// Normalize nil slices (JSON decoding quirk)
	// Storage layer expects non-nil slices for Inputs/Outputs
	return normalizeInputsAndOutputs(runEvents), nil
}

// normalizeInputsAndOutputs ensures all Inputs/Outputs slices are non-nil.
// JSON un-marshaling may produce nil slices for omitted fields.
func normalizeInputsAndOutputs(events []*ingestion.RunEvent) []*ingestion.RunEvent {
	for i := range events {
		if events[i].Inputs == nil {
			events[i].Inputs = []ingestion.Dataset{}
		}

		if events[i].Outputs == nil {
			events[i].Outputs = []ingestion.Dataset{}
		}
	}

	return events
}

// validateEvents validates event sequence and individual events.
// Returns sorted events, validation errors per event, or a ProblemDetail if sequence validation fails.
//
// Performs:
//   - Event sequence validation (for single-run batches only)
//   - Sorting by eventTime
//   - Individual event validation using domain validator
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

	// Validate individual events using domain validator
	validationErrors := make([]error, len(sortedEvents))

	for i := range sortedEvents {
		// Validate using shared validator (created once in constructor)
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
		validResults, err := s.ingestionStore.StoreEvents(ctx, validEvents)
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

// buildLineageResponse builds OpenLineage-compliant batch response.
// Only includes failed events (OpenLineage spec), not successful ones.
//
// Classifies errors as retriable vs non-retriable:
//   - Non-retriable: Validation errors, missing required fields
//   - Retriable: Storage errors (transient failures)
func (s *Server) buildLineageResponse(
	correlationID string,
	events []*ingestion.RunEvent,
	validationErrors []error,
	storeResults []*ingestion.EventStoreResult,
) *LineageResponse {
	failedEvents := make([]FailedEvent, 0)
	successful, failed, retriable, nonRetriable := 0, 0, 0, 0

	for i := range events {
		// Check validation error first
		if validationErrors[i] != nil {
			reason := validationErrors[i].Error()
			failedEvents = append(failedEvents, FailedEvent{
				Index:     i,
				Reason:    reason,
				Retriable: false, // Validation errors are permanent (bad request)
			})
			failed++
			nonRetriable++

			s.logger.Warn("Event validation failed",
				slog.String("correlation_id", correlationID),
				slog.Int("event_index", i),
				slog.String("reason", reason),
			)

			continue
		}

		// Storage result should exist if validation passed
		storeResult := storeResults[i]
		if storeResult == nil {
			// Shouldn't happen, but handle gracefully
			failedEvents = append(failedEvents, FailedEvent{
				Index:     i,
				Reason:    "storage result missing",
				Retriable: false,
			})
			failed++
			nonRetriable++

			s.logger.Error("Storage result missing for valid event",
				slog.String("correlation_id", correlationID),
				slog.Int("event_index", i),
			)

			continue
		}

		// Check storage error
		if storeResult.Error != nil {
			reason := storeResult.Error.Error()
			failedEvents = append(failedEvents, FailedEvent{
				Index:     i,
				Reason:    reason,
				Retriable: false, // Storage errors are typically constraint violations (non-retriable)
			})
			failed++
			nonRetriable++

			s.logger.Warn("Event storage failed",
				slog.String("correlation_id", correlationID),
				slog.Int("event_index", i),
				slog.String("reason", reason),
			)

			continue
		}

		// Success (stored or duplicate)
		// OpenLineage spec: duplicates are idempotent success (not failures)
		successful++
	}

	// Determine overall status
	status := "success"
	if failed > 0 && successful == 0 {
		status = "error" // All failed
	}

	return &LineageResponse{
		Status: status,
		Summary: ResponseSummary{
			Received:     len(events),
			Successful:   successful,
			Failed:       failed,
			Retriable:    retriable,
			NonRetriable: nonRetriable,
		},
		FailedEvents:  failedEvents,
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
	}
}

// determineStatusCode determines HTTP status code from OpenLineage response.
//
// Status code logic:
//   - 200 OK: All events succeeded
//   - 207 Multi-Status: Partial success (some succeeded, some failed)
//   - 422 Unprocessable Entity: All events failed
func determineStatusCode(response *LineageResponse) int {
	if response.Summary.Failed == 0 {
		// All success
		return http.StatusOK
	} else if response.Summary.Successful > 0 {
		// Partial success
		response.Status = "partial_success" // set to partial_success as per openlineage api specification

		return http.StatusMultiStatus
	}

	// All failed
	return http.StatusUnprocessableEntity
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

// mapLineageRequest maps an API request type to the domain model.
// This explicit mapping layer decouples the API contract from internal domain types.
//
// The mapping performs:
//   - Whitespace trimming on string fields
//   - Dataset URN normalization (critical for multi-tool correlation)
//   - Nil facets initialization to empty maps
//
// Validation is delegated to the domain layer (ingestion.Validator.ValidateRunEvent)
// following Clean Architecture principles: domain owns its invariants.
func mapLineageRequest(req *LineageEvent) *ingestion.RunEvent {
	return &ingestion.RunEvent{
		EventTime: req.EventTime,
		EventType: ingestion.EventType(strings.TrimSpace(req.EventType)),
		Producer:  strings.TrimSpace(req.Producer),
		SchemaURL: strings.TrimSpace(req.SchemaURL),
		Run:       mapRunRequest(&req.Run),
		Job:       mapJobRequest(&req.Job),
		Inputs:    mapDatasets(req.Inputs),
		Outputs:   mapDatasets(req.Outputs),
	}
}

// mapRunRequest maps API Run model to domain Run model.
// Trims whitespace from run ID and initializes nil facets to empty map.
func mapRunRequest(req *Run) ingestion.Run {
	facets := req.Facets
	if facets == nil {
		facets = make(map[string]interface{})
	}

	return ingestion.Run{
		ID:     strings.TrimSpace(req.ID),
		Facets: facets,
	}
}

// mapJobRequest maps API Job model to domain Job model.
// Trims whitespace from namespace and name, initializes nil facets to empty map.
func mapJobRequest(req *Job) ingestion.Job {
	facets := req.Facets
	if facets == nil {
		facets = make(map[string]interface{})
	}

	return ingestion.Job{
		Namespace: strings.TrimSpace(req.Namespace),
		Name:      strings.TrimSpace(req.Name),
		Facets:    facets,
	}
}

// mapDatasets maps API Dataset slice to domain Dataset slice.
// Normalizes dataset URNs (namespace + name) and initializes nil facets to empty maps.
//
// URN normalization is critical for multi-tool correlation:
//   - postgres:// → postgresql:// (dbt vs Great Expectations)
//   - Removes default ports (postgres://db:5432 → postgres://db)
//   - Normalizes S3 schemes (s3a:// → s3://)
//
// Returns empty slice if input is nil.
func mapDatasets(requests []Dataset) []ingestion.Dataset {
	if requests == nil {
		return []ingestion.Dataset{}
	}

	datasets := make([]ingestion.Dataset, len(requests))

	for i, req := range requests {
		// Trim whitespace
		namespace := strings.TrimSpace(req.Namespace)
		name := strings.TrimSpace(req.Name)

		// Normalize namespace + name to canonical URN
		// This prevents correlation failures when different tools use different URI schemes
		// Example: ParseDatasetURN extracts parts, GenerateDatasetURN normalizes and recombines
		if namespace != "" && name != "" {
			// Parse and regenerate to normalize (handles postgres→postgresql, removes default ports)
			normalizedURN := canonicalization.GenerateDatasetURN(namespace, name)

			// Extract normalized components back
			normalizedNamespace, normalizedName, err := canonicalization.ParseDatasetURN(normalizedURN)
			if err == nil {
				namespace = normalizedNamespace
				name = normalizedName
			}
			// If parsing fails, use original values (graceful degradation)
			// consider adding logging or metrics here.
		}

		// Initialize nil facets to empty maps
		facets := req.Facets
		if facets == nil {
			facets = make(map[string]interface{})
		}

		inputFacets := req.InputFacets
		if inputFacets == nil {
			inputFacets = make(map[string]interface{})
		}

		outputFacets := req.OutputFacets
		if outputFacets == nil {
			outputFacets = make(map[string]interface{})
		}

		datasets[i] = ingestion.Dataset{
			Namespace:    namespace,
			Name:         name,
			Facets:       facets,
			InputFacets:  inputFacets,
			OutputFacets: outputFacets,
		}
	}

	return datasets
}
