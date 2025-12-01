package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
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
		slog.Int("total", len(response.Results)),
		slog.Int("stored", response.Stored),
		slog.Int("duplicates", response.Duplicates),
		slog.Int("failed", response.Failed),
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

// normalizeNilSlices converts event values to pointers and normalizes nil slices to empty slices.
//
// JSON unmarshaling has a quirk where omitted array fields become nil instead of empty arrays.
// For example, if an OpenLineage event doesn't include "inputs" field, json.Unmarshal creates:
//
//	event.Inputs = nil  (not []Dataset{})
//
// The storage layer's defensive validation explicitly checks for nil slices:
//
//	if event.Inputs == nil {
//	    return fmt.Errorf("event.Inputs is nil")
//	}
//
// This normalization ensures that:
//   - Omitted fields become empty arrays []Dataset{} (not nil)
//   - Storage layer receives consistent input (never nil slices)
//   - No panic risk from nil pointer dereference
//
// Example transformation:
//
//	Input:  RunEvent{Inputs: nil, Outputs: nil}
//	Output: &RunEvent{Inputs: []Dataset{}, Outputs: []Dataset{}}
//
// Returns slice of event pointers with normalized slices (never nil).
func (s *Server) normalizeNilSlices(events []ingestion.RunEvent) []*ingestion.RunEvent {
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

	return eventPointers
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

	// Parse JSON array (with size limit - ultimate protection)
	var events []ingestion.RunEvent

	decoder := json.NewDecoder(io.LimitReader(r.Body, s.config.MaxRequestSize))
	if err := decoder.Decode(&events); err != nil {
		return nil, BadRequest("Invalid JSON: " + err.Error())
	}

	// Empty array check
	if len(events) == 0 {
		return nil, BadRequest("Event array cannot be empty")
	}

	// Convert to pointers and normalize nil slices (JSON decoding quirk)
	// Storage layer expects non-nil slices for Inputs/Outputs
	return s.normalizeNilSlices(events), nil
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

	// Validate individual events using shared validator (created once in constructor)
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
