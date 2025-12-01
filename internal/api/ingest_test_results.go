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

// mapTestResultRequest maps API request to domain model and validates.
//
// Validation approach:
//   - API layer (here): Maps types, normalizes URNs, calls domain validation
//   - Domain layer (TestResult.Validate): ALL business rule validation
//   - Storage layer: FK constraints, database-level checks
//
// This avoids the anti-pattern of duplicating validation logic across layers.
//
// Dataset URN Normalization:
// External test result producers (dbt-tests, Great Expectations, custom frameworks)
// send dataset URNs following OpenLineage naming conventions (namespace/name format).
// We normalize these URNs to ensure they match the normalized format stored in the
// database from OpenLineage event ingestion (postgres:// → postgresql://, default port removal).
//
// Example:
//   - Input:  "postgres://db:5432/analytics.public.orders"
//   - Output: "postgresql://db/analytics.public.orders" (normalized)
//
// Spec: https://openlineage.io/docs/spec/naming/
//
// Returns TestResult and validation error.
func mapTestResultRequest(req *TestResultRequest) (*ingestion.TestResult, error) {
	// Parse and normalize status (case-insensitive)
	// Real-world frameworks send: "FAILED", "Failed", "failed", etc.
	status, err := ingestion.ParseTestStatus(req.Status)
	if err != nil {
		return nil, err
	}

	// Normalize dataset URN
	// External producers send URNs in OpenLineage format (namespace/name)
	// Parse and regenerate to apply Correlator's internal normalization rules
	// This ensures FK constraint will match datasets stored from OpenLineage events
	// Spec: https://openlineage.io/docs/spec/naming/
	datasetURN := strings.TrimSpace(req.DatasetURN)

	namespace, name, err := canonicalization.ParseDatasetURN(datasetURN)
	if err != nil {
		return nil, fmt.Errorf("invalid dataset_urn format: %w", err)
	}

	// Map API → Domain with normalization
	result := &ingestion.TestResult{
		TestName:   strings.TrimSpace(req.TestName),
		TestType:   strings.TrimSpace(req.TestType),
		DatasetURN: canonicalization.GenerateDatasetURN(namespace, name),
		JobRunID:   strings.TrimSpace(req.JobRunID),
		Status:     status,
		Message:    req.Message,
		Metadata:   req.Metadata, // Clean 1:1 mapping (framework-specific structure)
		ExecutedAt: req.ExecutedAt,
		DurationMs: req.DurationMs,
	}

	// Validate using domain logic (single source of truth)
	if err := result.Validate(); err != nil {
		return nil, err
	}

	return result, nil
}

// handleTestResults handles test results ingestion.
// POST /api/v1/test-results - Ingest single or batch test results
//
// Request validation (returns 4xx):
//   - 405 Method Not Allowed: Only POST is allowed (handled by route pattern)
//   - 415 Unsupported Media Type: Content-Type must be application/json
//   - 413 Payload Too Large: Request body exceeds MaxRequestSize
//   - 400 Bad Request: Empty body, invalid JSON, or empty test result array
//   - 422 Unprocessable Entity: All test results fail validation or FK violations
//
// Success responses:
//   - 200 OK: All test results stored or updated (UPSERT behavior)
//   - 207 Multi-Status: Partial success (some stored, some failed)
//
// Architecture: Follows same pattern as handleLineageEvents for consistency.
// Key difference: mapTestResultRequest() provides explicit API → Domain mapping
// to avoid tight coupling between API and domain types.
func (s *Server) handleTestResults(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	correlationID := middleware.GetCorrelationID(r.Context())

	// Content-Type validation
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		WriteErrorResponse(w, r, s.logger, UnsupportedMediaType("Content-Type must be application/json"))

		return
	}

	// Parse and validate request
	testResults, problem := s.parseTestResultsRequest(r)
	if problem != nil {
		WriteErrorResponse(w, r, s.logger, problem)

		return
	}

	// Map API → Domain and validate
	mappedResults, validationErrors := s.mapAndValidateTestResults(testResults)

	// Store valid test results (filter out invalid ones)
	storeResults, problem := s.storeValidTestResults(r.Context(), mappedResults, validationErrors)
	if problem != nil {
		WriteErrorResponse(w, r, s.logger, problem)

		return
	}

	// Build response
	response := s.buildTestResultResponse(correlationID, mappedResults, validationErrors, storeResults)

	// Send response (returns status code for logging)
	statusCode := s.sendTestResultResponse(w, r, response)

	// Log success with duration
	duration := time.Since(startTime)
	s.logger.Info("Test results processed",
		slog.String("correlation_id", response.CorrelationID),
		slog.Int("total", len(response.Results)),
		slog.Int("stored", response.Stored),
		slog.Int("failed", response.Failed),
		slog.Int("status_code", statusCode),
		slog.Duration("duration", duration),
	)
}

// parseTestResultsRequest parses and validates the HTTP request body for test results.
// Returns parsed test results or a ProblemDetail if validation fails.
//
// Validates:
//   - Request size (optimization for known oversized requests)
//   - Empty body check (better UX than JSON decode error)
//   - JSON parsing
//   - Empty array check
func (s *Server) parseTestResultsRequest(r *http.Request) ([]TestResultRequest, *ProblemDetail) {
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
	var testResults []TestResultRequest

	decoder := json.NewDecoder(io.LimitReader(r.Body, s.config.MaxRequestSize))
	if err := decoder.Decode(&testResults); err != nil {
		return nil, BadRequest("Invalid JSON: " + err.Error())
	}

	// Empty array check
	if len(testResults) == 0 {
		return nil, BadRequest("Test result array cannot be empty")
	}

	return testResults, nil
}

// mapAndValidateTestResults maps API requests to domain models and validates.
// Returns domain models and per-result validation errors.
//
// This function implements two validation layers:
//   - API-level validation (mapTestResultRequest): Missing required fields, format checks
//   - Domain-level validation (TestResult.Validate): Business rule validation
func (s *Server) mapAndValidateTestResults(
	requests []TestResultRequest,
) ([]*ingestion.TestResult, []error) {
	testResults := make([]*ingestion.TestResult, len(requests))
	validationErrors := make([]error, len(requests))

	for i := range requests {
		// Map API → Domain (API-level validation)
		testResult, err := mapTestResultRequest(&requests[i])
		if err != nil {
			validationErrors[i] = err

			continue
		}

		testResults[i] = testResult
	}

	return testResults, validationErrors
}

// storeValidTestResults filters valid test results and stores them in the database.
// Returns store results (sparse array with nil for invalid results) or a ProblemDetail on catastrophic failure.
//
// Follows same pattern as storeValidEvents: filters out invalid results before storage.
func (s *Server) storeValidTestResults(
	ctx context.Context,
	testResults []*ingestion.TestResult,
	validationErrors []error,
) ([]*ingestion.TestResultStoreResult, *ProblemDetail) {
	correlationID := middleware.GetCorrelationID(ctx)

	// Filter out invalid test results (don't send nil pointers to storage)
	validResults := make([]*ingestion.TestResult, 0, len(testResults))
	validIndexes := make([]int, 0, len(testResults))

	for i := range testResults {
		if validationErrors[i] == nil && testResults[i] != nil {
			validResults = append(validResults, testResults[i])
			validIndexes = append(validIndexes, i)
		}
	}

	// Store only valid test results
	storeResults := make([]*ingestion.TestResultStoreResult, len(testResults))

	if len(validResults) > 0 {
		validStoreResults, err := s.lineageStore.StoreTestResults(ctx, validResults)
		if err != nil {
			s.logger.Error("Failed to store test results",
				slog.String("correlation_id", correlationID),
				slog.String("error", err.Error()),
			)

			return nil, InternalServerError("Failed to store test results")
		}

		// Map results back to original indexes (sparse array)
		for i, validIdx := range validIndexes {
			storeResults[validIdx] = validStoreResults[i]
		}
	}

	return storeResults, nil
}

// buildTestResultResponse aggregates per-result results into a TestResultResponse.
// This helper method combines validation errors and storage results to build
// the final HTTP response with detailed per-result status.
//
// Uses early returns (continue) to reduce nesting and improve readability.
func (s *Server) buildTestResultResponse(
	correlationID string,
	testResults []*ingestion.TestResult,
	validationErrors []error,
	storeResults []*ingestion.TestResultStoreResult,
) *TestResultResponse {
	results := make([]TestResultStatus, len(testResults))
	stored, failed := 0, 0

	for i := range testResults {
		// Check validation error first (early return)
		if validationErrors[i] != nil {
			results[i] = TestResultStatus{
				Index:   i,
				Status:  http.StatusUnprocessableEntity,
				Message: "",
				Error:   validationErrors[i].Error(),
			}

			failed++

			continue
		}

		// Storage result should exist if validation passed
		storeResult := storeResults[i]
		if storeResult == nil {
			// Shouldn't happen, but handle gracefully
			results[i] = TestResultStatus{
				Index:   i,
				Status:  http.StatusUnprocessableEntity,
				Message: "",
				Error:   "storage result missing",
			}

			failed++

			continue
		}

		// Check storage error (early return)
		if storeResult.Error != nil {
			results[i] = TestResultStatus{
				Index:   i,
				Status:  http.StatusUnprocessableEntity,
				Message: "",
				Error:   storeResult.Error.Error(),
			}

			failed++

			continue
		}

		// Success: stored or updated (UPSERT behavior)
		// Note: We don't distinguish between insert and update in response message
		// Both are 200 OK with "stored" message (UPSERT is implementation detail)
		results[i] = TestResultStatus{
			Index:   i,
			Status:  http.StatusOK,
			Message: "stored",
			Error:   "",
		}

		stored++
	}

	return &TestResultResponse{
		CorrelationID: correlationID,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Stored:        stored,
		Failed:        failed,
		Results:       results,
	}
}

// determineTestResultStatusCode determines the HTTP status code based on the response.
//
// Status code logic:
//   - 200 OK: All test results succeeded (stored or updated)
//   - 207 Multi-Status: Partial success (some stored, some failed)
//   - 422 Unprocessable Entity: All test results failed validation
func determineTestResultStatusCode(response *TestResultResponse) int {
	if response.Failed == 0 {
		// All success (stored or updated)
		return http.StatusOK
	} else if response.Stored > 0 {
		// Partial success
		return http.StatusMultiStatus
	}

	// All failed
	return http.StatusUnprocessableEntity
}

// sendTestResultResponse marshals and sends the test result response to the client.
// Returns the HTTP status code for logging purposes.
//
// The response parameter should be pre-built using buildTestResultResponse().
// This function focuses solely on HTTP transmission: marshaling, setting headers, and writing the response.
func (s *Server) sendTestResultResponse(
	w http.ResponseWriter,
	r *http.Request,
	response *TestResultResponse,
) int {
	// Determine status code
	statusCode := determineTestResultStatusCode(response)

	// Marshal response (fail fast before headers)
	data, err := json.Marshal(response)
	if err != nil {
		s.logger.Error("Failed to marshal test result response",
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
		s.logger.Error("Failed to write test result response",
			slog.String("correlation_id", response.CorrelationID),
			slog.String("error", err.Error()),
		)

		return statusCode
	}

	return statusCode
}
