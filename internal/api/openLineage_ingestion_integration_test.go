// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/ingestion"
	"github.com/correlator-io/correlator/internal/storage"
)

// lineageTestServer encapsulates test server dependencies for lineage handler tests.
// Only stores fields used by helper methods (server, apiKey).
// Cleanup dependencies (keyStore, lineageStore, testDB) are captured in t.Cleanup closures.
type lineageTestServer struct {
	server *Server
	apiKey string
}

// setupLineageTestServer creates a fully configured test server with all dependencies.
// This helper eliminates ~100 lines of duplicated setup code per test.
func setupLineageTestServer(ctx context.Context, t *testing.T) *lineageTestServer {
	t.Helper()

	// Setup database with migrations
	testDB := setupTestDatabase(ctx, t)
	storageConn := &storage.Connection{DB: testDB.connection}

	// Create stores
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour) //nolint:contextcheck
	require.NoError(t, err, "Failed to create lineage store")

	// Create and register API key
	apiKey, err := storage.GenerateAPIKey("test-plugin")
	require.NoError(t, err, "Failed to generate API key")

	err = keyStore.Add(ctx, &storage.APIKey{
		ID:          "test-key-id",
		Key:         apiKey,
		PluginID:    "test-plugin",
		Name:        "Test Plugin",
		Permissions: []string{"lineage:write"},
		CreatedAt:   time.Now(),
		Active:      true,
	})
	require.NoError(t, err, "Failed to add API key")

	// Create server
	config := &ServerConfig{
		Port:            8080,
		Host:            "localhost",
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		ShutdownTimeout: 30 * time.Second,
		MaxRequestSize:  defaultMaxRequestSize,
	}
	server := NewServer(config, keyStore, nil, lineageStore)

	// Register cleanup (closure captures keyStore and testDB)
	t.Cleanup(func() {
		_ = keyStore.Close()
		_ = testDB.connection.Close()
		_ = testcontainers.TerminateContainer(testDB.container)
	})

	return &lineageTestServer{
		server: server,
		apiKey: apiKey,
	}
}

// createValidTestEvent creates a complete, valid OpenLineage event for testing.
// Includes all required fields (SchemaURL, Inputs, Outputs) to pass validation.
// Uses UUID v5 (deterministic) for run IDs to match database schema expectations.
func createValidTestEvent(runID string, eventType ingestion.EventType, eventTime time.Time) ingestion.RunEvent {
	// Generate deterministic UUID v5 from string run ID
	// This matches the approach in lineage_store_integration_test.go
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // DNS namespace
	runUUID := uuid.NewSHA1(namespace, []byte(runID)).String()

	return ingestion.RunEvent{
		EventType: eventType,
		EventTime: eventTime,
		Run: ingestion.Run{
			ID:     runUUID,
			Facets: ingestion.Facets{}, // Initialize (not nil)
		},
		Job: ingestion.Job{
			Namespace: "default",
			Name:      "test-job",
			Facets:    ingestion.Facets{}, // Initialize (not nil)
		},
		Producer:  "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Inputs:    []ingestion.Dataset{}, // Empty array (NOT nil!)
		Outputs:   []ingestion.Dataset{}, // Empty array (NOT nil!)
	}
}

// postLineageEvents is a helper to POST events to the lineage endpoint.
func (ts *lineageTestServer) postLineageEvents(t *testing.T, events []ingestion.RunEvent) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(events)
	require.NoError(t, err, "Failed to marshal events")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	return rr
}

// validateLineageResponse validates response structure, headers, and parses the response.
// Returns parsed LineageResponse for further assertions.
// Note: 422 responses can be either RFC 7807 (sequence errors) or LineageResponse (validation errors).
func validateLineageResponse(t *testing.T, rr *httptest.ResponseRecorder, expectedStatus int) *LineageResponse {
	t.Helper()

	// Check status code
	assert.Equal(t, expectedStatus, rr.Code, "Response body: %s", rr.Body.String())

	// Check Content-Type header to determine response format
	contentType := rr.Header().Get("Content-Type")

	// RFC 7807 error response (request-level errors)
	if contentType == contentTypeProblemJSON {
		return nil // Don't parse as LineageResponse
	}

	// Must be application/json for LineageResponse
	assert.Equal(t, "application/json", contentType, "Expected JSON response")

	// Parse response
	var response LineageResponse

	err := json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err, "Failed to parse response JSON")

	// Validate required fields
	assert.NotEmpty(t, response.CorrelationID, "Missing correlation_id")
	assert.NotEmpty(t, response.Timestamp, "Missing timestamp")
	assert.NotNil(t, response.Results, "Missing results array")

	return &response
}

// validateRFC7807Response validates RFC 7807 Problem Details error response structure.
// This validates the API contract for error responses.
func validateRFC7807Response(t *testing.T, rr *httptest.ResponseRecorder, expectedStatus int) {
	t.Helper()

	// Check status code
	assert.Equal(t, expectedStatus, rr.Code, "Response body: %s", rr.Body.String())

	// Check Content-Type header
	assert.Equal( //nolint:testifylint
		t,
		contentTypeProblemJSON,
		rr.Header().Get("Content-Type"),
		"Expected RFC 7807 Content-Type",
	)

	// Parse RFC 7807 structure
	var problem ProblemDetail

	err := json.Unmarshal(rr.Body.Bytes(), &problem)
	require.NoError(t, err, "Failed to parse RFC 7807 response")

	// Validate required RFC 7807 fields
	assert.Equal(t, expectedStatus, problem.Status, "Status mismatch in problem detail")
	assert.NotEmpty(t, problem.Type, "Missing 'type' field in RFC 7807 response")
	assert.NotEmpty(t, problem.Title, "Missing 'title' field in RFC 7807 response")
	assert.NotEmpty(t, problem.Detail, "Missing 'detail' field in RFC 7807 response")
}

// TestLineageHandler_SingleEventSuccess tests successful ingestion of a single OpenLineage event.
// Expected: 200 OK with response showing 1 stored event.
func TestLineageHandler_SingleEventSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Create single event (array of 1)
	events := []ingestion.RunEvent{
		createValidTestEvent("test-run-1", ingestion.EventTypeStart, time.Now()),
	}

	rr := ts.postLineageEvents(t, events)

	// Validate response
	response := validateLineageResponse(t, rr, http.StatusOK)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 1, response.Stored, "Expected 1 stored event")
	assert.Equal(t, 0, response.Duplicates, "Expected 0 duplicates")
	assert.Equal(t, 0, response.Failed, "Expected 0 failed")
	assert.Len(t, response.Results, 1, "Expected 1 result")
	assert.Equal(t, 200, response.Results[0].Status, "Expected result status 200")
	assert.Equal(t, "stored", response.Results[0].Message, "Expected 'stored' message")
}

// TestLineageHandler_BatchAllSuccess tests successful ingestion of multiple events.
// Expected: 200 OK with response showing all events stored.
func TestLineageHandler_BatchAllSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Create batch of 3 events
	now := time.Now()
	events := []ingestion.RunEvent{
		createValidTestEvent("run-1", ingestion.EventTypeStart, now),
		createValidTestEvent("run-2", ingestion.EventTypeStart, now),
		createValidTestEvent("run-3", ingestion.EventTypeStart, now),
	}

	rr := ts.postLineageEvents(t, events)

	// Validate response
	response := validateLineageResponse(t, rr, http.StatusOK)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 3, response.Stored, "Expected 3 stored events")
	assert.Equal(t, 0, response.Duplicates, "Expected 0 duplicates")
	assert.Equal(t, 0, response.Failed, "Expected 0 failed")
	assert.Len(t, response.Results, 3, "Expected 3 results")
}

// TestLineageHandler_BatchPartialSuccess tests batch with mixed success/failure.
// Expected: 207 Multi-Status with detailed per-event results.
func TestLineageHandler_BatchPartialSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Batch: 2 valid + 1 invalid (missing job.name)
	now := time.Now()
	validEvent1 := createValidTestEvent("run-1", ingestion.EventTypeStart, now)
	invalidEvent := createValidTestEvent("run-2", ingestion.EventTypeStart, now)
	invalidEvent.Job.Name = "" // Invalid: missing required field
	validEvent2 := createValidTestEvent("run-3", ingestion.EventTypeStart, now)

	events := []ingestion.RunEvent{validEvent1, invalidEvent, validEvent2}

	rr := ts.postLineageEvents(t, events)

	// Validate response
	response := validateLineageResponse(t, rr, http.StatusMultiStatus)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 2, response.Stored, "Expected 2 stored events")
	assert.Equal(t, 0, response.Duplicates, "Expected 0 duplicates")
	assert.Equal(t, 1, response.Failed, "Expected 1 failed event")
	assert.Len(t, response.Results, 3, "Expected 3 results")

	// Check second result is failure with validation error about job name
	assert.Equal(t, 1, response.Results[1].Index, "Expected failure at index 1")
	assert.Equal(t, 422, response.Results[1].Status, "Expected 422 for invalid event")
	assert.NotEmpty(t, response.Results[1].Error, "Expected error message")
	assert.Contains(
		t, response.Results[1].Error, "job.name", "Error should mention missing job name",
	)
}

// TestLineageHandler_BatchAllRejected tests batch where all events fail validation.
// Expected: 422 Unprocessable Entity with per-event error details.
func TestLineageHandler_BatchAllRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Batch: all invalid (missing required fields)
	now := time.Now()
	event1 := createValidTestEvent("run-1", ingestion.EventTypeStart, now)
	event1.EventTime = time.Time{} // Invalid: zero time

	event2 := createValidTestEvent("run-2", ingestion.EventTypeStart, now)
	event2.Job.Name = "" // Invalid: missing name

	event3 := createValidTestEvent("run-3", ingestion.EventTypeStart, now)
	event3.EventType = "" // Invalid: missing event type

	events := []ingestion.RunEvent{event1, event2, event3}

	rr := ts.postLineageEvents(t, events)

	// Validate response
	response := validateLineageResponse(t, rr, http.StatusUnprocessableEntity)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 0, response.Stored, "Expected 0 stored events")
	assert.Equal(t, 0, response.Duplicates, "Expected 0 duplicates")
	assert.Equal(t, 3, response.Failed, "Expected 3 failed events")
	assert.Len(t, response.Results, 3, "Expected 3 results")

	// All results should have errors with key phrases
	for i, result := range response.Results {
		assert.Equal(t, 422, result.Status, "Expected 422 for result %d", i)
		assert.NotEmpty(t, result.Error, "Expected error message for result %d", i)
	}

	// Verify specific validation errors mention the relevant fields
	assert.Contains(t, response.Results[0].Error, "eventTime", "Error should mention eventTime")
	assert.Contains(t, response.Results[1].Error, "job.name", "Error should mention job name")
	assert.Contains(t, response.Results[2].Error, "eventType", "Error should mention eventType")
}

// TestLineageHandler_DuplicateEvent tests idempotency - same event twice returns 200 OK both times.
// Expected: First request stores, second request returns duplicate (both 200 OK).
func TestLineageHandler_DuplicateEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Same event (fixed eventTime for consistent idempotency key)
	eventTime := time.Now()
	events := []ingestion.RunEvent{
		createValidTestEvent("duplicate-run", ingestion.EventTypeStart, eventTime),
	}

	// First request
	rr1 := ts.postLineageEvents(t, events)
	response1 := validateLineageResponse(t, rr1, http.StatusOK)
	require.NotNil(t, response1, "Failed to validate response")

	assert.Equal(t, 1, response1.Stored, "First request: Expected 1 stored")
	assert.Equal(t, 0, response1.Duplicates, "First request: Expected 0 duplicates")

	// Second request (duplicate)
	rr2 := ts.postLineageEvents(t, events)
	response2 := validateLineageResponse(t, rr2, http.StatusOK)
	require.NotNil(t, response2, "Failed to validate response")

	assert.Equal(t, 0, response2.Stored, "Second request: Expected 0 stored")
	assert.Equal(t, 1, response2.Duplicates, "Second request: Expected 1 duplicate")
	assert.Equal(t, "duplicate", response2.Results[0].Message, "Expected 'duplicate' message")
}

// TestLineageHandler_RequestTooLarge tests request size limit enforcement.
// Expected: 413 Payload Too Large.
func TestLineageHandler_RequestTooLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Create request larger than MaxRequestSize
	largeBody := make([]byte, defaultMaxRequestSize+1)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(largeBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure
	validateRFC7807Response(t, rr, http.StatusRequestEntityTooLarge)
}

// TestLineageHandler_MissingAuth tests authentication requirement.
// Expected: 401 Unauthorized (middleware handles this).
func TestLineageHandler_MissingAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	events := []ingestion.RunEvent{
		createValidTestEvent("run-1", ingestion.EventTypeStart, time.Now()),
	}

	body, err := json.Marshal(events)
	require.NoError(t, err, "Failed to marshal events")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	// NO X-API-Key header

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure
	validateRFC7807Response(t, rr, http.StatusUnauthorized)
}

// TestLineageHandler_InvalidJSON tests malformed JSON handling.
// Expected: 400 Bad Request.
func TestLineageHandler_InvalidJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Malformed JSON
	body := []byte(`{"invalid json syntax`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure
	validateRFC7807Response(t, rr, http.StatusBadRequest)
}

// TestLineageHandler_InvalidStateSequence tests lifecycle validation for batches.
// Expected: 422 Unprocessable Entity for invalid state transitions (e.g., duplicate START).
func TestLineageHandler_InvalidStateSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Batch with invalid sequence: START → START (duplicate START for same runId)
	now := time.Now()
	events := []ingestion.RunEvent{
		createValidTestEvent("same-run", ingestion.EventTypeStart, now),
		createValidTestEvent("same-run", ingestion.EventTypeStart, now.Add(1*time.Second)), // Duplicate START!
		createValidTestEvent("same-run", ingestion.EventTypeComplete, now.Add(2*time.Second)),
	}

	body, err := json.Marshal(events)
	require.NoError(t, err, "Failed to marshal events")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response with key phrase about duplicate START
	validateRFC7807Response(t, rr, http.StatusUnprocessableEntity)
	bodyStr := rr.Body.String()
	assert.Contains(t, bodyStr, "duplicate", "Error should mention duplicate event")
}

// TestLineageHandler_EmptyBatch tests empty event array handling.
// Expected: 400 Bad Request.
func TestLineageHandler_EmptyBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	body := []byte(`[]`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure
	validateRFC7807Response(t, rr, http.StatusBadRequest)
}

// TestLineageHandler_WrongContentType tests Content-Type validation.
// Expected: 415 Unsupported Media Type.
func TestLineageHandler_WrongContentType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	events := []ingestion.RunEvent{
		createValidTestEvent("run-1", ingestion.EventTypeStart, time.Now()),
	}

	body, err := json.Marshal(events)
	require.NoError(t, err, "Failed to marshal events")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "text/plain") // Wrong Content-Type!
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure
	validateRFC7807Response(t, rr, http.StatusUnsupportedMediaType)
}

// TestLineageHandler_EmptyBody tests empty request body handling.
// Expected: 400 Bad Request.
func TestLineageHandler_EmptyBody(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader([]byte{}))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure
	validateRFC7807Response(t, rr, http.StatusBadRequest)
}

// TestLineageHandler_BatchOutOfOrderSorting tests that lifecycle.ValidateEventSequence() sorts events.
// Expected: Events arrive out-of-order → sorted by lifecycle → 200 OK.
func TestLineageHandler_BatchOutOfOrderSorting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Events arrive OUT OF ORDER: [COMPLETE, START, RUNNING]
	baseTime := time.Now()
	events := []ingestion.RunEvent{
		createValidTestEvent("run-1", ingestion.EventTypeComplete, baseTime.Add(10*time.Minute)), // Latest
		createValidTestEvent("run-1", ingestion.EventTypeStart, baseTime),                        // Earliest
		createValidTestEvent("run-1", ingestion.EventTypeRunning, baseTime.Add(5*time.Minute)),   // Middle
	}

	rr := ts.postLineageEvents(t, events)

	// Expected: lifecycle.ValidateEventSequence() sorts them → [START, RUNNING, COMPLETE]
	// Result: 200 OK (valid sequence after sorting)
	response := validateLineageResponse(t, rr, http.StatusOK)
	assert.Equal(t, 3, response.Stored, "Expected 3 stored events after sorting")
	assert.Equal(t, 0, response.Failed, "Expected 0 failed (valid sequence after sort)")
}

// TestLineageHandler_RealDBTEvent tests ingestion of a real dbt OpenLineage event from testdata.
// This catches real-world edge cases (complex facets, nested structures, special characters).
// Expected: 200 OK with event stored successfully.
func TestLineageHandler_RealDBTEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Load real dbt event from testdata (relative path from api package)
	eventData, err := os.ReadFile("../../internal/ingestion/testdata/dbt_complete_event.json")
	require.NoError(t, err, "Failed to read dbt_complete_event.json fixture")

	var event ingestion.RunEvent

	err = json.Unmarshal(eventData, &event)
	require.NoError(t, err, "Failed to parse dbt event")

	events := []ingestion.RunEvent{event}

	rr := ts.postLineageEvents(t, events)

	// Validate response
	response := validateLineageResponse(t, rr, http.StatusOK)
	assert.Equal(t, 1, response.Stored, "Expected real dbt event to be stored")
	assert.Equal(t, 0, response.Failed, "Expected real dbt event to pass validation")
}

// TestLineageHandler_BackwardTransition tests that backward state transitions are rejected.
// Expected: 422 Unprocessable Entity for RUNNING → START.
func TestLineageHandler_BackwardTransition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Batch with backward transition: START → RUNNING → START (backward!)
	now := time.Now()
	events := []ingestion.RunEvent{
		createValidTestEvent("run-1", ingestion.EventTypeStart, now),
		createValidTestEvent("run-1", ingestion.EventTypeRunning, now.Add(1*time.Second)),
		createValidTestEvent("run-1", ingestion.EventTypeStart, now.Add(2*time.Second)), // Backward transition!
	}

	body, err := json.Marshal(events)
	require.NoError(t, err, "Failed to marshal events")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response with key phrase about transition
	validateRFC7807Response(t, rr, http.StatusUnprocessableEntity)
	bodyStr := rr.Body.String()
	assert.Contains(t, bodyStr, "transition", "Error should mention invalid transition")
}

// TestLineageHandler_TerminalStateMutation tests that terminal states cannot be mutated.
// Expected: 422 Unprocessable Entity for COMPLETE → RUNNING.
func TestLineageHandler_TerminalStateMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Batch with terminal state mutation: START → COMPLETE → RUNNING (after terminal!)
	now := time.Now()
	events := []ingestion.RunEvent{
		createValidTestEvent("run-2", ingestion.EventTypeStart, now),
		createValidTestEvent("run-2", ingestion.EventTypeComplete, now.Add(1*time.Second)),
		createValidTestEvent("run-2", ingestion.EventTypeRunning, now.Add(2*time.Second)), // After terminal state!
	}

	body, err := json.Marshal(events)
	require.NoError(t, err, "Failed to marshal events")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response with key phrase about terminal state
	validateRFC7807Response(t, rr, http.StatusUnprocessableEntity)
	bodyStr := rr.Body.String()
	// Should mention either "terminal" or "COMPLETE" or "immutable"
	hasTerminalError := assert.Contains(
		t, bodyStr, "terminal", "Expected error about terminal state") ||
		assert.Contains(t, bodyStr, "COMPLETE", "Expected error about COMPLETE state") ||
		assert.Contains(t, bodyStr, "immutable", "Expected error about immutability")
	assert.True(t, hasTerminalError, "Expected error about terminal state mutation")
}

// TestLineageHandler_InvalidMethod tests that only POST is allowed.
// Expected: 404 Not Found for GET (method-specific route pattern doesn't match).
// Note: Go 1.22+ ServeMux with "POST /path" returns 404 for GET /path (not 405),
// because GET /path doesn't match any registered route pattern.
func TestLineageHandler_InvalidMethod(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupLineageTestServer(ctx, t)

	// Try GET (should not match route pattern)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/lineage/events", nil)
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure (404 from catch-all handler)
	validateRFC7807Response(t, rr, http.StatusNotFound)
}
