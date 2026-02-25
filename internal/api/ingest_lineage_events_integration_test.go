// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/canonicalization"
	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/storage"
)

// testServer encapsulates test server dependencies for lineage integration tests.
// Only stores fields used by helper methods - cleanup dependencies captured in t.Cleanup closures.
type testServer struct {
	server       *Server
	apiKey       string
	db           *sql.DB               // For database verification helpers
	lineageStore *storage.LineageStore // For calling InitResolvedDatasets in tests
}

// setupTestServer creates a fully configured test server with all dependencies.
// This helper eliminates duplicated setup code per test.
func setupTestServer(ctx context.Context, t *testing.T) *testServer {
	t.Helper()

	// Setup database with migrations (uses shared helper from config package)
	testDB := config.SetupTestDatabase(ctx, t)
	storageConn := &storage.Connection{DB: testDB.Connection}

	// Create stores
	keyStore, err := storage.NewPersistentKeyStore(storageConn)
	require.NoError(t, err, "Failed to create key store")

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour) //nolint:contextcheck
	require.NoError(t, err, "Failed to create lineage store")

	// Create and register API key
	testAPIKey, err := storage.GenerateAPIKey("test-plugin")
	require.NoError(t, err, "Failed to generate API key")

	err = keyStore.Add(ctx, &storage.APIKey{
		ID:          "test-key-id",
		Key:         testAPIKey,
		PluginID:    "test-plugin",
		Name:        "Test Plugin",
		Permissions: []string{"lineage:write", "lineage:read"},
		CreatedAt:   time.Now(),
		Active:      true,
	})
	require.NoError(t, err, "Failed to add API key")

	// Create server config
	cfg := &ServerConfig{
		Port:               8080,
		Host:               "localhost",
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		LogLevel:           slog.LevelInfo,
		MaxRequestSize:     defaultMaxRequestSize,
		CORSAllowedOrigins: []string{"*"},
		CORSAllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		CORSAllowedHeaders: []string{"Content-Type", "Authorization", "X-Correlation-ID", "X-API-Key"},
		CORSMaxAge:         86400,
	}

	// Create server with dependencies (no rate limiter for lineage tests)
	// lineageStore implements both ingestion.Store and correlation.Store
	server := NewServer(cfg, keyStore, nil, lineageStore, lineageStore)

	// Register cleanup (closure captures dependencies)
	t.Cleanup(func() {
		_ = keyStore.Close()
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	return &testServer{
		server:       server,
		apiKey:       testAPIKey,
		db:           testDB.Connection,
		lineageStore: lineageStore,
	}
}

// postLineageEvents is a helper to POST OpenLineage events to the lineage endpoint.
// Accepts API contract types (LineageEvent), not domain types (ingestion.RunEvent).
func (ts *testServer) postLineageEvents(t *testing.T, events []LineageEvent) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(events)
	require.NoError(t, err, "Failed to marshal lineage events")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	return rr
}

// createValidLineageEvent creates a valid API request for testing.
// This is a helper for tests - not used by production code.
// The returned request has all required fields populated for successful validation.
//
// Note: This creates API contract types (LineageEvent), not domain types (ingestion.RunEvent).
// The handler will map these API types to domain types internally.
// Includes 1 input and 1 output dataset to test the full lineage pipeline.
func createValidLineageEvent(runID string, eventType string, eventTime time.Time) LineageEvent {
	// Generate deterministic UUID v5 from string run ID
	// This matches the approach in lineage_store_integration_test.go
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // DNS namespace
	runUUID := uuid.NewSHA1(namespace, []byte(runID)).String()

	return LineageEvent{
		EventType: eventType,
		EventTime: eventTime,
		Run: Run{
			ID:     runUUID,
			Facets: map[string]interface{}{}, // Initialize (not nil)
		},
		Job: Job{
			Namespace: "default",
			Name:      "test-job",
			Facets:    map[string]interface{}{}, // Initialize (not nil)
		},
		Producer:  "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Inputs: []Dataset{
			{
				Namespace:    "postgres://prod-db:5432",
				Name:         "public.orders",
				Facets:       map[string]interface{}{},
				InputFacets:  map[string]interface{}{},
				OutputFacets: map[string]interface{}{},
			},
		},
		Outputs: []Dataset{
			{
				Namespace:    "s3://data-lake",
				Name:         "/analytics/orders.parquet",
				Facets:       map[string]interface{}{},
				InputFacets:  map[string]interface{}{},
				OutputFacets: map[string]interface{}{},
			},
		},
	}
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

	// Validate required fields (OpenLineage format)
	assert.NotNil(t, response.FailedEvents, "Failed_events should be array (not nil)")

	// Validate additional required fields (Correlator)
	assert.NotEmpty(t, response.CorrelationID, "Missing correlation_id")
	assert.NotEmpty(t, response.Timestamp, "Missing timestamp")

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

// ============================================================================
// Database Verification Helpers - End-to-end data integrity
// ============================================================================

// countStoredEvents counts lineage events in the database by job_run_id.
// Used for idempotency tests where count matters (expect 1 after duplicate).
func (ts *testServer) countStoredEvents(ctx context.Context, t *testing.T, jobRunID string) int {
	t.Helper()

	var count int

	query := "SELECT COUNT(*) FROM job_runs WHERE job_run_id = $1"

	err := ts.db.QueryRowContext(ctx, query, jobRunID).Scan(&count)
	require.NoError(t, err, "Failed to count job_runs")

	return count
}

// assertEventNotStored verifies an invalid event was NOT persisted to the database.
// Used in negative tests where validation should prevent storage.
func (ts *testServer) assertEventNotStored(ctx context.Context, t *testing.T, jobRunID string) {
	t.Helper()

	var count int

	query := "SELECT COUNT(*) FROM job_runs WHERE job_run_id = $1"

	err := ts.db.QueryRowContext(ctx, query, jobRunID).Scan(&count)
	require.NoError(t, err, "Failed to query job_runs")

	assert.Equal(t, 0, count, "Invalid event should not be stored: job_run_id=%s", jobRunID)
}

// verifyEventStored verifies a lineage event was persisted to the database.
// Checks critical fields to ensure end-to-end data integrity (API → Domain → Storage).
// Expects 2 edges per job_run_id (1 input + 1 output, deduplicated by UPSERT).
//
// This catches bugs where:
//   - Storage layer silently fails but returns success
//   - Transaction commits but data not written
//   - Canonical ID format is incorrect
//   - Field mapping between layers is broken
//   - upsertDatasetsAndEdges() fails silently (lineage_edges not created)
func (ts *testServer) verifyEventStored(
	ctx context.Context,
	t *testing.T,
	jobRunID string,
	expectedEventType string,
) {
	t.Helper()

	// 1. Verify job_runs table
	var (
		eventType    string
		jobName      string
		jobNamespace string
	)

	query := `
		SELECT event_type, job_name, job_namespace
		FROM job_runs
		WHERE job_run_id = $1
	`

	err := ts.db.QueryRowContext(ctx, query, jobRunID).Scan(
		&eventType,
		&jobName,
		&jobNamespace,
	)

	require.NoError(t, err, "Event should be stored in database: job_run_id=%s", jobRunID)
	assert.Equal(t, expectedEventType, eventType, "Event type should match")
	assert.NotEmpty(t, jobName, "Job name should be persisted")
	assert.NotEmpty(t, jobNamespace, "Job namespace should be persisted")

	// 2. Verify lineage_edges table (tests upsertDatasetsAndEdges)
	// Expect 2 edges: 1 input + 1 output (deduplicated by UPSERT for same job_run_id)
	var edgeCount int

	edgeQuery := "SELECT COUNT(*) FROM lineage_edges WHERE job_run_id = $1"
	err = ts.db.QueryRowContext(ctx, edgeQuery, jobRunID).Scan(&edgeCount)
	require.NoError(t, err, "Failed to query lineage_edges")

	assert.Equal(t, 2, edgeCount, "Expected 2 lineage edges (1 input + 1 output)")

	// 3. Verify datasets table (tests upsertDatasetsAndEdges)
	var datasetCount int

	datasetQuery := `
		SELECT COUNT(DISTINCT d.dataset_urn)
		FROM datasets d
		WHERE d.dataset_urn IN (
			SELECT dataset_urn FROM lineage_edges WHERE job_run_id = $1
		)
	`
	err = ts.db.QueryRowContext(ctx, datasetQuery, jobRunID).Scan(&datasetCount)
	require.NoError(t, err, "Failed to query datasets")

	// Test events have 2 distinct datasets (1 input + 1 output)
	assert.Equal(t, 2, datasetCount, "Expected 2 distinct datasets (1 input + 1 output)")
}

// TestLineageHandler_SingleEventSuccess tests successful ingestion of a single OpenLineage event.
// Expected: 200 OK with response showing 1 stored event.
func TestLineageHandler_SingleEventSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create single event (array of 1)
	event := createValidLineageEvent("test-run-1", "START", time.Now())
	events := []LineageEvent{event}

	// Generate expected job_run_id (format: "tool:runID")
	jobRunID := canonicalization.GenerateJobRunID(event.Job.Namespace, event.Run.ID)

	rr := ts.postLineageEvents(t, events)

	// Validate API response (OpenLineage format)
	response := validateLineageResponse(t, rr, http.StatusOK)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 1, response.Summary.Received, "Expected 1 received event")
	assert.Equal(t, 1, response.Summary.Successful, "Expected 1 successful event")
	assert.Equal(t, 0, response.Summary.Failed, "Expected 0 failed")
	assert.Empty(t, response.FailedEvents, "Expected no failed events")

	// Verify database state (end-to-end verification)
	ts.verifyEventStored(ctx, t, jobRunID, "START")
}

// TestLineageHandler_BatchAllSuccess tests successful ingestion of multiple events.
// Expected: 200 OK with response showing all events stored.
func TestLineageHandler_BatchAllSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create batch of 3 events
	now := time.Now()
	event1 := createValidLineageEvent("run-1", "START", now)
	event2 := createValidLineageEvent("run-2", "START", now)
	event3 := createValidLineageEvent("run-3", "START", now)

	events := []LineageEvent{event1, event2, event3}

	// Generate expected job_run_ids
	jobRunID1 := canonicalization.GenerateJobRunID(event1.Job.Namespace, event1.Run.ID)
	jobRunID2 := canonicalization.GenerateJobRunID(event2.Job.Namespace, event2.Run.ID)
	jobRunID3 := canonicalization.GenerateJobRunID(event3.Job.Namespace, event3.Run.ID)

	rr := ts.postLineageEvents(t, events)

	// Validate API response (OpenLineage format)
	response := validateLineageResponse(t, rr, http.StatusOK)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 3, response.Summary.Received, "Expected 3 received events")
	assert.Equal(t, 3, response.Summary.Successful, "Expected 3 successful events")
	assert.Equal(t, 0, response.Summary.Failed, "Expected 0 failed")
	assert.Empty(t, response.FailedEvents, "Expected no failed events")

	// Verify database state (end-to-end verification for all 3 events)
	ts.verifyEventStored(ctx, t, jobRunID1, "START")
	ts.verifyEventStored(ctx, t, jobRunID2, "START")
	ts.verifyEventStored(ctx, t, jobRunID3, "START")
}

// TestLineageHandler_BatchPartialSuccess tests batch with mixed success/failure.
// Expected: 207 Multi-Status with detailed per-event results.
func TestLineageHandler_BatchPartialSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Batch: 2 valid + 1 invalid (missing job.name)
	now := time.Now()
	validEvent1 := createValidLineageEvent("run-1", "START", now)
	invalidEvent := createValidLineageEvent("run-2", "START", now)
	invalidEvent.Job.Name = "" // Invalid: missing required field
	validEvent2 := createValidLineageEvent("run-3", "START", now)

	events := []LineageEvent{validEvent1, invalidEvent, validEvent2}

	// Generate expected job_run_ids for valid events only
	jobRunID1 := canonicalization.GenerateJobRunID(validEvent1.Job.Namespace, validEvent1.Run.ID)
	jobRunID3 := canonicalization.GenerateJobRunID(validEvent2.Job.Namespace, validEvent2.Run.ID)

	rr := ts.postLineageEvents(t, events)

	// Validate API response (OpenLineage format)
	response := validateLineageResponse(t, rr, http.StatusMultiStatus)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, "partial_success", response.Status, "Expected partial success status")
	assert.Equal(t, 3, response.Summary.Received, "Expected 3 received events")
	assert.Equal(t, 2, response.Summary.Successful, "Expected 2 successful events")
	assert.Equal(t, 1, response.Summary.Failed, "Expected 1 failed event")
	assert.Len(t, response.FailedEvents, 1, "Expected 1 failed event entry")

	// Check second event is failure with validation error about job name
	failedEvent := response.FailedEvents[0]
	assert.Equal(t, 1, failedEvent.Index, "Expected failure at index 1")
	assert.Contains(t, failedEvent.Reason, "job.name", "Error should mention missing job name")
	assert.False(t, failedEvent.Retriable, "Validation errors are non-retriable")

	// Verify database state (end-to-end verification - only 2 events stored, not 3!)
	ts.verifyEventStored(ctx, t, jobRunID1, "START")
	ts.verifyEventStored(ctx, t, jobRunID3, "START")

	// Invalid event should NOT be in database
	ts.assertEventNotStored(
		ctx, t, canonicalization.GenerateJobRunID(invalidEvent.Job.Namespace, invalidEvent.Run.ID),
	)
}

// TestLineageHandler_BatchAllRejected tests batch where all events fail validation.
// Expected: 422 Unprocessable Entity with per-event error details.
func TestLineageHandler_BatchAllRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Batch: all invalid (missing required fields)
	now := time.Now()
	event1 := createValidLineageEvent("run-1", "START", now)
	event1.EventTime = time.Time{} // Invalid: zero time

	event2 := createValidLineageEvent("run-2", "START", now)
	event2.Job.Name = "" // Invalid: missing name

	event3 := createValidLineageEvent("run-3", "START", now)
	event3.EventType = "" // Invalid: missing event type

	events := []LineageEvent{event1, event2, event3}

	rr := ts.postLineageEvents(t, events)

	// Validate response (OpenLineage format)
	response := validateLineageResponse(t, rr, http.StatusUnprocessableEntity)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, "error", response.Status, "Expected status 'error' (all failed)")
	assert.Equal(t, 3, response.Summary.Received, "Expected 3 received events")
	assert.Equal(t, 0, response.Summary.Successful, "Expected 0 successful events")
	assert.Equal(t, 3, response.Summary.Failed, "Expected 3 failed events")
	assert.Len(t, response.FailedEvents, 3, "Expected 3 failed event entries")

	// All failed events should have reasons
	for i, failedEvent := range response.FailedEvents {
		assert.Equal(t, i, failedEvent.Index, "Expected failure at index %d", i)
		assert.NotEmpty(t, failedEvent.Reason, "Expected reason for failed event %d", i)
		assert.False(t, failedEvent.Retriable, "Validation errors are non-retriable")
	}

	// Verify specific validation errors mention the relevant fields
	assert.Contains(t, response.FailedEvents[0].Reason, "eventTime", "Error should mention eventTime")
	assert.Contains(t, response.FailedEvents[1].Reason, "job.name", "Error should mention job name")
	assert.Contains(t, response.FailedEvents[2].Reason, "eventType", "Error should mention eventType")

	// Invalid event should NOT be in database
	invalidEvents := []LineageEvent{event1, event2, event3}
	for _, invalidEvent := range invalidEvents {
		ts.assertEventNotStored(
			ctx, t, canonicalization.GenerateJobRunID(invalidEvent.Job.Namespace, invalidEvent.Run.ID),
		)
	}
}

// TestLineageHandler_DuplicateEvent tests idempotency - same event twice returns 200 OK both times.
// Expected: First request stores, second request returns duplicate (both 200 OK).
func TestLineageHandler_DuplicateEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Same event (fixed eventTime for consistent idempotency key)
	eventTime := time.Now()
	event := createValidLineageEvent("duplicate-run", "START", eventTime)
	events := []LineageEvent{event}

	// Generate expected job_run_id
	jobRunID := canonicalization.GenerateJobRunID(event.Job.Namespace, event.Run.ID)

	// First request
	rr1 := ts.postLineageEvents(t, events)
	response1 := validateLineageResponse(t, rr1, http.StatusOK)
	require.NotNil(t, response1, "Failed to validate response")

	assert.Equal(t, "success", response1.Status, "First request: Expected status 'success'")
	assert.Equal(t, 1, response1.Summary.Successful, "First request: Expected 1 successful")
	assert.Equal(t, 0, response1.Summary.Failed, "First request: Expected 0 failed")

	// Verify database state after first request
	count1 := ts.countStoredEvents(ctx, t, jobRunID)
	assert.Equal(t, 1, count1, "First request: Should store 1 event")

	// Second request (duplicate - OpenLineage considers duplicates as success)
	rr2 := ts.postLineageEvents(t, events)
	response2 := validateLineageResponse(t, rr2, http.StatusOK)
	require.NotNil(t, response2, "Failed to validate response")

	assert.Equal(t, "success", response2.Status, "Second request: Expected status 'success'")
	assert.Equal(t, 1, response2.Summary.Successful, "Second request: Duplicate is success (idempotent)")
	assert.Equal(t, 0, response2.Summary.Failed, "Second request: Expected 0 failed")

	// Verify database state after second request (should still be 1, not 2)
	count2 := ts.countStoredEvents(ctx, t, jobRunID)
	assert.Equal(t, 1, count2, "Second request: Count should stay at 1 (idempotency)")

	// Verify database state (end-to-end verification - only 1 event stored, not 2!)
	ts.verifyEventStored(ctx, t, jobRunID, "START")
}

// TestLineageHandler_RequestTooLarge tests request size limit enforcement.
// Expected: 413 Payload Too Large.
func TestLineageHandler_RequestTooLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

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
	ts := setupTestServer(ctx, t)

	events := []LineageEvent{
		createValidLineageEvent("run-1", "START", time.Now()),
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

	ts.assertEventNotStored(
		ctx, t, canonicalization.GenerateJobRunID(events[0].Job.Namespace, events[0].Run.ID),
	)
}

// TestLineageHandler_InvalidJSON tests malformed JSON handling.
// Expected: 400 Bad Request.
func TestLineageHandler_InvalidJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

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

// TestLineageHandler_EmptyBatch tests empty event array handling.
// Expected: 400 Bad Request.
func TestLineageHandler_EmptyBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

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
	ts := setupTestServer(ctx, t)

	events := []LineageEvent{
		createValidLineageEvent("run-1", "START", time.Now()),
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
	ts := setupTestServer(ctx, t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/lineage/events", bytes.NewReader([]byte{}))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure
	validateRFC7807Response(t, rr, http.StatusBadRequest)
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
	ts := setupTestServer(ctx, t)

	// Try GET (should not match route pattern)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/lineage/events", nil)
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate RFC 7807 error response structure (404 from catch-all handler)
	validateRFC7807Response(t, rr, http.StatusNotFound)
}

// TestLineageHandler_RealDBTEvent tests ingestion of a real dbt OpenLineage event from testdata.
// This catches real-world edge cases (complex facets, nested structures, special characters).
// Expected: 200 OK with event stored successfully.
func TestLineageHandler_RealDBTEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Load real dbt event from testdata (relative path from api package)
	eventData, err := os.ReadFile("../../internal/ingestion/testdata/dbt_complete_event.json")
	require.NoError(t, err, "Failed to read dbt_complete_event.json fixture")

	var event LineageEvent

	err = json.Unmarshal(eventData, &event)
	require.NoError(t, err, "Failed to parse dbt event")

	events := []LineageEvent{event}

	rr := ts.postLineageEvents(t, events)

	// Validate response (OpenLineage format)
	response := validateLineageResponse(t, rr, http.StatusOK)
	require.NotNil(t, response)

	assert.Equal(t, 1, response.Summary.Received, "Expected 1 received event")
	assert.Equal(t, 1, response.Summary.Successful, "Expected real dbt event to be stored")
	assert.Equal(t, 0, response.Summary.Failed, "Expected real dbt event to pass validation")

	// expect only one stored event
	count := ts.countStoredEvents(
		ctx, t, canonicalization.GenerateJobRunID(events[0].Job.Namespace, events[0].Run.ID),
	)
	assert.Equal(t, 1, count, "expected 1 event to be stored in database")
}

// ============================================================================
// Test Batch processing
// ============================================================================

// TestLineageHandler_BatchOutOfOrderSorting tests that lifecycle.ValidateEventSequence() sorts events.
// Expected: Events arrive out-of-order → sorted by lifecycle → 200 OK.
func TestLineageHandler_BatchOutOfOrderSorting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Events arrive OUT OF ORDER: [COMPLETE, START, RUNNING]
	baseTime := time.Now()
	events := []LineageEvent{
		createValidLineageEvent("run-1", "COMPLETE", baseTime.Add(10*time.Minute)), // Latest
		createValidLineageEvent("run-1", "START", baseTime),                        // Earliest
		createValidLineageEvent("run-1", "RUNNING", baseTime.Add(5*time.Minute)),   // Middle
	}

	rr := ts.postLineageEvents(t, events)

	// Expected: lifecycle.ValidateEventSequence() sorts them → [START, RUNNING, COMPLETE]
	// Result: 200 OK (valid sequence after sorting)
	response := validateLineageResponse(t, rr, http.StatusOK)
	require.NotNil(t, response)

	assert.Equal(t, 3, response.Summary.Received, "Expected 3 received events")
	assert.Equal(t, 3, response.Summary.Successful, "Expected 3 successful events after sorting")
	assert.Equal(t, 0, response.Summary.Failed, "Expected 0 failed (valid sequence after sort)")

	// Verify database state (end-to-end verification - only 1 event stored, not 3! + terminal state only)
	// 3 events sent for same run = 2 edges (1 input + 1 output, deduplicated by UPSERT)
	ts.verifyEventStored(
		ctx, t,
		canonicalization.GenerateJobRunID(events[0].Job.Namespace, events[0].Run.ID), "COMPLETE",
	)

	// expect only one stored event
	count := ts.countStoredEvents(
		ctx, t, canonicalization.GenerateJobRunID(events[0].Job.Namespace, events[0].Run.ID),
	)
	assert.Equal(t, 1, count, "expected 1 event to be stored in database")
}

// TestLineageHandler_InvalidStateSequence tests lifecycle validation for batches.
// Expected: 422 Unprocessable Entity for invalid state transitions (e.g., duplicate START).
func TestLineageHandler_InvalidStateSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Batch with invalid sequence: START → START (duplicate START for same runId)
	now := time.Now()
	events := []LineageEvent{
		createValidLineageEvent("same-run", "START", now),
		createValidLineageEvent("same-run", "START", now.Add(1*time.Second)), // Duplicate START!
		createValidLineageEvent("same-run", "COMPLETE", now.Add(2*time.Second)),
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

	// Invalid batch of events should NOT be in database
	ts.assertEventNotStored(
		ctx, t, canonicalization.GenerateJobRunID(events[0].Job.Namespace, events[0].Run.ID),
	)
}

// TestLineageHandler_BackwardTransition tests that backward state transitions are rejected.
// Expected: 422 Unprocessable Entity for RUNNING → START.
func TestLineageHandler_BackwardTransition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Batch with backward transition: START → RUNNING → START (backward!)
	now := time.Now()
	events := []LineageEvent{
		createValidLineageEvent("run-1", "START", now),
		createValidLineageEvent("run-1", "RUNNING", now.Add(1*time.Second)),
		createValidLineageEvent("run-1", "START", now.Add(2*time.Second)), // Backward transition!
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

	ts.assertEventNotStored(
		ctx, t, canonicalization.GenerateJobRunID(events[0].Job.Namespace, events[0].Run.ID),
	)
}

// TestLineageHandler_TerminalStateMutation tests that terminal states cannot be mutated.
// Expected: 422 Unprocessable Entity for COMPLETE → RUNNING.
func TestLineageHandler_TerminalStateMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Batch with terminal state mutation: START → COMPLETE → RUNNING (after terminal!)
	now := time.Now()
	events := []LineageEvent{
		createValidLineageEvent("run-2", "START", now),
		createValidLineageEvent("run-2", "COMPLETE", now.Add(1*time.Second)),
		createValidLineageEvent("run-2", "RUNNING", now.Add(2*time.Second)), // After terminal state!
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

	// Verify database state: NO events should be stored (invalid batch)
	ts.assertEventNotStored(
		ctx, t, canonicalization.GenerateJobRunID(events[0].Job.Namespace, events[0].Run.ID),
	)
}
