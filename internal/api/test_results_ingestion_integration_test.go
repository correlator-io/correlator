// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/storage"
)

// testServer encapsulates test server dependencies for API integration tests.
// This is the standard test fixture for all API handler integration tests.
//
// Usage:
//
//	ts := setupTestServer(ctx, t)
//	rr := ts.postTestResults(t, testResults)
//	count := ts.countTestResults(ctx, t, "test_name")
//
// Future: This will replace lineageTestServer when refactoring OpenLineage tests.
type testServer struct {
	server *Server
	apiKey string
	db     *sql.DB
}

// setupTestServer creates a fully configured test server with all dependencies.
// This uses the standard test helpers from internal/config for consistency.
func setupTestServer(ctx context.Context, t *testing.T) *testServer {
	t.Helper()

	// Setup database with migrations using standard test helper
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create storage connection
	storageConn := &storage.Connection{DB: testDB.Connection}

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
	serverConfig := &ServerConfig{
		Port:            8080,
		Host:            "localhost",
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		ShutdownTimeout: 30 * time.Second,
		MaxRequestSize:  defaultMaxRequestSize,
	}
	server := NewServer(serverConfig, keyStore, nil, lineageStore)

	// Register cleanup for stores
	t.Cleanup(func() {
		_ = keyStore.Close()
		_ = lineageStore.Close()
	})

	return &testServer{
		server: server,
		apiKey: apiKey,
		db:     testDB.Connection,
	}
}

// postTestResults is a helper to POST test results to the test results endpoint.
func (ts *testServer) postTestResults(t *testing.T, results []TestResultRequest) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(results)
	require.NoError(t, err, "Failed to marshal test results")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/test-results", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	return rr
}

// countTestResults queries the database to count test_results rows matching the given test_name.
// This verifies that test results were actually persisted to the database.
//
// Design note: This directly queries the database using the test DB connection.
// This follows the pattern from internal/storage/correlation_views_integration_test.go
// where we verify end-to-end integration by checking database state directly.
func (ts *testServer) countTestResults(ctx context.Context, t *testing.T, testName string) int {
	t.Helper()

	var count int

	query := "SELECT COUNT(*) FROM test_results WHERE test_name = $1"

	err := ts.db.QueryRowContext(ctx, query, testName).Scan(&count)
	require.NoError(t, err, "Failed to count test_results")

	return count
}

// validateTestResultResponse validates response structure and parses the response.
func validateTestResultResponse(t *testing.T, rr *httptest.ResponseRecorder, expectedStatus int) *TestResultResponse {
	t.Helper()

	// Check status code
	assert.Equal(t, expectedStatus, rr.Code, "Response body: %s", rr.Body.String())

	// Check Content-Type (might be RFC 7807 for errors)
	contentType := rr.Header().Get("Content-Type")

	if contentType == "application/problem+json" { //nolint:goconst
		return nil // RFC 7807 error response
	}

	// Must be application/json for TestResultResponse
	assert.Equal(t, "application/json", contentType, "Expected JSON response")

	// Parse response
	var response TestResultResponse

	err := json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err, "Failed to parse response JSON")

	// Validate required fields
	assert.NotEmpty(t, response.CorrelationID, "Missing correlation_id")
	assert.NotEmpty(t, response.Timestamp, "Missing timestamp")
	assert.NotNil(t, response.Results, "Missing results array")

	return &response
}

// createTestJobRun creates a job_run directly in the database.
// Returns the job_run_id that can be used in test results.
func createTestJobRun(ctx context.Context, t *testing.T, db *sql.DB, jobRunID string) {
	t.Helper()

	now := time.Now()

	_, err := db.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, event_type, event_time, started_at, completed_at, current_state)
		VALUES ($1, gen_random_uuid(), $2, $3, $4, $5, $6, $7, $8)
	`, jobRunID, "test-job", "test", "COMPLETE", now, now, now, "COMPLETE")
	require.NoError(t, err, "Failed to create job_run")
}

// createTestDataset creates a dataset directly in the database.
func createTestDataset(ctx context.Context, t *testing.T, db *sql.DB, datasetURN string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, namespace, name, created_at)
		VALUES ($1, $2, $3, $4)
	`, datasetURN, "test", datasetURN, time.Now())
	require.NoError(t, err, "Failed to create dataset")
}

// TestTestResultsHandler_SingleSuccess tests successful ingestion of a single test result.
// Expected: 200 OK with response showing 1 stored test result.
func TestTestResultsHandler_SingleSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create prerequisites (job_run and dataset)
	jobRunID := "test-run-single"
	datasetURN := "postgresql://localhost/db.schema.table_single"

	createTestJobRun(ctx, t, ts.db, jobRunID)
	createTestDataset(ctx, t, ts.db, datasetURN)

	// Create single test result
	testResults := []TestResultRequest{
		{
			TestName:   "test_column_not_null",
			DatasetURN: datasetURN,
			JobRunID:   jobRunID,
			Status:     "failed",
			Message:    "Column 'user_id' contains NULL values",
			ExecutedAt: time.Now(),
			DurationMs: 150,
			TestType:   "data_quality",
			Metadata: map[string]interface{}{
				"column":   "user_id",
				"severity": "high",
			},
		},
	}

	rr := ts.postTestResults(t, testResults)

	// Validate response
	response := validateTestResultResponse(t, rr, http.StatusOK)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 1, response.Stored, "Expected 1 stored test result")
	assert.Equal(t, 0, response.Failed, "Expected 0 failed")
	assert.Len(t, response.Results, 1, "Expected 1 result")
	assert.Equal(t, 200, response.Results[0].Status, "Expected result status 200")
	assert.Equal(t, "stored", response.Results[0].Message, "Expected 'stored' message")

	// Verify database persistence
	count := ts.countTestResults(ctx, t, "test_column_not_null")
	assert.Equal(t, 1, count, "Expected 1 row in test_results table")
}

// TestTestResultsHandler_BatchSuccess tests successful ingestion of multiple test results.
// Expected: 200 OK with response showing all test results stored.
func TestTestResultsHandler_BatchSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create 5 job runs and 5 datasets
	jobRunIDs := []string{"run-batch-1", "run-batch-2", "run-batch-3", "run-batch-4", "run-batch-5"}
	datasetURNs := []string{
		"postgresql://localhost/db.schema.table_1",
		"postgresql://localhost/db.schema.table_2",
		"postgresql://localhost/db.schema.table_3",
		"postgresql://localhost/db.schema.table_4",
		"postgresql://localhost/db.schema.table_5",
	}

	for i := 0; i < 5; i++ {
		createTestJobRun(ctx, t, ts.db, jobRunIDs[i])
		createTestDataset(ctx, t, ts.db, datasetURNs[i])
	}

	// Create batch of 5 test results
	now := time.Now()
	testResults := make([]TestResultRequest, 5)

	for i := 0; i < 5; i++ {
		testResults[i] = TestResultRequest{
			TestName:   fmt.Sprintf("test_data_quality_%d", i),
			DatasetURN: datasetURNs[i],
			JobRunID:   jobRunIDs[i],
			Status:     "failed",
			Message:    fmt.Sprintf("Test failed for dataset %d", i),
			ExecutedAt: now,
			DurationMs: 100 + i*10,
		}
	}

	rr := ts.postTestResults(t, testResults)

	// Validate response
	response := validateTestResultResponse(t, rr, http.StatusOK)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 5, response.Stored, "Expected 5 stored test results")
	assert.Equal(t, 0, response.Failed, "Expected 0 failed")
	assert.Len(t, response.Results, 5, "Expected 5 results")

	// Verify all 5 test results were persisted
	for i := 0; i < 5; i++ {
		testName := fmt.Sprintf("test_data_quality_%d", i)
		count := ts.countTestResults(ctx, t, testName)
		assert.Equal(t, 1, count, "Expected 1 row for test %s", testName)
	}
}

// TestTestResultsHandler_MissingJobRunId tests validation of missing required field.
// Expected: 422 Unprocessable Entity with error message indicating missing job_run_id.
func TestTestResultsHandler_MissingJobRunId(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create dataset but no job_run (to test missing job_run_id validation)
	datasetURN := "postgresql://localhost/db.schema.table_missing"

	createTestJobRun(ctx, t, ts.db, "temp-job")
	createTestDataset(ctx, t, ts.db, datasetURN)

	// Test result with empty job_run_id
	testResults := []TestResultRequest{
		{
			TestName:   "test_missing_job_run",
			DatasetURN: datasetURN,
			JobRunID:   "", // Missing required field
			Status:     "failed",
			ExecutedAt: time.Now(),
		},
	}

	rr := ts.postTestResults(t, testResults)

	// Validate response (should be 422 for validation error)
	response := validateTestResultResponse(t, rr, http.StatusUnprocessableEntity)

	if response != nil {
		// TestResultResponse format (per-result errors)
		assert.Equal(t, 0, response.Stored, "Expected 0 stored")
		assert.Equal(t, 1, response.Failed, "Expected 1 failed")
		assert.Contains(t, response.Results[0].Error, "job_run_id", "Error should mention job_run_id")
	} else {
		// RFC 7807 format (request-level error)
		validateRFC7807Response(t, rr, http.StatusUnprocessableEntity)
		bodyStr := rr.Body.String()
		assert.Contains(t, bodyStr, "job_run_id", "Error should mention job_run_id")
	}
}

// TestTestResultsHandler_InvalidDatasetURN tests validation of malformed dataset URN.
// Expected: 422 Unprocessable Entity with error message indicating invalid URN format.
func TestTestResultsHandler_InvalidDatasetURN(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create job_run
	jobRunID := "test-run-invalid-urn"
	createTestJobRun(ctx, t, ts.db, jobRunID)

	// Test result with invalid dataset URN (no colon separator)
	testResults := []TestResultRequest{
		{
			TestName:   "test_invalid_urn",
			DatasetURN: "invalid-urn-no-colon", // Invalid URN format
			JobRunID:   jobRunID,
			Status:     "failed",
			ExecutedAt: time.Now(),
		},
	}

	rr := ts.postTestResults(t, testResults)

	// Validate response (should be 422 for validation error)
	response := validateTestResultResponse(t, rr, http.StatusUnprocessableEntity)

	if response != nil {
		// TestResultResponse format (per-result errors)
		assert.Equal(t, 0, response.Stored, "Expected 0 stored")
		assert.Equal(t, 1, response.Failed, "Expected 1 failed")
		assert.NotEmpty(t, response.Results[0].Error, "Expected error message")
		// Error should mention URN or dataset_urn
		errorMsg := response.Results[0].Error
		hasURNError := strings.Contains(errorMsg, "URN") || strings.Contains(errorMsg, "dataset_urn")
		assert.True(t, hasURNError, "Expected error about invalid URN, got: %s", errorMsg)
	} else {
		// RFC 7807 format (request-level error)
		validateRFC7807Response(t, rr, http.StatusUnprocessableEntity)
		bodyStr := rr.Body.String()
		hasURNError := strings.Contains(bodyStr, "URN") || strings.Contains(bodyStr, "dataset_urn")
		assert.True(t, hasURNError, "Expected error about invalid URN, got: %s", bodyStr)
	}
}

// TestTestResultsHandler_DuplicateTest tests UPSERT behavior for duplicate test results.
// Expected: Both requests return 200 OK, only 1 row in database (not 2).
func TestTestResultsHandler_DuplicateTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create prerequisites
	jobRunID := "test-run-duplicate"
	datasetURN := "postgresql://localhost/db.schema.table_duplicate"

	createTestJobRun(ctx, t, ts.db, jobRunID)
	createTestDataset(ctx, t, ts.db, datasetURN)

	// Fixed timestamp for consistent UPSERT key (test_name, dataset_urn, executed_at)
	executedAt := time.Now().Truncate(time.Second)

	testResult := TestResultRequest{
		TestName:   "test_duplicate_detection",
		DatasetURN: datasetURN,
		JobRunID:   jobRunID,
		Status:     "failed",
		Message:    "Initial failure message",
		ExecutedAt: executedAt,
	}

	// First request (should store)
	rr1 := ts.postTestResults(t, []TestResultRequest{testResult})
	response1 := validateTestResultResponse(t, rr1, http.StatusOK)
	require.NotNil(t, response1, "Failed to validate first response")

	assert.Equal(t, 1, response1.Stored, "First request: Expected 1 stored")
	assert.Equal(t, 0, response1.Failed, "First request: Expected 0 failed")

	// Second request (duplicate - should upsert, not insert)
	testResult.Message = "Updated failure message" // Different message, same key
	rr2 := ts.postTestResults(t, []TestResultRequest{testResult})
	response2 := validateTestResultResponse(t, rr2, http.StatusOK)
	require.NotNil(t, response2, "Failed to validate second response")

	assert.Equal(t, 1, response2.Stored, "Second request: Expected 1 stored (upsert)")
	assert.Equal(t, 0, response2.Failed, "Second request: Expected 0 failed")

	// Verify only 1 row exists (UPSERT behavior, not INSERT)
	count := ts.countTestResults(ctx, t, "test_duplicate_detection")
	assert.Equal(t, 1, count, "Expected exactly 1 row (UPSERT, not INSERT)")
}

// TestTestResultsHandler_PartialSuccess tests 207 Multi-Status for partial success.
// Expected: Some test results stored, some failed (FK violations) → 207 Multi-Status.
func TestTestResultsHandler_PartialSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create 2 job_runs and 2 datasets (prerequisites for 2 successful test results)
	jobRunID1 := uuid.New().String()
	jobRunID2 := uuid.New().String()
	datasetURN1 := "postgresql://localhost/db.schema.table_partial_1"
	datasetURN2 := "postgresql://localhost/db.schema.table_partial_2"

	createTestJobRun(ctx, t, ts.db, jobRunID1)
	createTestJobRun(ctx, t, ts.db, jobRunID2)
	createTestDataset(ctx, t, ts.db, datasetURN1)
	createTestDataset(ctx, t, ts.db, datasetURN2)

	// Create batch with 3 test results: 2 valid (FK satisfied), 1 invalid (missing dataset FK)
	now := time.Now()
	testResults := []TestResultRequest{
		{
			TestName:   "test_partial_success_1",
			DatasetURN: datasetURN1,
			JobRunID:   jobRunID1,
			Status:     "failed",
			ExecutedAt: now,
		},
		{
			TestName:   "test_partial_success_2",
			DatasetURN: datasetURN2,
			JobRunID:   jobRunID2,
			Status:     "failed",
			ExecutedAt: now,
		},
		{
			TestName:   "test_partial_failure",
			DatasetURN: "postgresql://nonexistent/db.table", // FK violation
			JobRunID:   jobRunID1,
			Status:     "failed",
			ExecutedAt: now,
		},
	}

	rr := ts.postTestResults(t, testResults)

	// Validate response: 207 Multi-Status (partial success)
	response := validateTestResultResponse(t, rr, http.StatusMultiStatus)
	require.NotNil(t, response, "Failed to validate response")

	assert.Equal(t, 2, response.Stored, "Expected 2 stored")
	assert.Equal(t, 1, response.Failed, "Expected 1 failed")
	assert.Len(t, response.Results, 3, "Expected 3 results")

	// Verify individual result statuses
	assert.Equal(t, http.StatusOK, response.Results[0].Status, "First result should succeed")
	assert.Equal(t, "stored", response.Results[0].Message)

	assert.Equal(t, http.StatusOK, response.Results[1].Status, "Second result should succeed")
	assert.Equal(t, "stored", response.Results[1].Message)

	assert.Equal(t, http.StatusUnprocessableEntity, response.Results[2].Status, "Third result should fail")
	assert.NotEmpty(t, response.Results[2].Error, "Third result should have error message")
	assert.Contains(t, response.Results[2].Error, "foreign key", "Error should mention FK violation")

	// Verify database: Only 2 rows stored (not 3)
	count1 := ts.countTestResults(ctx, t, "test_partial_success_1")
	assert.Equal(t, 1, count1, "First test result should be stored")

	count2 := ts.countTestResults(ctx, t, "test_partial_success_2")
	assert.Equal(t, 1, count2, "Second test result should be stored")

	count3 := ts.countTestResults(ctx, t, "test_partial_failure")
	assert.Equal(t, 0, count3, "Third test result should NOT be stored (FK violation)")
}

// TestTestResultsHandler_PayloadTooLarge tests 413 Payload Too Large for oversized requests.
// Expected: Request exceeding MaxRequestSize returns 413 with RFC 7807 error.
func TestTestResultsHandler_PayloadTooLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Create an oversized request (larger than defaultMaxRequestSize = 10MB)
	// Generate a test result with a huge metadata field
	largeMetadata := make(map[string]interface{})
	// Add ~11MB of data (exceeds 10MB limit)
	largeMetadata["data"] = strings.Repeat("x", 11*1024*1024)

	testResults := []TestResultRequest{
		{
			TestName:   "test_large_payload",
			DatasetURN: "postgresql://localhost/db.table",
			JobRunID:   "some-job-run-id",
			Status:     "failed",
			ExecutedAt: time.Now(),
			Metadata:   largeMetadata,
		},
	}

	body, err := json.Marshal(testResults)
	require.NoError(t, err, "Failed to marshal large test results")

	// Verify payload is actually large
	assert.Greater(t, len(body), 10*1024*1024, "Payload should exceed 10MB")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/test-results", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", ts.apiKey)

	rr := httptest.NewRecorder()
	ts.server.httpServer.Handler.ServeHTTP(rr, req)

	// Validate response: 413 Payload Too Large with RFC 7807 format
	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code,
		"Expected 413 Payload Too Large, got %d. Response: %s", rr.Code, rr.Body.String())

	// Verify RFC 7807 response format
	validateRFC7807Response(t, rr, http.StatusRequestEntityTooLarge)

	// Verify error message mentions size limit
	bodyStr := rr.Body.String()
	assert.Contains(t, bodyStr, "maximum size", "Error should mention maximum size")
}
