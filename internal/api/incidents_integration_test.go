package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListIncidents_Integration tests GET /api/v1/incidents endpoint.
func TestListIncidents_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Setup test data using canonical URN format (see canonicalization.GenerateDatasetURN)
	now := time.Now()
	jobRunID := "dbt:" + uuid.New().String()
	datasetURN := "postgresql://prod-db/public.customers"

	setupIncidentTestData(ctx, t, ts, jobRunID, datasetURN, now)

	// Refresh materialized views
	_, err := ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
	require.NoError(t, err, "Failed to refresh views")

	t.Run("ListIncidents_ReturnsIncidents", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())
		assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

		var response IncidentListResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		assert.GreaterOrEqual(t, response.Total, 1, "Should have at least 1 incident")
		assert.NotEmpty(t, response.Incidents, "Incidents should not be empty")
		assert.Equal(t, 20, response.Limit, "Default limit should be 20")
		assert.Equal(t, 0, response.Offset, "Default offset should be 0")

		// Verify incident structure
		inc := response.Incidents[0]
		assert.NotEmpty(t, inc.ID, "ID should not be empty")
		assert.NotEmpty(t, inc.TestName, "TestName should not be empty")
		assert.NotEmpty(t, inc.DatasetURN, "DatasetURN should not be empty")
	})

	t.Run("ListIncidents_WithPagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents?limit=5&offset=0", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		var response IncidentListResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)

		assert.Equal(t, 5, response.Limit, "Limit should be 5")
		assert.Equal(t, 0, response.Offset, "Offset should be 0")
		assert.LessOrEqual(t, len(response.Incidents), 5, "Should return at most 5 incidents")
	})

	t.Run("ListIncidents_WithSinceFilter", func(t *testing.T) {
		// Filter for incidents in the last hour
		// URL-encode the timestamp (contains + for timezone)
		since := url.QueryEscape(now.Add(-1 * time.Hour).Format(time.RFC3339))
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents?since="+since, nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		var response IncidentListResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)

		// All incidents should be after the since time
		for _, inc := range response.Incidents {
			assert.True(t, inc.ExecutedAt.After(now.Add(-1*time.Hour)),
				"Incident should be after since time")
		}
	})

	t.Run("ListIncidents_InvalidLimit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents?limit=999", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("ListIncidents_InvalidOffset", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents?offset=-1", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("ListIncidents_InvalidSince", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents?since=invalid", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

// TestGetIncidentDetails_Integration tests GET /api/v1/incidents/{id} endpoint.
func TestGetIncidentDetails_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Setup test data
	now := time.Now()
	jobRunID := "dbt:" + uuid.New().String()
	datasetURN := "postgresql://prod-db/public.orders"

	testResultID := setupIncidentTestData(ctx, t, ts, jobRunID, datasetURN, now)

	// Refresh materialized views
	_, err := ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
	require.NoError(t, err, "Failed to refresh views")

	t.Run("GetIncidentDetails_Success", func(t *testing.T) {
		endpoint := fmt.Sprintf("/api/v1/incidents/%d", testResultID)
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())
		assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

		var response IncidentDetailResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		// Verify response structure
		assert.Equal(t, strconv.FormatInt(testResultID, 10), response.ID)
		assert.NotEmpty(t, response.Test.Name, "Test name should not be empty")
		assert.NotEmpty(t, response.Dataset.URN, "Dataset URN should not be empty")
		assert.NotNil(t, response.Job, "Job should not be nil for correlated incident")
		assert.Equal(t, CorrelationStatusCorrelated, response.CorrelationStatus)

		// Verify test details
		assert.Equal(t, "failed", response.Test.Status)
		assert.NotEmpty(t, response.Test.Message)

		// Verify job details
		assert.Equal(t, jobRunID, response.Job.RunID)
		assert.Equal(t, "dbt", response.Job.Producer)
	})

	t.Run("GetIncidentDetails_NotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents/999999", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("GetIncidentDetails_InvalidID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents/invalid", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

// TestIncidentsWithDownstream_Integration tests downstream lineage in incident details.
func TestIncidentsWithDownstream_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	// Setup test data with downstream lineage
	now := time.Now()
	jobRunID1 := "dbt:" + uuid.New().String()
	jobRunID2 := "dbt:" + uuid.New().String()
	datasetA := "postgresql://prod-db/public.source"
	datasetB := "postgresql://prod-db/public.derived"

	// Job 1 produces datasetA
	testResultID := setupIncidentTestData(ctx, t, ts, jobRunID1, datasetA, now)

	// Job 2 consumes datasetA and produces datasetB (downstream)
	setupDownstreamJob(ctx, t, ts, jobRunID2, datasetA, datasetB, now)

	// Refresh materialized views
	_, err := ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
	require.NoError(t, err, "Failed to refresh views")

	t.Run("IncidentWithDownstream", func(t *testing.T) {
		endpoint := fmt.Sprintf("/api/v1/incidents/%d", testResultID)
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response IncidentDetailResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err)

		// Verify downstream is populated
		assert.NotEmpty(t, response.Downstream, "Downstream should not be empty")

		// Verify downstream structure
		for _, ds := range response.Downstream {
			assert.NotEmpty(t, ds.URN, "Downstream URN should not be empty")
			assert.NotEmpty(t, ds.Name, "Downstream name should not be empty")
			assert.GreaterOrEqual(t, ds.Depth, 1, "Downstream depth should be >= 1")
			assert.NotEmpty(t, ds.ParentURN, "ParentURN should not be empty")
		}
	})
}

// setupIncidentTestData creates test data for incident tests.
// Returns the test_result_id for use in detail endpoint tests.
func setupIncidentTestData(
	ctx context.Context,
	t *testing.T,
	ts *testServer,
	jobRunID, datasetURN string,
	now time.Time,
) int64 {
	t.Helper()

	// Insert job run
	_, err := ts.db.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type,
			event_time, started_at, producer_name
		) VALUES ($1, $2, 'test-job', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt')
	`, jobRunID, uuid.New().String(), now, now.Add(-5*time.Minute))
	require.NoError(t, err, "Failed to insert job run")

	// Insert dataset
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, $2, 'public')
		ON CONFLICT (dataset_urn) DO NOTHING
	`, datasetURN, extractDatasetName(datasetURN))
	require.NoError(t, err, "Failed to insert dataset")

	// Insert lineage edge (job produces dataset)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, jobRunID, datasetURN)
	require.NoError(t, err, "Failed to insert lineage edge")

	// Insert test result (failed test)
	var testResultID int64

	err = ts.db.QueryRowContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message,
			executed_at, duration_ms
		) VALUES ($1, $2, $3, $4, 'failed', 'Found 3 null values', $5, 150)
		RETURNING id
	`, "not_null_test", "not_null", datasetURN, jobRunID, now).Scan(&testResultID)
	require.NoError(t, err, "Failed to insert test result")

	return testResultID
}

// setupDownstreamJob creates a downstream job that consumes one dataset and produces another.
func setupDownstreamJob(
	ctx context.Context,
	t *testing.T,
	ts *testServer,
	jobRunID, inputURN, outputURN string,
	now time.Time,
) {
	t.Helper()

	// Insert job run
	_, err := ts.db.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type,
			event_time, started_at, producer_name
		) VALUES ($1, $2, 'downstream-job', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt')
	`, jobRunID, uuid.New().String(), now, now.Add(-2*time.Minute))
	require.NoError(t, err, "Failed to insert downstream job run")

	// Insert output dataset
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, $2, 'public')
		ON CONFLICT (dataset_urn) DO NOTHING
	`, outputURN, extractDatasetName(outputURN))
	require.NoError(t, err, "Failed to insert output dataset")

	// Insert input edge (job consumes inputURN)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'input')
	`, jobRunID, inputURN)
	require.NoError(t, err, "Failed to insert input edge")

	// Insert output edge (job produces outputURN)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, jobRunID, outputURN)
	require.NoError(t, err, "Failed to insert output edge")
}

// extractDatasetName extracts the table name from a dataset URN.
func extractDatasetName(urn string) string {
	// Simple extraction - in production use canonicalization package
	// URN format: postgresql://prod-db/public.customers -> customers
	for i := len(urn) - 1; i >= 0; i-- {
		if urn[i] == '.' {
			return urn[i+1:]
		}
	}

	return urn
}
