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

	// Setup test data
	now := time.Now()
	runID := uuid.New().String()
	datasetURN := "postgresql://prod-db/public.customers"

	setupIncidentTestData(ctx, t, ts, runID, datasetURN, now)

	// Refresh materialized views
	require.NoError(t, ts.lineageStore.InitResolvedDatasets(ctx))

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
	runID := uuid.New().String()
	datasetURN := "postgresql://prod-db/public.orders"

	testResultID := setupIncidentTestData(ctx, t, ts, runID, datasetURN, now)

	// Refresh materialized views
	require.NoError(t, ts.lineageStore.InitResolvedDatasets(ctx))

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
		assert.Equal(t, runID, response.Job.RunID)
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

// TestIncidents_HasCorrelationIssue_Integration tests has_correlation_issue field in incident list.
//
// Note: An incident only appears in incident_correlation_view if its dataset has an output edge.
// The has_correlation_issue field is true when the incident's namespace is in the orphan set.
// A namespace is orphan when it has test results but NO output edges for ANY dataset in that namespace.
//
// This test verifies:
// 1. Incidents in namespaces with output edges have has_correlation_issue: false
// 2. The correlation_status field is correctly set based on orphan detection.
func TestIncidents_HasCorrelationIssue_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	now := time.Now()

	// Set up a correlated incident:
	// - Namespace has output edges from dbt (data producer)
	// - This is a healthy namespace, so has_correlation_issue: false
	correlatedRunID := uuid.New().String()
	correlatedNamespace := "postgresql://prod/correlated"
	correlatedDatasetURN := correlatedNamespace + ".customers"

	correlatedTestResultID := setupIncidentTestData(ctx, t, ts, correlatedRunID, correlatedDatasetURN, now)

	// Update the dataset namespace to match our test namespace
	_, err := ts.db.ExecContext(ctx, `
		UPDATE datasets SET namespace = $1 WHERE dataset_urn = $2
	`, correlatedNamespace, correlatedDatasetURN)
	require.NoError(t, err, "Failed to update dataset namespace")

	// Refresh materialized views
	require.NoError(t, ts.lineageStore.InitResolvedDatasets(ctx))

	_, err = ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
	require.NoError(t, err, "Failed to refresh views")

	t.Run("CorrelatedIncident_HasCorrelationIssue_False", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response IncidentListResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		// Find the correlated incident
		var correlatedIncident *IncidentSummary

		for i := range response.Incidents {
			if response.Incidents[i].ID == strconv.FormatInt(correlatedTestResultID, 10) {
				correlatedIncident = &response.Incidents[i]

				break
			}
		}

		require.NotNil(t, correlatedIncident, "Should find correlated incident in response")
		assert.False(t, correlatedIncident.HasCorrelationIssue,
			"Incident in namespace with output edges should have has_correlation_issue: false")
	})

	t.Run("CorrelatedIncidentDetail_CorrelationStatus_Correlated", func(t *testing.T) {
		endpoint := fmt.Sprintf("/api/v1/incidents/%d", correlatedTestResultID)
		req := httptest.NewRequest(http.MethodGet, endpoint, nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response IncidentDetailResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		assert.Equal(t, CorrelationStatusCorrelated, response.CorrelationStatus,
			"Incident in namespace with output edges should have correlation_status: correlated")
	})
}

// TestIncidents_OrphanNamespace_Integration tests that incidents in orphan namespaces
// are correctly flagged with has_correlation_issue: true.
//
// An orphan namespace scenario requires:
// 1. A namespace with test results but NO output edges (orphan)
// 2. A DIFFERENT namespace with output edges (so the incident appears in the view)
// 3. The incident's dataset must have an output edge to appear in the view
//
// This is a complex scenario that tests the edge case where:
//   - The incident appears in the view (has output edge for its dataset)
//   - But the namespace is still considered "orphan" because... actually this can't happen
//     by the current design since having an output edge means the namespace is not orphan.
//
// Therefore, this test verifies the orphan detection at the health endpoint level,
// not at the incident list level (since incidents in truly orphan namespaces don't appear).
func TestIncidents_OrphanNamespace_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	now := time.Now()

	// Set up an orphan namespace scenario:
	// - Namespace "postgres_orphan" has test results but NO output edges
	// - This namespace will appear in the orphan list via health endpoint
	// - But incidents in this namespace won't appear in incident list (by design)
	geRunID := uuid.New().String()
	orphanNamespace := "postgres_orphan_" + uuid.New().String()[:8]
	orphanDatasetURN := orphanNamespace + "/public.orphan_table"

	// Insert GE job run
	_, err := ts.db.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type,
			event_time, started_at, producer_name
		) VALUES ($1, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $2, $3, 'great_expectations')
	`, geRunID, now, now.Add(-5*time.Minute))
	require.NoError(t, err, "Failed to insert GE job run")

	// Insert orphan dataset (NO output edges for this namespace)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'orphan_table', $2)
	`, orphanDatasetURN, orphanNamespace)
	require.NoError(t, err, "Failed to insert orphan dataset")

	// Insert failed test result for orphan dataset (NO output edges!)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message,
			executed_at, duration_ms
		) VALUES ($1, $2, $3, $4, 'failed', 'Found nulls', $5, 100)
	`, "orphan_test_"+uuid.New().String()[:8], "not_null", orphanDatasetURN, geRunID, now)
	require.NoError(t, err, "Failed to insert test result")

	// Refresh materialized views
	require.NoError(t, ts.lineageStore.InitResolvedDatasets(ctx))

	_, err = ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
	require.NoError(t, err, "Failed to refresh views")

	t.Run("OrphanDataset_DetectedInHealthEndpoint", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/correlation", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response CorrelationHealthResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		// Verify orphan dataset is detected
		var foundOrphan bool

		for _, o := range response.OrphanDatasets {
			if o.DatasetURN == orphanDatasetURN {
				foundOrphan = true

				assert.GreaterOrEqual(t, o.TestCount, 1)

				break
			}
		}

		assert.True(t, foundOrphan, "Should detect orphan dataset: %s", orphanDatasetURN)
	})

	t.Run("OrphanIncident_NotInIncidentList", func(t *testing.T) {
		// Incidents in orphan namespaces don't appear in incident_correlation_view
		// because the view requires an output edge for the dataset
		req := httptest.NewRequest(http.MethodGet, "/api/v1/incidents", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response IncidentListResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		assert.Equal(t, 1, response.OrphanCount, "Should report 1 orphan dataset")

		// Verify orphan incident is NOT in the list (by design)
		for _, inc := range response.Incidents {
			assert.NotContains(t, inc.DatasetURN, orphanNamespace,
				"Incidents in orphan namespaces should not appear in incident list")
		}
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
	runID1 := uuid.New().String()
	runID2 := uuid.New().String()
	datasetA := "postgresql://prod-db/public.source"
	datasetB := "postgresql://prod-db/public.derived"

	// Job 1 produces datasetA
	testResultID := setupIncidentTestData(ctx, t, ts, runID1, datasetA, now)

	// Job 2 consumes datasetA and produces datasetB (downstream)
	setupDownstreamJob(ctx, t, ts, runID2, datasetA, datasetB, now)

	// Refresh materialized views
	require.NoError(t, ts.lineageStore.InitResolvedDatasets(ctx))

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
	runID, datasetURN string,
	now time.Time,
) int64 {
	t.Helper()

	// Insert job run
	_, err := ts.db.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type,
			event_time, started_at, producer_name
		) VALUES ($1, 'test-job', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt')
	`, runID, now, now.Add(-5*time.Minute))
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
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, runID, datasetURN)
	require.NoError(t, err, "Failed to insert lineage edge")

	// Insert test result (failed test)
	var testResultID int64

	err = ts.db.QueryRowContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message,
			executed_at, duration_ms
		) VALUES ($1, $2, $3, $4, 'failed', 'Found 3 null values', $5, 150)
		RETURNING id
	`, "not_null_test", "not_null", datasetURN, runID, now).Scan(&testResultID)
	require.NoError(t, err, "Failed to insert test result")

	return testResultID
}

// setupDownstreamJob creates a downstream job that consumes one dataset and produces another.
func setupDownstreamJob(
	ctx context.Context,
	t *testing.T,
	ts *testServer,
	runID, inputURN, outputURN string,
	now time.Time,
) {
	t.Helper()

	// Insert job run
	_, err := ts.db.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type,
			event_time, started_at, producer_name
		) VALUES ($1, 'downstream-job', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt')
	`, runID, now, now.Add(-2*time.Minute))
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
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'input')
	`, runID, inputURN)
	require.NoError(t, err, "Failed to insert input edge")

	// Insert output edge (job produces outputURN)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, runID, outputURN)
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
