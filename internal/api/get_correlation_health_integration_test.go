package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetCorrelationHealth_Integration tests GET /api/v1/health/correlation endpoint.
func TestGetCorrelationHealth_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	ts := setupTestServer(ctx, t)

	t.Run("CorrelationHealth_EmptyState", func(t *testing.T) {
		// Test with no data - should return healthy (rate 1.0)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/correlation", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())
		assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

		var response CorrelationHealthResponse

		err := json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		assert.InDelta(t, 1.0, response.CorrelationRate, 0.001, "Empty state should be healthy (rate 1.0)")
		assert.Equal(t, 0, response.TotalDatasets, "No datasets")
		assert.Empty(t, response.OrphanNamespaces, "No orphan namespaces")
	})

	t.Run("CorrelationHealth_WithCorrelatedData", func(t *testing.T) {
		// Setup correlated test data (namespace has both test results and output edges)
		now := time.Now()
		jobRunID := "dbt:" + uuid.New().String()
		namespace := "postgresql://prod/public"
		datasetURN := namespace + ".customers"

		setupCorrelatedTestData(ctx, t, ts, jobRunID, datasetURN, now)

		// Refresh materialized views
		_, err := ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
		require.NoError(t, err, "Failed to refresh views")

		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/correlation", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response CorrelationHealthResponse

		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		assert.InDelta(t, 1.0, response.CorrelationRate, 0.001, "All correlated = rate 1.0")
		assert.GreaterOrEqual(t, response.TotalDatasets, 1, "Should have at least 1 dataset")
		assert.Empty(t, response.OrphanNamespaces, "No orphan namespaces")
	})

	t.Run("CorrelationHealth_WithOrphanNamespace", func(t *testing.T) {
		// Setup orphan namespace (test results exist but no output edges for this namespace)
		now := time.Now()
		geJobRunID := "great_expectations:" + uuid.New().String()
		orphanNamespace := "postgres_orphan_" + uuid.New().String()[:8]
		orphanDatasetURN := orphanNamespace + "/public.orphan_table"

		setupOrphanTestData(ctx, t, ts, geJobRunID, orphanNamespace, orphanDatasetURN, now)

		// Refresh materialized views
		_, err := ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
		require.NoError(t, err, "Failed to refresh views")

		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/correlation", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response CorrelationHealthResponse

		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		// Verify orphan namespace is detected
		assert.NotEmpty(t, response.OrphanNamespaces, "Should have orphan namespaces")

		// Find the orphan namespace we created
		var foundOrphan bool

		for _, o := range response.OrphanNamespaces {
			if o.Namespace == orphanNamespace {
				foundOrphan = true

				assert.Equal(t, "great_expectations", o.Producer)
				assert.GreaterOrEqual(t, o.EventCount, 1)

				break
			}
		}

		assert.True(t, foundOrphan, "Should find our orphan namespace: %s", orphanNamespace)
	})

	t.Run("CorrelationHealth_MixedState", func(t *testing.T) {
		// Setup mixed state: 1 correlated, 1 orphan (should give ~50% rate)
		now := time.Now()

		// Correlated data
		dbtJobRunID := "dbt:" + uuid.New().String()
		correlatedNS := "postgresql://prod/mixed"
		correlatedDataset := correlatedNS + ".correlated_table"

		setupCorrelatedTestData(ctx, t, ts, dbtJobRunID, correlatedDataset, now)

		// Orphan data
		geJobRunID := "great_expectations:" + uuid.New().String()
		orphanNS := "postgres_mixed_orphan_" + uuid.New().String()[:8]
		orphanDataset := orphanNS + "/public.orphan_mixed"

		setupOrphanTestData(ctx, t, ts, geJobRunID, orphanNS, orphanDataset, now)

		// Refresh materialized views
		_, err := ts.db.ExecContext(ctx, "SELECT refresh_correlation_views()")
		require.NoError(t, err, "Failed to refresh views")

		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/correlation", nil)
		req.Header.Set("X-Api-Key", ts.apiKey)

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code, "Response: %s", rr.Body.String())

		var response CorrelationHealthResponse

		err = json.Unmarshal(rr.Body.Bytes(), &response)
		require.NoError(t, err, "Failed to parse response")

		// Rate should be less than 1.0 since we have orphan data
		assert.Less(t, response.CorrelationRate, 1.0, "Should have rate < 1.0 with orphan data")
		assert.NotEmpty(t, response.OrphanNamespaces, "Should have orphan namespaces")
	})

	t.Run("CorrelationHealth_RequiresAuth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/health/correlation", nil)
		// No API key set

		rr := httptest.NewRecorder()
		ts.server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusUnauthorized, rr.Code, "Should require authentication")
	})
}

// setupCorrelatedTestData creates test data for a fully correlated scenario.
// The namespace has both test results AND producer output edges.
func setupCorrelatedTestData(
	ctx context.Context,
	t *testing.T,
	ts *testServer,
	jobRunID, datasetURN string,
	now time.Time,
) {
	t.Helper()

	// Insert job run
	_, err := ts.db.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type,
			event_time, started_at, producer_name
		) VALUES ($1, $2, 'test-job', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt')
	`, jobRunID, uuid.New().String(), now, now.Add(-5*time.Minute))
	require.NoError(t, err, "Failed to insert job run")

	// Extract namespace from URN (simple extraction for test)
	namespace := extractNamespaceFromURN(datasetURN)

	// Insert dataset
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, $2, $3)
		ON CONFLICT (dataset_urn) DO NOTHING
	`, datasetURN, extractDatasetName(datasetURN), namespace)
	require.NoError(t, err, "Failed to insert dataset")

	// Insert lineage edge (job produces dataset) - THIS MAKES IT CORRELATED
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, jobRunID, datasetURN)
	require.NoError(t, err, "Failed to insert lineage edge")

	// Insert test result (failed test)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message,
			executed_at, duration_ms
		) VALUES ($1, $2, $3, $4, 'failed', 'Found 3 null values', $5, 150)
	`, "not_null_test_"+uuid.New().String()[:8], "not_null", datasetURN, jobRunID, now)
	require.NoError(t, err, "Failed to insert test result")
}

// setupOrphanTestData creates test data for an orphan namespace scenario.
// The namespace has test results but NO producer output edges.
func setupOrphanTestData(
	ctx context.Context,
	t *testing.T,
	ts *testServer,
	jobRunID, namespace, datasetURN string,
	now time.Time,
) {
	t.Helper()

	// Insert GE job run
	_, err := ts.db.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type,
			event_time, started_at, producer_name
		) VALUES ($1, $2, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $3, $4, 'great_expectations')
	`, jobRunID, uuid.New().String(), now, now.Add(-5*time.Minute))
	require.NoError(t, err, "Failed to insert GE job run")

	// Insert orphan dataset (NO output edges for this namespace!)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'orphan_table', $2)
		ON CONFLICT (dataset_urn) DO NOTHING
	`, datasetURN, namespace)
	require.NoError(t, err, "Failed to insert orphan dataset")

	// Insert failed test result for orphan dataset (NO output edges!)
	_, err = ts.db.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message,
			executed_at, duration_ms
		) VALUES ($1, $2, $3, $4, 'failed', 'Found nulls', $5, 100)
	`, "orphan_test_"+uuid.New().String()[:8], "not_null", datasetURN, jobRunID, now)
	require.NoError(t, err, "Failed to insert test result")
}

// extractNamespaceFromURN extracts the namespace part from a dataset URN.
// URN format: postgresql://prod/public.customers -> postgresql://prod/public
func extractNamespaceFromURN(urn string) string {
	// Find last dot and return everything before it
	for i := len(urn) - 1; i >= 0; i-- {
		if urn[i] == '.' {
			return urn[:i]
		}
	}

	return urn
}
