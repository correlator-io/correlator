package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/aliasing"
	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/correlation"
	"github.com/correlator-io/correlator/internal/ingestion"
)

// TestRefreshCorrelationViews tests the RefreshCorrelationViews function.
func TestRefreshCorrelationViews(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}

	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Refresh should succeed even with no data
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err, "Refresh should succeed with empty tables")
}

// TestQueryIncidentCorrelation tests the QueryIncidentCorrelation function.
func TestQueryIncidentCorrelation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup test data
	now := time.Now()
	runID1 := uuid.New().String()
	runID2 := uuid.New().String()
	datasetURN1 := "urn:postgres:warehouse:public.customers"
	datasetURN2 := "urn:postgres:warehouse:public.orders"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, 'transform_customers', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt'),
			($4, 'extract_orders', 'airflow_prod', 'FAIL', 'FAIL', $5, $6, 'airflow')
	`, runID1, now, now.Add(-5*time.Minute),
		runID2, now.Add(-1*time.Hour), now.Add(-65*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', 'public'), ($2, 'orders', 'public')
	`, datasetURN1, datasetURN2)
	require.NoError(t, err)

	// Insert lineage edges
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output'), ($3, $4, 'output')
	`, runID1, datasetURN1, runID2, datasetURN2)
	require.NoError(t, err)

	// Insert test results
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
		  id, test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES
			(1, 'not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found 2 nulls', $3, 120),
			(2, 'unique_orders_id', 'unique', $4, $5, 'passed', 'All unique', $6, 150)
	`, datasetURN1, runID1, now,
		datasetURN2, runID2, now.Add(-1*time.Hour))
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)

	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test 1: Query all incidents (no filter, no pagination)
	// Note: incident_correlation_view only returns failed/error tests, not passed tests
	result, err := store.QueryIncidents(ctx, nil, nil)
	require.NoError(t, err, "Query should succeed")

	assert.Len(t, result.Incidents, 1, "Should return 1 incident (view filters failed/error only)")
	assert.Equal(t, 1, result.Total, "Total should be 1")

	// Test 2: Filter by producer
	producer := "dbt"
	filter := &correlation.IncidentFilter{
		ProducerName: &producer,
	}

	result, err = store.QueryIncidents(ctx, filter, nil)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 dbt incident")
	assert.Equal(t, "dbt", result.Incidents[0].ProducerName)

	// Test 3: Filter by run_id
	filter = &correlation.IncidentFilter{
		RunID: &runID1,
	}

	result, err = store.QueryIncidents(ctx, filter, nil)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 incident for run_id")
	assert.Equal(t, runID1, result.Incidents[0].RunID)

	// Test 4: Filter by time range (recent tests only)
	recentTime := now.Add(-30 * time.Minute)
	filter = &correlation.IncidentFilter{
		TestExecutedAfter: &recentTime,
	}

	result, err = store.QueryIncidents(ctx, filter, nil)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 recent test")

	// Test 5: Pagination
	pagination := &correlation.Pagination{Limit: 10, Offset: 0}
	result, err = store.QueryIncidents(ctx, nil, pagination)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 incident with pagination")
	assert.Equal(t, 1, result.Total, "Total should reflect all matching incidents")

	// Test 6: Pagination with offset beyond results
	// Note: When offset exceeds total rows, COUNT(*) OVER() returns 0 because there are no rows to scan.
	// This is a known limitation of the window function approach. For MVP, this is acceptable.
	pagination = &correlation.Pagination{Limit: 10, Offset: 100}
	result, err = store.QueryIncidents(ctx, nil, pagination)
	require.NoError(t, err)

	assert.Empty(t, result.Incidents, "Should return no incidents when offset exceeds total")
	assert.Equal(t, 0, result.Total, "Total is 0 when offset exceeds results (COUNT(*) OVER() limitation)")
}

// TestQueryUpstreamWithChildren tests the QueryUpstreamWithChildren function.
// This is the inverse of downstream - tracing data provenance backward through lineage.
func TestQueryUpstreamWithChildren(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup test data: 3-level lineage chain
	// job1 -> datasetA -> job2 -> datasetB -> job3 -> datasetC
	// Upstream from job3/datasetC: datasetB (depth 1), datasetA (depth 2)
	now := time.Now()
	runID1 := uuid.New().String()
	runID2 := uuid.New().String()
	runID3 := uuid.New().String()
	datasetA := "urn:postgres:warehouse:public.raw_orders"
	datasetB := "urn:postgres:warehouse:public.staged_orders"
	datasetC := "urn:postgres:warehouse:public.fact_orders"

	// Insert job runs (with producer info)
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, 'extract_orders', 'etl', 'COMPLETE', 'COMPLETE', $2, $3, 'airflow'),
			($4, 'stage_orders', 'etl', 'COMPLETE', 'COMPLETE', $5, $6, 'airflow'),
			($7, 'transform_orders', 'dbt', 'COMPLETE', 'COMPLETE', $8, $9, 'dbt')
	`, runID1, now, now.Add(-10*time.Minute),
		runID2, now, now.Add(-8*time.Minute),
		runID3, now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES
			($1, 'raw_orders', 'public'),
			($2, 'staged_orders', 'public'),
			($3, 'fact_orders', 'public')
	`, datasetA, datasetB, datasetC)
	require.NoError(t, err)

	// Create lineage chain: job1 -> datasetA -> job2 -> datasetB -> job3 -> datasetC
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES
			($1, $2, 'output'),  -- job1 produces datasetA
			($3, $2, 'input'),   -- job2 consumes datasetA
			($3, $4, 'output'),  -- job2 produces datasetB
			($5, $4, 'input'),   -- job3 consumes datasetB
			($5, $6, 'output')   -- job3 produces datasetC
	`, runID1, datasetA,
		runID2, datasetB,
		runID3, datasetC)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test 1: Query upstream from job3/datasetC (should get datasetB at depth 1, datasetA at depth 2)
	upstream, err := store.QueryUpstreamWithChildren(ctx, datasetC, runID3, 10)
	require.NoError(t, err)

	assert.Len(t, upstream, 2, "Should have 2 upstream datasets")

	// Verify depth 1 (direct input)
	var depth1Results []correlation.UpstreamResult

	for _, r := range upstream {
		if r.Depth == 1 {
			depth1Results = append(depth1Results, r)
		}
	}

	require.Len(t, depth1Results, 1, "Should have 1 dataset at depth 1")
	assert.Equal(t, datasetB, depth1Results[0].DatasetURN, "Depth 1 should be datasetB")
	assert.Equal(t, datasetC, depth1Results[0].ChildURN, "ChildURN should be root dataset")
	assert.Equal(t, "airflow", depth1Results[0].Producer, "Producer should be airflow (job2)")

	// Verify depth 2 (upstream of upstream)
	var depth2Results []correlation.UpstreamResult

	for _, r := range upstream {
		if r.Depth == 2 {
			depth2Results = append(depth2Results, r)
		}
	}

	require.Len(t, depth2Results, 1, "Should have 1 dataset at depth 2")
	assert.Equal(t, datasetA, depth2Results[0].DatasetURN, "Depth 2 should be datasetA")
	assert.Equal(t, datasetB, depth2Results[0].ChildURN, "ChildURN should be datasetB")
	assert.Equal(t, "airflow", depth2Results[0].Producer, "Producer should be airflow (job1)")

	// Test 2: Query with maxDepth = 1 (should only get datasetB)
	upstream, err = store.QueryUpstreamWithChildren(ctx, datasetC, runID3, 1)
	require.NoError(t, err)

	assert.Len(t, upstream, 1, "Should have 1 upstream dataset with maxDepth=1")
	assert.Equal(t, datasetB, upstream[0].DatasetURN)
	assert.Equal(t, 1, upstream[0].Depth)

	// Test 3: Query from job1 (no inputs - should return empty)
	upstream, err = store.QueryUpstreamWithChildren(ctx, datasetA, runID1, 10)
	require.NoError(t, err)

	assert.Empty(t, upstream, "Job1 has no inputs, upstream should be empty")

	// Test 4: Non-existent job run should return empty slice
	upstream, err = store.QueryUpstreamWithChildren(ctx, "non-existent-urn", "00000000-0000-0000-0000-000000000000", 10)
	require.NoError(t, err)

	assert.Empty(t, upstream, "Should return empty slice for non-existent job")
}

// TestQueryCorrelationHealth_EmptyState tests correlation health with no data.
func TestQueryCorrelationHealth_EmptyState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create LineageStore with no data
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Empty state should return healthy (correlation_rate = 1.0)
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	assert.InDelta(t, 1.0, health.CorrelationRate, 0.001, "No incidents = healthy (rate 1.0)")
	assert.Equal(t, 0, health.TotalDatasets, "No datasets")
	assert.Empty(t, health.OrphanDatasets, "No orphan namespaces")
}

// TestQueryCorrelationHealth_FullyCorrelated tests 100% correlation rate.
func TestQueryCorrelationHealth_FullyCorrelated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: All incidents have matching producer output edges
	now := time.Now()
	dbtRunID := uuid.New().String()
	namespace := "postgresql://prod/public"
	datasetURN := namespace + ".customers"

	// Insert job run
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES ($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt')
	`, dbtRunID, now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	// Insert dataset
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2)
	`, datasetURN, namespace)
	require.NoError(t, err)

	// Insert output edge (producer)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, datasetURN)
	require.NoError(t, err)

	// Insert failed test result
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, datasetURN, dbtRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: 100% correlation rate
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	assert.InDelta(t, 1.0, health.CorrelationRate, 0.001, "All incidents correlated = rate 1.0")
	assert.Equal(t, 1, health.TotalDatasets, "1 dataset with test results")
	assert.Empty(t, health.OrphanDatasets, "No orphan namespaces")
}

// TestQueryCorrelationHealth_ZeroCorrelated tests 0% correlation rate (all orphan).
func TestQueryCorrelationHealth_ZeroCorrelated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: All incidents are in orphan namespaces (no producer output edges)
	now := time.Now()
	geRunID := uuid.New().String()
	dbtRunID := uuid.New().String() // Producer job, but for DIFFERENT namespace
	orphanNamespace := "postgres_prod"
	orphanDatasetURN := orphanNamespace + "/public.orders"
	producerNamespace := "postgresql://other/db"
	producerDatasetURN := producerNamespace + ".other_table"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $2, $3, 'great_expectations'),
			($4, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $5, $6, 'dbt')
	`, geRunID, now, now.Add(-5*time.Minute),
		dbtRunID, now, now.Add(-10*time.Minute))
	require.NoError(t, err)

	// Insert datasets (different namespaces)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'orders', $2), ($3, 'other_table', $4)
	`, orphanDatasetURN, orphanNamespace, producerDatasetURN, producerNamespace)
	require.NoError(t, err)

	// Insert output edge for producer namespace ONLY (not for orphan namespace)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, producerDatasetURN)
	require.NoError(t, err)

	// Insert failed test result in ORPHAN namespace
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_orders_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, orphanDatasetURN, geRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: 0% correlation rate (all incidents in orphan namespace)
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	assert.InDelta(t, 0.0, health.CorrelationRate, 0.001, "All incidents orphan = rate 0.0")
	assert.Equal(t, 1, health.TotalDatasets, "1 dataset with test results")
	assert.Len(t, health.OrphanDatasets, 1, "1 orphan dataset")
	assert.Equal(t, orphanDatasetURN, health.OrphanDatasets[0].DatasetURN)
}

// TestQueryCorrelationHealth_MixedState tests partial correlation rate.
func TestQueryCorrelationHealth_MixedState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: 2 incidents - 1 correlated, 1 orphan (50% rate)
	now := time.Now()
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	// Correlated namespace (has output edges)
	correlatedNS := "postgresql://prod/public"
	correlatedDataset := correlatedNS + ".customers"

	// Orphan namespace (no output edges)
	orphanNS := "postgres_prod"
	orphanDataset := orphanNS + "/public.orders"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt'),
			($4, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $5, $6, 'great_expectations')
	`, dbtRunID, now, now.Add(-5*time.Minute),
		geRunID, now, now.Add(-10*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2), ($3, 'orders', $4)
	`, correlatedDataset, correlatedNS, orphanDataset, orphanNS)
	require.NoError(t, err)

	// Insert output edge ONLY for correlated namespace
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, correlatedDataset)
	require.NoError(t, err)

	// Insert failed test results for BOTH namespaces
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES
			('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100),
			('not_null_orders_id', 'not_null', $4, $5, 'failed', 'Found nulls', $6, 100)
	`, correlatedDataset, dbtRunID, now,
		orphanDataset, geRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: 50% correlation rate (1 correlated, 1 orphan)
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	assert.InDelta(t, 0.5, health.CorrelationRate, 0.001, "1 of 2 correlated = rate 0.5")
	assert.Equal(t, 2, health.TotalDatasets, "2 datasets with test results")
	assert.Len(t, health.OrphanDatasets, 1, "1 orphan dataset")
	assert.Equal(t, orphanDataset, health.OrphanDatasets[0].DatasetURN)
}

// =============================================================================
// Dataset-Level Orphan Detection Tests (Task 4.X.3)
// =============================================================================
//
// These tests verify orphan detection at the dataset level (not namespace level),
// with automatic matching to likely producer datasets via table name extraction.

// TestDetectOrphanDatasets tests dataset-level orphan detection with likely matches.
// This is the core test for TC-002 Entity Resolution.
func TestDetectOrphanDatasets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup TC-002 scenario:
	// - dbt produces: postgresql://demo/marts.customers
	// - GE tests: demo_postgres/customers (ORPHAN - different URN format)
	// Expected: GE dataset is orphan with likely_match pointing to dbt dataset
	now := time.Now()

	// Job runs
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	// dbt producer dataset (canonical format)
	dbtNamespace := "postgresql://demo/marts"
	dbtDatasetURN := dbtNamespace + ".customers"

	// GE validator dataset (different format, same logical table)
	geNamespace := "demo_postgres"
	geDatasetURN := geNamespace + "/customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt'),
			($4, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $5, $6, 'great_expectations')
	`, dbtRunID, now, now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2), ($3, 'customers', $4)
	`, dbtDatasetURN, dbtNamespace, geDatasetURN, geNamespace)
	require.NoError(t, err)

	// dbt produces output (has lineage edge)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, dbtDatasetURN)
	require.NoError(t, err)

	// GE has test results but NO output edge (orphan)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES
			('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100),
			('unique_customers_email', 'unique', $1, $2, 'failed', 'Duplicates', $4, 120)
	`, geDatasetURN, geRunID, now, now.Add(1*time.Minute))
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Detect orphan datasets
	orphans, err := store.QueryOrphanDatasets(ctx)
	require.NoError(t, err)

	// Verify orphan detection
	require.Len(t, orphans, 1, "Should detect 1 orphan dataset")

	orphan := orphans[0]
	assert.Equal(t, geDatasetURN, orphan.DatasetURN, "Orphan should be GE dataset")
	assert.Equal(t, 2, orphan.TestCount, "Should have 2 test results")
	assert.False(t, orphan.LastSeen.IsZero(), "LastSeen should be set")

	// Verify likely match (TC-002 core assertion)
	require.NotNil(t, orphan.LikelyMatch, "Should have likely match")
	assert.Equal(t, dbtDatasetURN, orphan.LikelyMatch.DatasetURN, "Likely match should be dbt dataset")
	assert.InDelta(t, 1.0, orphan.LikelyMatch.Confidence, 0.001, "Confidence should be 1.0 for exact table name match")
	assert.Equal(t, "exact_table_name", orphan.LikelyMatch.MatchReason, "Match reason should be exact_table_name")
}

// TestDetectOrphanDatasets_NoMatch tests orphan detection when no matching producer exists.
func TestDetectOrphanDatasets_NoMatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: Orphan dataset with no matching producer (different table names)
	now := time.Now()
	geRunID := uuid.New().String()
	dbtRunID := uuid.New().String()

	// GE tests "orders" but dbt only produces "customers" (no match)
	geDatasetURN := "demo_postgres/orders"
	dbtDatasetURN := "postgresql://demo/marts.customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $2, $3, 'great_expectations'),
			($4, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $5, $6, 'dbt')
	`, geRunID, now, now.Add(-5*time.Minute),
		dbtRunID, now, now.Add(-10*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'orders', 'demo_postgres'), ($2, 'customers', 'postgresql://demo/marts')
	`, geDatasetURN, dbtDatasetURN)
	require.NoError(t, err)

	// dbt produces customers (not orders)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, dbtDatasetURN)
	require.NoError(t, err)

	// GE tests orders (no producer for this table)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_orders_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, geDatasetURN, geRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Detect orphan datasets
	orphans, err := store.QueryOrphanDatasets(ctx)
	require.NoError(t, err)

	// Verify orphan with no match
	require.Len(t, orphans, 1, "Should detect 1 orphan dataset")
	assert.Equal(t, geDatasetURN, orphans[0].DatasetURN)
	assert.Nil(t, orphans[0].LikelyMatch, "Should have no likely match (different table names)")
}

// TestDetectOrphanDatasets_MultipleOrphans tests detection of multiple orphan datasets.
func TestDetectOrphanDatasets_MultipleOrphans(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: 2 orphan datasets, both with matching producers
	now := time.Now()
	geRunID := uuid.New().String()
	dbtRunID := uuid.New().String()

	// GE orphans
	geCustomersURN := "demo_postgres/customers"
	geOrdersURN := "demo_postgres/orders"

	// dbt producers (matching table names)
	dbtCustomersURN := "postgresql://demo/marts.customers"
	dbtOrdersURN := "postgresql://demo/marts.orders"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $2, $3, 'great_expectations'),
			($4, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $5, $6, 'dbt')
	`, geRunID, now, now.Add(-5*time.Minute),
		dbtRunID, now, now.Add(-10*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES
			($1, 'customers', 'demo_postgres'),
			($2, 'orders', 'demo_postgres'),
			($3, 'customers', 'postgresql://demo/marts'),
			($4, 'orders', 'postgresql://demo/marts')
	`, geCustomersURN, geOrdersURN, dbtCustomersURN, dbtOrdersURN)
	require.NoError(t, err)

	// dbt produces both tables
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output'), ($1, $3, 'output')
	`, dbtRunID, dbtCustomersURN, dbtOrdersURN)
	require.NoError(t, err)

	// GE tests both tables (orphans - no output edges for GE URN format)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES
			('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100),
			('not_null_orders_id', 'not_null', $4, $2, 'failed', 'Found nulls', $5, 100)
	`, geCustomersURN, geRunID, now, geOrdersURN, now.Add(1*time.Minute))
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Detect orphan datasets
	orphans, err := store.QueryOrphanDatasets(ctx)
	require.NoError(t, err)

	// Verify multiple orphans
	require.Len(t, orphans, 2, "Should detect 2 orphan datasets")

	// Build map for easier assertion
	orphanMap := make(map[string]correlation.OrphanDataset)
	for _, o := range orphans {
		orphanMap[o.DatasetURN] = o
	}

	// Verify customers orphan
	customersOrphan, ok := orphanMap[geCustomersURN]
	require.True(t, ok, "Should have customers orphan")
	require.NotNil(t, customersOrphan.LikelyMatch, "Customers should have likely match")
	assert.Equal(t, dbtCustomersURN, customersOrphan.LikelyMatch.DatasetURN)

	// Verify orders orphan
	ordersOrphan, ok := orphanMap[geOrdersURN]
	require.True(t, ok, "Should have orders orphan")
	require.NotNil(t, ordersOrphan.LikelyMatch, "Orders should have likely match")
	assert.Equal(t, dbtOrdersURN, ordersOrphan.LikelyMatch.DatasetURN)
}

// TestDetectOrphanDatasets_EmptyState tests behavior with no data.
func TestDetectOrphanDatasets_EmptyState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create LineageStore with no data
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Empty state should return empty slice
	orphans, err := store.QueryOrphanDatasets(ctx)
	require.NoError(t, err)
	assert.Empty(t, orphans, "Should return empty slice when no data exists")
}

// TestDetectOrphanDatasets_HealthyState tests that no orphans are returned when all datasets are correlated.
func TestDetectOrphanDatasets_HealthyState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: Same URN format for both producer and validator (fully correlated)
	now := time.Now()
	dbtRunID := uuid.New().String()
	datasetURN := "postgresql://demo/marts.customers"

	// Insert job run
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES ($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt')
	`, dbtRunID, now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	// Insert dataset
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', 'postgresql://demo/marts')
	`, datasetURN)
	require.NoError(t, err)

	// dbt produces AND tests the same URN (not orphan)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, datasetURN)
	require.NoError(t, err)

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, datasetURN, dbtRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: No orphans when dataset has output edge
	orphans, err := store.QueryOrphanDatasets(ctx)
	require.NoError(t, err)
	assert.Empty(t, orphans, "Should return no orphans when dataset has producer output edge")
}

// =============================================================================
// Pattern Resolution Tests (Task 4.X.6)
// =============================================================================
//
// These tests verify that QueryIncidents applies pattern resolution to correlate
// incidents across different URN formats (TC-002 Entity Resolution).

// TestQueryIncidents_WithPatternResolution tests TC-002: GE test failures correlate
// to dbt-produced datasets via pattern matching.
//
// This is the core test for the Entity Resolution feature where:
//   - dbt produces: postgresql://demo/marts.customers
//   - GE tests: demo_postgres/customers (different URN format, same logical table)
//   - Pattern: demo_postgres/{name} → postgresql://demo/marts.{name}
//   - Expected: GE incident correlates to dbt job run
func TestQueryIncidents_WithPatternResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup TC-002 scenario
	now := time.Now()

	// Job runs
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	// dbt produces in canonical format
	dbtNamespace := "postgresql://demo/marts"
	dbtDatasetURN := dbtNamespace + ".customers"

	// GE tests in different format (same logical table)
	geNamespace := "demo_postgres"
	geDatasetURN := geNamespace + "/customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES
			($1, 'dbt_transform_customers', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'dbt'),
			($5, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $6, $7, $8, 'great_expectations')
	`, dbtRunID, now.Add(-10*time.Minute), now.Add(-15*time.Minute), now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2), ($3, 'customers', $4)
	`, dbtDatasetURN, dbtNamespace, geDatasetURN, geNamespace)
	require.NoError(t, err)

	// dbt produces output (has lineage edge)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, dbtDatasetURN)
	require.NoError(t, err)

	// GE has test results on the GE-format URN (which is orphan without pattern resolution)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES
			('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found 3 nulls', $3, 100),
			('unique_customers_email', 'unique', $1, $2, 'failed', 'Found 2 duplicates', $4, 150)
	`, geDatasetURN, geRunID, now, now.Add(1*time.Minute))
	require.NoError(t, err)

	// Create pattern resolver: demo_postgres/{name} → postgresql://demo/marts.{name}
	patternCfg := &aliasing.Config{
		DatasetPatterns: []aliasing.DatasetPattern{
			{
				Pattern:   "demo_postgres/{name}",
				Canonical: "postgresql://demo/marts.{name}",
			},
		},
	}
	resolver := aliasing.NewResolver(patternCfg)
	require.Equal(t, 1, resolver.GetPatternCount(), "Should have 1 pattern")

	// Verify pattern resolves correctly (sanity check)
	resolved := resolver.Resolve(geDatasetURN)
	require.Equal(t, dbtDatasetURN, resolved, "Pattern should resolve GE URN to dbt URN")

	// Create LineageStore WITH pattern resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour, WithAliasResolver(resolver))
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views (needed for incident_correlation_view)
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Query incidents with pattern resolution
	result, err := store.QueryIncidents(ctx, nil, nil)
	require.NoError(t, err)

	// TC-002 Core Assertion: GE incidents should correlate to dbt job run
	assert.Len(t, result.Incidents, 2, "Should have 2 incidents (both GE test failures)")

	// Verify each incident correlates to the dbt job run (not GE job run)
	for _, incident := range result.Incidents {
		assert.Equal(t, dbtDatasetURN, incident.DatasetURN,
			"Incident should show canonical dataset URN (dbt format)")
		assert.Equal(t, dbtRunID, incident.RunID,
			"Incident should correlate to dbt job run (producer)")
		assert.Equal(t, "dbt", incident.ProducerName,
			"Producer should be dbt")
		assert.Equal(t, "dbt_transform_customers", incident.JobName,
			"Job name should be dbt transform job")
	}
}

// TestQueryIncidents_WithPatternResolution_MultipleDatasets tests pattern resolution
// across multiple datasets with the same pattern.
func TestQueryIncidents_WithPatternResolution_MultipleDatasets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: Multiple datasets resolved by same pattern
	now := time.Now()
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	// dbt produces two tables
	dbtCustomersURN := "postgresql://demo/marts.customers"
	dbtOrdersURN := "postgresql://demo/marts.orders"

	// GE tests in different format
	geCustomersURN := "demo_postgres/customers"
	geOrdersURN := "demo_postgres/orders"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES
			($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'dbt'),
			($5, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $6, $7, $8, 'great_expectations')
	`, dbtRunID, now.Add(-10*time.Minute), now.Add(-15*time.Minute), now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES
			($1, 'customers', 'postgresql://demo/marts'),
			($2, 'orders', 'postgresql://demo/marts'),
			($3, 'customers', 'demo_postgres'),
			($4, 'orders', 'demo_postgres')
	`, dbtCustomersURN, dbtOrdersURN, geCustomersURN, geOrdersURN)
	require.NoError(t, err)

	// dbt produces both tables
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output'), ($1, $3, 'output')
	`, dbtRunID, dbtCustomersURN, dbtOrdersURN)
	require.NoError(t, err)

	// GE tests both tables (fails on customers, passes on orders)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES
			('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100),
			('not_null_orders_id', 'not_null', $4, $2, 'failed', 'Found nulls', $5, 100)
	`, geCustomersURN, geRunID, now, geOrdersURN, now.Add(1*time.Minute))
	require.NoError(t, err)

	// Create pattern resolver
	patternCfg := &aliasing.Config{
		DatasetPatterns: []aliasing.DatasetPattern{
			{
				Pattern:   "demo_postgres/{name}",
				Canonical: "postgresql://demo/marts.{name}",
			},
		},
	}
	resolver := aliasing.NewResolver(patternCfg)

	// Create LineageStore WITH pattern resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour, WithAliasResolver(resolver))
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Query incidents
	result, err := store.QueryIncidents(ctx, nil, nil)
	require.NoError(t, err)

	// Should have 2 incidents (one per dataset), both correlated to dbt job
	assert.Len(t, result.Incidents, 2, "Should have 2 incidents")

	// Build map for easier assertion
	incidentsByDataset := make(map[string]*correlation.Incident)
	for i := range result.Incidents {
		incidentsByDataset[result.Incidents[i].DatasetURN] = &result.Incidents[i]
	}

	// Verify customers incident
	customersIncident, ok := incidentsByDataset[dbtCustomersURN]
	require.True(t, ok, "Should have customers incident with canonical URN")
	assert.Equal(t, dbtRunID, customersIncident.RunID)

	// Verify orders incident
	ordersIncident, ok := incidentsByDataset[dbtOrdersURN]
	require.True(t, ok, "Should have orders incident with canonical URN")
	assert.Equal(t, dbtRunID, ordersIncident.RunID)
}

// TestQueryIncidents_NoPatternResolver tests that without resolver, uncorrelated
// incidents are not returned (existing behavior).
func TestQueryIncidents_NoPatternResolver(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: Same data as TC-002 but WITHOUT pattern resolver
	now := time.Now()
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	dbtDatasetURN := "postgresql://demo/marts.customers"
	geDatasetURN := "demo_postgres/customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES
			($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'dbt'),
			($5, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $6, $7, $8, 'great_expectations')
	`, dbtRunID, now.Add(-10*time.Minute), now.Add(-15*time.Minute), now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', 'postgresql://demo/marts'), ($2, 'customers', 'demo_postgres')
	`, dbtDatasetURN, geDatasetURN)
	require.NoError(t, err)

	// dbt produces output
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, dbtDatasetURN)
	require.NoError(t, err)

	// GE tests with different URN format
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, geDatasetURN, geRunID, now)
	require.NoError(t, err)

	// Create LineageStore WITHOUT pattern resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour) // No WithAliasResolver
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Without pattern resolver, GE incident should NOT be correlated
	result, err := store.QueryIncidents(ctx, nil, nil)
	require.NoError(t, err)

	// The GE test failure is on a dataset_urn that has no output edge (orphan)
	// So it won't appear in incident_correlation_view
	assert.Empty(t, result.Incidents, "Without pattern resolver, orphan incidents are not returned")
}

// TestCorrelationHealth_WithPatternResolution tests that correlation rate improves
// when patterns are configured.
func TestCorrelationHealth_WithPatternResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup TC-002 scenario
	now := time.Now()
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	dbtDatasetURN := "postgresql://demo/marts.customers"
	geDatasetURN := "demo_postgres/customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES
			($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'dbt'),
			($5, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $6, $7, $8, 'great_expectations')
	`, dbtRunID, now.Add(-10*time.Minute), now.Add(-15*time.Minute), now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', 'postgresql://demo/marts'), ($2, 'customers', 'demo_postgres')
	`, dbtDatasetURN, geDatasetURN)
	require.NoError(t, err)

	// dbt produces output
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, dbtDatasetURN)
	require.NoError(t, err)

	// GE tests with different URN format
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, geDatasetURN, geRunID, now)
	require.NoError(t, err)

	// Create pattern resolver
	patternCfg := &aliasing.Config{
		DatasetPatterns: []aliasing.DatasetPattern{
			{
				Pattern:   "demo_postgres/{name}",
				Canonical: "postgresql://demo/marts.{name}",
			},
		},
	}
	resolver := aliasing.NewResolver(patternCfg)

	// Create LineageStore WITH pattern resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour, WithAliasResolver(resolver))
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Correlation health should show 100% with pattern resolution
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	// With pattern resolution, the GE test failure should correlate
	// The correlation rate should be 100% (1 correlated / 1 total)
	assert.InDelta(t, 1.0, health.CorrelationRate, 0.001,
		"With pattern resolution, correlation rate should be 100%")
	assert.Empty(t, health.OrphanDatasets,
		"With pattern resolution, no orphan datasets should exist")
}

// TestQueryIncidents_WithPatternResolution_LargeDataset tests pagination with many incidents.
// This verifies memory-efficient pagination (two-phase query approach).
func TestQueryIncidents_WithPatternResolution_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	now := time.Now()
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES
			($1, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'dbt'),
			($5, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $6, $7, $8, 'great_expectations')
	`, dbtRunID, now.Add(-10*time.Minute), now.Add(-15*time.Minute), now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert 100 datasets and test results (simulating larger dataset)
	const numDatasets = 100

	for i := range numDatasets {
		tableName := fmt.Sprintf("table_%03d", i)
		dbtURN := "postgresql://demo/marts." + tableName
		geURN := "demo_postgres/" + tableName

		// Insert datasets
		_, err := testDB.Connection.ExecContext(ctx, `
			INSERT INTO datasets (dataset_urn, name, namespace)
			VALUES ($1, $2, 'postgresql://demo/marts'), ($3, $2, 'demo_postgres')
		`, dbtURN, tableName, geURN)
		require.NoError(t, err)

		// dbt produces output
		_, err = testDB.Connection.ExecContext(ctx, `
			INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
			VALUES ($1, $2, 'output')
		`, dbtRunID, dbtURN)
		require.NoError(t, err)

		// GE tests in different format
		_, err = testDB.Connection.ExecContext(ctx, `
			INSERT INTO test_results (
				test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
			)
			VALUES ($1, 'not_null', $2, $3, 'failed', 'Found nulls', $4, 100)
		`, fmt.Sprintf("not_null_%s_id", tableName), geURN, geRunID, now.Add(time.Duration(i)*time.Second))
		require.NoError(t, err)
	}

	// Create pattern resolver
	patternCfg := &aliasing.Config{
		DatasetPatterns: []aliasing.DatasetPattern{
			{
				Pattern:   "demo_postgres/{name}",
				Canonical: "postgresql://demo/marts.{name}",
			},
		},
	}
	resolver := aliasing.NewResolver(patternCfg)

	// Create LineageStore with resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour, WithAliasResolver(resolver))
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test pagination with pattern resolution
	pageSize := 10
	pagination := &correlation.Pagination{Offset: 0, Limit: pageSize}

	result, err := store.QueryIncidents(ctx, nil, pagination)
	require.NoError(t, err)

	// Verify pagination works correctly
	assert.Equal(t, numDatasets, result.Total, "Total should be %d", numDatasets)
	assert.Len(t, result.Incidents, pageSize, "Should return exactly page size")

	// Verify all incidents correlate to dbt job
	for _, incident := range result.Incidents {
		assert.Equal(t, dbtRunID, incident.RunID, "Should correlate to dbt job")
		assert.Equal(t, "dbt", incident.ProducerName)
		assert.Contains(t, incident.DatasetURN, "postgresql://demo/marts.",
			"Should use canonical URN")
	}

	// Test second page
	pagination.Offset = 10
	result2, err := store.QueryIncidents(ctx, nil, pagination)
	require.NoError(t, err)

	assert.Equal(t, numDatasets, result2.Total, "Total should still be %d", numDatasets)
	assert.Len(t, result2.Incidents, pageSize, "Should return second page")

	// Verify no overlap between pages
	page1IDs := make(map[int64]bool)
	for _, inc := range result.Incidents {
		page1IDs[inc.TestResultID] = true
	}

	for _, inc := range result2.Incidents {
		assert.False(t, page1IDs[inc.TestResultID], "Pages should not overlap")
	}

	t.Logf("Successfully paginated %d incidents with pattern resolution", numDatasets)
}

// TestQueryIncidentByID_WithPatternResolution tests that QueryIncidentByID applies
// pattern resolution to correlate a single incident across different URN formats.
//
// This test verifies that when a user clicks on a GE incident in the UI, the
// incident detail page shows the correct correlation to the dbt producer job.
func TestQueryIncidentByID_WithPatternResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup TC-002 scenario (same as TestQueryIncidents_WithPatternResolution)
	now := time.Now()

	// Job runs
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	// dbt produces in canonical format
	dbtNamespace := "postgresql://demo/marts"
	dbtDatasetURN := dbtNamespace + ".customers"

	// GE tests in different format (same logical table)
	geNamespace := "demo_postgres"
	geDatasetURN := geNamespace + "/customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES
			($1, 'dbt_transform_customers', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'dbt'),
			($5, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $6, $7, $8, 'great_expectations')
	`, dbtRunID, now.Add(-10*time.Minute), now.Add(-15*time.Minute), now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2), ($3, 'customers', $4)
	`, dbtDatasetURN, dbtNamespace, geDatasetURN, geNamespace)
	require.NoError(t, err)

	// dbt produces output (has lineage edge)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, dbtDatasetURN)
	require.NoError(t, err)

	// GE has test results on the GE-format URN
	// Insert and capture the test result ID
	var testResultID int64

	err = testDB.Connection.QueryRowContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found 3 nulls', $3, 100)
		RETURNING id
	`, geDatasetURN, geRunID, now).Scan(&testResultID)
	require.NoError(t, err)

	// Create pattern resolver: demo_postgres/{name} → postgresql://demo/marts.{name}
	patternCfg := &aliasing.Config{
		DatasetPatterns: []aliasing.DatasetPattern{
			{
				Pattern:   "demo_postgres/{name}",
				Canonical: "postgresql://demo/marts.{name}",
			},
		},
	}
	resolver := aliasing.NewResolver(patternCfg)
	require.Equal(t, 1, resolver.GetPatternCount(), "Should have 1 pattern")

	// Create LineageStore WITH pattern resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour, WithAliasResolver(resolver))
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views (needed for incident_correlation_view)
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Query incident by ID with pattern resolution
	incident, err := store.QueryIncidentByID(ctx, testResultID)
	require.NoError(t, err)
	require.NotNil(t, incident, "Incident should be found via pattern resolution")

	// Core Assertions: Incident should correlate to dbt job run
	assert.Equal(t, testResultID, incident.TestResultID,
		"Test result ID should match")
	assert.Equal(t, "not_null_customers_id", incident.TestName,
		"Test name should be from GE test")
	assert.Equal(t, "not_null", incident.TestType,
		"Test type should be from GE test")
	assert.Equal(t, "failed", incident.TestStatus,
		"Test status should be from GE test")
	assert.Equal(t, "Found 3 nulls", incident.TestMessage,
		"Test message should be from GE test")

	// Dataset should show canonical URN (dbt format)
	assert.Equal(t, dbtDatasetURN, incident.DatasetURN,
		"Incident should show canonical dataset URN (dbt format)")

	// Job should be dbt (producer), not GE (test runner)
	assert.Equal(t, dbtRunID, incident.RunID,
		"Incident should correlate to dbt job run (producer)")
	assert.Equal(t, "dbt", incident.ProducerName,
		"Producer should be dbt")
	assert.Equal(t, "dbt_transform_customers", incident.JobName,
		"Job name should be dbt transform job")
}

// TestQueryIncidentByID_WithPatternResolution_NotFound tests that QueryIncidentByID
// returns nil when the test result exists but doesn't correlate to any producer.
func TestQueryIncidentByID_WithPatternResolution_NotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	now := time.Now()
	geRunID := uuid.New().String()

	// GE tests on a dataset with NO producer
	geNamespace := "orphan_namespace"
	geDatasetURN := geNamespace + "/orphan_table"

	// Insert job run
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES ($1, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'great_expectations')
	`, geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert dataset (no lineage edge - orphan)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'orphan_table', $2)
	`, geDatasetURN, geNamespace)
	require.NoError(t, err)

	// Insert test result
	var testResultID int64

	err = testDB.Connection.QueryRowContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_orphan_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
		RETURNING id
	`, geDatasetURN, geRunID, now).Scan(&testResultID)
	require.NoError(t, err)

	// Create pattern resolver with a pattern that doesn't match the orphan
	patternCfg := &aliasing.Config{
		DatasetPatterns: []aliasing.DatasetPattern{
			{
				Pattern:   "demo_postgres/{name}",
				Canonical: "postgresql://demo/marts.{name}",
			},
		},
	}
	resolver := aliasing.NewResolver(patternCfg)

	// Create LineageStore WITH pattern resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour, WithAliasResolver(resolver))
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Query incident by ID - should return nil (no producer found)
	incident, err := store.QueryIncidentByID(ctx, testResultID)
	require.NoError(t, err)
	assert.Nil(t, incident, "Incident should be nil when no producer found")
}

// TestQueryIncidentByID_WithPatternResolution_PassedTest tests that QueryIncidentByID
// returns nil for passed tests (only failed/error tests are incidents).
func TestQueryIncidentByID_WithPatternResolution_PassedTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	now := time.Now()
	dbtRunID := uuid.New().String()
	geRunID := uuid.New().String()

	dbtNamespace := "postgresql://demo/marts"
	dbtDatasetURN := dbtNamespace + ".customers"
	geNamespace := "demo_postgres"
	geDatasetURN := geNamespace + "/customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			run_id, job_name, job_namespace, current_state,
			event_type, event_time, started_at, completed_at, producer_name
		)
		VALUES
			($1, 'dbt_transform_customers', 'dbt_prod', 'COMPLETE', 'COMPLETE', $2, $3, $4, 'dbt'),
			($5, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $6, $7, $8, 'great_expectations')
	`, dbtRunID, now.Add(-10*time.Minute), now.Add(-15*time.Minute), now.Add(-10*time.Minute),
		geRunID, now, now.Add(-5*time.Minute), now)
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2), ($3, 'customers', $4)
	`, dbtDatasetURN, dbtNamespace, geDatasetURN, geNamespace)
	require.NoError(t, err)

	// dbt produces output
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtRunID, dbtDatasetURN)
	require.NoError(t, err)

	// Insert PASSED test result (not an incident)
	var testResultID int64

	err = testDB.Connection.QueryRowContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'passed', 'All values non-null', $3, 100)
		RETURNING id
	`, geDatasetURN, geRunID, now).Scan(&testResultID)
	require.NoError(t, err)

	// Create pattern resolver
	patternCfg := &aliasing.Config{
		DatasetPatterns: []aliasing.DatasetPattern{
			{
				Pattern:   "demo_postgres/{name}",
				Canonical: "postgresql://demo/marts.{name}",
			},
		},
	}
	resolver := aliasing.NewResolver(patternCfg)

	// Create LineageStore WITH pattern resolver
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour, WithAliasResolver(resolver))
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Test: Query incident by ID - should return nil (passed test is not an incident)
	incident, err := store.QueryIncidentByID(ctx, testResultID)
	require.NoError(t, err)
	assert.Nil(t, incident, "Incident should be nil for passed tests")
}

// TestParentRunFacetCorrelation tests that parent-child job relationships are correctly
// correlated through the materialized view and returned in incident queries.
//
// This test verifies:
//  1. Materialized view correctly JOINs parent job data via parent_run_id.
//  2. QueryIncidentByID returns parent job fields (name, status, completed_at).
//
// Note: ParentRunFacet extraction and storage is tested in lineage_store_integration_test.go.
func TestParentRunFacetCorrelation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test data
	parentRunUUID := uuid.New().String()
	childRunUUID := uuid.New().String()
	parentJobNamespace := "dbt://demo"
	parentJobName := "jaffle_shop.build"
	childJobName := "model.jaffle_shop.orders"
	datasetURN := "postgresql://demo/marts.orders"
	now := time.Now()

	// Build ParentRunFacet as map[string]interface{} (as it comes from JSON)
	parentRunFacet := map[string]interface{}{
		"job": map[string]interface{}{
			"namespace": parentJobNamespace,
			"name":      parentJobName,
		},
		"run": map[string]interface{}{
			"runId": parentRunUUID,
		},
	}

	// Step 1: Ingest parent job (START then COMPLETE)
	parentStartEvent := &ingestion.RunEvent{
		EventTime: now.Add(-5 * time.Minute),
		EventType: ingestion.EventTypeStart,
		Producer:  "https://github.com/correlator-io/correlator-dbt/0.1.2",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: ingestion.Run{
			ID:     parentRunUUID,
			Facets: map[string]interface{}{},
		},
		Job: ingestion.Job{
			Namespace: parentJobNamespace,
			Name:      parentJobName,
			Facets:    map[string]interface{}{},
		},
		Inputs:  []ingestion.Dataset{},
		Outputs: []ingestion.Dataset{},
	}

	stored, _, err := store.StoreEvent(ctx, parentStartEvent)
	require.NoError(t, err)
	require.True(t, stored, "Parent START event should be stored")

	parentCompleteEvent := &ingestion.RunEvent{
		EventTime: now.Add(-1 * time.Minute),
		EventType: ingestion.EventTypeComplete,
		Producer:  "https://github.com/correlator-io/correlator-dbt/0.1.2",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: ingestion.Run{
			ID:     parentRunUUID,
			Facets: map[string]interface{}{},
		},
		Job: ingestion.Job{
			Namespace: parentJobNamespace,
			Name:      parentJobName,
			Facets:    map[string]interface{}{},
		},
		Inputs:  []ingestion.Dataset{},
		Outputs: []ingestion.Dataset{},
	}

	stored, _, err = store.StoreEvent(ctx, parentCompleteEvent)
	require.NoError(t, err)
	require.True(t, stored, "Parent COMPLETE event should be stored")

	// Step 2: Ingest child job with ParentRunFacet
	childEvent := &ingestion.RunEvent{
		EventTime: now.Add(-3 * time.Minute),
		EventType: ingestion.EventTypeRunning,
		Producer:  "https://github.com/correlator-io/correlator-dbt/0.1.2",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: ingestion.Run{
			ID: childRunUUID,
			Facets: map[string]interface{}{
				"parent": parentRunFacet,
			},
		},
		Job: ingestion.Job{
			Namespace: parentJobNamespace,
			Name:      childJobName,
			Facets:    map[string]interface{}{},
		},
		Inputs: []ingestion.Dataset{},
		Outputs: []ingestion.Dataset{
			{
				Namespace: "postgresql://demo",
				Name:      "marts.orders",
				Facets:    map[string]interface{}{},
			},
		},
	}

	stored, _, err = store.StoreEvent(ctx, childEvent)
	require.NoError(t, err)
	require.True(t, stored, "Child event with ParentRunFacet should be stored")

	expectedParentRunID := parentRunUUID
	childRunID := childEvent.Run.ID

	// Step 3: Insert a failed test result for the child's output dataset
	var testResultID int64 = 1

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			id, test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms, producer_name
		) VALUES ($1, 'not_null_orders_id', 'not_null', $2, $3, 'failed', 'Found 3 null values', $4, 150, 'correlator-ge')
	`, testResultID, datasetURN, childRunID, now)
	require.NoError(t, err)

	// Step 4: Refresh materialized views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Step 5: Query incident and verify parent fields
	incident, err := store.QueryIncidentByID(ctx, testResultID)
	require.NoError(t, err)
	require.NotNil(t, incident, "Incident should be found")

	// Verify child job fields
	assert.Equal(t, childJobName, incident.JobName)
	assert.Equal(t, "RUNNING", incident.JobStatus, "Child job should be RUNNING")

	// Verify parent job fields are populated
	assert.Equal(t, expectedParentRunID, incident.ParentRunID, "ParentRunID should match")
	assert.Equal(t, parentJobName, incident.ParentJobName, "ParentJobName should match")
	assert.Equal(t, "COMPLETE", incident.ParentJobStatus, "ParentJobStatus should be COMPLETE")
	assert.NotNil(t, incident.ParentJobCompletedAt, "ParentJobCompletedAt should be populated")
}

// TestParentRunFacetOutOfOrder tests that child events can arrive before parent.
func TestParentRunFacetOutOfOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test data
	parentRunUUID := uuid.New().String()
	childRunUUID := uuid.New().String()
	parentJobNamespace := "dbt://demo"
	parentJobName := "jaffle_shop.build"
	childJobName := "model.jaffle_shop.orders"
	datasetURN := "postgresql://demo/marts.orders"
	now := time.Now()

	parentRunFacet := map[string]interface{}{
		"job": map[string]interface{}{
			"namespace": parentJobNamespace,
			"name":      parentJobName,
		},
		"run": map[string]interface{}{
			"runId": parentRunUUID,
		},
	}

	// Step 1: Ingest ONLY child job (parent hasn't arrived yet)
	childEvent := &ingestion.RunEvent{
		EventTime: now.Add(-3 * time.Minute),
		EventType: ingestion.EventTypeRunning,
		Producer:  "https://github.com/correlator-io/correlator-dbt/0.1.2",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: ingestion.Run{
			ID: childRunUUID,
			Facets: map[string]interface{}{
				"parent": parentRunFacet,
			},
		},
		Job: ingestion.Job{
			Namespace: parentJobNamespace,
			Name:      childJobName,
			Facets:    map[string]interface{}{},
		},
		Inputs: []ingestion.Dataset{},
		Outputs: []ingestion.Dataset{
			{
				Namespace: "postgresql://demo",
				Name:      "marts.orders",
				Facets:    map[string]interface{}{},
			},
		},
	}

	stored, _, err := store.StoreEvent(ctx, childEvent)
	require.NoError(t, err)
	require.True(t, stored)

	childRunID := childEvent.Run.ID

	// Step 2: Insert test result
	var testResultID int64 = 1

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			id, test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms, producer_name
		) VALUES ($1, 'not_null_orders_id', 'not_null', $2, $3, 'failed', 'Found 3 null values', $4, 150, 'correlator-ge')
	`, testResultID, datasetURN, childRunID, now)
	require.NoError(t, err)

	// Step 3: Refresh views
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Step 4: Query incident - parent fields should be empty (parent not ingested)
	incident, err := store.QueryIncidentByID(ctx, testResultID)
	require.NoError(t, err)
	require.NotNil(t, incident)

	// Parent run ID is stored (reference exists)
	expectedParentRunID := parentRunUUID
	assert.Equal(t, expectedParentRunID, incident.ParentRunID, "ParentRunID should be stored")

	// But parent fields are empty because parent job_run doesn't exist yet
	assert.Empty(t, incident.ParentJobName, "ParentJobName should be empty")
	assert.Empty(t, incident.ParentJobStatus, "ParentJobStatus should be empty")
	assert.Nil(t, incident.ParentJobCompletedAt, "ParentJobCompletedAt should be nil")

	// Step 5: Now ingest parent job
	parentEvent := &ingestion.RunEvent{
		EventTime: now.Add(-1 * time.Minute),
		EventType: ingestion.EventTypeComplete,
		Producer:  "https://github.com/correlator-io/correlator-dbt/0.1.2",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: ingestion.Run{
			ID:     parentRunUUID,
			Facets: map[string]interface{}{},
		},
		Job: ingestion.Job{
			Namespace: parentJobNamespace,
			Name:      parentJobName,
			Facets:    map[string]interface{}{},
		},
		Inputs:  []ingestion.Dataset{},
		Outputs: []ingestion.Dataset{},
	}

	stored, _, err = store.StoreEvent(ctx, parentEvent)
	require.NoError(t, err)
	require.True(t, stored)

	// Step 6: Refresh views again
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)

	err = store.refreshViews(ctx)
	require.NoError(t, err)

	// Step 7: Query incident again - parent fields should now be populated
	incident, err = store.QueryIncidentByID(ctx, testResultID)
	require.NoError(t, err)
	require.NotNil(t, incident)

	assert.Equal(t, expectedParentRunID, incident.ParentRunID)
	assert.Equal(t, parentJobName, incident.ParentJobName, "ParentJobName should now be populated")
	assert.Equal(t, "COMPLETE", incident.ParentJobStatus, "ParentJobStatus should be COMPLETE")
	assert.NotNil(t, incident.ParentJobCompletedAt, "ParentJobCompletedAt should be populated")
}
