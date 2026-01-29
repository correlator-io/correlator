package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/correlation"
)

// filterImpactResults is a test helper that filters impact results by jobRunID and depth.
func filterImpactResults(results []correlation.ImpactResult, jobRunID string, depth int) []correlation.ImpactResult {
	var filtered []correlation.ImpactResult

	for _, r := range results {
		if r.JobRunID == jobRunID && r.Depth == depth {
			filtered = append(filtered, r)
		}
	}

	return filtered
}

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
	err = store.RefreshViews(ctx)
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
	// Use canonical job_run_id format: "tool:runID"
	jobRunID1 := "dbt:" + uuid.New().String()
	jobRunID2 := "airflow:" + uuid.New().String()
	datasetURN1 := "urn:postgres:warehouse:public.customers"
	datasetURN2 := "urn:postgres:warehouse:public.orders"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, $2, 'transform_customers', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt'),
			($5, $6, 'extract_orders', 'airflow_prod', 'FAIL', 'FAIL', $7, $8, 'airflow')
	`, jobRunID1, uuid.New().String(), now, now.Add(-5*time.Minute),
		jobRunID2, uuid.New().String(), now.Add(-1*time.Hour), now.Add(-65*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', 'public'), ($2, 'orders', 'public')
	`, datasetURN1, datasetURN2)
	require.NoError(t, err)

	// Insert lineage edges
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output'), ($3, $4, 'output')
	`, jobRunID1, datasetURN1, jobRunID2, datasetURN2)
	require.NoError(t, err)

	// Insert test results
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
		  id, test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms
		)
		VALUES
			(1, 'not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found 2 nulls', $3, 120),
			(2, 'unique_orders_id', 'unique', $4, $5, 'passed', 'All unique', $6, 150)
	`, datasetURN1, jobRunID1, now,
		datasetURN2, jobRunID2, now.Add(-1*time.Hour))
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)

	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.RefreshViews(ctx)
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

	// Test 3: Filter by job_run_id
	filter = &correlation.IncidentFilter{
		JobRunID: &jobRunID1,
	}

	result, err = store.QueryIncidents(ctx, filter, nil)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 incident for job_run_id")
	assert.Equal(t, jobRunID1, result.Incidents[0].JobRunID)

	// Test 4: Filter by tool (extracted from canonical job_run_id)
	toolDBT := "dbt"
	filter = &correlation.IncidentFilter{
		Tool: &toolDBT,
	}

	result, err = store.QueryIncidents(ctx, filter, nil)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 dbt incident")
	assert.Equal(t, "dbt", result.Incidents[0].ProducerName)
	// Verify job_run_id starts with "dbt:"
	assert.Contains(t, result.Incidents[0].JobRunID, "dbt:", "Job run ID should contain 'dbt:' prefix")

	// Test 4b: Filter by tool that doesn't exist
	toolSpark := "spark"
	filter = &correlation.IncidentFilter{
		Tool: &toolSpark,
	}

	result, err = store.QueryIncidents(ctx, filter, nil)
	require.NoError(t, err)

	assert.Empty(t, result.Incidents, "Should return 0 spark incidents")
	assert.Equal(t, 0, result.Total, "Total should be 0")

	// Test 5: Filter by time range (recent tests only)
	recentTime := now.Add(-30 * time.Minute)
	filter = &correlation.IncidentFilter{
		TestExecutedAfter: &recentTime,
	}

	result, err = store.QueryIncidents(ctx, filter, nil)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 recent test")

	// Test 6: Pagination
	pagination := &correlation.Pagination{Limit: 10, Offset: 0}
	result, err = store.QueryIncidents(ctx, nil, pagination)
	require.NoError(t, err)

	assert.Len(t, result.Incidents, 1, "Should return 1 incident with pagination")
	assert.Equal(t, 1, result.Total, "Total should reflect all matching incidents")

	// Test 7: Pagination with offset beyond results
	// Note: When offset exceeds total rows, COUNT(*) OVER() returns 0 because there are no rows to scan.
	// This is a known limitation of the window function approach. For MVP, this is acceptable.
	pagination = &correlation.Pagination{Limit: 10, Offset: 100}
	result, err = store.QueryIncidents(ctx, nil, pagination)
	require.NoError(t, err)

	assert.Empty(t, result.Incidents, "Should return no incidents when offset exceeds total")
	assert.Equal(t, 0, result.Total, "Total is 0 when offset exceeds results (COUNT(*) OVER() limitation)")
}

// TestQueryLineageImpact tests the QueryLineageImpact function.
func TestQueryLineageImpact(t *testing.T) {
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
	now := time.Now()
	jobRunID1 := uuid.New().String()
	jobRunID2 := uuid.New().String()
	jobRunID3 := uuid.New().String()
	datasetA := "urn:postgres:warehouse:public.raw_orders"
	datasetB := "urn:postgres:warehouse:public.staged_orders"
	datasetC := "urn:postgres:warehouse:public.fact_orders"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, $2, 'extract_orders', 'etl', 'COMPLETE', 'COMPLETE', $3, $4, 'airflow'),
			($5, $6, 'stage_orders', 'etl', 'COMPLETE', 'COMPLETE', $7, $8, 'airflow'),
			($9, $10, 'transform_orders', 'dbt', 'COMPLETE', 'COMPLETE', $11, $12, 'dbt')
	`, jobRunID1, uuid.New().String(), now, now.Add(-10*time.Minute),
		jobRunID2, uuid.New().String(), now, now.Add(-10*time.Minute),
		jobRunID3, uuid.New().String(), now, now.Add(-10*time.Minute))
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
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES
			($1, $2, 'output'),  -- job1 produces datasetA
			($3, $2, 'input'),   -- job2 consumes datasetA
			($3, $4, 'output'),  -- job2 produces datasetB
			($5, $4, 'input'),   -- job3 consumes datasetB
			($5, $6, 'output')   -- job3 produces datasetC
	`, jobRunID1, datasetA,
		jobRunID2, datasetB,
		jobRunID3, datasetC)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)

	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.RefreshViews(ctx)
	require.NoError(t, err)

	// Test 1: Query all depths (maxDepth = 0)
	impact, err := store.QueryLineageImpact(ctx, jobRunID1, 0)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, len(impact), 1, "Should have at least 1 impact result")

	// Verify direct output (depth 0)
	directOutputs := filterImpactResults(impact, jobRunID1, 0)
	assert.Len(t, directOutputs, 1, "Job1 should have 1 direct output")
	assert.Equal(t, datasetA, directOutputs[0].DatasetURN)

	// Test 2: Query only direct outputs (maxDepth = -1)
	impact, err = store.QueryLineageImpact(ctx, jobRunID2, -1)
	require.NoError(t, err)

	assert.Len(t, impact, 1, "Should return only direct outputs")
	assert.Equal(t, 0, impact[0].Depth, "Depth should be 0")
	assert.Equal(t, datasetB, impact[0].DatasetURN)

	// Test 3: Query with depth limit (maxDepth = 1)
	impact, err = store.QueryLineageImpact(ctx, jobRunID1, 1)
	require.NoError(t, err)

	// Verify all depths <= 1
	for _, r := range impact {
		assert.LessOrEqual(t, r.Depth, 1, "Depth should be <= 1")
	}

	// Test 4: Non-existent job run should return empty slice
	impact, err = store.QueryLineageImpact(ctx, "non-existent-id", 0)
	require.NoError(t, err)

	assert.Empty(t, impact, "Should return empty slice for non-existent job")
}

// TestQueryOrphanNamespaces tests orphan namespace detection.
// An orphan namespace is one where validation tests exist but no data producer output edges exist.
func TestQueryOrphanNamespaces(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup test data:
	// - Namespace "postgres_prod" has test results from GE but NO producer output edges (ORPHAN)
	// - Namespace "postgresql://prod/public" has BOTH test results AND producer output edges (NOT ORPHAN)
	now := time.Now()

	// Job runs for both scenarios
	geJobRunID := "great_expectations:" + uuid.New().String()   // Validator job (GE)
	dbtJobRunID := "dbt:" + uuid.New().String()                 // Producer job (dbt)
	sodaJobRunID := "soda:" + uuid.New().String()               // Another validator job (Soda) for orphan namespace

	// Orphan namespace: "postgres_prod" - only has test results, no output edges
	orphanNamespace := "postgres_prod"
	orphanDatasetURN := orphanNamespace + "/public.orders"

	// Healthy namespace: "postgresql://prod/public" - has both test results AND output edges
	healthyNamespace := "postgresql://prod/public"
	healthyDatasetURN := healthyNamespace + ".customers"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, $2, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $3, $4, 'great_expectations'),
			($5, $6, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $7, $8, 'dbt'),
			($9, $10, 'soda_check', 'validation', 'COMPLETE', 'COMPLETE', $11, $12, 'soda')
	`, geJobRunID, uuid.New().String(), now, now.Add(-5*time.Minute),
		dbtJobRunID, uuid.New().String(), now, now.Add(-10*time.Minute),
		sodaJobRunID, uuid.New().String(), now.Add(-1*time.Hour), now.Add(-65*time.Minute))
	require.NoError(t, err)

	// Insert datasets with different namespaces
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES
			($1, 'orders', $2),
			($3, 'customers', $4)
	`, orphanDatasetURN, orphanNamespace, healthyDatasetURN, healthyNamespace)
	require.NoError(t, err)

	// Insert lineage edges:
	// - dbt produces output for healthy namespace (NOT orphan)
	// - NO output edges for orphan namespace
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtJobRunID, healthyDatasetURN)
	require.NoError(t, err)

	// Insert test results for BOTH namespaces
	// GE tests orphan namespace, dbt tests healthy namespace
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms
		)
		VALUES
			('not_null_orders_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100),
			('unique_orders_id', 'unique', $1, $4, 'failed', 'Duplicates found', $5, 120),
			('not_null_customers_id', 'not_null', $6, $7, 'failed', 'Found nulls', $8, 80)
	`, orphanDatasetURN, geJobRunID, now,
		sodaJobRunID, now.Add(-1*time.Hour),
		healthyDatasetURN, dbtJobRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.RefreshViews(ctx)
	require.NoError(t, err)

	// Test: Query orphan namespaces
	orphans, err := store.QueryOrphanNamespaces(ctx)
	require.NoError(t, err)

	// Verify only the orphan namespace is returned
	assert.Len(t, orphans, 2, "Should return 2 orphan entries (GE and Soda for same namespace)")

	// Build map of orphan namespaces for easier assertion
	orphanMap := make(map[string][]correlation.OrphanNamespace)
	for _, o := range orphans {
		orphanMap[o.Namespace] = append(orphanMap[o.Namespace], o)
	}

	// Verify postgres_prod is orphan (has test results but no output edges)
	assert.Contains(t, orphanMap, orphanNamespace, "postgres_prod should be orphan")
	assert.Len(t, orphanMap[orphanNamespace], 2, "Should have 2 entries for orphan namespace (GE and Soda)")

	// Verify healthy namespace is NOT orphan
	assert.NotContains(t, orphanMap, healthyNamespace, "postgresql://prod/public should NOT be orphan")

	// Verify GE entry details
	var geEntry *correlation.OrphanNamespace

	for i := range orphanMap[orphanNamespace] {
		if orphanMap[orphanNamespace][i].Producer == "great_expectations" {
			geEntry = &orphanMap[orphanNamespace][i]

			break
		}
	}

	require.NotNil(t, geEntry, "Should have GE entry for orphan namespace")
	assert.Equal(t, "great_expectations", geEntry.Producer)
	assert.Equal(t, 1, geEntry.EventCount, "GE should have 1 test result")
	assert.False(t, geEntry.LastSeen.IsZero(), "LastSeen should be set")
}

// TestQueryOrphanNamespaces_HealthyState tests that no orphans are returned when all namespaces have producers.
func TestQueryOrphanNamespaces_HealthyState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: Namespace has BOTH validator tests AND producer output edges
	now := time.Now()
	dbtJobRunID := "dbt:" + uuid.New().String()
	namespace := "postgresql://prod/public"
	datasetURN := namespace + ".customers"

	// Insert job run
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES ($1, $2, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt')
	`, dbtJobRunID, uuid.New().String(), now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	// Insert dataset
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2)
	`, datasetURN, namespace)
	require.NoError(t, err)

	// Insert output edge (producer)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtJobRunID, datasetURN)
	require.NoError(t, err)

	// Insert test result (validator)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, datasetURN, dbtJobRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.RefreshViews(ctx)
	require.NoError(t, err)

	// Test: No orphan namespaces
	orphans, err := store.QueryOrphanNamespaces(ctx)
	require.NoError(t, err)

	assert.Empty(t, orphans, "Should return no orphans when all namespaces have producer output edges")
}

// TestQueryOrphanNamespaces_EmptyState tests behavior when no data exists.
func TestQueryOrphanNamespaces_EmptyState(t *testing.T) {
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
	orphans, err := store.QueryOrphanNamespaces(ctx)
	require.NoError(t, err)

	assert.Empty(t, orphans, "Should return empty slice when no data exists")
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
	assert.Empty(t, health.OrphanNamespaces, "No orphan namespaces")
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
	dbtJobRunID := "dbt:" + uuid.New().String()
	namespace := "postgresql://prod/public"
	datasetURN := namespace + ".customers"

	// Insert job run
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES ($1, $2, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt')
	`, dbtJobRunID, uuid.New().String(), now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	// Insert dataset
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2)
	`, datasetURN, namespace)
	require.NoError(t, err)

	// Insert output edge (producer)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtJobRunID, datasetURN)
	require.NoError(t, err)

	// Insert failed test result
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, datasetURN, dbtJobRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.RefreshViews(ctx)
	require.NoError(t, err)

	// Test: 100% correlation rate
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	assert.InDelta(t, 1.0, health.CorrelationRate, 0.001, "All incidents correlated = rate 1.0")
	assert.Equal(t, 1, health.TotalDatasets, "1 dataset with test results")
	assert.Empty(t, health.OrphanNamespaces, "No orphan namespaces")
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
	geJobRunID := "great_expectations:" + uuid.New().String()
	dbtJobRunID := "dbt:" + uuid.New().String() // Producer job, but for DIFFERENT namespace
	orphanNamespace := "postgres_prod"
	orphanDatasetURN := orphanNamespace + "/public.orders"
	producerNamespace := "postgresql://other/db"
	producerDatasetURN := producerNamespace + ".other_table"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, $2, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $3, $4, 'great_expectations'),
			($5, $6, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $7, $8, 'dbt')
	`, geJobRunID, uuid.New().String(), now, now.Add(-5*time.Minute),
		dbtJobRunID, uuid.New().String(), now, now.Add(-10*time.Minute))
	require.NoError(t, err)

	// Insert datasets (different namespaces)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'orders', $2), ($3, 'other_table', $4)
	`, orphanDatasetURN, orphanNamespace, producerDatasetURN, producerNamespace)
	require.NoError(t, err)

	// Insert output edge for producer namespace ONLY (not for orphan namespace)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtJobRunID, producerDatasetURN)
	require.NoError(t, err)

	// Insert failed test result in ORPHAN namespace
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms
		)
		VALUES ('not_null_orders_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100)
	`, orphanDatasetURN, geJobRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.RefreshViews(ctx)
	require.NoError(t, err)

	// Test: 0% correlation rate (all incidents in orphan namespace)
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	assert.InDelta(t, 0.0, health.CorrelationRate, 0.001, "All incidents orphan = rate 0.0")
	assert.Equal(t, 1, health.TotalDatasets, "1 dataset with test results")
	assert.Len(t, health.OrphanNamespaces, 1, "1 orphan namespace")
	assert.Equal(t, orphanNamespace, health.OrphanNamespaces[0].Namespace)
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
	dbtJobRunID := "dbt:" + uuid.New().String()
	geJobRunID := "great_expectations:" + uuid.New().String()

	// Correlated namespace (has output edges)
	correlatedNS := "postgresql://prod/public"
	correlatedDataset := correlatedNS + ".customers"

	// Orphan namespace (no output edges)
	orphanNS := "postgres_prod"
	orphanDataset := orphanNS + "/public.orders"

	// Insert job runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, $2, 'dbt_transform', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt'),
			($5, $6, 'ge_validation', 'validation', 'COMPLETE', 'COMPLETE', $7, $8, 'great_expectations')
	`, dbtJobRunID, uuid.New().String(), now, now.Add(-5*time.Minute),
		geJobRunID, uuid.New().String(), now, now.Add(-10*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', $2), ($3, 'orders', $4)
	`, correlatedDataset, correlatedNS, orphanDataset, orphanNS)
	require.NoError(t, err)

	// Insert output edge ONLY for correlated namespace
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, dbtJobRunID, correlatedDataset)
	require.NoError(t, err)

	// Insert failed test results for BOTH namespaces
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms
		)
		VALUES
			('not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found nulls', $3, 100),
			('not_null_orders_id', 'not_null', $4, $5, 'failed', 'Found nulls', $6, 100)
	`, correlatedDataset, dbtJobRunID, now,
		orphanDataset, geJobRunID, now)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views
	err = store.RefreshViews(ctx)
	require.NoError(t, err)

	// Test: 50% correlation rate (1 correlated, 1 orphan)
	health, err := store.QueryCorrelationHealth(ctx)
	require.NoError(t, err)

	assert.InDelta(t, 0.5, health.CorrelationRate, 0.001, "1 of 2 correlated = rate 0.5")
	assert.Equal(t, 2, health.TotalDatasets, "2 datasets with test results")
	assert.Len(t, health.OrphanNamespaces, 1, "1 orphan namespace")
	assert.Equal(t, orphanNS, health.OrphanNamespaces[0].Namespace)
}

// TestQueryRecentIncidents tests the QueryRecentIncidents function.
func TestQueryRecentIncidents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Get database NOW() to match view's 7-day window
	var dbNow time.Time

	err := testDB.Connection.QueryRowContext(ctx, "SELECT NOW()").Scan(&dbNow)
	require.NoError(t, err)

	// Setup test data: 2 recent incidents
	recentTime := dbNow.Add(-2 * 24 * time.Hour)
	jobRunID1 := uuid.New().String()
	jobRunID2 := uuid.New().String()
	datasetURN1 := "urn:postgres:warehouse:public.customers"
	datasetURN2 := "urn:postgres:warehouse:public.orders"

	// Insert job runs
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES
			($1, $2, 'transform_customers', 'dbt_prod', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt'),
			($5, $6, 'transform_orders', 'dbt_prod', 'COMPLETE', 'COMPLETE', $7, $8, 'dbt')
	`, jobRunID1, uuid.New().String(), recentTime, recentTime.Add(-5*time.Minute),
		jobRunID2, uuid.New().String(), recentTime.Add(-1*time.Hour), recentTime.Add(-65*time.Minute))
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'customers', 'public'), ($2, 'orders', 'public')
	`, datasetURN1, datasetURN2)
	require.NoError(t, err)

	// Insert lineage edges
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output'), ($3, $4, 'output')
	`, jobRunID1, datasetURN1, jobRunID2, datasetURN2)
	require.NoError(t, err)

	// Insert test results (2 failures for job1, 1 failure for job2)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
		  id, test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms
		)
		VALUES
			(1, 'not_null_customers_id', 'not_null', $1, $2, 'failed', 'Found 2 nulls', $3, 120),
			(2, 'unique_customers_email', 'unique', $1, $2, 'failed', 'Found duplicates', $4, 150),
			(3, 'not_null_orders_id', 'not_null', $5, $6, 'failed', 'Found 1 null', $7, 100)
	`, datasetURN1, jobRunID1, recentTime, recentTime.Add(1*time.Minute),
		datasetURN2, jobRunID2, recentTime.Add(-1*time.Hour))
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)

	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Refresh views (must refresh both incident_correlation_view and recent_incidents_summary)
	err = store.RefreshViews(ctx)
	require.NoError(t, err)

	// Test 1: Query all recent incidents (no limit)
	incidents, err := store.QueryRecentIncidents(ctx, 0)
	require.NoError(t, err)

	assert.Len(t, incidents, 2, "Should return 2 recent incidents")

	// Verify incidents are sorted by most recent failure first
	assert.True(t, incidents[0].LastTestFailureAt.After(incidents[1].LastTestFailureAt),
		"Incidents should be sorted by most recent failure")

	// Test 2: Query with limit
	incidents, err = store.QueryRecentIncidents(ctx, 1)
	require.NoError(t, err)

	assert.Len(t, incidents, 1, "Should return only 1 incident (limit)")

	// Test 3: Verify aggregated data for job1 (2 failures)
	var job1Incident *correlation.RecentIncidentSummary

	for i := range incidents {
		if incidents[i].JobRunID == jobRunID1 {
			job1Incident = &incidents[i]

			break
		}
	}

	if job1Incident != nil {
		assert.Equal(t, int64(2), job1Incident.FailedTestCount, "Job1 should have 2 failed tests")
		assert.Equal(t, int64(1), job1Incident.AffectedDatasetCount, "Job1 should have 1 affected dataset")
		assert.Len(t, job1Incident.FailedTestNames, 2, "Should have 2 failed test names")
		assert.Equal(t, "dbt", job1Incident.ProducerName)
		assert.Equal(t, "transform_customers", job1Incident.JobName)
	}
}
