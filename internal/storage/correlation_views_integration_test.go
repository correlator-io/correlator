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
	jobRunID1 := uuid.New().String()
	jobRunID2 := uuid.New().String()
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

	// Test 1: Query all incidents (no filter)
	// Note: incident_correlation_view only returns failed/error tests, not passed tests
	incidents, err := store.QueryIncidents(ctx, nil)
	require.NoError(t, err, "Query should succeed")

	assert.Len(t, incidents, 1, "Should return 1 incident (view filters failed/error only)")

	// Test 2: Filter by test status (failed)
	failedStatus := "failed" //nolint:goconst
	filter := &correlation.IncidentFilter{
		TestStatus: &failedStatus,
	}

	incidents, err = store.QueryIncidents(ctx, filter)
	require.NoError(t, err)

	assert.Len(t, incidents, 1, "Should return 1 failed test")
	assert.Equal(t, "failed", incidents[0].TestStatus)
	assert.Equal(t, "not_null_customers_id", incidents[0].TestName)

	// Test 3: Filter by producer
	producer := "dbt"
	filter = &correlation.IncidentFilter{
		ProducerName: &producer,
	}

	incidents, err = store.QueryIncidents(ctx, filter)
	require.NoError(t, err)

	assert.Len(t, incidents, 1, "Should return 1 dbt incident")
	assert.Equal(t, "dbt", incidents[0].ProducerName)

	// Test 4: Filter by job_run_id
	filter = &correlation.IncidentFilter{
		JobRunID: &jobRunID1,
	}

	incidents, err = store.QueryIncidents(ctx, filter)
	require.NoError(t, err)

	assert.Len(t, incidents, 1, "Should return 1 incident for job_run_id")
	assert.Equal(t, jobRunID1, incidents[0].JobRunID)

	// Test 5: Filter by time range (recent tests only)
	recentTime := now.Add(-30 * time.Minute)
	filter = &correlation.IncidentFilter{
		TestExecutedAfter: &recentTime,
	}

	incidents, err = store.QueryIncidents(ctx, filter)
	require.NoError(t, err)

	assert.Len(t, incidents, 1, "Should return 1 recent test")
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
	directOutputs := correlation.FilterImpactResults(impact, jobRunID1, 0)
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
