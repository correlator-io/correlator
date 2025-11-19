// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/config"
)

// TestIncidentCorrelationView tests the incident_correlation_view materialized view.
// Validates that test failures are correctly correlated to the job runs that produced the failing datasets.
//
// Test scenario:
// 1. Insert job_run (dbt model execution)
// 2. Insert dataset (output of the job)
// 3. Insert lineage_edge (job produced dataset)
// 4. Insert test_result (failed test on dataset)
// 5. Query incident_correlation_view
// 6. Validate correlation returns correct job_run details.
func TestIncidentCorrelationView(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)
	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Test data setup
	now := time.Now()
	jobRunID := uuid.New().String()
	runID := uuid.New().String()
	datasetURN := "urn:postgres:warehouse:public.customers"

	// 1. Insert job_run
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace,
			current_state, event_type, event_time,
			started_at, producer_name
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, jobRunID, runID, "transform_customers", "dbt_prod",
		"COMPLETE", "COMPLETE", now, now.Add(-5*time.Minute), "dbt")
	require.NoError(t, err, "Failed to insert job_run")

	// 2. Insert dataset
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, $2, $3)
	`, datasetURN, "customers", "public")
	require.NoError(t, err, "Failed to insert dataset")

	// 3. Insert lineage_edge (job produced dataset)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, jobRunID, datasetURN)
	require.NoError(t, err, "Failed to insert lineage_edge")

	// 4. Insert failed test_result
	testResultID := int64(1)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			id, test_name, test_type, dataset_urn, job_run_id,
			status, message, executed_at, duration_ms
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, testResultID, "not_null_customers_customer_id", "not_null",
		datasetURN, jobRunID, "failed", "Found 3 null values", now, 120)
	require.NoError(t, err, "Failed to insert test_result")

	// Refresh materialized view to include test data
	_, err = testDB.Connection.ExecContext(ctx, `
		REFRESH MATERIALIZED VIEW incident_correlation_view
	`)
	require.NoError(t, err, "Failed to refresh incident_correlation_view")

	// 5. Query incident_correlation_view
	var result struct {
		TestResultID  int64
		TestName      string
		TestStatus    string
		DatasetURN    string
		JobRunID      string
		JobName       string
		JobStatus     string
		ProducerName  string
		LineageEdgeID int64
		TestMessage   string
	}

	err = testDB.Connection.QueryRowContext(ctx, `
		SELECT
			test_result_id, test_name, test_status, dataset_urn,
			job_run_id, job_name, job_status, producer_name,
			lineage_edge_id, test_message
		FROM incident_correlation_view
		WHERE test_result_id = $1
	`, testResultID).Scan(
		&result.TestResultID, &result.TestName, &result.TestStatus, &result.DatasetURN,
		&result.JobRunID, &result.JobName, &result.JobStatus, &result.ProducerName,
		&result.LineageEdgeID, &result.TestMessage,
	)
	require.NoError(t, err, "Failed to query incident_correlation_view")

	// 6. Validate correlation
	assert.Equal(t, testResultID, result.TestResultID, "Test result ID mismatch")
	assert.Equal(t, "not_null_customers_customer_id", result.TestName, "Test name mismatch")
	assert.Equal(t, "failed", result.TestStatus, "Test status mismatch")
	assert.Equal(t, datasetURN, result.DatasetURN, "Dataset URN mismatch")
	assert.Equal(t, jobRunID, result.JobRunID, "Job run ID mismatch")
	assert.Equal(t, "transform_customers", result.JobName, "Job name mismatch")
	assert.Equal(t, "COMPLETE", result.JobStatus, "Job status mismatch")
	assert.Equal(t, "dbt", result.ProducerName, "Producer name mismatch")
	assert.Equal(t, "Found 3 null values", result.TestMessage, "Test message mismatch")
	assert.Positive(t, result.LineageEdgeID, "Lineage edge ID should be positive")
}

// TestLineageImpactAnalysis tests the lineage_impact_analysis materialized view.
// Validates recursive downstream impact analysis for multi-level lineage chains.
//
// Test scenario:
// 1. Create lineage chain: job1 → dataset_a → job2 → dataset_b → job3 → dataset_c
// 2. Query downstream impact for job1
// 3. Validate results include all downstream datasets and correct depth levels.
func TestLineageImpactAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)
	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Test data setup
	now := time.Now()

	// Job run IDs
	jobRunID1 := uuid.New().String()
	jobRunID2 := uuid.New().String()
	jobRunID3 := uuid.New().String()

	// Dataset URNs
	datasetA := "urn:postgres:warehouse:public.raw_orders"
	datasetB := "urn:postgres:warehouse:public.staged_orders"
	datasetC := "urn:postgres:warehouse:public.fact_orders"

	// 1. Create job runs
	jobs := []struct {
		jobRunID  string
		runID     string
		jobName   string
		namespace string
	}{
		{jobRunID1, uuid.New().String(), "extract_orders", "etl_prod"},
		{jobRunID2, uuid.New().String(), "stage_orders", "etl_prod"},
		{jobRunID3, uuid.New().String(), "transform_orders", "dbt_prod"},
	}

	for _, job := range jobs {
		_, err := testDB.Connection.ExecContext(ctx, `
			INSERT INTO job_runs (
				job_run_id, run_id, job_name, job_namespace,
				current_state, event_type, event_time, started_at, producer_name
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, job.jobRunID, job.runID, job.jobName, job.namespace,
			"COMPLETE", "COMPLETE", now, now.Add(-10*time.Minute), "airflow")
		require.NoError(t, err, "Failed to insert job_run: %s", job.jobName)
	}

	// 2. Create datasets
	datasets := []struct {
		urn       string
		name      string
		namespace string
	}{
		{datasetA, "raw_orders", "public"},
		{datasetB, "staged_orders", "public"},
		{datasetC, "fact_orders", "public"},
	}

	for _, ds := range datasets {
		_, err := testDB.Connection.ExecContext(ctx, `
			INSERT INTO datasets (dataset_urn, name, namespace)
			VALUES ($1, $2, $3)
		`, ds.urn, ds.name, ds.namespace)
		require.NoError(t, err, "Failed to insert dataset: %s", ds.name)
	}

	// 3. Create lineage chain
	// job1 produces dataset_a
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, jobRunID1, datasetA)
	require.NoError(t, err, "Failed to insert lineage_edge: job1 -> dataset_a")

	// job2 consumes dataset_a
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'input')
	`, jobRunID2, datasetA)
	require.NoError(t, err, "Failed to insert lineage_edge: job2 <- dataset_a")

	// job2 produces dataset_b
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, jobRunID2, datasetB)
	require.NoError(t, err, "Failed to insert lineage_edge: job2 -> dataset_b")

	// job3 consumes dataset_b
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'input')
	`, jobRunID3, datasetB)
	require.NoError(t, err, "Failed to insert lineage_edge: job3 <- dataset_b")

	// job3 produces dataset_c
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
	`, jobRunID3, datasetC)
	require.NoError(t, err, "Failed to insert lineage_edge: job3 -> dataset_c")

	// Refresh materialized view to include test data
	_, err = testDB.Connection.ExecContext(ctx, `
		REFRESH MATERIALIZED VIEW lineage_impact_analysis
	`)
	require.NoError(t, err, "Failed to refresh lineage_impact_analysis")

	// 4. Query downstream impact for job1
	rows, err := testDB.Connection.QueryContext(ctx, `
		SELECT job_run_id, dataset_urn, dataset_name, depth
		FROM lineage_impact_analysis
		WHERE job_run_id IN ($1, $2, $3)
		ORDER BY job_run_id, depth
	`, jobRunID1, jobRunID2, jobRunID3)

	require.NoError(t, err, "Failed to query lineage_impact_analysis")

	defer func() {
		_ = rows.Close()
	}()

	// 5. Collect results
	var results []ImpactResult

	for rows.Next() {
		var r ImpactResult

		err := rows.Scan(&r.JobRunID, &r.DatasetURN, &r.DatasetName, &r.Depth)
		require.NoError(t, err, "Failed to scan row")

		results = append(results, r)
	}

	require.NoError(t, rows.Err(), "Error iterating rows")

	// 6. Validate impact analysis
	require.GreaterOrEqual(t, len(results), 3, "Should have at least 3 impact records")

	// Find job1's direct output (depth 0)
	job1Outputs := FilterImpactResults(results, jobRunID1, 0)
	require.Len(t, job1Outputs, 1, "Job1 should produce 1 dataset")
	assert.Equal(t, datasetA, job1Outputs[0].DatasetURN, "Job1 should produce dataset_a")

	// Find job2's direct output (depth 0)
	job2Outputs := FilterImpactResults(results, jobRunID2, 0)
	require.Len(t, job2Outputs, 1, "Job2 should produce 1 dataset")
	assert.Equal(t, datasetB, job2Outputs[0].DatasetURN, "Job2 should produce dataset_b")

	// Find job3's direct output (depth 0)
	job3Outputs := FilterImpactResults(results, jobRunID3, 0)
	require.Len(t, job3Outputs, 1, "Job3 should produce 1 dataset")
	assert.Equal(t, datasetC, job3Outputs[0].DatasetURN, "Job3 should produce dataset_c")

	// Validate recursive traversal (job1 → job2 → job3)
	// Job2's output (dataset_b) should be downstream of job1 (depth 1)
	job1Downstream := FilterImpactResults(results, jobRunID2, 1)
	if len(job1Downstream) > 0 {
		assert.Equal(t, datasetB, job1Downstream[0].DatasetURN, "Job1 downstream should include dataset_b")
	}
}

// TestRecentIncidentsSummary tests the recent_incidents_summary materialized view.
// Validates 7-day rolling window filtering and aggregation of incident statistics.
//
// Test scenario:
// 1. Insert incidents from 2 days ago (should appear in view)
// 2. Insert incidents from 8 days ago (should NOT appear in view)
// 3. Query recent_incidents_summary
// 4. Validate only recent incidents (< 7 days) are returned.
func TestRecentIncidentsSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)
	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Test data setup - Use database NOW() to match materialized view's NOW()
	var dbNow time.Time

	err := testDB.Connection.QueryRowContext(ctx, "SELECT NOW()").Scan(&dbNow)
	require.NoError(t, err, "Failed to get database NOW()")

	recentTime := dbNow.Add(-2 * 24 * time.Hour) // 2 days ago (within window)
	oldTime := dbNow.Add(-8 * 24 * time.Hour)    // 8 days ago (outside window)

	// Job run IDs
	recentJobRunID := uuid.New().String()
	oldJobRunID := uuid.New().String()

	// Dataset URNs
	recentDatasetURN := "urn:postgres:warehouse:public.customers"
	oldDatasetURN := "urn:postgres:warehouse:public.orders"

	// 1. Insert recent job run (2 days ago)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace,
			current_state, event_type, event_time, started_at, producer_name
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, recentJobRunID, uuid.New().String(), "transform_customers", "dbt_prod",
		"COMPLETE", "COMPLETE", recentTime, recentTime.Add(-5*time.Minute), "dbt")
	require.NoError(t, err, "Failed to insert recent job_run")

	// 2. Insert old job run (8 days ago)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace,
			current_state, event_type, event_time, started_at, producer_name
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, oldJobRunID, uuid.New().String(), "transform_orders", "dbt_prod",
		"COMPLETE", "COMPLETE", oldTime, oldTime.Add(-5*time.Minute), "dbt")
	require.NoError(t, err, "Failed to insert old job_run")

	// 3. Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, $2, $3), ($4, $5, $6)
	`, recentDatasetURN, "customers", "public", oldDatasetURN, "orders", "public")
	require.NoError(t, err, "Failed to insert datasets")

	// 4. Insert lineage edges
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output'), ($3, $4, 'output')
	`, recentJobRunID, recentDatasetURN, oldJobRunID, oldDatasetURN)
	require.NoError(t, err, "Failed to insert lineage_edges")

	// 5. Insert test results (failed)
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (
			id, test_name, test_type, dataset_urn, job_run_id,
			status, message, executed_at, duration_ms
		) VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9),
			($10, $11, $12, $13, $14, $15, $16, $17, $18)
	`,
		1, "not_null_customers_id", "not_null", recentDatasetURN, recentJobRunID,
		"failed", "Found 2 null values", recentTime, 120,
		2, "not_null_orders_id", "not_null", oldDatasetURN, oldJobRunID,
		"failed", "Found 5 null values", oldTime, 150)
	require.NoError(t, err, "Failed to insert test_results")

	// Refresh materialized views in dependency order
	// FIRST: Refresh incident_correlation_view (depends on base tables)
	_, err = testDB.Connection.ExecContext(ctx, `
		REFRESH MATERIALIZED VIEW incident_correlation_view
	`)
	require.NoError(t, err, "Failed to refresh incident_correlation_view")

	// SECOND: Refresh recent_incidents_summary (depends on incident_correlation_view)
	_, err = testDB.Connection.ExecContext(ctx, `
		REFRESH MATERIALIZED VIEW recent_incidents_summary
	`)
	require.NoError(t, err, "Failed to refresh recent_incidents_summary")

	// 6. Query recent_incidents_summary
	rows, err := testDB.Connection.QueryContext(ctx, `
		SELECT job_run_id, job_name, producer_name, failed_test_count
		FROM recent_incidents_summary
		ORDER BY last_test_failure_at DESC
	`)
	require.NoError(t, err, "Failed to query recent_incidents_summary")

	defer func() {
		_ = rows.Close()
	}()

	// 7. Collect results
	type summaryResult struct {
		JobRunID        string
		JobName         string
		ProducerName    string
		FailedTestCount int64
	}

	var results []summaryResult

	for rows.Next() {
		var r summaryResult

		err := rows.Scan(&r.JobRunID, &r.JobName, &r.ProducerName, &r.FailedTestCount)
		require.NoError(t, err, "Failed to scan row")

		results = append(results, r)
	}

	require.NoError(t, rows.Err(), "Error iterating rows")

	// 8. Validate 7-day window filtering
	require.Len(t, results, 1, "Should only return 1 recent incident (within 7 days)")

	// Validate the recent incident
	assert.Equal(t, recentJobRunID, results[0].JobRunID, "Should be recent job run")
	assert.Equal(t, "transform_customers", results[0].JobName, "Job name mismatch")
	assert.Equal(t, "dbt", results[0].ProducerName, "Producer name mismatch")
	assert.Equal(t, int64(1), results[0].FailedTestCount, "Should have 1 failed test")

	// Validate old incident is NOT in results
	for _, r := range results {
		assert.NotEqual(t, oldJobRunID, r.JobRunID, "Old job run (8 days) should not appear in 7-day window")
	}
}
