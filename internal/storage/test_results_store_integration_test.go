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

	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/ingestion"
)

// TestStoreTestResult_Success tests successful single test result storage.
func TestStoreTestResult_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup prerequisites: job_run and dataset
	jobRunID := uuid.New().String()
	datasetURN := "postgres://test-db:5432/db.schema.table"
	now := time.Now()

	// Insert job_run
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name)
		VALUES ($1, $2, 'test_job', 'dbt://test', 'COMPLETE', 'COMPLETE', $3, $3, 'dbt')
	`, jobRunID, uuid.New().String(), now)
	require.NoError(t, err)

	// Insert dataset
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'table', 'db.schema')
	`, datasetURN)
	require.NoError(t, err)

	// Create LineageStore
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Store test result
	testResult := &ingestion.TestResult{
		TestName:   "test_column_not_null",
		TestType:   "data_quality",
		DatasetURN: datasetURN,
		JobRunID:   jobRunID,
		Status:  ingestion.TestStatusFailed,
		Message: "Column contains NULL values",
		Metadata: map[string]interface{}{
			"column":       "user_id",
			"min_rows":     1000,
			"actual_rows":  850,
			"severity":     "high",
			"rule":         "not_null",
		},
		ExecutedAt: now,
		DurationMs: 150,
	}

	stored, duplicate, err := store.StoreTestResult(ctx, testResult)
	require.NoError(t, err)
	assert.True(t, stored, "Test result should be stored")
	assert.False(t, duplicate, "Should not be duplicate (no UPSERT in MVP)")

	// Verify: Check database
	var count int

	err = testDB.Connection.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM test_results
		WHERE test_name = $1 AND dataset_urn = $2 AND job_run_id = $3
	`, testResult.TestName, testResult.DatasetURN, testResult.JobRunID).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 1, count, "Should have exactly 1 test result")
}

// TestStoreTestResult_MissingDatasetFK tests FK violation when dataset_urn doesn't exist.
func TestStoreTestResult_MissingDatasetFK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: Only insert job_run, NOT dataset (cause FK violation)
	jobRunID := uuid.New().String()
	now := time.Now()

	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name)
		VALUES ($1, $2, 'test_job', 'dbt://test', 'COMPLETE', 'COMPLETE', $3, $3, 'dbt')
	`, jobRunID, uuid.New().String(), now)
	require.NoError(t, err)

	// Create store
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Store test result with non-existent dataset_urn
	testResult := &ingestion.TestResult{
		TestName:   "test_missing_dataset",
		DatasetURN: "postgres://nonexistent:5432/db.table",
		JobRunID:   jobRunID,
		Status:     ingestion.TestStatusFailed,
		ExecutedAt: now,
	}

	stored, duplicate, err := store.StoreTestResult(ctx, testResult)
	require.Error(t, err, "Should fail due to FK violation")
	assert.False(t, stored, "Should not be stored")
	assert.False(t, duplicate, "Should not be duplicate")
	assert.ErrorIs(t, err, ErrTestResultFKViolation, "Should return FK violation error")
}

// TestStoreTestResult_MissingJobRunFK tests FK violation when job_run_id doesn't exist.
func TestStoreTestResult_MissingJobRunFK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: Only insert dataset, NOT job_run (cause FK violation)
	datasetURN := "postgres://test-db:5432/db.table"

	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'table', 'db')
	`, datasetURN)
	require.NoError(t, err)

	// Create store
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Store test result with non-existent job_run_id
	testResult := &ingestion.TestResult{
		TestName:   "test_missing_job_run",
		DatasetURN: datasetURN,
		JobRunID:   "nonexistent-job-run-id",
		Status:     ingestion.TestStatusFailed,
		ExecutedAt: time.Now(),
	}

	stored, duplicate, err := store.StoreTestResult(ctx, testResult)
	require.Error(t, err, "Should fail due to FK violation")
	assert.False(t, stored, "Should not be stored")
	assert.False(t, duplicate, "Should not be duplicate")
	assert.ErrorIs(t, err, ErrTestResultFKViolation, "Should return FK violation error")
}

// TestStoreTestResult_ValidationError tests domain validation before storage.
func TestStoreTestResult_ValidationError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Create store
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Store invalid test result (empty test_name)
	testResult := &ingestion.TestResult{
		TestName:   "", // Invalid: empty
		DatasetURN: "postgres://test:5432/db.table",
		JobRunID:   "some-job-run-id",
		Status:     ingestion.TestStatusFailed,
		ExecutedAt: time.Now(),
	}

	stored, duplicate, err := store.StoreTestResult(ctx, testResult)
	require.Error(t, err, "Should fail validation")
	assert.False(t, stored, "Should not be stored")
	assert.False(t, duplicate, "Should not be duplicate")
	assert.ErrorIs(t, err, ErrTestResultStoreFailed, "Should wrap validation error")
}

// TestStoreTestResults_Batch tests batch storage with partial success.
func TestStoreTestResults_Batch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: 2 job_runs and 2 datasets
	jobRunID1 := uuid.New().String()
	jobRunID2 := uuid.New().String()
	datasetURN1 := "postgres://test:5432/db.table1"
	datasetURN2 := "postgres://test:5432/db.table2"
	now := time.Now()

	// Insert job_runs
	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name)
		VALUES
			($1, $2, 'job1', 'dbt://test', 'COMPLETE', 'COMPLETE', $3, $3, 'dbt'),
			($4, $5, 'job2', 'dbt://test', 'FAIL', 'FAIL', $6, $6, 'dbt')
	`, jobRunID1, uuid.New().String(), now, jobRunID2, uuid.New().String(), now)
	require.NoError(t, err)

	// Insert datasets
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'table1', 'db'), ($2, 'table2', 'db')
	`, datasetURN1, datasetURN2)
	require.NoError(t, err)

	// Create store
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Store batch with 1 success, 1 FK violation
	testResults := []*ingestion.TestResult{
		{
			TestName:   "test_success",
			DatasetURN: datasetURN1,
			JobRunID:   jobRunID1,
			Status:     ingestion.TestStatusPassed,
			ExecutedAt: now,
		},
		{
			TestName:   "test_fk_violation",
			DatasetURN: "postgres://nonexistent:5432/db.table",
			JobRunID:   jobRunID2,
			Status:     ingestion.TestStatusFailed,
			ExecutedAt: now,
		},
	}

	results, err := store.StoreTestResults(ctx, testResults)
	require.NoError(t, err, "Batch operation itself should not error")
	require.Len(t, results, 2)

	// Verify: First result success
	assert.True(t, results[0].Stored, "First test result should be stored")
	require.NoError(t, results[0].Error, "First test result should be error")

	// Verify: Second result FK violation
	assert.False(t, results[1].Stored, "Second test result should not be stored")
	assert.Error(t, results[1].Error, "Second test result should have FK error") //nolint: testifylint
	assert.ErrorIs(t, results[1].Error, ErrTestResultFKViolation)                //nolint: testifylint

	// Verify: Database has only 1 test result (partial success)
	var count int

	err = testDB.Connection.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_results`).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 1, count, "Should have exactly 1 test result (partial success)")
}

// TestStoreTestResults_AllSuccess tests batch storage with all successes.
func TestStoreTestResults_AllSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: 3 job_runs and 3 datasets
	jobRunIDs := []string{uuid.New().String(), uuid.New().String(), uuid.New().String()}
	datasetURNs := []string{
		"postgres://test:5432/db.table1",
		"postgres://test:5432/db.table2",
		"postgres://test:5432/db.table3",
	}
	now := time.Now()

	// Insert job_runs and datasets
	for i := 0; i < 3; i++ {
		_, err := testDB.Connection.ExecContext(ctx, `
			INSERT INTO job_runs (
			  job_run_id, run_id, job_name, job_namespace, current_state, event_type,
			  event_time, started_at, producer_name
			)
			VALUES ($1, $2, $3, 'dbt://test', 'COMPLETE', 'COMPLETE', $4, $4, 'dbt')
		`, jobRunIDs[i], uuid.New().String(), fmt.Sprintf("job%d", i+1), now)
		require.NoError(t, err)

		_, err = testDB.Connection.ExecContext(ctx, `
			INSERT INTO datasets (dataset_urn, name, namespace)
			VALUES ($1, $2, 'db')
		`, datasetURNs[i], fmt.Sprintf("table%d", i+1))
		require.NoError(t, err)
	}

	// Create store
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test: Store batch with all successes
	testResults := make([]*ingestion.TestResult, 3)
	for i := 0; i < 3; i++ {
		testResults[i] = &ingestion.TestResult{
			TestName:   fmt.Sprintf("test_%d", i+1),
			DatasetURN: datasetURNs[i],
			JobRunID:   jobRunIDs[i],
			Status:     ingestion.TestStatusPassed,
			ExecutedAt: now,
		}
	}

	results, err := store.StoreTestResults(ctx, testResults)
	require.NoError(t, err)
	require.Len(t, results, 3)

	// Verify: All results success
	for i, result := range results {
		assert.True(t, result.Stored, "Test result %d should be stored", i)
		require.NoError(t, result.Error, "Test result %d should have no error", i)
	}

	// Verify: Database has 3 test results
	var count int

	err = testDB.Connection.QueryRowContext(ctx, `SELECT COUNT(*) FROM test_results`).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 3, count, "Should have exactly 3 test results")
}

// TestStoreTestResult_UpsertBehavior tests UPSERT behavior on conflict.
func TestStoreTestResult_UpsertBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	// Setup: job_run and dataset
	jobRunID := uuid.New().String()
	datasetURN := "postgres://test-db:5432/db.table"
	now := time.Now()

	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (
		  job_run_id, run_id, job_name, job_namespace, current_state, event_type, event_time, started_at, producer_name
		)
		VALUES ($1, $2, 'test_job', 'dbt://test', 'COMPLETE', 'COMPLETE', $3, $3, 'dbt')
	`, jobRunID, uuid.New().String(), now)
	require.NoError(t, err)

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'table', 'db')
	`, datasetURN)
	require.NoError(t, err)

	// Create store
	conn := &Connection{DB: testDB.Connection}
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() {
		_ = store.Close()
	}()

	// Test 1: First insert (should create new row)
	testResult := &ingestion.TestResult{
		TestName:   "test_upsert",
		TestType:   "data_quality",
		DatasetURN: datasetURN,
		JobRunID:   jobRunID,
		Status:     ingestion.TestStatusFailed,
		Message:    "Original failure message",
		ExecutedAt: now,
		DurationMs: 100,
	}

	stored, duplicate, err := store.StoreTestResult(ctx, testResult)
	require.NoError(t, err)
	assert.True(t, stored, "First insert should be stored")
	assert.False(t, duplicate, "First insert should NOT be duplicate")

	// Verify: 1 row exists
	var count int

	err = testDB.Connection.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM test_results WHERE test_name = $1
	`, testResult.TestName).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 1, count, "Should have exactly 1 row after first insert")

	// Test 2: Second insert with same unique key (test_name, dataset_urn, executed_at)
	// Should UPSERT (update existing row)
	testResult.Status = ingestion.TestStatusPassed // Change status
	testResult.Message = "Updated success message" // Change message
	testResult.DurationMs = 200                    // Change duration

	stored, duplicate, err = store.StoreTestResult(ctx, testResult)
	require.NoError(t, err)
	assert.True(t, stored, "UPSERT should be stored")
	assert.True(t, duplicate, "UPSERT should be marked as duplicate")

	// Verify: Still only 1 row (not 2)
	err = testDB.Connection.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM test_results WHERE test_name = $1
	`, testResult.TestName).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 1, count, "Should STILL have exactly 1 row after UPSERT (not 2)")

	// Verify: Updated values are reflected in database
	var (
		status, message string
		durationMs      int
	)

	err = testDB.Connection.QueryRowContext(ctx, `
		SELECT status, message, duration_ms
		FROM test_results
		WHERE test_name = $1 AND dataset_urn = $2 AND executed_at = $3
	`, testResult.TestName, testResult.DatasetURN, testResult.ExecutedAt).Scan(&status, &message, &durationMs)
	require.NoError(t, err)

	assert.Equal(t, "passed", status, "Status should be updated to 'passed'")
	assert.Equal(t, "Updated success message", message, "Message should be updated")
	assert.Equal(t, 200, durationMs, "Duration should be updated to 200")
}
