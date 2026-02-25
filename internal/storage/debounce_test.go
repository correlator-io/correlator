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
)

// setupDebounceStore creates a LineageStore with the given debounce delay backed
// by a real PostgreSQL testcontainer. Caller is responsible for cleanup via t.Cleanup.
func setupDebounceStore(t *testing.T, delay time.Duration) *LineageStore {
	t.Helper()

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	conn := &Connection{DB: testDB.Connection}

	store, err := NewLineageStore(conn, 1*time.Hour, WithViewRefreshDelay(delay))
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	return store
}

// seedMinimalCorrelationData inserts one job run, dataset, lineage edge, and
// failed test result — the minimum needed to produce a row in incident_correlation_view
// after resolved_datasets + view refresh.
func seedMinimalCorrelationData(t *testing.T, store *LineageStore) {
	t.Helper()

	ctx := context.Background()
	now := time.Now()
	jobRunID := "dbt:" + uuid.New().String()
	datasetURN := "postgresql://prod-db/public.debounce_test_" + uuid.New().String()[:8]

	db := store.conn.DB

	_, err := db.ExecContext(ctx, `
		INSERT INTO job_runs (
			job_run_id, run_id, job_name, job_namespace, current_state,
		    event_type, event_time, started_at, producer_name)
		VALUES ($1, $2, 'debounce_test_job', 'test', 'COMPLETE', 'COMPLETE', $3, $4, 'dbt')
	`, jobRunID, uuid.New().String(), now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace) VALUES ($1, 'debounce_test_ds', 'test')
	`, datasetURN)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO lineage_edges (job_run_id, dataset_urn, edge_type) VALUES ($1, $2, 'output')
	`, jobRunID, datasetURN)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		INSERT INTO test_results (test_name, test_type, dataset_urn, job_run_id, status, message, executed_at, duration_ms)
		VALUES ('test_not_null', 'not_null', $1, $2, 'failed', 'found nulls', $3, 50)
	`, datasetURN, jobRunID, now)
	require.NoError(t, err)
}

// countIncidentViewRows returns the current row count of incident_correlation_view.
func countIncidentViewRows(t *testing.T, store *LineageStore) int {
	t.Helper()

	var count int

	err := store.conn.DB.QueryRowContext(context.Background(),
		"SELECT count(*) FROM incident_correlation_view",
	).Scan(&count)
	require.NoError(t, err)

	return count
}

// countResolvedDatasets returns the current row count of the resolved_datasets table.
func countResolvedDatasets(t *testing.T, store *LineageStore) int {
	t.Helper()

	var count int

	err := store.conn.DB.QueryRowContext(context.Background(),
		"SELECT count(*) FROM resolved_datasets",
	).Scan(&count)
	require.NoError(t, err)

	return count
}

// TestDebounceBurst verifies that 50 rapid notifyDataChanged calls produce
// exactly one refresh cycle (resolved_datasets populated once, view refreshed once).
func TestDebounceBurst(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupDebounceStore(t, 200*time.Millisecond)
	seedMinimalCorrelationData(t, store)

	assert.Equal(t, 0, countResolvedDatasets(t, store),
		"resolved_datasets should be empty before any refresh")

	for range 50 {
		store.notifyDataChanged()
	}

	// Wait for the single debounced refresh to complete
	time.Sleep(500 * time.Millisecond)

	assert.Positive(t, countResolvedDatasets(t, store),
		"resolved_datasets should be populated after debounced refresh")
	assert.Equal(t, 1, countIncidentViewRows(t, store),
		"1 correlated incident should appear after single refresh cycle")
}

// TestDebounceDisabledWhenZeroDelay verifies that notifyDataChanged is a no-op
// when the refresh delay is zero — no refresh cycle runs.
func TestDebounceDisabledWhenZeroDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupDebounceStore(t, 0)
	seedMinimalCorrelationData(t, store)

	store.notifyDataChanged()
	store.notifyDataChanged()
	store.notifyDataChanged()

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 0, countResolvedDatasets(t, store),
		"resolved_datasets should remain empty when debounce is disabled")
	assert.Equal(t, 0, countIncidentViewRows(t, store),
		"view should remain empty when debounce is disabled")
}

// TestCloseCancelsPendingRefresh verifies that Close() cancels a pending
// debounce timer before it fires — no refresh cycle runs.
func TestCloseCancelsPendingRefresh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	testDB := config.SetupTestDatabase(ctx, t)

	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	conn := &Connection{DB: testDB.Connection}

	store, err := NewLineageStore(conn, 1*time.Hour, WithViewRefreshDelay(2*time.Second))
	require.NoError(t, err)

	seedMinimalCorrelationData(t, store)

	// Trigger debounce (timer will fire in 2s)
	store.notifyDataChanged()

	// Close immediately — should cancel the pending timer
	err = store.Close()
	require.NoError(t, err)

	// Wait past the original debounce delay
	time.Sleep(2500 * time.Millisecond)

	assert.Equal(t, 0, countResolvedDatasets(t, store),
		"resolved_datasets should remain empty — Close() cancelled the pending refresh")
}

// TestCloseWaitsForInflightRefresh verifies that Close() blocks until an
// already-running refresh cycle completes.
func TestCloseWaitsForInflightRefresh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupDebounceStore(t, 10*time.Millisecond)
	seedMinimalCorrelationData(t, store)

	store.notifyDataChanged()

	// Wait for the refresh goroutine to start executing
	time.Sleep(50 * time.Millisecond)

	// Close() must block until the in-flight refresh finishes
	err := store.Close()
	require.NoError(t, err)

	assert.Positive(t, countResolvedDatasets(t, store),
		"resolved_datasets should be populated — Close() waited for in-flight refresh")
	assert.Equal(t, 1, countIncidentViewRows(t, store),
		"view should be populated — Close() waited for in-flight refresh")
}

// TestDebounceTimerResets verifies that a second notifyDataChanged call resets
// the debounce timer — the refresh fires only after the LAST call's delay.
func TestDebounceTimerResets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	store := setupDebounceStore(t, 300*time.Millisecond)
	seedMinimalCorrelationData(t, store)

	// First notification starts the 300ms timer
	store.notifyDataChanged()

	// At 200ms, fire a second notification which resets the timer
	time.Sleep(200 * time.Millisecond)
	store.notifyDataChanged()

	// At 350ms total (150ms after second call) — the original timer would have
	// fired at 300ms but was reset; the new timer hasn't fired yet.
	time.Sleep(150 * time.Millisecond)

	assert.Equal(t, 0, countResolvedDatasets(t, store),
		"resolved_datasets should still be empty — timer was reset")

	// Wait for the reset timer to fire (300ms after second call = ~500ms total)
	time.Sleep(300 * time.Millisecond)

	assert.Positive(t, countResolvedDatasets(t, store),
		"resolved_datasets should be populated after reset timer fires")
	assert.Equal(t, 1, countIncidentViewRows(t, store),
		"view should be populated after reset timer fires")
}

// TestWithViewRefreshDelay verifies the functional option sets the delay.
func TestWithViewRefreshDelay(t *testing.T) {
	store := &LineageStore{}

	opt := WithViewRefreshDelay(5 * time.Second)
	opt(store)

	assert.Equal(t, 5*time.Second, store.refreshDelay)
}

// TestWithViewRefreshDelay_Zero verifies that zero delay disables refresh.
func TestWithViewRefreshDelay_Zero(t *testing.T) {
	store := &LineageStore{}

	opt := WithViewRefreshDelay(0)
	opt(store)

	assert.Equal(t, time.Duration(0), store.refreshDelay)
}
