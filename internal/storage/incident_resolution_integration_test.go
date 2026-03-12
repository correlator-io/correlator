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
	"github.com/correlator-io/correlator/internal/ingestion"
)

// seedIncidentData inserts the minimum data needed for resolution tests:
// a job run, dataset, lineage edge, and a failed test result.
// Returns the test_result_id for use in resolution operations.
func seedIncidentData(
	t *testing.T, ctx context.Context, //nolint:revive // t.Helper must be first call, ctx follows
	testDB *config.TestDatabase,
	testResultID int64, testName, datasetURN, status string, executedAt time.Time,
) {
	t.Helper()

	runID := uuid.New().String()
	now := time.Now()

	_, err := testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (run_id, job_name, job_namespace, current_state,
		    event_type, event_time, started_at, producer_name)
		VALUES ($1, 'test_job', 'test_ns', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt')
		ON CONFLICT (run_id) DO NOTHING
	`, runID, now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO datasets (dataset_urn, name, namespace)
		VALUES ($1, 'test_dataset', 'public')
		ON CONFLICT (dataset_urn) DO NOTHING
	`, datasetURN)
	require.NoError(t, err)

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO lineage_edges (run_id, dataset_urn, edge_type)
		VALUES ($1, $2, 'output')
		ON CONFLICT (run_id, dataset_urn, edge_type) DO NOTHING
	`, runID, datasetURN)
	require.NoError(t, err)

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (id, test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms)
		VALUES ($1, $2, 'not_null', $3, $4, $5, 'test message', $6, 100)
		ON CONFLICT (test_name, dataset_urn, run_id) DO NOTHING
	`, testResultID, testName, datasetURN, runID, status, executedAt)
	require.NoError(t, err)
}

func TestGetResolution_NoRow(t *testing.T) {
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
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	datasetURN := "urn:postgres:warehouse:public.customers"
	seedIncidentData(t, ctx, testDB, 1, "not_null_id", datasetURN, "failed", time.Now())

	res, err := store.GetResolution(ctx, 1)
	require.NoError(t, err)
	assert.Nil(t, res, "No resolution row should return nil without error")
}

func TestSetResolution_StateTransitions(t *testing.T) {
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
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	datasetURN := "urn:postgres:warehouse:public.customers"

	t.Run("open to acknowledged", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 100, "test_ack", datasetURN, "failed", time.Now())

		res, err := store.SetResolution(ctx, 100, correlation.ResolutionRequest{
			Status: correlation.ResolutionAcknowledged,
			Reason: "manual",
		}, "user")
		require.NoError(t, err)
		assert.Equal(t, correlation.ResolutionAcknowledged, res.Status)
		assert.Equal(t, "user", res.ResolvedBy)
		assert.Equal(t, "manual", res.ResolutionReason)
		assert.Nil(t, res.MuteExpiresAt)

		got, err := store.GetResolution(ctx, 100)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, correlation.ResolutionAcknowledged, got.Status)
	})

	t.Run("open to resolved", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 101, "test_resolve", datasetURN, "failed", time.Now())

		res, err := store.SetResolution(ctx, 101, correlation.ResolutionRequest{
			Status: correlation.ResolutionResolved,
			Reason: "manual",
			Note:   "root cause identified",
		}, "user")
		require.NoError(t, err)
		assert.Equal(t, correlation.ResolutionResolved, res.Status)
		assert.Equal(t, "root cause identified", res.ResolutionNote)
	})

	t.Run("open to muted with expiry", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 102, "test_mute", datasetURN, "failed", time.Now())

		res, err := store.SetResolution(ctx, 102, correlation.ResolutionRequest{
			Status:   correlation.ResolutionMuted,
			Reason:   "false_positive",
			MuteDays: 30,
		}, "user")
		require.NoError(t, err)
		assert.Equal(t, correlation.ResolutionMuted, res.Status)
		assert.Equal(t, "false_positive", res.ResolutionReason)
		require.NotNil(t, res.MuteExpiresAt)

		expectedExpiry := time.Now().Add(30 * 24 * time.Hour)
		assert.WithinDuration(t, expectedExpiry, *res.MuteExpiresAt, 5*time.Second)
	})

	t.Run("acknowledged to resolved", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 103, "test_ack_resolve", datasetURN, "failed", time.Now())

		_, err := store.SetResolution(ctx, 103, correlation.ResolutionRequest{
			Status: correlation.ResolutionAcknowledged,
			Reason: "manual",
		}, "user")
		require.NoError(t, err)

		res, err := store.SetResolution(ctx, 103, correlation.ResolutionRequest{
			Status: correlation.ResolutionResolved,
			Reason: "manual",
			Note:   "fixed upstream",
		}, "user")
		require.NoError(t, err)
		assert.Equal(t, correlation.ResolutionResolved, res.Status)
	})

	t.Run("acknowledged to muted", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 104, "test_ack_mute", datasetURN, "failed", time.Now())

		_, err := store.SetResolution(ctx, 104, correlation.ResolutionRequest{
			Status: correlation.ResolutionAcknowledged,
			Reason: "manual",
		}, "user")
		require.NoError(t, err)

		res, err := store.SetResolution(ctx, 104, correlation.ResolutionRequest{
			Status:   correlation.ResolutionMuted,
			Reason:   "expected",
			MuteDays: 7,
		}, "user")
		require.NoError(t, err)
		assert.Equal(t, correlation.ResolutionMuted, res.Status)
	})
}

func TestSetResolution_InvalidTransitions(t *testing.T) {
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
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	datasetURN := "urn:postgres:warehouse:public.customers"

	t.Run("resolved is terminal", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 200, "test_resolved_terminal", datasetURN, "failed", time.Now())

		_, err := store.SetResolution(ctx, 200, correlation.ResolutionRequest{
			Status: correlation.ResolutionResolved,
			Reason: "manual",
		}, "user")
		require.NoError(t, err)

		_, err = store.SetResolution(ctx, 200, correlation.ResolutionRequest{
			Status: correlation.ResolutionAcknowledged,
			Reason: "manual",
		}, "user")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidResolutionTransition)
	})

	t.Run("muted is terminal", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 201, "test_muted_terminal", datasetURN, "failed", time.Now())

		_, err := store.SetResolution(ctx, 201, correlation.ResolutionRequest{
			Status:   correlation.ResolutionMuted,
			Reason:   "false_positive",
			MuteDays: 30,
		}, "user")
		require.NoError(t, err)

		_, err = store.SetResolution(ctx, 201, correlation.ResolutionRequest{
			Status: correlation.ResolutionResolved,
			Reason: "manual",
		}, "user")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidResolutionTransition)
	})

	t.Run("open to open is invalid", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 202, "test_open_to_open", datasetURN, "failed", time.Now())

		_, err := store.SetResolution(ctx, 202, correlation.ResolutionRequest{
			Status: correlation.ResolutionOpen,
			Reason: "manual",
		}, "user")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidResolutionTransition)
	})

	t.Run("acknowledged to open is invalid", func(t *testing.T) {
		seedIncidentData(t, ctx, testDB, 203, "test_acknowledged_to_open", datasetURN, "failed", time.Now())

		_, err := store.SetResolution(ctx, 203, correlation.ResolutionRequest{
			Status: correlation.ResolutionAcknowledged,
			Reason: "manual",
		}, "user")
		require.NoError(t, err)

		_, err = store.SetResolution(ctx, 202, correlation.ResolutionRequest{
			Status: correlation.ResolutionOpen,
			Reason: "manual",
		}, "user")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidResolutionTransition)
	})
}

func TestAutoResolveOnPass(t *testing.T) {
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
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	datasetURN := "urn:postgres:warehouse:public.customers"
	testName := "not_null_customers_id"
	gracePeriod := 1 * time.Hour

	t.Run("resolves open incident outside grace period", func(t *testing.T) {
		failedAt := time.Now().Add(-2 * time.Hour) // Well outside grace period
		seedIncidentData(t, ctx, testDB, 300, testName, datasetURN, "failed", failedAt)

		passedAt := time.Now()
		seedIncidentData(t, ctx, testDB, 301, testName, datasetURN, "passed", passedAt)

		count, err := store.AutoResolveIncidents(ctx, testName, datasetURN, 301, gracePeriod)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		res, err := store.GetResolution(ctx, 300)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, correlation.ResolutionResolved, res.Status)
		assert.Equal(t, "auto", res.ResolvedBy)
		assert.Equal(t, "auto_pass", res.ResolutionReason)
		require.NotNil(t, res.ResolvedByTestResultID)
		assert.Equal(t, int64(301), *res.ResolvedByTestResultID)
	})

	t.Run("resolves acknowledged incident outside grace period", func(t *testing.T) {
		failedAt := time.Now().Add(-2 * time.Hour)
		seedIncidentData(t, ctx, testDB, 302, "test_ack_auto", datasetURN, "failed", failedAt)

		_, err := store.SetResolution(ctx, 302, correlation.ResolutionRequest{
			Status: correlation.ResolutionAcknowledged,
			Reason: "manual",
		}, "user")
		require.NoError(t, err)

		seedIncidentData(t, ctx, testDB, 303, "test_ack_auto", datasetURN, "passed", time.Now())

		count, err := store.AutoResolveIncidents(ctx, "test_ack_auto", datasetURN, 303, gracePeriod)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		res, err := store.GetResolution(ctx, 302)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, correlation.ResolutionResolved, res.Status)
		assert.Equal(t, "auto", res.ResolvedBy)
	})

	t.Run("anti-flapping: does NOT resolve within grace period", func(t *testing.T) {
		failedAt := time.Now().Add(-30 * time.Minute) // Within 1-hour grace period
		seedIncidentData(t, ctx, testDB, 304, "test_flap", datasetURN, "failed", failedAt)

		seedIncidentData(t, ctx, testDB, 305, "test_flap", datasetURN, "passed", time.Now())

		count, err := store.AutoResolveIncidents(ctx, "test_flap", datasetURN, 305, gracePeriod)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "Should not auto-resolve within grace period")

		res, err := store.GetResolution(ctx, 304)
		require.NoError(t, err)
		assert.Nil(t, res, "Incident should remain implicitly open")
	})

	t.Run("does not touch already resolved incidents", func(t *testing.T) {
		failedAt := time.Now().Add(-2 * time.Hour)
		seedIncidentData(t, ctx, testDB, 306, "test_already_resolved", datasetURN, "failed", failedAt)

		_, err := store.SetResolution(ctx, 306, correlation.ResolutionRequest{
			Status: correlation.ResolutionResolved,
			Reason: "manual",
		}, "user")
		require.NoError(t, err)

		seedIncidentData(t, ctx, testDB, 307, "test_already_resolved", datasetURN, "passed", time.Now())

		count, err := store.AutoResolveIncidents(ctx, "test_already_resolved", datasetURN, 307, gracePeriod)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "Already-resolved incidents should not be touched")
	})

	t.Run("does not touch muted incidents", func(t *testing.T) {
		failedAt := time.Now().Add(-2 * time.Hour)
		seedIncidentData(t, ctx, testDB, 308, "test_muted_skip", datasetURN, "failed", failedAt)

		_, err := store.SetResolution(ctx, 308, correlation.ResolutionRequest{
			Status:   correlation.ResolutionMuted,
			Reason:   "false_positive",
			MuteDays: 30,
		}, "user")
		require.NoError(t, err)

		seedIncidentData(t, ctx, testDB, 309, "test_muted_skip", datasetURN, "passed", time.Now())

		count, err := store.AutoResolveIncidents(ctx, "test_muted_skip", datasetURN, 309, gracePeriod)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "Muted incidents should not be auto-resolved")
	})

	t.Run("no matching incidents returns zero", func(t *testing.T) {
		count, err := store.AutoResolveIncidents(ctx, "nonexistent_test", "nonexistent_urn", 999, gracePeriod)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestQueryIncidents_WithResolutionStatusFilter(t *testing.T) {
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
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	// Use distinct dataset URNs to avoid cross-join amplification in the materialized view.
	// The view joins test_results → lineage_edges(output), so sharing a dataset URN across
	// multiple run_ids produces a cartesian product.
	now := time.Now()
	runID := uuid.New().String()

	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO job_runs (run_id, job_name, job_namespace, current_state,
		    event_type, event_time, started_at, producer_name)
		VALUES ($1, 'test_job', 'test_ns', 'COMPLETE', 'COMPLETE', $2, $3, 'dbt')
	`, runID, now, now.Add(-5*time.Minute))
	require.NoError(t, err)

	urns := []string{
		"urn:postgres:warehouse:public.open_table",
		"urn:postgres:warehouse:public.acked_table",
		"urn:postgres:warehouse:public.resolved_table",
		"urn:postgres:warehouse:public.muted_table",
	}
	for _, urn := range urns {
		_, err = testDB.Connection.ExecContext(ctx, `
			INSERT INTO datasets (dataset_urn, name, namespace) VALUES ($1, $1, 'public')
		`, urn)
		require.NoError(t, err)

		_, err = testDB.Connection.ExecContext(ctx, `
			INSERT INTO lineage_edges (run_id, dataset_urn, edge_type) VALUES ($1, $2, 'output')
		`, runID, urn)
		require.NoError(t, err)
	}

	// Insert 4 failed test results, one per dataset
	_, err = testDB.Connection.ExecContext(ctx, `
		INSERT INTO test_results (id, test_name, test_type, dataset_urn, run_id, status, message, executed_at, duration_ms)
		VALUES
			(400, 'test_open',     'not_null', $1, $5, 'failed', 'msg', $6, 100),
			(401, 'test_acked',    'not_null', $2, $5, 'failed', 'msg', $6, 100),
			(402, 'test_resolved', 'not_null', $3, $5, 'failed', 'msg', $6, 100),
			(403, 'test_muted',    'not_null', $4, $5, 'failed', 'msg', $6, 100)
	`, urns[0], urns[1], urns[2], urns[3], runID, now)
	require.NoError(t, err)

	// Set resolutions: 400=open (implicit), 401=acknowledged, 402=resolved, 403=muted
	_, err = store.SetResolution(ctx, 401, correlation.ResolutionRequest{
		Status: correlation.ResolutionAcknowledged,
		Reason: "manual",
	}, "user")
	require.NoError(t, err)

	_, err = store.SetResolution(ctx, 402, correlation.ResolutionRequest{
		Status: correlation.ResolutionResolved,
		Reason: "manual",
	}, "user")
	require.NoError(t, err)

	_, err = store.SetResolution(ctx, 403, correlation.ResolutionRequest{
		Status:   correlation.ResolutionMuted,
		Reason:   "false_positive",
		MuteDays: 30,
	}, "user")
	require.NoError(t, err)

	// Refresh views so the materialized view picks up the test data
	err = store.InitResolvedDatasets(ctx)
	require.NoError(t, err)
	err = store.refreshViews(ctx)
	require.NoError(t, err)

	t.Run("active filter returns open and acknowledged", func(t *testing.T) {
		result, err := store.QueryIncidents(ctx, &correlation.IncidentFilter{
			StatusFilter: correlation.StatusFilterActive,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, result.Total, "Active should return open + acknowledged")

		statuses := make(map[correlation.ResolutionStatus]bool)
		for _, inc := range result.Incidents {
			statuses[inc.ResolutionStatus] = true
		}

		assert.True(t, statuses[correlation.ResolutionOpen])
		assert.True(t, statuses[correlation.ResolutionAcknowledged])
	})

	t.Run("resolved filter returns only resolved", func(t *testing.T) {
		result, err := store.QueryIncidents(ctx, &correlation.IncidentFilter{
			StatusFilter: correlation.StatusFilterResolved,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Total)
		assert.Equal(t, correlation.ResolutionResolved, result.Incidents[0].ResolutionStatus)
	})

	t.Run("muted filter returns only muted", func(t *testing.T) {
		result, err := store.QueryIncidents(ctx, &correlation.IncidentFilter{
			StatusFilter: correlation.StatusFilterMuted,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, result.Total)
		assert.Equal(t, correlation.ResolutionMuted, result.Incidents[0].ResolutionStatus)
	})

	t.Run("all filter returns everything", func(t *testing.T) {
		result, err := store.QueryIncidents(ctx, &correlation.IncidentFilter{
			StatusFilter: correlation.StatusFilterAll,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 4, result.Total)
	})

	t.Run("empty status filter defaults to active", func(t *testing.T) {
		result, err := store.QueryIncidents(ctx, &correlation.IncidentFilter{}, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, result.Total, "Default (empty) should behave like active")
	})
}

// TestAutoResolveOnIngestion_E2E tests the full ingestion path:
// ingest a failing test event → ingest a passing test event → verify auto-resolve.
func TestAutoResolveOnIngestion_E2E(t *testing.T) {
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
	store, err := NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	t.Run("ingest fail then pass triggers auto-resolve", func(t *testing.T) {
		// Step 1: Ingest a FAILING test event with executed_at 2 hours ago (outside grace period)
		failEventTime := time.Now().Add(-2 * time.Hour)
		failEvent := createTestEventWithTime("auto-resolve-e2e-fail", ingestion.EventTypeComplete, 1, 1, failEventTime)
		failEvent.Inputs[0].InputFacets = ingestion.Facets{
			"dataQualityAssertions": map[string]interface{}{
				"_producer":  "https://github.com/correlator-io/dbt-correlator/0.1.0",
				"_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
				"assertions": []interface{}{
					map[string]interface{}{
						"assertion": "not_null_customers_id",
						"success":   false,
					},
				},
			},
		}

		stored, _, err := store.StoreEvent(ctx, failEvent)
		require.NoError(t, err)
		assert.True(t, stored, "Fail event should be stored")

		// Verify the failed test result exists
		failedCount := countTestResultsByStatus(ctx, t, conn, failEvent.Run.ID, "failed")
		assert.Equal(t, 1, failedCount, "Should have 1 failed test result")

		// Get the failing test_result_id for verification
		var failedTestResultID int64

		err = testDB.Connection.QueryRowContext(ctx,
			"SELECT id FROM test_results WHERE run_id = $1 AND status = 'failed'",
			failEvent.Run.ID).Scan(&failedTestResultID)
		require.NoError(t, err)

		// Verify no resolution exists yet (implicitly open)
		res, err := store.GetResolution(ctx, failedTestResultID)
		require.NoError(t, err)
		assert.Nil(t, res, "No resolution should exist before pass event")

		// Step 2: Ingest a PASSING test event for the same (test_name, dataset_urn)
		// Use a different run_id (new job run) but same input dataset and test name
		passEvent := createTestEvent("auto-resolve-e2e-pass", ingestion.EventTypeComplete, 1, 1)
		passEvent.Inputs[0] = failEvent.Inputs[0] // Same dataset
		passEvent.Inputs[0].InputFacets = ingestion.Facets{
			"dataQualityAssertions": map[string]interface{}{
				"_producer":  "https://github.com/correlator-io/dbt-correlator/0.1.0",
				"_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
				"assertions": []interface{}{
					map[string]interface{}{
						"assertion": "not_null_customers_id",
						"success":   true,
					},
				},
			},
		}

		stored, _, err = store.StoreEvent(ctx, passEvent)
		require.NoError(t, err)
		assert.True(t, stored, "Pass event should be stored")

		// Step 3: Verify auto-resolve was triggered
		res, err = store.GetResolution(ctx, failedTestResultID)
		require.NoError(t, err)
		require.NotNil(t, res, "Resolution should exist after pass event")
		assert.Equal(t, correlation.ResolutionResolved, res.Status)
		assert.Equal(t, "auto", res.ResolvedBy)
		assert.Equal(t, "auto_pass", res.ResolutionReason)
		require.NotNil(t, res.ResolvedByTestResultID, "Should reference the passing test result")
	})

	t.Run("anti-flapping: pass within grace period does NOT auto-resolve", func(t *testing.T) {
		// Step 1: Ingest a FAILING test event with executed_at 30 min ago (WITHIN grace period)
		failEventTime := time.Now().Add(-30 * time.Minute)
		failEvent := createTestEventWithTime("anti-flap-fail", ingestion.EventTypeComplete, 1, 1, failEventTime)
		failEvent.Inputs[0].InputFacets = ingestion.Facets{
			"dataQualityAssertions": map[string]interface{}{
				"_producer":  "https://github.com/correlator-io/dbt-correlator/0.1.0",
				"_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
				"assertions": []interface{}{
					map[string]interface{}{
						"assertion": "unique_orders_id",
						"success":   false,
					},
				},
			},
		}

		stored, _, err := store.StoreEvent(ctx, failEvent)
		require.NoError(t, err)
		assert.True(t, stored)

		var failedTestResultID int64

		err = testDB.Connection.QueryRowContext(ctx,
			"SELECT id FROM test_results WHERE run_id = $1 AND status = 'failed'",
			failEvent.Run.ID).Scan(&failedTestResultID)
		require.NoError(t, err)

		// Step 2: Ingest a PASSING test event immediately
		passEvent := createTestEvent("anti-flap-pass", ingestion.EventTypeComplete, 1, 1)
		passEvent.Inputs[0] = failEvent.Inputs[0]
		passEvent.Inputs[0].InputFacets = ingestion.Facets{
			"dataQualityAssertions": map[string]interface{}{
				"_producer":  "https://github.com/correlator-io/dbt-correlator/0.1.0",
				"_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
				"assertions": []interface{}{
					map[string]interface{}{
						"assertion": "unique_orders_id",
						"success":   true,
					},
				},
			},
		}

		stored, _, err = store.StoreEvent(ctx, passEvent)
		require.NoError(t, err)
		assert.True(t, stored)

		// Step 3: Verify incident is NOT resolved (grace period not elapsed)
		res, err := store.GetResolution(ctx, failedTestResultID)
		require.NoError(t, err)
		assert.Nil(t, res, "Incident should remain open — failure is within grace period")
	})
}
