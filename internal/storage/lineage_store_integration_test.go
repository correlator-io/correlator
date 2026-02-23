package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/correlator-io/correlator/internal/ingestion"
)

const defaultTestProducer = "https://github.com/dbt-labs/dbt-core/tree/1.5.0"

type (
	// assertionData holds test data for creating assertions.
	assertionData struct {
		assertion  string
		success    bool
		column     string
		durationMs int    // Extended field: test execution duration in milliseconds
		message    string // Extended field: test failure message
	}

	// testResultRow holds query results for test result verification.
	testResultRow struct {
		testName   string
		status     string
		datasetURN string
		jobRunID   string
		durationMs int    // Extended field
		message    string // Extended field
	}
)

// TestLineageStoreIntegration runs all integration tests for LineageStore.
func TestLineageStoreIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, t)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewLineageStore(conn, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewLineageStore() error = %v", err)
	}

	// Run all storage tests using the shared store
	t.Run("StoreEvent_SingleSuccess", testStoreEventSingleSuccess(ctx, store, conn))
	t.Run("StoreEvent_Duplicate", testStoreEventDuplicate(ctx, store, conn))
	t.Run("StoreEvent_OutOfOrder", testStoreEventOutOfOrder(ctx, store, conn))
	t.Run("StoreEvent_TerminalStateProtection", testStoreEventTerminalStateProtection(ctx, store, conn))
	t.Run("StoreEvent_MultipleInputsOutputs", testStoreEventMultipleInputsOutputs(ctx, store, conn))
	t.Run("StoreEvent_IdempotencyTTL", testStoreEventIdempotencyTTL(ctx, store, conn))
	t.Run("StoreEvents_AllSuccess", testStoreEventsAllSuccess(ctx, store))
	t.Run("StoreEvents_PartialSuccess", testStoreEventsPartialSuccess(ctx, store))
	t.Run("StoreEvents_AllDuplicates", testStoreEventsAllDuplicates(ctx, store))
	t.Run("DeferredFKConstraints_TableLevel", testDeferredFKConstraintsAtTableLevel(ctx, conn))
	t.Run("StoreEvent_StateHistoryUpdate", testStoreEventStateHistoryUpdate(ctx, store, conn))
	t.Run("StoreEvent_SameStateNoRedundantTransitions", testStoreEventSameStateNoRedundantTransitions(ctx, store, conn))
	t.Run("StoreEvent_ProducerExtraction", testStoreEventProducerExtraction(ctx, store, conn))
	t.Run("StoreEvent_DatasetFacetMerge", testStoreEventDatasetFacetMerge(ctx, store, conn))
	t.Run("StoreEvent_InputValidation", testStoreEventInputValidation(ctx, store))
	t.Run("StoreEvent_ContextCancellation", testStoreEventContextCancellation(ctx, store))
	t.Run("StoreEvent_ParentRunFacet", testStoreEventParentRunFacet(ctx, store, conn))

	// Close the main store BEFORE running cleanup tests to prevent goroutine interference
	// Cleanup tests create their own stores with custom intervals
	_ = store.Close()

	// Run cleanup tests (each creates its own store)
	t.Run("Cleanup_ExpiredIdempotencyKeys", testCleanupExpiredIdempotencyKeys(ctx, conn))
	t.Run("Cleanup_GracefulShutdown", testCleanupGracefulShutdown(ctx, conn))
	t.Run("Cleanup_ConcurrentOperations", testCleanupConcurrentOperations(ctx, conn))
}

// testStoreEventSingleSuccess verifies storing a single dbt START event.
// Expected: Event stored successfully, all tables populated correctly.
func testStoreEventSingleSuccess(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		event := createTestEvent(
			"dbt-start-1",
			ingestion.EventTypeStart,
			1, // 1 input
			1, // 1 output
		)

		stored, duplicate, err := store.StoreEvent(ctx, event)
		if err != nil {
			t.Fatalf("StoreEvent() error = %v", err)
		}

		if !stored {
			t.Errorf("StoreEvent() stored = false, want true")
		}

		if duplicate {
			t.Errorf("StoreEvent() duplicate = true, want false")
		}

		// Verify job_runs table
		verifyJobRunExists(ctx, t, conn, event)

		// Verify datasets table (1 input + 1 output = 2 datasets)
		verifyDatasetCountForJobRun(ctx, t, conn, event.JobRunID(), 2)

		// Verify lineage_edges table (1 input edge + 1 output edge = 2 edges)
		verifyLineageEdgeCount(ctx, t, conn, event.JobRunID(), 2)

		// Verify idempotency key recorded
		verifyIdempotencyKeyExists(ctx, t, conn, event.IdempotencyKey())
	}
}

// testStoreEventDuplicate verifies duplicate event handling (idempotency).
// Expected: Second store returns (false, true, nil) → HTTP 200 OK, not 409 Conflict.
func testStoreEventDuplicate(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		event := createTestEvent(
			"dbt-duplicate-1",
			ingestion.EventTypeStart,
			1,
			1,
		)

		// First store - should succeed
		stored1, duplicate1, err1 := store.StoreEvent(ctx, event)
		if err1 != nil {
			t.Fatalf("First StoreEvent() error = %v", err1)
		}

		if !stored1 {
			t.Errorf("First StoreEvent() stored = false, want true")
		}

		if duplicate1 {
			t.Errorf("First StoreEvent() duplicate = true, want false")
		}

		// Second store - should return duplicate (not error)
		stored2, duplicate2, err2 := store.StoreEvent(ctx, event)
		if err2 != nil {
			t.Errorf("Second StoreEvent() error = %v, want nil (duplicates are success)", err2)
		}

		if stored2 {
			t.Errorf("Second StoreEvent() stored = true, want false (duplicate not stored)")
		}

		if !duplicate2 {
			t.Errorf("Second StoreEvent() duplicate = false, want true")
		}

		// Verify only one job_run entry exists (not two)
		count := countJobRuns(ctx, t, conn, event.JobRunID())
		if count != 1 {
			t.Errorf("Expected 1 job_run, got %d (duplicate should not create new row)", count)
		}
	}
}

// testStoreEventOutOfOrder verifies out-of-order event handling.
// Expected: COMPLETE arrives before START, events sorted by eventTime.
func testStoreEventOutOfOrder(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		baseTime := time.Now().Add(-1 * time.Hour)

		// Create COMPLETE event (later in time)
		completeEvent := createTestEventWithTime(
			"dbt-outoforder-1",
			ingestion.EventTypeComplete,
			1,
			1,
			baseTime.Add(10*time.Minute), // T+10 minutes
		)

		// Create START event (earlier in time)
		startEvent := createTestEventWithTime(
			"dbt-outoforder-1", // Same run_id
			ingestion.EventTypeStart,
			1,
			1,
			baseTime, // T+0 (earlier)
		)

		// Store COMPLETE first (out of order)
		stored1, _, err1 := store.StoreEvent(ctx, completeEvent)
		if err1 != nil {
			t.Fatalf("StoreEvent(COMPLETE) error = %v", err1)
		}

		if !stored1 {
			t.Errorf("StoreEvent(COMPLETE) stored = false, want true")
		}

		// Store START second (should handle out-of-order via eventTime comparison)
		_, _, err2 := store.StoreEvent(ctx, startEvent)
		if err2 != nil {
			t.Fatalf("StoreEvent(START) error = %v", err2)
		}

		// 1. Verify final state is COMPLETE (not overwritten by older START event)
		finalState := getJobRunState(ctx, t, conn, completeEvent.JobRunID())
		if finalState != string(ingestion.EventTypeComplete) {
			t.Errorf("Final state = %s, want COMPLETE (newer event should win)", finalState)
		}

		// 2. Verify event_time is GREATEST (COMPLETE event time, not START)
		eventTime := getJobRunEventTime(ctx, t, conn, completeEvent.JobRunID())
		expectedTime := baseTime.Add(10 * time.Minute) // COMPLETE event time
		// Use time comparison with tolerance (database may have microsecond differences)
		timeDiff := eventTime.Sub(expectedTime)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		if timeDiff > time.Second {
			t.Errorf("event_time = %v, want %v (COMPLETE event time, GREATEST)", eventTime, expectedTime)
		}

		// 3. Verify metadata was updated to COMPLETE event's metadata (newer wins)
		metadata := getJobRunMetadata(ctx, t, conn, completeEvent.JobRunID())
		if schemaURL, ok := metadata["schema_url"].(string); !ok || schemaURL != completeEvent.SchemaURL {
			t.Errorf("metadata.schema_url = %v, want %s (COMPLETE event metadata should win)",
				metadata["schema_url"], completeEvent.SchemaURL)
		}

		if producer, ok := metadata["producer"].(string); !ok || producer != completeEvent.Producer {
			t.Errorf("metadata.producer = %v, want %s (COMPLETE event metadata should win)",
				metadata["producer"], completeEvent.Producer)
		}
	}
}

// testStoreEventTerminalStateProtection verifies database trigger protection.
// Expected: COMPLETE → START transition rejected by trigger.
func testStoreEventTerminalStateProtection(
	ctx context.Context,
	store *LineageStore,
	conn *Connection,
) func(*testing.T) {
	return func(t *testing.T) {
		baseTime := time.Now()

		// Create COMPLETE event
		completeEvent := createTestEventWithTime(
			"dbt-terminal-1",
			ingestion.EventTypeComplete,
			1,
			1,
			baseTime,
		)

		// Store COMPLETE first
		_, _, err1 := store.StoreEvent(ctx, completeEvent)
		if err1 != nil {
			t.Fatalf("StoreEvent(COMPLETE) error = %v", err1)
		}

		// Create START event with LATER timestamp (attempting invalid transition)
		startEvent := createTestEventWithTime(
			"dbt-terminal-1", // Same run_id
			ingestion.EventTypeStart,
			1,
			1,
			baseTime.Add(1*time.Minute), // Later timestamp
		)

		// Store START - should fail due to trigger protection
		_, _, err2 := store.StoreEvent(ctx, startEvent)
		if err2 == nil {
			t.Errorf("StoreEvent(START) should have returned an error")
		}

		// Terminal state protection is now in the application layer (lineage_store.go)
		if !containsString(err2.Error(), "invalid state transition from terminal state") {
			t.Errorf("StoreEvent(START) error should mention terminal state, got: %v", err2)
		}

		if !containsString(err2.Error(), "COMPLETE") || !containsString(err2.Error(), "START") {
			t.Errorf("StoreEvent(START) error should mention states COMPLETE and START, got: %v", err2)
		}

		// We expect this to either fail or be ignored (implementation dependent)
		// The important thing is terminal state is NOT changed

		// Verify state remains COMPLETE
		finalState := getJobRunState(ctx, t, conn, completeEvent.JobRunID())
		if finalState != string(ingestion.EventTypeComplete) {
			t.Errorf("State changed from COMPLETE to %s (terminal state should be immutable)", finalState)
		}
	}
}

// testStoreEventMultipleInputsOutputs verifies multiple datasets per event.
// Expected: Event with 3 inputs + 2 outputs → 5 lineage_edges rows.
func testStoreEventMultipleInputsOutputs(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		event := createTestEvent(
			"dbt-multi-1",
			ingestion.EventTypeComplete,
			3, // 3 inputs
			2, // 2 outputs
		)

		stored, _, err := store.StoreEvent(ctx, event)
		if err != nil {
			t.Fatalf("StoreEvent() error = %v", err)
		}

		if !stored {
			t.Errorf("StoreEvent() stored = false, want true")
		}

		// Verify 5 datasets created (3 inputs + 2 outputs)
		verifyDatasetCountForJobRun(ctx, t, conn, event.JobRunID(), 5)

		// Verify 5 lineage edges (3 input edges + 2 output edges)
		verifyLineageEdgeCount(ctx, t, conn, event.JobRunID(), 5)

		// Verify edge types
		inputCount := countLineageEdgesByType(ctx, t, conn, event.JobRunID(), "input")
		outputCount := countLineageEdgesByType(ctx, t, conn, event.JobRunID(), "output")

		if inputCount != 3 {
			t.Errorf("Input edge count = %d, want 3", inputCount)
		}

		if outputCount != 2 {
			t.Errorf("Output edge count = %d, want 2", outputCount)
		}
	}
}

// testStoreEventIdempotencyTTL verifies idempotency key expiration.
// Expected: Expired idempotency key (>24 hours) allows re-storage.
func testStoreEventIdempotencyTTL(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		event := createTestEvent(
			"dbt-ttl-1",
			ingestion.EventTypeStart,
			1,
			1,
		)

		// Store event first time
		stored1, _, err1 := store.StoreEvent(ctx, event)
		if err1 != nil {
			t.Fatalf("First StoreEvent() error = %v", err1)
		}

		if !stored1 {
			t.Errorf("First StoreEvent() stored = false, want true")
		}

		// Verify expires_at is set to ~24 hours from now
		expiresAt := getIdempotencyExpiration(ctx, t, conn, event.IdempotencyKey())
		expectedExpiration := time.Now().Add(24 * time.Hour)

		timeDiff := expiresAt.Sub(expectedExpiration)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		// Allow 5 second tolerance for test execution time
		if timeDiff > 5*time.Second {
			t.Errorf("expires_at = %v, expected ~%v (diff: %v, should be ~24 hours)",
				expiresAt, expectedExpiration, timeDiff)
		}

		// Manually expire the idempotency key (simulate 25 hours passed)
		expireIdempotencyKey(ctx, t, conn, event.IdempotencyKey())

		// Store same event again - should succeed (idempotency expired)
		stored2, duplicate2, err2 := store.StoreEvent(ctx, event)
		if err2 != nil {
			t.Fatalf("Second StoreEvent() error = %v", err2)
		}

		if !stored2 {
			t.Errorf("Second StoreEvent() stored = false, want true (idempotency expired)")
		}

		if duplicate2 {
			t.Errorf("Second StoreEvent() duplicate = true, want false (idempotency expired)")
		}
	}
}

// testStoreEventsAllSuccess verifies batch storage with all events succeeding.
// Expected: All 5 events stored successfully, results all show stored=true.
func testStoreEventsAllSuccess(ctx context.Context, store *LineageStore) func(*testing.T) {
	return func(t *testing.T) {
		events := []*ingestion.RunEvent{
			createTestEvent("dbt-batch-1", ingestion.EventTypeStart, 1, 1),
			createTestEvent("dbt-batch-2", ingestion.EventTypeStart, 1, 1),
			createTestEvent("dbt-batch-3", ingestion.EventTypeStart, 1, 1),
			createTestEvent("dbt-batch-4", ingestion.EventTypeStart, 1, 1),
			createTestEvent("dbt-batch-5", ingestion.EventTypeStart, 1, 1),
		}

		results, err := store.StoreEvents(ctx, events)
		if err != nil {
			t.Fatalf("StoreEvents() error = %v", err)
		}

		if len(results) != 5 {
			t.Fatalf("StoreEvents() returned %d results, want 5", len(results))
		}

		// All should be stored successfully
		for i, result := range results {
			if !result.Stored {
				t.Errorf("Result[%d] stored = false, want true", i)
			}

			if result.Duplicate {
				t.Errorf("Result[%d] duplicate = true, want false", i)
			}

			if result.Error != nil {
				t.Errorf("Result[%d] error = %v, want nil", i, result.Error)
			}
		}
	}
}

// testStoreEventsPartialSuccess verifies partial success pattern (207 Multi-Status).
// Expected: 1 duplicate + 3 success → results show mixed outcomes.
func testStoreEventsPartialSuccess(ctx context.Context, store *LineageStore) func(*testing.T) {
	return func(t *testing.T) {
		// Pre-store one event to create duplicate scenario
		duplicate := createTestEvent(
			"dbt-partial-dup",
			ingestion.EventTypeStart,
			1,
			1,
		)
		_, _, _ = store.StoreEvent(ctx, duplicate)

		events := []*ingestion.RunEvent{
			duplicate, // This will be a duplicate
			createTestEvent("dbt-partial-1", ingestion.EventTypeStart, 1, 1),
			createTestEvent("dbt-partial-2", ingestion.EventTypeStart, 1, 1),
			createTestEvent("dbt-partial-3", ingestion.EventTypeStart, 1, 1),
		}

		results, err := store.StoreEvents(ctx, events)
		if err != nil {
			t.Fatalf("StoreEvents() error = %v", err)
		}

		if len(results) != 4 {
			t.Fatalf("StoreEvents() returned %d results, want 4", len(results))
		}

		// First should be duplicate
		if !results[0].Duplicate {
			t.Errorf("Result[0] duplicate = false, want true")
		}

		if results[0].Stored {
			t.Errorf("Result[0] stored = true, want false")
		}

		// Rest should be stored
		for i := 1; i < 4; i++ {
			if !results[i].Stored {
				t.Errorf("Result[%d] stored = false, want true", i)
			}

			if results[i].Duplicate {
				t.Errorf("Result[%d] duplicate = true, want false", i)
			}
		}
	}
}

// testStoreEventsAllDuplicates verifies all duplicates scenario.
// Expected: All events return duplicate=true (not errors).
func testStoreEventsAllDuplicates(ctx context.Context, store *LineageStore) func(*testing.T) {
	return func(t *testing.T) {
		events := []*ingestion.RunEvent{
			createTestEvent("dbt-alldup-1", ingestion.EventTypeStart, 1, 1),
			createTestEvent("dbt-alldup-2", ingestion.EventTypeStart, 1, 1),
		}

		// Store all events first time
		_, _ = store.StoreEvents(ctx, events)

		// Store same events again - all should be duplicates
		results, err := store.StoreEvents(ctx, events)
		if err != nil {
			t.Fatalf("StoreEvents() error = %v", err)
		}

		if len(results) != 2 {
			t.Fatalf("StoreEvents() returned %d results, want 2", len(results))
		}

		// All should be duplicates
		for i, result := range results {
			if result.Stored {
				t.Errorf("Result[%d] stored = true, want false (duplicate)", i)
			}

			if !result.Duplicate {
				t.Errorf("Result[%d] duplicate = false, want true", i)
			}

			if result.Error != nil {
				t.Errorf("Result[%d] error = %v, want nil (duplicates are not errors)", i, result.Error)
			}
		}
	}
}

// testDeferredFKConstraintsAtTableLevel verifies PostgreSQL deferred FK constraints work correctly.
// This test directly verifies the database-level deferred FK behavior by inserting a lineage_edge
// BEFORE the referenced dataset exists, which would fail with immediate FK constraints.
//
// Expected: Can insert lineage_edge before dataset exists, FK constraint checked at COMMIT.
func testDeferredFKConstraintsAtTableLevel(ctx context.Context, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		jobRunID := "test-deferred-fk-" + uuid.NewString()
		datasetURN := "urn:postgresql://prod-db:5432/analytics.public.test_deferred_table"

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		defer func() {
			_ = tx.Rollback()
		}()

		// Step 1: Create job_run (required by job_run_id FK in lineage_edges)
		//nolint:dupword
		_, err = tx.ExecContext(ctx, `
			INSERT INTO job_runs (
				job_run_id, run_id, job_name, job_namespace,
				current_state, event_type, event_time, state_history,
				metadata, producer_name, started_at, created_at, updated_at
			) VALUES (
				$1, $2, 'test_job', 'test://namespace',
				'START', 'START', NOW(), '{"transitions": []}',
				'{}', 'test-producer', NOW(), NOW(), NOW()
			)
		`, jobRunID, uuid.NewString())
		if err != nil {
			t.Fatalf("Failed to insert job_run: %v", err)
		}

		// Step 2: CRITICAL - Insert lineage_edge BEFORE dataset exists
		// This creates a temporary FK violation (dataset_urn -> datasets.dataset_urn)
		// WITHOUT deferred FK: Would fail immediately with FK constraint error
		// WITH deferred FK: Allowed, constraint checked at COMMIT time
		_, err = tx.ExecContext(ctx, `
			INSERT INTO lineage_edges (
				job_run_id, dataset_urn, edge_type, created_at
			) VALUES ($1, $2, 'input', NOW())
		`, jobRunID, datasetURN)
		if err != nil {
			t.Fatalf("Inserting edge before dataset should succeed with deferred FK, got: %v", err)
		}

		// At this point, FK is violated but transaction hasn't committed yet
		// This proves FK constraint check is DEFERRED (not immediate)

		// Step 3: Insert dataset (parent row) to satisfy FK constraint before COMMIT
		_, err = tx.ExecContext(ctx, `
			INSERT INTO datasets (
				dataset_urn, name, namespace, facets, created_at, updated_at
			) VALUES ($1, $2, $3, '{}', NOW(), NOW())
		`, datasetURN, "analytics.public.test_deferred_table", "postgresql://prod-db:5432")
		if err != nil {
			t.Fatalf("Failed to insert dataset: %v", err)
		}

		// Step 4: COMMIT - Deferred FK constraint check happens HERE
		// Should succeed because FK is now satisfied (dataset exists)
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed, but FK should be satisfied: %v", err)
		}

		// Step 5: Verify both rows were committed successfully
		var edgeCount int

		err = conn.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM lineage_edges WHERE job_run_id = $1
		`, jobRunID).Scan(&edgeCount)
		if err != nil {
			t.Fatalf("Failed to query lineage_edges: %v", err)
		}

		if edgeCount != 1 {
			t.Errorf("Expected 1 lineage_edge, got %d", edgeCount)
		}

		var datasetCount int

		err = conn.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM datasets WHERE dataset_urn = $1
		`, datasetURN).Scan(&datasetCount)
		if err != nil {
			t.Fatalf("Failed to query datasets: %v", err)
		}

		if datasetCount != 1 {
			t.Errorf("Expected 1 dataset, got %d", datasetCount)
		}
	}
}

// testStoreEventStateHistoryUpdate verifies state_history JSONB updates.
// Expected: Full happy path START → RUNNING → COMPLETE with validated transitions.
// The first event establishes the initial state (from: null → to: START).
func testStoreEventStateHistoryUpdate(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		baseTime := time.Now()

		// Store START event
		startEvent := createTestEventWithTime(
			"dbt-history-1",
			ingestion.EventTypeStart,
			1,
			1,
			baseTime,
		)

		_, _, err1 := store.StoreEvent(ctx, startEvent)
		if err1 != nil {
			t.Fatalf("StoreEvent(START) error = %v", err1)
		}

		// Verify initial state is START with initial transition recorded
		currentState := getJobRunState(ctx, t, conn, startEvent.JobRunID())
		if currentState != string(ingestion.EventTypeStart) {
			t.Errorf("After START: current_state = %s, want START", currentState)
		}

		// Verify initial transition (null → START) is recorded
		initialHistory := getStateHistory(ctx, t, conn, startEvent.JobRunID())
		if len(initialHistory) != 1 {
			t.Errorf("After START: state_history length = %d, want 1", len(initialHistory))
		} else {
			// First transition should be null → START
			if initialHistory[0]["from"] != nil {
				t.Errorf("Initial transition: from = %v, want nil", initialHistory[0]["from"])
			}

			if toState, ok := initialHistory[0]["to"].(string); !ok || toState != "START" {
				t.Errorf("Initial transition: to = %v, want START", initialHistory[0]["to"])
			}
		}

		// Store RUNNING event (transition: START → RUNNING)
		runningEvent := createTestEventWithTime(
			"dbt-history-1", // Same run_id
			ingestion.EventTypeRunning,
			1,
			1,
			baseTime.Add(2*time.Minute),
		)

		_, _, err2 := store.StoreEvent(ctx, runningEvent)
		if err2 != nil {
			t.Fatalf("StoreEvent(RUNNING) error = %v", err2)
		}

		// Verify state transitioned to RUNNING
		currentState = getJobRunState(ctx, t, conn, runningEvent.JobRunID())
		if currentState != string(ingestion.EventTypeRunning) {
			t.Errorf("After RUNNING: current_state = %s, want RUNNING", currentState)
		}

		// Store COMPLETE event (transition: RUNNING → COMPLETE)
		completeEvent := createTestEventWithTime(
			"dbt-history-1", // Same run_id
			ingestion.EventTypeComplete,
			1,
			1,
			baseTime.Add(5*time.Minute),
		)

		_, _, err3 := store.StoreEvent(ctx, completeEvent)
		if err3 != nil {
			t.Fatalf("StoreEvent(COMPLETE) error = %v", err3)
		}

		// Verify final state is COMPLETE
		currentState = getJobRunState(ctx, t, conn, completeEvent.JobRunID())
		if currentState != string(ingestion.EventTypeComplete) {
			t.Errorf("After COMPLETE: current_state = %s, want COMPLETE", currentState)
		}

		// Verify state_history contains all 3 transitions
		stateHistory := getStateHistory(ctx, t, conn, completeEvent.JobRunID())

		// Debug: Print actual transitions if count is wrong
		if len(stateHistory) != 3 {
			t.Logf("Got %d transitions instead of 3:", len(stateHistory))

			for i, trans := range stateHistory {
				t.Logf("  Transition %d: %+v", i, trans)
			}

			t.Fatalf("state_history length = %d, want 3 transitions", len(stateHistory))
		}

		// Validate transition 0: null → START (initial state)
		transition0 := stateHistory[0]
		if transition0["from"] != nil {
			t.Errorf("Transition 0: from = %v, want nil", transition0["from"])
		}

		if toState, ok := transition0["to"].(string); !ok || toState != "START" {
			t.Errorf("Transition 0: to = %v, want START", transition0["to"])
		}

		// Validate transition 1: START → RUNNING
		transition1 := stateHistory[1]
		if fromState, ok := transition1["from"].(string); !ok || fromState != "START" {
			t.Errorf("Transition 1: from = %v, want START", transition1["from"])
		}

		if toState, ok := transition1["to"].(string); !ok || toState != "RUNNING" {
			t.Errorf("Transition 1: to = %v, want RUNNING", transition1["to"])
		}

		// Validate transition 2: RUNNING → COMPLETE
		transition2 := stateHistory[2]
		if fromState, ok := transition2["from"].(string); !ok || fromState != "RUNNING" {
			t.Errorf("Transition 2: from = %v, want RUNNING", transition2["from"])
		}

		if toState, ok := transition2["to"].(string); !ok || toState != "COMPLETE" {
			t.Errorf("Transition 2: to = %v, want COMPLETE", transition2["to"])
		}

		// Verify event_time and updated_at are present in all transitions
		for i, trans := range stateHistory {
			if _, ok := trans["event_time"].(string); !ok {
				t.Errorf("Transition %d: event_time missing or not a string", i)
			}

			if _, ok := trans["updated_at"].(string); !ok {
				t.Errorf("Transition %d: updated_at missing or not a string", i)
			}
		}
	}
}

// testStoreEventSameStateNoRedundantTransitions verifies that same-state events
// do NOT create redundant state_history transitions.
// This is the key test for the state transition refactoring.
//
// Scenario: Multiple COMPLETE events with different facet data (e.g., metrics)
// Expected: Only ONE transition recorded (START → COMPLETE), not multiple COMPLETE → COMPLETE.
func testStoreEventSameStateNoRedundantTransitions(
	ctx context.Context,
	store *LineageStore,
	conn *Connection,
) func(*testing.T) {
	return func(t *testing.T) {
		baseTime := time.Now()

		// Store START event
		startEvent := createTestEventWithTime(
			"dbt-same-state-1",
			ingestion.EventTypeStart,
			1,
			1,
			baseTime,
		)

		_, _, err1 := store.StoreEvent(ctx, startEvent)
		if err1 != nil {
			t.Fatalf("StoreEvent(START) error = %v", err1)
		}

		// Store first COMPLETE event
		completeEvent1 := createTestEventWithTime(
			"dbt-same-state-1", // Same run_id
			ingestion.EventTypeComplete,
			1,
			1,
			baseTime.Add(2*time.Minute),
		)

		_, _, err2 := store.StoreEvent(ctx, completeEvent1)
		if err2 != nil {
			t.Fatalf("StoreEvent(COMPLETE 1) error = %v", err2)
		}

		// Store second COMPLETE event (e.g., with metrics facet data)
		completeEvent2 := createTestEventWithTime(
			"dbt-same-state-1", // Same run_id
			ingestion.EventTypeComplete,
			1,
			1,
			baseTime.Add(3*time.Minute),
		)

		_, _, err3 := store.StoreEvent(ctx, completeEvent2)
		if err3 != nil {
			t.Fatalf("StoreEvent(COMPLETE 2) error = %v", err3)
		}

		// Store third COMPLETE event (more facet updates)
		completeEvent3 := createTestEventWithTime(
			"dbt-same-state-1", // Same run_id
			ingestion.EventTypeComplete,
			1,
			1,
			baseTime.Add(4*time.Minute),
		)

		_, _, err4 := store.StoreEvent(ctx, completeEvent3)
		if err4 != nil {
			t.Fatalf("StoreEvent(COMPLETE 3) error = %v", err4)
		}

		// Verify state_history contains exactly 2 transitions:
		// 1. null → START (initial state)
		// 2. START → COMPLETE
		// NOT: COMPLETE → COMPLETE (redundant)
		stateHistory := getStateHistory(ctx, t, conn, startEvent.JobRunID())

		if len(stateHistory) != 2 {
			t.Logf("Got %d transitions instead of 2:", len(stateHistory))

			for i, trans := range stateHistory {
				t.Logf("  Transition %d: %+v", i, trans)
			}

			t.Fatalf("state_history length = %d, want 2 (same-state events should not create transitions)", len(stateHistory))
		}

		// Verify first transition: null → START
		if stateHistory[0]["from"] != nil {
			t.Errorf("Transition 0: from = %v, want nil", stateHistory[0]["from"])
		}

		if toState, ok := stateHistory[0]["to"].(string); !ok || toState != "START" {
			t.Errorf("Transition 0: to = %v, want START", stateHistory[0]["to"])
		}

		// Verify second transition: START → COMPLETE
		if fromState, ok := stateHistory[1]["from"].(string); !ok || fromState != "START" {
			t.Errorf("Transition 1: from = %v, want START", stateHistory[1]["from"])
		}

		if toState, ok := stateHistory[1]["to"].(string); !ok || toState != "COMPLETE" {
			t.Errorf("Transition 1: to = %v, want COMPLETE", stateHistory[1]["to"])
		}

		// Verify final state is still COMPLETE
		finalState := getJobRunState(ctx, t, conn, startEvent.JobRunID())
		if finalState != string(ingestion.EventTypeComplete) {
			t.Errorf("Final state = %s, want COMPLETE", finalState)
		}
	}
}

// testStoreEventProducerExtraction verifies producer name extraction.
// Expected: "https://github.com/dbt-labs/dbt-core/1.5.0" → producer_name="dbt-core".
func testStoreEventProducerExtraction(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		event := createTestEvent(
			"dbt-producer-1",
			ingestion.EventTypeStart,
			1,
			1,
		)

		stored, _, err := store.StoreEvent(ctx, event)
		if err != nil {
			t.Fatalf("StoreEvent() error = %v", err)
		}

		if !stored {
			t.Errorf("StoreEvent() stored = false, want true")
		}

		// Verify producer_name extracted correctly
		producerName := getProducerName(ctx, t, conn, event.JobRunID())
		if producerName != "dbt-core" {
			t.Errorf("producer_name = %q, want %q", producerName, "dbt-core")
		}
	}
}

// testStoreEventDatasetFacetMerge verifies JSONB facet merge behavior.
// Expected: Facets accumulate over time, newer values override older ones.
func testStoreEventDatasetFacetMerge(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		// Event 1: Dataset with facets {"schema": "v1", "owner": "alice"}
		event1 := createTestEvent("facet-merge-1", ingestion.EventTypeStart, 1, 0)
		event1.Inputs[0].Facets = ingestion.Facets{
			"schema": map[string]interface{}{"version": "v1"},
			"owner":  "alice",
		}

		stored1, _, err1 := store.StoreEvent(ctx, event1)
		if err1 != nil {
			t.Fatalf("First StoreEvent() error = %v", err1)
		}

		if !stored1 {
			t.Errorf("First StoreEvent() stored = false, want true")
		}

		// Event 2: Same dataset with facets {"rows": 1000, "schema": "v2"}
		event2 := createTestEvent("facet-merge-2", ingestion.EventTypeComplete, 1, 0)
		event2.Inputs[0].Namespace = event1.Inputs[0].Namespace
		event2.Inputs[0].Name = event1.Inputs[0].Name // Same dataset!
		event2.Inputs[0].Facets = ingestion.Facets{
			"rows":   float64(1000),
			"schema": map[string]interface{}{"version": "v2"},
		}

		stored2, _, err2 := store.StoreEvent(ctx, event2)
		if err2 != nil {
			t.Fatalf("Second StoreEvent() error = %v", err2)
		}

		if !stored2 {
			t.Errorf("Second StoreEvent() stored = false, want true")
		}

		// Verify merged facets: {"schema": "v2", "owner": "alice", "rows": 1000}
		facets := getDatasetFacets(ctx, t, conn, event1.Inputs[0].URN())

		// Check schema was updated to v2 (newer value wins)
		if schema, ok := facets["schema"].(map[string]interface{}); !ok {
			t.Errorf("schema facet missing or wrong type")
		} else if version, ok := schema["version"].(string); !ok || version != "v2" {
			t.Errorf("schema.version = %v, want v2 (newer should win)", schema["version"])
		}

		// Check owner preserved from Event 1
		if owner, ok := facets["owner"].(string); !ok || owner != "alice" {
			t.Errorf("owner = %v, want alice (should be preserved)", facets["owner"])
		}

		// Check rows added from Event 2
		if rows, ok := facets["rows"].(float64); !ok || int(rows) != 1000 {
			t.Errorf("rows = %v, want 1000 (should be added)", facets["rows"])
		}
	}
}

// testStoreEventInputValidation verifies defensive input validation.
// Expected: Nil and invalid inputs return appropriate errors.
func testStoreEventInputValidation(ctx context.Context, store *LineageStore) func(*testing.T) {
	return func(t *testing.T) {
		tests := []struct {
			name    string
			mutate  func(*ingestion.RunEvent)
			wantErr string
		}{
			{
				name: "nil event",
				mutate: func(_ *ingestion.RunEvent) {
					// Will test nil separately
				},
				wantErr: "event is nil",
			},
			{
				name: "nil inputs",
				mutate: func(e *ingestion.RunEvent) {
					e.Inputs = nil
				},
				wantErr: "event.Inputs is nil",
			},
			{
				name: "nil outputs",
				mutate: func(e *ingestion.RunEvent) {
					e.Outputs = nil
				},
				wantErr: "event.Outputs is nil",
			},
			{
				name: "empty run ID",
				mutate: func(e *ingestion.RunEvent) {
					e.Run.ID = ""
				},
				wantErr: "event.Run.ID is empty",
			},
			{
				name: "empty job name",
				mutate: func(e *ingestion.RunEvent) {
					e.Job.Name = ""
				},
				wantErr: "event.Job.Name is empty",
			},
			{
				name: "zero event time",
				mutate: func(e *ingestion.RunEvent) {
					e.EventTime = time.Time{}
				},
				wantErr: "event.EventTime is zero",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.name == "nil event" {
					_, _, err := store.StoreEvent(ctx, nil)
					if err == nil {
						t.Errorf("Expected error, got nil")
					} else if !containsString(err.Error(), tt.wantErr) {
						t.Errorf("Expected error containing %q, got %v", tt.wantErr, err)
					}

					return
				}

				event := createTestEvent("validation-test", ingestion.EventTypeStart, 1, 1)
				tt.mutate(event)

				_, _, err := store.StoreEvent(ctx, event)
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if !containsString(err.Error(), tt.wantErr) {
					t.Errorf("Expected error containing %q, got %v", tt.wantErr, err)
				}
			})
		}
	}
}

// testStoreEventContextCancellation verifies graceful context cancellation handling.
// Expected: Cancelled context returns appropriate error.
func testStoreEventContextCancellation(ctx context.Context, store *LineageStore) func(*testing.T) {
	return func(t *testing.T) {
		event := createTestEvent("ctx-cancel", ingestion.EventTypeStart, 1, 1)

		// Create context that's already cancelled
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		_, _, err := store.StoreEvent(cancelledCtx, event)
		if err == nil {
			t.Error("Expected error with cancelled context, got nil")
		}

		// Verify error mentions context cancellation or related terms
		errMsg := err.Error()
		if !containsString(errMsg, "context canceled") &&
			!containsString(errMsg, "request cancelled") &&
			!containsString(errMsg, "operation timeout") {
			t.Errorf("Expected context cancellation error, got: %v", err)
		}
	}
}

// Helper functions for test setup and verification

// containsString is a helper that checks if a string contains a substring.
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}

// Helper functions for test setup and verification

// createTestEvent creates a test OpenLineage event with specified parameters.
func createTestEvent(
	runID string,
	eventType ingestion.EventType,
	numInputs,
	numOutputs int,
) *ingestion.RunEvent {
	return createTestEventWithTime(runID, eventType, numInputs, numOutputs, time.Now())
}

// createTestEventWithTime creates a test event with explicit timestamp.
func createTestEventWithTime(
	runID string,
	eventType ingestion.EventType,
	numInputs,
	numOutputs int,
	eventTime time.Time,
) *ingestion.RunEvent {
	// Generate a deterministic UUID for Run.ID based on runID string
	// This ensures events with the same runID get the same UUID (for correlation)
	// Use UUID v5 (name-based) with DNS namespace for deterministic generation
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // DNS namespace
	runUUID := uuid.NewSHA1(namespace, []byte(runID)).String()

	event := &ingestion.RunEvent{
		EventTime: eventTime,
		EventType: eventType,
		Producer:  defaultTestProducer,
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: ingestion.Run{
			ID:     runUUID, // Use UUID instead of plain string
			Facets: ingestion.Facets{},
		},
		Job: ingestion.Job{
			Namespace: "dbt://analytics",
			Name:      "transform_orders_" + runID, // Keep runID for unique job names
			Facets:    ingestion.Facets{},
		},
		Inputs:  make([]ingestion.Dataset, numInputs),
		Outputs: make([]ingestion.Dataset, numOutputs),
	}

	// Create input datasets
	for i := 0; i < numInputs; i++ {
		event.Inputs[i] = ingestion.Dataset{
			Namespace: "postgresql://prod-db:5432",
			Name:      "analytics.public.input_" + runID + "_" + string(rune('a'+i)),
			Facets:    ingestion.Facets{},
		}
	}

	// Create output datasets
	for i := 0; i < numOutputs; i++ {
		event.Outputs[i] = ingestion.Dataset{
			Namespace: "postgresql://prod-db:5432",
			Name:      "analytics.public.output_" + runID + "_" + string(rune('a'+i)),
			Facets:    ingestion.Facets{},
		}
	}

	return event
}

// Verification helper functions

func verifyJobRunExists(ctx context.Context, t *testing.T, conn *Connection, event *ingestion.RunEvent) {
	t.Helper()

	query := "SELECT COUNT(*) FROM job_runs WHERE job_run_id = $1"

	var count int

	err := conn.QueryRowContext(ctx, query, event.JobRunID()).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query job_runs: %v", err)
	}

	if count != 1 {
		t.Errorf("job_runs count = %d, want 1", count)
	}
}

func verifyLineageEdgeCount(ctx context.Context, t *testing.T, conn *Connection, jobRunID string, expectedCount int) {
	t.Helper()

	query := "SELECT COUNT(*) FROM lineage_edges WHERE job_run_id = $1"

	var count int

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query lineage_edges: %v", err)
	}

	if count != expectedCount {
		t.Errorf("lineage_edges count = %d, want %d", count, expectedCount)
	}
}

func verifyIdempotencyKeyExists(ctx context.Context, t *testing.T, conn *Connection, idempotencyKey string) {
	t.Helper()

	query := "SELECT COUNT(*) FROM lineage_event_idempotency WHERE idempotency_key = $1"

	var count int

	err := conn.QueryRowContext(ctx, query, idempotencyKey).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query lineage_event_idempotency: %v", err)
	}

	if count != 1 {
		t.Errorf("idempotency key count = %d, want 1", count)
	}
}

func countJobRuns(ctx context.Context, t *testing.T, conn *Connection, jobRunID string) int {
	t.Helper()

	query := "SELECT COUNT(*) FROM job_runs WHERE job_run_id = $1"

	var count int

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count job_runs: %v", err)
	}

	return count
}

func countLineageEdgesByType(
	ctx context.Context,
	t *testing.T,
	conn *Connection,
	jobRunID string,
	edgeType string,
) int {
	t.Helper()

	query := "SELECT COUNT(*) FROM lineage_edges WHERE job_run_id = $1 AND edge_type = $2"

	var count int

	err := conn.QueryRowContext(ctx, query, jobRunID, edgeType).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count lineage edges by type: %v", err)
	}

	return count
}

func getJobRunState(ctx context.Context, t *testing.T, conn *Connection, jobRunID string) string {
	t.Helper()

	query := "SELECT current_state FROM job_runs WHERE job_run_id = $1"

	var state string

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&state)
	if err != nil {
		t.Fatalf("Failed to get job run state: %v", err)
	}

	return state
}

func getProducerName(ctx context.Context, t *testing.T, conn *Connection, jobRunID string) string {
	t.Helper()

	query := "SELECT producer_name FROM job_runs WHERE job_run_id = $1"

	var producerName string

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&producerName)
	if err != nil {
		t.Fatalf("Failed to get producer name: %v", err)
	}

	return producerName
}

func getStateHistory(ctx context.Context, t *testing.T, conn *Connection, jobRunID string) []map[string]interface{} {
	t.Helper()

	query := "SELECT state_history FROM job_runs WHERE job_run_id = $1"

	var stateHistoryJSON []byte

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&stateHistoryJSON)
	if err != nil {
		t.Fatalf("Failed to get state history: %v", err)
	}

	var stateHistory struct {
		Transitions []map[string]interface{} `json:"transitions"`
	}

	if err := json.Unmarshal(stateHistoryJSON, &stateHistory); err != nil {
		t.Fatalf("Failed to parse state history JSON: %v", err)
	}

	return stateHistory.Transitions
}

func verifyDatasetCountForJobRun(
	ctx context.Context,
	t *testing.T,
	conn *Connection,
	jobRunID string,
	expectedCount int,
) {
	t.Helper()

	// Count distinct datasets referenced by this job run's lineage edges
	query := `
		SELECT COUNT(DISTINCT dataset_urn)
		FROM lineage_edges
		WHERE job_run_id = $1
	`

	var count int

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query datasets for job run: %v", err)
	}

	if count != expectedCount {
		t.Errorf("dataset count for job_run = %d, want %d", count, expectedCount)
	}
}

func expireIdempotencyKey(ctx context.Context, t *testing.T, conn *Connection, idempotencyKey string) {
	t.Helper()

	// Delete the idempotency key to simulate expiration (25 hours passed)
	// This simulates the application-level TTL cleanup that would normally run
	// In production, a background goroutine would delete expired keys
	query := "DELETE FROM lineage_event_idempotency WHERE idempotency_key = $1"

	_, err := conn.ExecContext(ctx, query, idempotencyKey)
	if err != nil {
		t.Fatalf("Failed to expire idempotency key: %v", err)
	}
}

func getJobRunEventTime(ctx context.Context, t *testing.T, conn *Connection, jobRunID string) time.Time {
	t.Helper()

	query := "SELECT event_time FROM job_runs WHERE job_run_id = $1"

	var eventTime time.Time

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&eventTime)
	if err != nil {
		t.Fatalf("Failed to get event_time: %v", err)
	}

	return eventTime
}

func getJobRunMetadata(ctx context.Context, t *testing.T, conn *Connection, jobRunID string) map[string]interface{} {
	t.Helper()

	query := "SELECT metadata FROM job_runs WHERE job_run_id = $1"

	var metadataJSON []byte

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&metadataJSON)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	var metadata map[string]interface{}

	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		t.Fatalf("Failed to parse metadata JSON: %v", err)
	}

	return metadata
}

func getIdempotencyExpiration(ctx context.Context, t *testing.T, conn *Connection, key string) time.Time {
	t.Helper()

	query := "SELECT expires_at FROM lineage_event_idempotency WHERE idempotency_key = $1"

	var expiresAt time.Time

	err := conn.QueryRowContext(ctx, query, key).Scan(&expiresAt)
	if err != nil {
		t.Fatalf("Failed to get expires_at: %v", err)
	}

	return expiresAt
}

func getDatasetFacets(ctx context.Context, t *testing.T, conn *Connection, datasetURN string) map[string]interface{} {
	t.Helper()

	query := "SELECT facets FROM datasets WHERE dataset_urn = $1"

	var facetsJSON []byte

	err := conn.QueryRowContext(ctx, query, datasetURN).Scan(&facetsJSON)
	if err != nil {
		t.Fatalf("Failed to get dataset facets: %v", err)
	}

	var facets map[string]interface{}

	if err := json.Unmarshal(facetsJSON, &facets); err != nil {
		t.Fatalf("Failed to parse facets JSON: %v", err)
	}

	return facets
}

// testCleanupExpiredIdempotencyKeys verifies cleanup deletes only expired keys.
// Expected: Expired keys deleted, valid keys preserved.
func testCleanupExpiredIdempotencyKeys(ctx context.Context, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		// Create LineageStore with 1-second cleanup interval (for testing)
		store, err := NewLineageStore(conn, 1*time.Second) //nolint:contextcheck
		if err != nil {
			t.Fatalf("NewLineageStore() error = %v", err)
		}

		defer func() {
			_ = store.Close()
		}()

		// Insert 3 test keys with unique prefix to avoid pollution from other tests
		// Use a unique prefix to distinguish this test's keys from keys inserted by storage tests
		testPrefix := "cleanup-test-"
		validKey1 := testPrefix + "valid-key-1"
		validKey2 := testPrefix + "valid-key-2"
		expiredKey := testPrefix + "expired-key-1"

		// Insert valid keys (24 hours from now)
		insertIdempotencyKey(ctx, t, conn, validKey1, time.Now().Add(24*time.Hour))
		insertIdempotencyKey(ctx, t, conn, validKey2, time.Now().Add(24*time.Hour))

		// Insert expired key (1 hour ago)
		insertIdempotencyKey(ctx, t, conn, expiredKey, time.Now().Add(-1*time.Hour))

		// Verify all 3 test keys exist before cleanup
		// Note: We only count keys with our test prefix to avoid pollution from storage tests
		if !idempotencyKeyExists(ctx, t, conn, validKey1) {
			t.Fatalf("Valid key 1 was not inserted")
		}

		if !idempotencyKeyExists(ctx, t, conn, validKey2) {
			t.Fatalf("Valid key 2 was not inserted")
		}

		if !idempotencyKeyExists(ctx, t, conn, expiredKey) {
			t.Fatalf("Expired key was not inserted")
		}

		// Call cleanup directly (don't wait for ticker)
		// Use context with timeout to match production behavior
		cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 30*time.Second)
		defer cleanupCancel()

		store.cleanupExpiredIdempotencyKeys(cleanupCtx)

		// Verify valid keys still exist (not deleted by cleanup)
		if !idempotencyKeyExists(ctx, t, conn, validKey1) {
			t.Errorf("Valid key 1 was deleted (should be preserved)")
		}

		if !idempotencyKeyExists(ctx, t, conn, validKey2) {
			t.Errorf("Valid key 2 was deleted (should be preserved)")
		}

		// Verify expired key was deleted by cleanup
		if idempotencyKeyExists(ctx, t, conn, expiredKey) {
			t.Errorf("Expired key still exists (should be deleted)")
		}
	}
}

// testCleanupGracefulShutdown verifies cleanup goroutine stops gracefully.
// Expected: Cleanup goroutine stops within timeout when Close() is called.
func testCleanupGracefulShutdown(_ context.Context, conn *Connection) func(*testing.T) {
	return func(t *testing.T) { //nolint:contextcheck
		// Create LineageStore with 1-second cleanup interval
		store, err := NewLineageStore(conn, 1*time.Second)
		if err != nil {
			t.Fatalf("NewLineageStore() error = %v", err)
		}

		// Give goroutine time to start
		time.Sleep(100 * time.Millisecond)

		// Close should complete within 10 seconds (generous timeout for slow CI)
		done := make(chan error, 1)

		go func() {
			done <- store.Close()
		}()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Close() returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("Close() did not complete within 10 seconds (cleanup goroutine failed to stop)")
		}
	}
}

// testCleanupConcurrentOperations verifies cleanup doesn't interfere with normal operations.
// Expected: No race conditions or deadlocks when cleanup runs concurrently with StoreEvent.
func testCleanupConcurrentOperations(ctx context.Context, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		// Create LineageStore with 100ms cleanup interval (aggressive for testing)
		store, err := NewLineageStore(conn, 100*time.Millisecond) //nolint:contextcheck
		if err != nil {
			t.Fatalf("NewLineageStore() error = %v", err)
		}
		defer store.Close()

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: Store events continuously
		go func() {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				event := createTestEvent(
					fmt.Sprintf("concurrent-%d", i),
					ingestion.EventTypeStart,
					1,
					1,
				)

				_, _, err := store.StoreEvent(ctx, event)
				if err != nil {
					t.Errorf("StoreEvent() error: %v", err)

					return
				}

				time.Sleep(20 * time.Millisecond) // 50 events per second
			}
		}()

		// Goroutine 2: Let cleanup run several times
		go func() {
			defer wg.Done()

			time.Sleep(1 * time.Second) // Allow multiple cleanup cycles (100ms interval)
		}()

		// Wait for both goroutines with timeout
		done := make(chan struct{})

		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success - both goroutines completed
		case <-time.After(10 * time.Second):
			t.Fatal("Concurrent operations did not complete within 10 seconds")
		}

		// Verify at least some events were stored successfully
		count := countAllIdempotencyKeys(ctx, t, conn)
		if count == 0 {
			t.Error("No events were stored during concurrent operations (possible deadlock or race condition)")
		}
	}
}

// Helper functions for cleanup tests

// insertIdempotencyKey inserts a test idempotency key with specified expiration.
func insertIdempotencyKey(ctx context.Context, t *testing.T, conn *Connection, key string, expiresAt time.Time) {
	t.Helper()

	// Set created_at to 1 hour before expires_at to satisfy CHECK constraint
	createdAt := expiresAt.Add(-1 * time.Hour)

	query := `
		INSERT INTO lineage_event_idempotency (
			idempotency_key,
			created_at,
			expires_at,
			event_metadata
		) VALUES ($1, $2, $3, '{}')
	`

	_, err := conn.ExecContext(ctx, query, key, createdAt, expiresAt)
	if err != nil {
		t.Fatalf("Failed to insert idempotency key: %v", err)
	}
}

// countAllIdempotencyKeys counts all idempotency keys (including expired).
func countAllIdempotencyKeys(ctx context.Context, t *testing.T, conn *Connection) int {
	t.Helper()

	query := "SELECT COUNT(*) FROM lineage_event_idempotency"

	var count int

	err := conn.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count idempotency keys: %v", err)
	}

	return count
}

// idempotencyKeyExists checks if a specific idempotency key exists.
func idempotencyKeyExists(ctx context.Context, t *testing.T, conn *Connection, key string) bool {
	t.Helper()

	query := "SELECT COUNT(*) FROM lineage_event_idempotency WHERE idempotency_key = $1"

	var count int

	err := conn.QueryRowContext(ctx, query, key).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to check idempotency key existence: %v", err)
	}

	return count > 0
}

// ============================================================================
// dataQualityAssertions Facet Extraction Tests
// ============================================================================

// TestExtractDataQualityAssertions tests extraction of test results from
// dataQualityAssertions facets in OpenLineage events.
func TestExtractDataQualityAssertions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, t)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewLineageStore(conn, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewLineageStore() error = %v", err)
	}

	defer func() {
		_ = store.Close()
	}()

	// Run all facet extraction tests using the shared store
	t.Run("SingleAssertion", func(t *testing.T) {
		testExtractSingleAssertion(ctx, t, store, conn)
	})
	t.Run("MultipleAssertions", func(t *testing.T) {
		testExtractMultipleAssertions(ctx, t, store, conn)
	})
	t.Run("NoFacet", func(t *testing.T) {
		testExtractNoFacet(ctx, t, store, conn)
	})
	t.Run("MalformedFacet", func(t *testing.T) {
		testExtractMalformedFacet(ctx, t, store, conn)
	})
	t.Run("ExtendedFields", func(t *testing.T) {
		testExtractExtendedFields(ctx, t, store, conn)
	})
}

// testExtractSingleAssertion verifies extraction of a single assertion
// from the dataQualityAssertions facet in an OpenLineage event.
func testExtractSingleAssertion(ctx context.Context, t *testing.T, store *LineageStore, conn *Connection) {
	t.Helper()

	// Create event with dataQualityAssertions facet
	event := createEventWithAssertions(
		"single-assertion-test",
		[]assertionData{
			{
				assertion: "not_null_orders_order_id",
				success:   true,
				column:    "order_id",
			},
		},
	)

	// Store event
	stored, duplicate, err := store.StoreEvent(ctx, event)
	require.NoError(t, err)
	assert.True(t, stored, "Event should be stored")
	assert.False(t, duplicate, "Event should not be duplicate")

	// Verify test_results table
	count := countTestResultsForJobRun(ctx, t, conn, event.JobRunID())
	assert.Equal(t, 1, count, "Should have 1 test result extracted from facet")

	// Verify test result details
	testResult := getTestResultByTestName(ctx, t, conn, "not_null_orders_order_id")
	assert.Equal(t, "passed", testResult.status, "Assertion success=true should map to 'passed'")
	assert.Equal(t, event.JobRunID(), testResult.jobRunID, "Job run ID should match")

	// Issue 6 fix: Use exact URN match instead of Contains
	// Note: URN() method normalizes by removing default PostgreSQL port (5432)
	expectedDatasetURN := "postgresql://prod-db/analytics.public.input_single-assertion-test_a"
	assert.Equal(t, expectedDatasetURN, testResult.datasetURN, "Dataset URN should match exactly")
}

// testExtractMultipleAssertions verifies extraction of multiple assertions
// from a single dataQualityAssertions facet.
func testExtractMultipleAssertions(ctx context.Context, t *testing.T, store *LineageStore, conn *Connection) {
	t.Helper()

	// Create event with 5 assertions on same dataset
	event := createEventWithAssertions(
		"multi-assertion-test",
		[]assertionData{
			{assertion: "not_null_orders_order_id", success: true, column: "order_id"},
			{assertion: "unique_orders_order_id", success: true, column: "order_id"},
			{assertion: "not_null_orders_customer_id", success: false, column: "customer_id"},
			{assertion: "accepted_values_orders_status", success: true, column: "status"},
			{assertion: "relationships_orders_customer", success: false, column: "customer_id"},
		},
	)

	// Store event
	stored, _, err := store.StoreEvent(ctx, event)
	require.NoError(t, err)
	assert.True(t, stored)

	// Verify all 5 assertions extracted
	count := countTestResultsForJobRun(ctx, t, conn, event.JobRunID())
	assert.Equal(t, 5, count, "Should have 5 test results extracted")

	// Verify status mapping
	passedCount := countTestResultsByStatus(ctx, t, conn, event.JobRunID(), "passed")
	failedCount := countTestResultsByStatus(ctx, t, conn, event.JobRunID(), "failed")
	assert.Equal(t, 3, passedCount, "Should have 3 passed tests")
	assert.Equal(t, 2, failedCount, "Should have 2 failed tests")
}

// testExtractNoFacet verifies graceful handling when dataQualityAssertions
// facet is not present.
func testExtractNoFacet(ctx context.Context, t *testing.T, store *LineageStore, conn *Connection) {
	t.Helper()

	// Create event WITHOUT dataQualityAssertions facet
	event := createTestEvent("no-facet-test", ingestion.EventTypeComplete, 1, 1)

	// Store event
	stored, _, err := store.StoreEvent(ctx, event)
	require.NoError(t, err)
	assert.True(t, stored, "Event should be stored even without facet")

	// Verify no test_results created
	count := countTestResultsForJobRun(ctx, t, conn, event.JobRunID())
	assert.Equal(t, 0, count, "Should have 0 test results (no facet)")
}

// testExtractMalformedFacet verifies non-blocking handling of malformed
// dataQualityAssertions facet.
func testExtractMalformedFacet(ctx context.Context, t *testing.T, store *LineageStore, conn *Connection) {
	t.Helper()

	// Create event with malformed facet (assertions is not an array)
	event := createTestEvent("malformed-facet-test", ingestion.EventTypeComplete, 1, 1)
	event.Inputs[0].InputFacets = ingestion.Facets{
		"dataQualityAssertions": map[string]interface{}{
			"_producer":  "test",
			"assertions": "not-an-array", // Invalid: should be []
		},
	}

	// Store event - should succeed (non-blocking extraction)
	stored, _, err := store.StoreEvent(ctx, event)
	require.NoError(t, err, "Event storage should succeed even with malformed facet")
	assert.True(t, stored, "Event should be stored")

	// Verify no test_results created (graceful failure)
	count := countTestResultsForJobRun(ctx, t, conn, event.JobRunID())
	assert.Equal(t, 0, count, "Should have 0 test results (malformed facet)")
}

// testExtractExtendedFields verifies extraction of extended fields (durationMs, message)
// from dataQualityAssertions facets. These are spec-compliant extensions allowed by
// OpenLineage's additionalProperties: true in the facet schema.
func testExtractExtendedFields(ctx context.Context, t *testing.T, store *LineageStore, conn *Connection) {
	t.Helper()

	// Create event with assertions containing extended fields
	event := createEventWithAssertions(
		"extended-fields-test",
		[]assertionData{
			{
				assertion:  "unique_customer_id",
				success:    false,
				column:     "customer_id",
				durationMs: 1250,
				message:    "Got 5 results, configured to fail if != 0",
			},
			{
				assertion:  "not_null_customer_name",
				success:    true,
				column:     "customer_name",
				durationMs: 823,
				message:    "", // Passed tests typically don't have message
			},
		},
	)

	// Store event
	stored, duplicate, err := store.StoreEvent(ctx, event)
	require.NoError(t, err)
	assert.True(t, stored, "Event should be stored")
	assert.False(t, duplicate, "Event should not be duplicate")

	// Verify test_results table
	count := countTestResultsForJobRun(ctx, t, conn, event.JobRunID())
	assert.Equal(t, 2, count, "Should have 2 test results extracted from facet")

	// Verify extended fields for failed test
	failedResult := getTestResultByTestName(ctx, t, conn, "unique_customer_id")
	assert.Equal(t, "failed", failedResult.status)
	assert.Equal(t, 1250, failedResult.durationMs, "durationMs should be extracted")
	assert.Equal(t, "Got 5 results, configured to fail if != 0", failedResult.message, "message should be extracted")

	// Verify extended fields for passed test
	passedResult := getTestResultByTestName(ctx, t, conn, "not_null_customer_name")
	assert.Equal(t, "passed", passedResult.status)
	assert.Equal(t, 823, passedResult.durationMs, "durationMs should be extracted for passed test")
	assert.Empty(t, passedResult.message, "message should be empty for passed test")
}

// ============================================================================
// Facet Extraction Helper Types and Functions
// ============================================================================

// createEventWithAssertions creates an OpenLineage event with dataQualityAssertions facet.
func createEventWithAssertions(runID string, assertions []assertionData) *ingestion.RunEvent {
	event := createTestEvent(runID, ingestion.EventTypeComplete, 1, 1)

	// Build assertions array
	assertionsArray := make([]interface{}, len(assertions))
	for i, a := range assertions {
		assertion := map[string]interface{}{
			"assertion": a.assertion,
			"success":   a.success,
		}
		if a.column != "" {
			assertion["column"] = a.column
		}
		// Add extended fields if provided
		if a.durationMs > 0 {
			assertion["durationMs"] = float64(a.durationMs) // JSON numbers are float64
		}

		if a.message != "" {
			assertion["message"] = a.message
		}

		assertionsArray[i] = assertion
	}

	// Add dataQualityAssertions facet to input
	event.Inputs[0].InputFacets = ingestion.Facets{
		"dataQualityAssertions": map[string]interface{}{
			"_producer":  "https://github.com/correlator-io/dbt-correlator/0.1.0",
			"_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
			"assertions": assertionsArray,
		},
	}

	return event
}

// countTestResultsForJobRun counts test_results rows for a job_run_id.
func countTestResultsForJobRun(ctx context.Context, t *testing.T, conn *Connection, jobRunID string) int {
	t.Helper()

	var count int

	query := "SELECT COUNT(*) FROM test_results WHERE job_run_id = $1"

	err := conn.QueryRowContext(ctx, query, jobRunID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count test_results: %v", err)
	}

	return count
}

// countTestResultsByStatus counts test_results by status for a job_run_id.
func countTestResultsByStatus(ctx context.Context, t *testing.T, conn *Connection, jobRunID, status string) int {
	t.Helper()

	var count int

	query := "SELECT COUNT(*) FROM test_results WHERE job_run_id = $1 AND status = $2"

	err := conn.QueryRowContext(ctx, query, jobRunID, status).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count test_results by status: %v", err)
	}

	return count
}

// getTestResultByTestName retrieves a test result by test name.
func getTestResultByTestName(ctx context.Context, t *testing.T, conn *Connection, testName string) testResultRow {
	t.Helper()

	var (
		result  testResultRow
		message sql.NullString
	)

	query := `SELECT test_name, status, dataset_urn, job_run_id, duration_ms, message
              FROM test_results WHERE test_name = $1`

	err := conn.QueryRowContext(ctx, query, testName).Scan(
		&result.testName, &result.status, &result.datasetURN, &result.jobRunID,
		&result.durationMs, &message,
	)
	if err != nil {
		t.Fatalf("Failed to get test result: %v", err)
	}

	result.message = message.String

	return result
}

// testStoreEventParentRunFacet verifies that ParentRunFacet is correctly extracted and stored.
// Expected: parent_job_run_id column is populated with canonical ID from ParentRunFacet.
func testStoreEventParentRunFacet(ctx context.Context, store *LineageStore, conn *Connection) func(*testing.T) {
	return func(t *testing.T) {
		parentRunUUID := uuid.New().String()
		childRunUUID := uuid.New().String()
		parentJobNamespace := "dbt://demo"
		parentJobName := "jaffle_shop.build"

		// Build ParentRunFacet as it comes from OpenLineage JSON
		parentRunFacet := map[string]interface{}{
			"job": map[string]interface{}{
				"namespace": parentJobNamespace,
				"name":      parentJobName,
			},
			"run": map[string]interface{}{
				"runId": parentRunUUID,
			},
		}

		// Create event with ParentRunFacet
		event := &ingestion.RunEvent{
			EventTime: time.Now(),
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
				Name:      "model.jaffle_shop.orders",
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

		stored, duplicate, err := store.StoreEvent(ctx, event)
		if err != nil {
			t.Fatalf("StoreEvent() error = %v", err)
		}

		if !stored {
			t.Error("StoreEvent() stored = false, want true")
		}

		if duplicate {
			t.Error("StoreEvent() duplicate = true, want false")
		}

		// Verify parent_job_run_id is stored correctly
		var storedParentJobRunID *string

		err = conn.QueryRowContext(ctx, `
			SELECT parent_job_run_id FROM job_runs WHERE run_id = $1
		`, childRunUUID).Scan(&storedParentJobRunID)
		if err != nil {
			t.Fatalf("Failed to query parent_job_run_id: %v", err)
		}

		if storedParentJobRunID == nil {
			t.Fatal("parent_job_run_id should not be NULL")
		}

		expectedParentJobRunID := "dbt:" + parentRunUUID
		if *storedParentJobRunID != expectedParentJobRunID {
			t.Errorf("parent_job_run_id = %q, want %q", *storedParentJobRunID, expectedParentJobRunID)
		}
	}
}
