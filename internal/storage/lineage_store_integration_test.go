package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/correlator-io/correlator/internal/ingestion"
)

const defaultTestProducer = "https://github.com/dbt-labs/dbt-core/tree/1.5.0"

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

	store, err := NewLineageStore(conn)
	if err != nil {
		t.Fatalf("NewLineageStore() error = %v", err)
	}

	// Run all test cases as subtests
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
	t.Run("StoreEvent_ProducerExtraction", testStoreEventProducerExtraction(ctx, store, conn))
	t.Run("StoreEvent_DatasetFacetMerge", testStoreEventDatasetFacetMerge(ctx, store, conn))
	t.Run("StoreEvent_InputValidation", testStoreEventInputValidation(ctx, store))
	t.Run("StoreEvent_ContextCancellation", testStoreEventContextCancellation(ctx, store))
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

		// Terminal state protection is happening at the database level. see migration 005.
		expectedErr := "lineage event storage failed: failed to upsert job_run: pq: Invalid state transition: COMPLETE -> START (terminal states are immutable)" //nolint:lll
		if err2.Error() != expectedErr {
			t.Errorf("StoreEvent(START) error = %v, wanted = %s", err2, expectedErr)
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

		// Verify initial state is START with no transitions yet
		currentState := getJobRunState(ctx, t, conn, startEvent.JobRunID())
		if currentState != string(ingestion.EventTypeStart) {
			t.Errorf("After START: current_state = %s, want START", currentState)
		}

		// Store RUNNING event (first transition: START → RUNNING)
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

		// Store COMPLETE event (second transition: RUNNING → COMPLETE)
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

		// Verify state_history contains both transitions
		stateHistory := getStateHistory(ctx, t, conn, completeEvent.JobRunID())

		// Debug: Print actual transitions if count is wrong
		if len(stateHistory) != 2 {
			t.Logf("Got %d transitions instead of 2:", len(stateHistory))

			for i, trans := range stateHistory {
				t.Logf("  Transition %d: %+v", i, trans)
			}

			t.Fatalf("state_history length = %d, want 2 transitions", len(stateHistory))
		}

		// Validate first transition: START → RUNNING
		// Note: Schema is set by job_run_state_validation trigger (migration 005)
		// Fields: {from, to, event_time, updated_at}
		transition1 := stateHistory[0]
		if fromState, ok := transition1["from"].(string); !ok || fromState != "START" {
			t.Errorf("Transition 1: from = %v, want START", transition1["from"])
		}

		if toState, ok := transition1["to"].(string); !ok || toState != "RUNNING" {
			t.Errorf("Transition 1: to = %v, want RUNNING", transition1["to"])
		}

		// Validate second transition: RUNNING → COMPLETE
		transition2 := stateHistory[1]
		if fromState, ok := transition2["from"].(string); !ok || fromState != "RUNNING" {
			t.Errorf("Transition 2: from = %v, want RUNNING", transition2["from"])
		}

		if toState, ok := transition2["to"].(string); !ok || toState != "COMPLETE" {
			t.Errorf("Transition 2: to = %v, want COMPLETE", transition2["to"])
		}

		// Verify event_time and updated_at are present in both transitions
		if _, ok := transition1["event_time"].(string); !ok {
			t.Errorf("Transition 1: event_time missing or not a string")
		}

		if _, ok := transition1["updated_at"].(string); !ok {
			t.Errorf("Transition 1: updated_at missing or not a string")
		}

		if _, ok := transition2["event_time"].(string); !ok {
			t.Errorf("Transition 2: event_time missing or not a string")
		}

		if _, ok := transition2["updated_at"].(string); !ok {
			t.Errorf("Transition 2: updated_at missing or not a string")
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

// Benchmark Tests

// BenchmarkLineageStore_StoreEventSingle benchmarks single event storage.
// Target: <100ms per event.
func BenchmarkLineageStore_StoreEventSingle(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, b)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewLineageStore(conn)
	if err != nil {
		b.Fatalf("NewLineageStore() error = %v", err)
	}

	events := make([]*ingestion.RunEvent, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = createTestEvent(
			fmt.Sprintf("bench-single-%d", i),
			ingestion.EventTypeStart,
			1, // 1 input
			1, // 1 output
		)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := store.StoreEvent(ctx, events[i])
		if err != nil {
			b.Fatalf("StoreEvent() error = %v", err)
		}
	}
}

// BenchmarkLineageStore_StoreEventsBatch benchmarks batch event storage.
// Target: <500ms for 100 events.
func BenchmarkLineageStore_StoreEventsBatch(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, b)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewLineageStore(conn)
	if err != nil {
		b.Fatalf("NewLineageStore() error = %v", err)
	}

	const batchSize = 100

	// Pre-create all batches (memory-intensive!)
	batches := make([][]*ingestion.RunEvent, b.N)
	for i := 0; i < b.N; i++ {
		batches[i] = make([]*ingestion.RunEvent, batchSize)
		for j := 0; j < batchSize; j++ {
			batches[i][j] = createTestEvent(
				fmt.Sprintf("bench-batch-%d-%d", i, j),
				ingestion.EventTypeStart,
				1, 1,
			)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.StoreEvents(ctx, batches[i])
		if err != nil {
			b.Fatalf("StoreEvents() error = %v", err)
		}
	}
}
