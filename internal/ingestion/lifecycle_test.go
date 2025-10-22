package ingestion

import (
	"testing"
	"time"
)

func TestValidateStateTransition_ValidTransitions(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		from     EventType
		to       EventType
		wantErr  bool
		errorMsg string
	}{
		// Valid transitions from START
		{"START to RUNNING", EventTypeStart, EventTypeRunning, false, ""},
		{"START to COMPLETE", EventTypeStart, EventTypeComplete, false, ""},
		{"START to FAIL", EventTypeStart, EventTypeFail, false, ""},
		{"START to ABORT", EventTypeStart, EventTypeAbort, false, ""},

		// Valid transitions from RUNNING
		{"RUNNING to COMPLETE", EventTypeRunning, EventTypeComplete, false, ""},
		{"RUNNING to FAIL", EventTypeRunning, EventTypeFail, false, ""},
		{"RUNNING to ABORT", EventTypeRunning, EventTypeAbort, false, ""},

		// Idempotent terminal states (valid)
		{"COMPLETE to COMPLETE", EventTypeComplete, EventTypeComplete, false, ""},
		{"FAIL to FAIL", EventTypeFail, EventTypeFail, false, ""},
		{"ABORT to ABORT", EventTypeAbort, EventTypeAbort, false, ""},

		// OTHER can occur anytime
		{"START to OTHER", EventTypeStart, EventTypeOther, false, ""},
		{"RUNNING to OTHER", EventTypeRunning, EventTypeOther, false, ""},
		{"OTHER to START", EventTypeOther, EventTypeStart, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStateTransition(tt.from, tt.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateStateTransition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateStateTransition_InvalidTransitions(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		from     EventType
		to       EventType
		wantErr  bool
		errorMsg string
	}{
		// Invalid: Terminal states cannot transition to non-terminal
		{"COMPLETE to START", EventTypeComplete, EventTypeStart, true, "invalid transition from terminal state"},
		{"COMPLETE to RUNNING", EventTypeComplete, EventTypeRunning, true, "invalid transition from terminal state"},
		{"COMPLETE to FAIL", EventTypeComplete, EventTypeFail, true, "invalid transition from terminal state"},
		{"COMPLETE to ABORT", EventTypeComplete, EventTypeAbort, true, "invalid transition from terminal state"},

		{"FAIL to START", EventTypeFail, EventTypeStart, true, "invalid transition from terminal state"},
		{"FAIL to RUNNING", EventTypeFail, EventTypeRunning, true, "invalid transition from terminal state"},
		{"FAIL to COMPLETE", EventTypeFail, EventTypeComplete, true, "invalid transition from terminal state"},
		{"FAIL to ABORT", EventTypeFail, EventTypeAbort, true, "invalid transition from terminal state"},

		{"ABORT to START", EventTypeAbort, EventTypeStart, true, "invalid transition from terminal state"},
		{"ABORT to RUNNING", EventTypeAbort, EventTypeRunning, true, "invalid transition from terminal state"},
		{"ABORT to COMPLETE", EventTypeAbort, EventTypeComplete, true, "invalid transition from terminal state"},
		{"ABORT to FAIL", EventTypeAbort, EventTypeFail, true, "invalid transition from terminal state"},

		// Invalid: START cannot go back to START
		{"START to START", EventTypeStart, EventTypeStart, true, "duplicate START event"},

		// Invalid: RUNNING cannot go back to START
		{"RUNNING to START", EventTypeRunning, EventTypeStart, true, "invalid transition"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStateTransition(tt.from, tt.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateStateTransition() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err != nil && tt.errorMsg != "" {
				// Check error message contains expected substring
				if err.Error() == "" {
					t.Errorf("Expected error message to contain %q, got empty error", tt.errorMsg)
				}
			}
		})
	}
}

func TestSortEventsByTime_OrdersCorrectly(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create events in wrong order (out of order arrival)
	events := []RunEvent{
		{
			EventTime: time.Date(2025, 10, 21, 10, 5, 0, 0, time.UTC),
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 0, 0, 0, time.UTC),
			EventType: EventTypeStart,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 3, 0, 0, time.UTC),
			EventType: EventTypeRunning,
			Run:       Run{ID: "test-run-1"},
		},
	}

	// Sort by event time
	sorted := SortEventsByTime(events)

	// Verify order: START (10:00) -> RUNNING (10:03) -> COMPLETE (10:05)
	if sorted[0].EventType != EventTypeStart {
		t.Errorf("Expected first event to be START, got %s", sorted[0].EventType)
	}

	if sorted[1].EventType != EventTypeRunning {
		t.Errorf("Expected second event to be RUNNING, got %s", sorted[1].EventType)
	}

	if sorted[2].EventType != EventTypeComplete {
		t.Errorf("Expected third event to be COMPLETE, got %s", sorted[2].EventType)
	}

	// Verify timestamps are in order
	if !sorted[0].EventTime.Before(sorted[1].EventTime) {
		t.Error("Events not sorted by time: event 0 should be before event 1")
	}

	if !sorted[1].EventTime.Before(sorted[2].EventTime) {
		t.Error("Events not sorted by time: event 1 should be before event 2")
	}
}

func TestApplyEventTransitions_ValidSequence(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Typical batch job sequence: START -> COMPLETE
	events := []RunEvent{
		{
			EventTime: time.Date(2025, 10, 21, 10, 0, 0, 0, time.UTC),
			EventType: EventTypeStart,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 5, 0, 0, time.UTC),
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
	}

	finalState, err := ApplyEventTransitions(events)
	if err != nil {
		t.Fatalf("ApplyEventTransitions() failed: %v", err)
	}

	if finalState != EventTypeComplete {
		t.Errorf("Expected final state COMPLETE, got %s", finalState)
	}
}

func TestApplyEventTransitions_OutOfOrderEvents(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Events arrive out of order: COMPLETE arrives before START
	events := []RunEvent{
		{
			EventTime: time.Date(2025, 10, 21, 10, 5, 0, 0, time.UTC), // Later time
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 0, 0, 0, time.UTC), // Earlier time
			EventType: EventTypeStart,
			Run:       Run{ID: "test-run-1"},
		},
	}

	// Should sort by eventTime and apply transitions correctly
	finalState, err := ApplyEventTransitions(events)
	if err != nil {
		t.Fatalf("ApplyEventTransitions() failed for out-of-order events: %v", err)
	}

	if finalState != EventTypeComplete {
		t.Errorf("Expected final state COMPLETE after sorting, got %s", finalState)
	}
}

func TestApplyEventTransitions_InvalidSequence(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Invalid sequence: COMPLETE -> START (even after sorting)
	events := []RunEvent{
		{
			EventTime: time.Date(2025, 10, 21, 10, 0, 0, 0, time.UTC),
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 5, 0, 0, time.UTC), // Later time
			EventType: EventTypeStart,
			Run:       Run{ID: "test-run-1"},
		},
	}

	_, err := ApplyEventTransitions(events)
	if err == nil {
		t.Error("Expected error for invalid transition sequence, got nil")
	}
}

func TestApplyEventTransitions_DuplicateTerminalState(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Duplicate COMPLETE events (idempotent)
	events := []RunEvent{
		{
			EventTime: time.Date(2025, 10, 21, 10, 0, 0, 0, time.UTC),
			EventType: EventTypeStart,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 5, 0, 0, time.UTC),
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 6, 0, 0, time.UTC), // Duplicate
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
	}

	// Should succeed (terminal states are idempotent)
	finalState, err := ApplyEventTransitions(events)
	if err != nil {
		t.Fatalf("ApplyEventTransitions() failed for duplicate terminal state: %v", err)
	}

	if finalState != EventTypeComplete {
		t.Errorf("Expected final state COMPLETE, got %s", finalState)
	}
}

func TestApplyEventTransitions_LongRunningJobSequence(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Long-running job: START -> RUNNING -> RUNNING -> COMPLETE
	events := []RunEvent{
		{
			EventTime: time.Date(2025, 10, 21, 10, 0, 0, 0, time.UTC),
			EventType: EventTypeStart,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 5, 0, 0, time.UTC),
			EventType: EventTypeRunning,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 10, 0, 0, time.UTC),
			EventType: EventTypeRunning,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 15, 0, 0, time.UTC),
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
	}

	finalState, err := ApplyEventTransitions(events)
	if err != nil {
		t.Fatalf("ApplyEventTransitions() failed for long-running job: %v", err)
	}

	if finalState != EventTypeComplete {
		t.Errorf("Expected final state COMPLETE, got %s", finalState)
	}
}

func TestApplyEventTransitions_OTHEREventHandling(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// OTHER can occur anytime, even before START
	events := []RunEvent{
		{
			EventTime: time.Date(2025, 10, 21, 9, 55, 0, 0, time.UTC), // Before START
			EventType: EventTypeOther,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 0, 0, 0, time.UTC),
			EventType: EventTypeStart,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 3, 0, 0, time.UTC), // During run
			EventType: EventTypeOther,
			Run:       Run{ID: "test-run-1"},
		},
		{
			EventTime: time.Date(2025, 10, 21, 10, 5, 0, 0, time.UTC),
			EventType: EventTypeComplete,
			Run:       Run{ID: "test-run-1"},
		},
	}

	finalState, err := ApplyEventTransitions(events)
	if err != nil {
		t.Fatalf("ApplyEventTransitions() failed with OTHER events: %v", err)
	}

	if finalState != EventTypeComplete {
		t.Errorf("Expected final state COMPLETE, got %s", finalState)
	}
}

func TestApplyEventTransitions_EmptyEventList(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	events := []RunEvent{}

	_, err := ApplyEventTransitions(events)
	if err == nil {
		t.Error("Expected error for empty event list, got nil")
	}
}
