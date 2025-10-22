// Package ingestion provides OpenLineage event lifecycle state machine.
// Handles state transitions, out-of-order events, and idempotency.
package ingestion

import (
	"errors"
	"fmt"
	"sort"
)

// Sentinel errors for state transition validation.
// These can be used with errors.Is() for error checking.
var (
	// ErrInvalidTransition indicates an invalid state transition.
	ErrInvalidTransition = errors.New("invalid state transition")

	// ErrTerminalStateImmutable indicates an attempt to transition from a terminal state.
	ErrTerminalStateImmutable = errors.New("terminal state is immutable")

	// ErrDuplicateStart indicates a duplicate START event for the same run.
	ErrDuplicateStart = errors.New("duplicate START event")

	// ErrBackwardTransition indicates an attempt to transition backwards (e.g., RUNNING → START).
	ErrBackwardTransition = errors.New("cannot transition backwards")

	// ErrEmptyEventList indicates an attempt to apply transitions to an empty event list.
	ErrEmptyEventList = errors.New("empty event list")
)

// ValidateStateTransition validates a state transition according to OpenLineage run cycle.
//
// Valid transitions:
//   - START → {RUNNING, COMPLETE, FAIL, ABORT}
//   - RUNNING → {COMPLETE, FAIL, ABORT}
//   - COMPLETE/FAIL/ABORT → same state (idempotent)
//   - OTHER → any state (special metadata event)
//   - any state → OTHER (special metadata event)
//
// Invalid transitions:
//   - Terminal states (COMPLETE, FAIL, ABORT) cannot transition to different states
//   - START → START (duplicate START)
//   - RUNNING → START (cannot go backwards)
//
// Spec: https://openlineage.io/docs/spec/run-cycle#run-states
func ValidateStateTransition(from, to EventType) error {
	// OTHER events can occur anytime (special metadata events)
	if from == EventTypeOther || to == EventTypeOther {
		return nil
	}

	// Terminal states can only transition to themselves (idempotent)
	if from.IsTerminal() {
		if from != to {
			return fmt.Errorf("%w: %s → %s (terminal states are immutable)", ErrTerminalStateImmutable, from, to)
		}

		return nil // Idempotent terminal state
	}

	// Duplicate START is invalid
	if from == EventTypeStart && to == EventTypeStart {
		return fmt.Errorf("%w: runId already has START state", ErrDuplicateStart)
	}

	// Valid transitions from START
	if from == EventTypeStart {
		validFromStart := map[EventType]bool{
			EventTypeRunning:  true,
			EventTypeComplete: true,
			EventTypeFail:     true,
			EventTypeAbort:    true,
		}
		if !validFromStart[to] {
			return fmt.Errorf("%w: START → %s", ErrInvalidTransition, to)
		}

		return nil
	}

	// Valid transitions from RUNNING
	if from == EventTypeRunning {
		if to == EventTypeStart {
			return fmt.Errorf("%w: RUNNING → START", ErrBackwardTransition)
		}
		// RUNNING can transition to COMPLETE, FAIL, ABORT, or stay RUNNING
		validFromRunning := map[EventType]bool{
			EventTypeRunning:  true, // Can send multiple RUNNING events
			EventTypeComplete: true,
			EventTypeFail:     true,
			EventTypeAbort:    true,
		}
		if !validFromRunning[to] {
			return fmt.Errorf("%w: RUNNING → %s", ErrInvalidTransition, to)
		}

		return nil
	}

	// If we get here, it's an invalid transition
	return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, from, to)
}

// SortEventsByTime sorts RunEvents by eventTime in ascending order.
// This is critical for handling out-of-order event arrival.
//
// OpenLineage events may arrive out of order due to:
//   - Network delays
//   - Retry mechanisms
//   - Distributed system timing
//
// Events must be sorted by eventTime (not arrival time) before applying state transitions.
func SortEventsByTime(events []RunEvent) []RunEvent {
	// Create a copy to avoid modifying the original slice
	sorted := make([]RunEvent, len(events))
	copy(sorted, events)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].EventTime.Before(sorted[j].EventTime)
	})

	return sorted
}

// ApplyEventTransitions applies a sequence of events and validates state transitions.
// Events are sorted by eventTime before applying transitions to handle out-of-order arrival.
//
// Returns the final state after applying all events, or an error if any transition is invalid.
//
// Example usage:
//
//	events := []RunEvent{
//	    {EventTime: t1, EventType: EventTypeStart, ...},
//	    {EventTime: t2, EventType: EventTypeComplete, ...},
//	}
//	finalState, err := ApplyEventTransitions(events)
//	if err != nil {
//	    // Handle invalid transition
//	}
//
// This function is used by the storage layer to validate event sequences before
// persisting to the database.
func ApplyEventTransitions(events []RunEvent) (EventType, error) {
	if len(events) == 0 {
		return "", ErrEmptyEventList
	}

	// Sort events by eventTime to handle out-of-order arrival
	sorted := SortEventsByTime(events)

	// Start with the first event type as initial state
	currentState := sorted[0].EventType

	// Apply transitions sequentially
	for i := 1; i < len(sorted); i++ {
		nextState := sorted[i].EventType

		// Validate transition
		err := ValidateStateTransition(currentState, nextState)
		if err != nil {
			return "", fmt.Errorf("transition %d failed (%s → %s at %s): %w",
				i, currentState, nextState, sorted[i].EventTime.Format("15:04:05"), err)
		}

		// Update current state (skip OTHER events as they don't change state)
		if nextState != EventTypeOther {
			currentState = nextState
		}
	}

	return currentState, nil
}
