// Package ingestion provides OpenLineage event lifecycle state machine.
// Handles state transitions, out-of-order events, and sequence validation.
//
// Usage:
//
//	This package is used by the HTTP layer to validate batch event sequences
//	BEFORE storage. Single events bypass this validation and rely on database
//	triggers for state transition protection (see migration 005).
//
// Architecture:
//   - Application Layer (lifecycle.go): Validates batch sequences (HTTP 422 on error)
//   - Database Layer (migration 005): Enforces terminal state immutability (raises exception)
//
// Why both layers?
//   - Application: Provides client-friendly error messages for batch validation
//   - Database: Ensures data integrity even if application validation is bypassed
//   - Defense in depth: Application validates sequences, database validates single transitions
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

// ValidateEventSequence validates state transitions and returns events in chronological order.
//
// Events are sorted by eventTime (not arrival time) to handle out-of-order delivery
// in distributed systems. State transitions are then validated according to the
// OpenLineage run cycle specification.
//
// The finalState returned is the last non-OTHER event type, since OTHER events
// provide metadata without affecting run state.
//
// Edge case handling:
//   - OTHER events at start: Validation starts from first non-OTHER event
//   - OTHER events at end: Final state is last non-OTHER event
//   - All OTHER events: Final state is OTHER
//
// Returns:
//   - sortedEvents: Events in chronological order (ready for persistence)
//   - finalState: The final run state (ignores OTHER events)
//   - error: Non-nil if any transition is invalid
//
// Example:
//
//	sorted, finalState, err := ValidateEventSequence(events)
//	if err != nil {
//	    return fmt.Errorf("invalid event sequence: %w", err)
//	}
//	return db.PersistEvents(sorted, finalState)
//
// Spec: https://openlineage.io/docs/spec/run-cycle
func ValidateEventSequence(events []RunEvent) ([]RunEvent, EventType, error) {
	if len(events) == 0 {
		return nil, "", ErrEmptyEventList
	}

	// Sort events by eventTime to handle out-of-order arrival
	sorted := SortEventsByTime(events)

	// Find the first non-OTHER event to establish initial state
	// Edge case: If all events are OTHER, start from first OTHER event
	var currentState EventType

	startIdx := 0

	for i, event := range sorted {
		if event.EventType != EventTypeOther {
			currentState = event.EventType
			startIdx = i + 1

			break
		}
	}

	// If no non-OTHER events found, all events are OTHER
	if currentState == "" {
		return sorted, EventTypeOther, nil
	}

	// Apply transitions sequentially from first non-OTHER event
	for i := startIdx; i < len(sorted); i++ {
		nextState := sorted[i].EventType

		// OTHER events can happen at any time (they provide metadata)
		if nextState == EventTypeOther {
			continue
		}

		// Validate non-OTHER transition
		err := ValidateStateTransition(currentState, nextState)
		if err != nil {
			return nil, "", fmt.Errorf("transition %d failed (%s → %s at %s): %w",
				i, currentState, nextState, sorted[i].EventTime.Format("15:04:05"), err)
		}

		// Update current state
		currentState = nextState
	}

	return sorted, currentState, nil
}
