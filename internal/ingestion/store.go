// Package ingestion provides OpenLineage domain models and event persistence interfaces.
//
// This package defines the Store interface which represents what the domain needs
// for event persistence, following the Dependency Inversion Principle. Concrete
// implementations (PostgreSQL, in-memory, etc.) live in the internal/storage package.
package ingestion

import "context"

// Store defines the interface for OpenLineage event persistence.
//
// The domain package defines this interface to specify what it needs for event
// storage, without depending on concrete implementations. This follows the
// Dependency Inversion Principle: high-level domain logic should not depend on
// low-level infrastructure details.
//
// Implementations must support:
//   - Idempotency: Duplicate events return success (200 OK), not error (409 Conflict)
//   - Out-of-order events: Events sorted by eventTime before applying state transitions
//   - Partial success: Per-event transactions for batch operations (207 Multi-Status)
//   - Deferred FK constraints: Handles concurrent event races (Event B before Event A)
//
// Pattern: This follows the same architectural pattern as storage.APIKeyStore
// where the domain defines the interface and storage provides implementations.
type Store interface {
	// StoreEvent stores a single OpenLineage event with idempotency checking.
	//
	// Returns (stored, duplicate, error) where:
	//   - stored=true: Event was successfully stored in the database
	//   - duplicate=true: Event was already processed (idempotency hit, returns 200 OK not 409)
	//   - error: Storage operation failed (database error, validation error, etc.)
	//
	// Idempotency behavior:
	//   - Duplicate events return (false, true, nil) → HTTP 200 OK
	//   - This matches industry standards (Stripe, AWS, Google)
	//
	// Out-of-order handling:
	//   - Events are sorted by eventTime before applying state transitions
	//   - Later events with older timestamps may be rejected by database triggers
	//
	// Example:
	//   stored, duplicate, err := store.StoreEvent(ctx, event)
	//   if err != nil {
	//       return fmt.Errorf("storage failed: %w", err)
	//   }
	//   if duplicate {
	//       return http.StatusOK, "event already processed"
	//   }
	//   return http.StatusOK, "event stored successfully"
	StoreEvent(ctx context.Context, event *RunEvent) (stored bool, duplicate bool, err error)

	// StoreEvents stores multiple events with per-event transaction pattern.
	//
	// Returns results for each event to support 207 Multi-Status responses.
	// Uses per-event transactions (NOT a single batch transaction) to enable
	// partial success: one bad event doesn't prevent other events from being stored.
	//
	// Why per-event transactions?
	//   - Partial success is critical for production reliability (99 good events shouldn't fail because of 1 bad event)
	//   - Enables 207 Multi-Status HTTP responses
	//   - Industry standard pattern (Stripe batches, AWS batch APIs)
	//
	// Example:
	//   results, err := store.StoreEvents(ctx, events)
	//   successCount := 0
	//   for _, result := range results {
	//       if result.Stored || result.Duplicate {
	//           successCount++
	//       }
	//   }
	//   // Return 207 if partial success, 200 if all success, 422 if all failed
	StoreEvents(ctx context.Context, events []*RunEvent) ([]*EventStoreResult, error)

	// HealthCheck verifies the storage backend is healthy and ready to serve requests.
	//
	// This is used by:
	//   - Kubernetes readiness probes
	//   - Health check endpoints (/ready, /health)
	//   - Monitoring systems
	//
	// Returns nil if healthy, error with details if unhealthy.
	HealthCheck(ctx context.Context) error

	// StoreTestResult stores a single test result with UPSERT behavior.
	//
	// Returns (stored, duplicate, error) where:
	//   - stored=true: Test result was successfully stored/updated in the database
	//   - duplicate=true: Test result already exists (UPSERT updated existing row)
	//   - error: Storage operation failed (FK violation, validation error, etc.)
	//
	// UPSERT behavior:
	//   - Unique key: (test_name, dataset_urn, executed_at)
	//   - Conflict: Updates existing row with new values
	//   - Returns (true, true, nil) for updates → HTTP 200 OK
	//
	// FK constraints:
	//   - dataset_urn must exist in datasets table
	//   - job_run_id must exist in job_runs table
	//   - Violations return error with context
	//
	// Example:
	//   stored, duplicate, err := store.StoreTestResult(ctx, testResult)
	//   if err != nil {
	//       return fmt.Errorf("storage failed: %w", err)
	//   }
	StoreTestResult(ctx context.Context, testResult *TestResult) (stored bool, duplicate bool, err error)

	// StoreTestResults stores multiple test results with per-result transaction pattern.
	//
	// Returns results for each test result to support 207 Multi-Status responses.
	// Uses per-result transactions (NOT a single batch transaction) to enable
	// partial success: one bad test result doesn't prevent others from being stored.
	//
	// Why per-result transactions?
	//   - Partial success is critical (99 good results shouldn't fail because of 1 bad FK)
	//   - Enables 207 Multi-Status HTTP responses
	//   - Consistent with StoreEvents() pattern
	//
	// Example:
	//   results, err := store.StoreTestResults(ctx, testResults)
	//   successCount := 0
	//   for _, result := range results {
	//       if result.Stored || result.Duplicate {
	//           successCount++
	//       }
	//   }
	StoreTestResults(ctx context.Context, testResults []*TestResult) ([]*TestResultStoreResult, error)
}

// EventStoreResult represents the storage result for a single event.
//
// This type is used for batch operations to enable partial success handling
// and 207 Multi-Status HTTP responses. Each event in a batch gets its own
// result, allowing the handler to report which events succeeded and which failed.
//
// Example usage in HTTP handler:
//
//	results, _ := store.StoreEvents(ctx, events)
//	response := make([]EventResponse, len(results))
//	for i, result := range results {
//	    if result.Error != nil {
//	        response[i] = EventResponse{Status: 422, Message: result.Error.Error()}
//	    } else if result.Duplicate {
//	        response[i] = EventResponse{Status: 200, Message: "duplicate"}
//	    } else {
//	        response[i] = EventResponse{Status: 200, Message: "stored"}
//	    }
//	}
//	// Return 207 if any failures, 200 if all success
type EventStoreResult struct {
	// Event is the OpenLineage event that was processed.
	Event *RunEvent

	// Stored indicates whether the event was successfully stored in the database.
	// True means the event was written (new data), false means it was not written
	// (either duplicate or error).
	Stored bool

	// Duplicate indicates whether the event was already processed (idempotency hit).
	// When true, this is NOT an error condition - it returns HTTP 200 OK.
	// This follows industry standard behavior for idempotent APIs.
	Duplicate bool

	// Error contains the storage error if the operation failed.
	// Nil if the event was stored successfully or was a duplicate.
	// Non-nil indicates a genuine failure (database error, validation error, etc.)
	Error error
}

// TestResultStoreResult represents the storage result for a single test result.
//
// This type follows the same pattern as EventStoreResult to enable partial success
// handling and 207 Multi-Status HTTP responses for batch test result ingestion.
//
// Example usage in HTTP handler:
//
//	results, _ := store.StoreTestResults(ctx, testResults)
//	response := make([]TestResultResponse, len(results))
//	for i, result := range results {
//	    if result.Error != nil {
//	        response[i] = TestResultResponse{Status: 422, Message: result.Error.Error()}
//	    } else if result.Duplicate {
//	        response[i] = TestResultResponse{Status: 200, Message: "updated"}
//	    } else {
//	        response[i] = TestResultResponse{Status: 200, Message: "stored"}
//	    }
//	}
type TestResultStoreResult struct {
	// TestResult is the test result that was processed.
	TestResult *TestResult

	// Stored indicates whether the test result was successfully stored/updated.
	// True means data was written (new or updated), false means error occurred.
	Stored bool

	// Duplicate indicates whether this was an UPSERT update (not a new insert).
	// When true, existing test result was updated with new values.
	Duplicate bool

	// Error contains the storage error if the operation failed.
	// Common errors: FK violations (missing dataset_urn or job_run_id), validation errors.
	Error error
}
