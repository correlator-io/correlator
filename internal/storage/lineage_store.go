package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/correlation"
	"github.com/correlator-io/correlator/internal/ingestion"
)

// Sentinel errors for lineage event storage operations.
var (
	// ErrLineageStoreFailed is returned when event storage operation fails.
	ErrLineageStoreFailed = errors.New("lineage event storage failed")

	// ErrIdempotencyCheckFailed is returned when idempotency verification fails.
	ErrIdempotencyCheckFailed = errors.New("idempotency check failed")

	// ErrInvalidEdgeType is returned when an invalid edge type (not "input" or "output") is provided.
	ErrInvalidEdgeType = errors.New("invalid edge type: must be 'input' or 'output'")

	// ErrInvalidCleanupInterval is returned when an invalid cleanup interval is provided.
	ErrInvalidCleanupInterval = errors.New("cleanup interval must be greater than zero")
)

// Compile-time interface assertions to ensure LineageStore implements both interfaces.
// This provides early compile-time errors if interface contracts change.
var (
	// LineageStore implements ingestion.Store (write interface for lineage events).
	_ ingestion.Store = (*LineageStore)(nil)

	// LineageStore implements correlation.Store (read interface for correlation queries)
	// Methods defined in correlation_views.go file (same package, same type).
	_ correlation.Store = (*LineageStore)(nil)
)

// Cleanup configuration constants.
const (
	// cleanupQueryTimeout is the maximum time allowed for a single cleanup query execution.
	cleanupQueryTimeout = 30 * time.Second
	// shutdownTimeout is the maximum time to wait for cleanup goroutine to stop during Close().
	shutdownTimeout = 5 * time.Second
	// cleanupBatchSize is the maximum number of rows to delete per batch to avoid long-running locks.
	cleanupBatchSize = 10000
	// batchSleepDuration is the sleep time between batches to avoid overwhelming the database.
	batchSleepDuration = 100 * time.Millisecond
)

// LineageStore implements ingestion.Store interface with PostgreSQL backend.
//
// This implementation provides production-ready OpenLineage event storage with:
//   - Idempotency: Prevents duplicate event processing (24-hour TTL)
//   - Out-of-order handling: Events sorted by eventTime before state transitions
//   - Partial success: Per-event transactions for batch operations
//   - Deferred FK constraints: Handles concurrent event races
//   - Background cleanup: Automatic TTL cleanup of expired idempotency keys
type LineageStore struct {
	conn            *Connection
	logger          *slog.Logger
	cleanupInterval time.Duration
	cleanupStop     chan struct{} // Signal to stop cleanup goroutine
	cleanupDone     chan struct{} // Signal cleanup has stopped
	closeOnce       sync.Once
}

// NewLineageStore creates a PostgreSQL-backed OpenLineage event store with background cleanup.
// Returns error if connection is nil (ErrNoDatabaseConnection).
//
// Parameters:
//   - conn: Database connection (required)
//   - cleanupInterval: Interval for TTL cleanup goroutine (e.g., 1 hour)
//
// The cleanup goroutine starts automatically and stops gracefully on Close().
func NewLineageStore(conn *Connection, cleanupInterval time.Duration) (*LineageStore, error) {
	if conn == nil {
		return nil, ErrNoDatabaseConnection
	}

	if cleanupInterval <= 0 {
		return nil, ErrInvalidCleanupInterval
	}

	store := &LineageStore{
		conn: conn,
		logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: config.GetEnvLogLevel("LOG_LEVEL", slog.LevelInfo),
		})),
		cleanupInterval: cleanupInterval,
		cleanupStop:     make(chan struct{}), // Signal to stop cleanup goroutine
		cleanupDone:     make(chan struct{}), // Signal cleanup has stopped
	}

	// Start cleanup goroutine
	go store.runCleanup()

	store.logger.Info("Started idempotency cleanup goroutine",
		slog.Duration("interval", cleanupInterval))

	return store, nil
}

// Close stops the cleanup goroutine gracefully.
// This method is safe to call multiple times.
//
// Note: Does NOT close the database connection, as the connection is managed externally
// via dependency injection. The caller is responsible for closing the connection.
//
// Shutdown sequence:
//  1. Signal cleanup goroutine to stop (close cleanupStop channel)
//  2. Wait for cleanup goroutine to finish (with 5-second timeout)
//
// Background goroutine uses channel-based cancellation via cleanupStop/cleanupDone channels.
func (s *LineageStore) Close() error {
	s.closeOnce.Do(func() {
		// Signal cleanup goroutine to stop
		close(s.cleanupStop)

		// Wait for cleanup to finish (with timeout)
		select {
		case <-s.cleanupDone:
			s.logger.Info("Cleanup goroutine stopped gracefully")
		case <-time.After(shutdownTimeout):
			s.logger.Warn("Cleanup goroutine did not stop within timeout")
		}
	})

	return nil
}

// HealthCheck verifies the database connection is healthy and ready to serve requests.
//
// Delegates to the underlying connection's health check with appropriate timeout.
// This method is used by:
//   - Kubernetes readiness probes
//   - Health check endpoints (/ready, /health)
//   - Monitoring systems
//
// Returns nil if healthy, error with details if connection is unavailable.
func (s *LineageStore) HealthCheck(ctx context.Context) error {
	if s.conn == nil {
		return ErrNoDatabaseConnection
	}

	return s.conn.HealthCheck(ctx)
}

// StoreEvent implements ingestion.Store interface.
// Stores a single OpenLineage event with idempotency, out-of-order handling, and deferred FK constraints.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control. The operation respects context cancellation.
//   - event: The OpenLineage RunEvent to store. Must not be nil and must have valid required fields.
//
// Returns three values: (stored, duplicate, error)
//   - stored (first bool): true if event was newly stored, false if duplicate or error occurred
//   - duplicate (second bool): true if event was a duplicate (idempotent), false otherwise
//   - error: non-nil if storage operation failed
//
// Return value combinations:
//   - (true, false, nil)  → Event stored successfully (new event)
//   - (false, true, nil)  → Duplicate event detected (idempotent, HTTP 200 OK)
//   - (false, false, err) → Storage operation failed (HTTP 500 or 422)
//
// The function performs the following operations in order:
//  1. Validates the event structure (nil checks, required fields)
//  2. Checks idempotency using SHA256-based key (24-hour TTL)
//  3. Begins transaction with deferred FK constraints
//  4. Upserts job_run record (handles out-of-order via eventTime comparison)
//  5. Upserts datasets and creates lineage edges (separate row per input/output)
//  6. Extracts dataQualityAssertions from input facets and stores test results
//  7. Records idempotency key with 24-hour expiration
//  8. Commits transaction
//
// Out-of-order handling: Events are compared by eventTime in SQL using CASE statements.
// Older events cannot overwrite newer state, but are recorded in state_history JSONB.
//
// Idempotency: Duplicate events (within 24 hours) return (false, true, nil) instead of
// storing again. This follows industry standard where duplicates return
// 200 OK (success) not 409 Conflict (error).
func (s *LineageStore) StoreEvent(ctx context.Context, event *ingestion.RunEvent) (bool, bool, error) {
	if err := s.validateRunEvent(event); err != nil {
		return false, false, err
	}

	// 1. Check idempotency (duplicate detection)
	idempotencyKey := event.IdempotencyKey()

	isDuplicate, err := s.checkIdempotency(ctx, idempotencyKey)
	if err != nil {
		return false, false, fmt.Errorf("%w: idempotency check failed: %w", ErrIdempotencyCheckFailed, err)
	}

	if isDuplicate {
		// Duplicate event - return success (200 OK, not 409 Conflict)
		s.logger.Debug("duplicate event detected",
			slog.String("idempotency_key", idempotencyKey),
			slog.String("job_run_id", event.JobRunID()),
		)

		return false, true, nil
	}

	// 2. Begin transaction with deferred FK constraints
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, false, fmt.Errorf("%w: failed to begin transaction: %w", ErrLineageStoreFailed, err)
	}

	defer func() {
		_ = tx.Rollback() // Safe to call even after commit
	}()

	// 3. Upsert job_run (handles out-of-order events via eventTime comparison)
	if err := s.upsertJobRun(ctx, tx, event); err != nil {
		return false, false, fmt.Errorf("%w: %w", ErrLineageStoreFailed, err)
	}

	// 4. Upsert datasets and create lineage edges
	if err := s.upsertDatasetsAndEdges(ctx, tx, event); err != nil {
		return false, false, fmt.Errorf("%w: %w", ErrLineageStoreFailed, err)
	}

	// 5. Extract test results from dataQualityAssertions facets (non-blocking)
	// This extracts test assertions from input datasets and stores them in test_results table
	// for correlation. Errors are logged but don't fail the event storage.
	s.extractDataQualityAssertions(ctx, tx, event)

	// 6. Record idempotency key (24-hour TTL)
	if err := s.recordIdempotency(ctx, tx, idempotencyKey, event); err != nil {
		return false, false, fmt.Errorf("%w: %w", ErrIdempotencyCheckFailed, err)
	}

	// 7. Commit transaction
	if err := tx.Commit(); err != nil {
		return false, false, fmt.Errorf("%w: %w", ErrLineageStoreFailed, err)
	}

	s.logger.Info("event stored successfully",
		slog.String("job_run_id", event.JobRunID()),
		slog.String("event_type", string(event.EventType)),
		slog.Time("event_time", event.EventTime),
	)

	return true, false, nil
}

// StoreEvents implements ingestion.Store interface.
// Stores multiple OpenLineage events with per-event transaction pattern.
//
// Uses per-event transactions (NOT a single batch transaction) to enable partial success:
// one bad event doesn't prevent other events from being stored. This is critical for
// production reliability where 99 good events shouldn't fail because of 1 bad event.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control.
//   - events: Slice of pointers to RunEvent structs to store. Pointers avoid copying large structs.
//
// Returns two values: (results, error)
//   - results: Slice of pointers to EventStoreResult, one per input event, for 207 Multi-Status responses.
//   - error: Non-nil only for catastrophic failures (context cancelled, database connection lost).
//
// Returns operation-level error only for catastrophic failures (context cancelled, database connection lost).
func (s *LineageStore) StoreEvents(
	ctx context.Context,
	events []*ingestion.RunEvent,
) ([]*ingestion.EventStoreResult, error) {
	results := make([]*ingestion.EventStoreResult, len(events))

	// Process each event independently (per-event transactions)
	for i := range events {
		// Check for operation-level failures (context cancellation)
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return results, fmt.Errorf("%w: request cancelled", ErrLineageStoreFailed)
			}

			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return results, fmt.Errorf("%w: operation timeout", ErrLineageStoreFailed)
			}
		}

		stored, duplicate, err := s.StoreEvent(ctx, events[i])
		results[i] = &ingestion.EventStoreResult{
			Event:     events[i],
			Stored:    stored,
			Duplicate: duplicate,
			Error:     err,
		}

		// Check if database connection was lost (catastrophic failure)
		if err != nil && isDatabaseConnectionError(err) {
			return results, fmt.Errorf("%w: database connection lost", ErrLineageStoreFailed)
		}
	}

	return results, nil
}

// validateRunEvent performs defensive validation of a RunEvent before storage.
// This prevents panics from malformed events at the storage layer boundary.
// It checks for nil pointers and empty required fields that would cause runtime errors
// during iteration or database operations.
//
// Returns ErrLineageStoreFailed if validation fails.
func (s *LineageStore) validateRunEvent(event *ingestion.RunEvent) error {
	// Defensive checks to prevent panics from malformed events
	if event == nil {
		return fmt.Errorf("%w: event is nil", ErrLineageStoreFailed)
	}

	if event.Inputs == nil {
		return fmt.Errorf("%w: event.Inputs is nil", ErrLineageStoreFailed)
	}

	if event.Outputs == nil {
		return fmt.Errorf("%w: event.Outputs is nil", ErrLineageStoreFailed)
	}

	if event.Run.ID == "" {
		return fmt.Errorf("%w: event.Run.ID is empty", ErrLineageStoreFailed)
	}

	if event.Job.Name == "" {
		return fmt.Errorf("%w: event.Job.Name is empty", ErrLineageStoreFailed)
	}

	if event.EventTime.IsZero() {
		return fmt.Errorf("%w: event.EventTime is zero", ErrLineageStoreFailed)
	}

	return nil
}

// isDatabaseConnectionError checks if an error indicates database connection failure.
// Uses PostgreSQL error codes (Class 08) and standard database/sql errors for robust detection.
func isDatabaseConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check PostgreSQL error codes (Class 08 = Connection Exception)
	// Per PostgreSQL documentation, all 08xxx errors are connection-related:
	//   08000 - connection_exception
	//   08003 - connection_does_not_exist
	//   08006 - connection_failure
	//   08001 - sqlclient_unable_to_establish_sqlconnection
	//   08004 - sqlserver_rejected_establishment_of_sqlconnection
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return strings.HasPrefix(string(pqErr.Code), "08")
	}

	// Check standard database/sql connection errors
	return errors.Is(err, sql.ErrConnDone) || errors.Is(err, driver.ErrBadConn)
}

// extractProducerName extracts the producer name from an OpenLineage producer URL.
//
// OpenLineage producers are typically URLs with version information:
//   - "https://github.com/dbt-labs/dbt-core/tree/1.5.0" → "dbt-core"
//   - "https://github.com/apache/airflow/tree/2.7.0" → "airflow"
//   - "https://github.com/great-expectations/great_expectations/tree/0.17.0" → "great_expectations"
//   - "https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/spark" → "spark"
//
// Falls back to the full URL if extraction fails (defensive programming).
//
// This is used to populate the producer_name column in the job_runs table for
// easier querying and filtering by data tool.
func extractProducerName(producerURL string) string {
	if producerURL == "" {
		return "unknown"
	}

	// Remove protocol
	producerURL = strings.TrimPrefix(producerURL, "https://")
	producerURL = strings.TrimPrefix(producerURL, "http://")

	// Split by slashes
	parts := strings.Split(producerURL, "/")

	// Handle common GitHub URL patterns:
	// github.com/org/repo/tree/version → "repo"
	// github.com/org/repo/tree/version/integration/tool → "tool"
	if len(parts) >= 3 && parts[0] == "github.com" {
		// Look for "integration" directory (Spark, Flink, etc.)
		for i, part := range parts {
			if part == "integration" && i+1 < len(parts) {
				return parts[i+1]
			}
		}

		// Default to repo name (index 2)
		return parts[2]
	}

	// For non-GitHub URLs or unexpected formats, return the first meaningful part
	if len(parts) > 0 {
		return parts[0]
	}

	// Fallback to full URL (defensive)
	return producerURL
}

// checkIdempotency checks if an event with the given idempotency key already exists.
// Returns (true, nil) if duplicate found, (false, nil) if not duplicate, (false, error) on query error.
func (s *LineageStore) checkIdempotency(ctx context.Context, idempotencyKey string) (bool, error) {
	query := `
		SELECT 1 FROM lineage_event_idempotency
		WHERE idempotency_key = $1 AND expires_at > NOW()
		LIMIT 1
	`

	var exists int

	err := s.conn.QueryRowContext(ctx, query, idempotencyKey).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		// Not a duplicate
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to query idempotency: %w", err)
	}

	// Duplicate found
	return true, nil
}

// upsertJobRun inserts or updates a job_run record.
// Handles out-of-order events by only updating if new event has later eventTime.
//
// Database Trigger Integration:
// This method triggers the job_run_state_validation database trigger (migration 005)
// which runs BEFORE UPDATE and provides two critical functions:
//
//  1. Terminal State Protection: Prevents invalid transitions from terminal states
//     (COMPLETE, FAIL, ABORT) to non-terminal states. Returns error if violated.
//
//  2. State History Tracking: Automatically records state transitions in the
//     state_history JSONB column with schema: {from, to, event_time, updated_at}.
//     This happens automatically on every UPDATE that changes current_state.
//
// Example state_history after START → RUNNING → COMPLETE:
//
//	{
//	  "transitions": [
//	    {"from": "START", "to": "RUNNING", "event_time": "...", "updated_at": "..."},
//	    {"from": "RUNNING", "to": "COMPLETE", "event_time": "...", "updated_at": "..."}
//	  ]
//	}
//
// Out-of-order Handling:
// Uses CASE statements with eventTime comparison to prevent older events from
// overwriting newer state. Only events with eventTime > existing eventTime update
// current_state and event_type fields.
func (s *LineageStore) upsertJobRun(ctx context.Context, tx *sql.Tx, event *ingestion.RunEvent) error {
	jobRunID := event.JobRunID()
	producerName := extractProducerName(event.Producer)

	// Combine job and run facets into metadata JSONB
	metadata := make(map[string]interface{})
	metadata["job_facets"] = event.Job.Facets
	metadata["run_facets"] = event.Run.Facets
	metadata["producer"] = event.Producer
	metadata["schema_url"] = event.SchemaURL

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Initialize state_history for new job runs
	stateHistory := map[string]interface{}{
		"transitions": []interface{}{},
	}

	stateHistoryJSON, err := json.Marshal(stateHistory)
	if err != nil {
		return fmt.Errorf("failed to marshal state history: %w", err)
	}

	// Upsert job_run with eventTime comparison to handle out-of-order events
	query := `
		INSERT INTO job_runs (
			job_run_id,
			run_id,
			job_name,
			job_namespace,
			current_state,
			event_type,
			event_time,
			state_history,
			metadata,
			producer_name,
			started_at,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW())
		ON CONFLICT (job_run_id) DO UPDATE
		SET
			current_state = CASE
				WHEN EXCLUDED.event_time > job_runs.event_time THEN EXCLUDED.current_state
				ELSE job_runs.current_state
			END,
			event_type = CASE
				WHEN EXCLUDED.event_time > job_runs.event_time THEN EXCLUDED.event_type
				ELSE job_runs.event_type
			END,
			event_time = GREATEST(job_runs.event_time, EXCLUDED.event_time),
			-- Note: state_history is updated by the job_run_state_validation trigger (migration 005)
			-- The trigger runs BEFORE UPDATE and records: {from, to, event_time, updated_at}
			metadata = CASE
				WHEN EXCLUDED.event_time > job_runs.event_time THEN EXCLUDED.metadata
				ELSE job_runs.metadata
			END,
			updated_at = NOW()
	`

	_, err = tx.ExecContext(
		ctx,
		query,
		jobRunID,
		event.Run.ID,
		event.Job.Name,
		event.Job.Namespace,
		string(event.EventType), // current_state
		string(event.EventType), // event_type
		event.EventTime,
		stateHistoryJSON,
		metadataJSON,
		producerName,
		event.EventTime, // Use eventTime as started_at for first event
	)
	if err != nil {
		return fmt.Errorf("failed to upsert job_run: %w", err)
	}

	return nil
}

// upsertDatasetsAndEdges upserts datasets and creates lineage edges.
// Creates separate lineage edge rows for each input and output dataset.
func (s *LineageStore) upsertDatasetsAndEdges(ctx context.Context, tx *sql.Tx, event *ingestion.RunEvent) error {
	jobRunID := event.JobRunID()

	// Upsert input datasets and create input edges
	for _, dataset := range event.Inputs {
		// Upsert dataset
		if err := s.upsertDataset(ctx, tx, &dataset); err != nil {
			return fmt.Errorf("failed to upsert input dataset: %w", err)
		}

		// Create input edge
		if err := s.createLineageEdge(ctx, tx, jobRunID, "input", dataset); err != nil {
			return fmt.Errorf("failed to create input edge: %w", err)
		}
	}

	// Upsert output datasets and create output edges
	for _, dataset := range event.Outputs {
		// Upsert dataset
		if err := s.upsertDataset(ctx, tx, &dataset); err != nil {
			return fmt.Errorf("failed to upsert output dataset: %w", err)
		}

		// Create output edge
		if err := s.createLineageEdge(ctx, tx, jobRunID, "output", dataset); err != nil {
			return fmt.Errorf("failed to create output edge: %w", err)
		}
	}

	return nil
}

// upsertDataset inserts or updates a dataset record.
func (s *LineageStore) upsertDataset(ctx context.Context, tx *sql.Tx, dataset *ingestion.Dataset) error {
	datasetURN := dataset.URN()

	// Combine all facets into single JSONB
	allFacets := make(map[string]interface{})

	for k, v := range dataset.Facets {
		allFacets[k] = v
	}

	for k, v := range dataset.InputFacets {
		allFacets["input_"+k] = v
	}

	for k, v := range dataset.OutputFacets {
		allFacets["output_"+k] = v
	}

	facetsJSON, err := json.Marshal(allFacets)
	if err != nil {
		return fmt.Errorf("failed to marshal facets: %w", err)
	}

	query := `
		INSERT INTO datasets (
			dataset_urn,
			name,
			namespace,
			facets,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, NOW(), NOW())
		ON CONFLICT (dataset_urn) DO UPDATE
		SET
			facets = datasets.facets || EXCLUDED.facets,
			updated_at = NOW()
	`

	_, err = tx.ExecContext(
		ctx,
		query,
		datasetURN,
		dataset.Name,
		dataset.Namespace,
		facetsJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert dataset: %w", err)
	}

	return nil
}

// createLineageEdge creates a lineage edge (input or output) for a job run.
func (s *LineageStore) createLineageEdge(
	ctx context.Context,
	tx *sql.Tx,
	jobRunID string,
	edgeType string,
	dataset ingestion.Dataset,
) error {
	// Select appropriate facets based on edge type with validation
	var facets ingestion.Facets

	switch edgeType {
	case "input":
		facets = dataset.InputFacets
	case "output":
		facets = dataset.OutputFacets
	default:
		return fmt.Errorf("%w: got %q", ErrInvalidEdgeType, edgeType)
	}

	facetsJSON, err := json.Marshal(facets)
	if err != nil {
		return fmt.Errorf("failed to marshal facets: %w", err)
	}

	// Use CASE statement to set appropriate facet column based on edge_type
	// This eliminates SQL string interpolation and enables query plan caching
	// We use ::text cast to help PostgreSQL deduce parameter types correctly
	query := `
		INSERT INTO lineage_edges (
			job_run_id,
			dataset_urn,
			edge_type,
			input_facets,
			output_facets,
			created_at
		) VALUES (
			$1, $2, $3::text,
			CASE WHEN $3::text = 'input' THEN $4::jsonb ELSE NULL END,
			CASE WHEN $3::text = 'output' THEN $4::jsonb ELSE NULL END,
			NOW()
		)
	`

	_, err = tx.ExecContext(
		ctx,
		query,
		jobRunID,
		dataset.URN(),
		edgeType,
		facetsJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to create lineage edge: %w", err)
	}

	return nil
}

// recordIdempotency records an idempotency key with 24-hour TTL and event metadata.
// The metadata enables querying which events were deduplicated and debugging duplicate detection.
func (s *LineageStore) recordIdempotency(
	ctx context.Context,
	tx *sql.Tx, idempotencyKey string,
	event *ingestion.RunEvent,
) error {
	// Store useful metadata for duplicate event tracking and debugging
	metadata := map[string]interface{}{
		"event_type":    string(event.EventType),
		"job_name":      event.Job.Name,
		"job_namespace": event.Job.Namespace,
		"event_time":    event.EventTime.Format("2006-01-02T15:04:05.000Z"),
		"job_run_id":    event.JobRunID(),
		"producer":      event.Producer,
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal event metadata: %w", err)
	}

	query := `
		INSERT INTO lineage_event_idempotency (
			idempotency_key,
			created_at,
			expires_at,
			event_metadata
		) VALUES ($1, NOW(), NOW() + INTERVAL '24 hours', $2)
	`

	_, err = tx.ExecContext(ctx, query, idempotencyKey, metadataJSON)
	if err != nil {
		return fmt.Errorf("failed to insert idempotency key: %w", err)
	}

	return nil
}

// runCleanup is the background goroutine that periodically cleans expired idempotency keys.
// Runs on ticker until cleanupStop channel is closed via Close().
//
// Design:
//   - Uses time.Ticker for periodic cleanup (default: 1 hour)
//   - Respects channel close signal for graceful shutdown
//   - Calls cleanupExpiredIdempotencyKeys() to perform actual cleanup
//   - Logs errors but doesn't crash on cleanup failures
func (s *LineageStore) runCleanup() {
	defer close(s.cleanupDone)

	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	// Create a cancellable context for cleanup operations
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-s.cleanupStop:
			cancel() // cancel parent context ctx
			s.logger.Info("Stopping idempotency cleanup goroutine")

			return
		case <-ticker.C:
			// Create context with timeout for cleanup query
			cleanupCtx, cleanupCancel := context.WithTimeout(ctx, cleanupQueryTimeout)
			s.cleanupExpiredIdempotencyKeys(cleanupCtx)
			cleanupCancel()
		}
	}
}

// cleanupExpiredIdempotencyKeys deletes expired idempotency keys from the database in batches.
// Called periodically by runCleanup() goroutine with a context that has a 30-second timeout.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control (typically with 30s timeout from runCleanup)
//
// Batching Strategy:
//   - Deletes up to cleanupBatchSize (10,000) rows per batch to avoid long-running table locks
//   - Loops until no more expired rows exist (handles large backlogs)
//   - Sleeps batchSleepDuration (100ms) between batches to avoid overwhelming database
//   - Respects context cancellation for graceful shutdown mid-cleanup
//
// Query uses idx_idempotency_expires index (migration 006) for efficient expired row lookup.
// ORDER BY ensures oldest expired rows are deleted first (FIFO cleanup).
//
// Logs metrics on success (rows deleted, batches, duration, status=success) and errors on failure.
// If cleanup succeeds but row count is unavailable, logs a warning with status=success.
// Failures are logged but don't crash the cleanup goroutine.
func (s *LineageStore) cleanupExpiredIdempotencyKeys(ctx context.Context) {
	if s.conn == nil {
		s.logger.Error("Cleanup skipped: database connection is nil")

		return
	}

	startTime := time.Now()
	totalDeleted := int64(0)
	batchCount := 0

	// Batch delete loop - continues until no more expired rows exist
	for {
		// Check if context is cancelled (shutdown requested or timeout exceeded)
		if ctx.Err() != nil {
			s.logger.Info("Cleanup cancelled",
				slog.Int64("rows_deleted", totalDeleted),
				slog.Int("batches_completed", batchCount),
				slog.Duration("duration", time.Since(startTime)))

			return
		}

		// Delete one batch using idx_idempotency_expires index for efficient lookup
		// ORDER BY ensures oldest expired rows are deleted first (FIFO)
		query := `
			DELETE FROM lineage_event_idempotency
			WHERE idempotency_key IN (
				SELECT idempotency_key
				FROM lineage_event_idempotency
				WHERE expires_at < NOW()
				ORDER BY expires_at ASC
				LIMIT $1
			)
		`

		result, err := s.conn.ExecContext(ctx, query, cleanupBatchSize)
		if err != nil {
			s.logger.Error("Failed to cleanup expired idempotency keys",
				slog.String("error", err.Error()),
				slog.Int64("rows_deleted_before_error", totalDeleted),
				slog.Int("batches_completed", batchCount),
				slog.String("status", "failed"))

			return
		}

		rowsDeleted, err := result.RowsAffected()
		if err != nil {
			// DELETE succeeded but can't get row count - log as warning with success status
			s.logger.Warn("Cleanup batch completed but row count unavailable",
				slog.String("error", err.Error()),
				slog.Int64("rows_deleted_before_error", totalDeleted),
				slog.Int("batches_completed", batchCount),
				slog.Duration("duration", time.Since(startTime)),
				slog.String("status", "success"))

			return
		}

		totalDeleted += rowsDeleted
		batchCount++

		// If we deleted fewer rows than batch size, we're done (no more expired rows)
		if rowsDeleted < cleanupBatchSize {
			break
		}

		// Small sleep between batches to avoid overwhelming database
		// Allows other queries to interleave with cleanup operations
		select {
		case <-ctx.Done():
			// Context cancelled during sleep - exit gracefully
			s.logger.Info("Cleanup cancelled between batches",
				slog.Int64("rows_deleted", totalDeleted),
				slog.Int("batches_completed", batchCount),
				slog.Duration("duration", time.Since(startTime)))

			return
		case <-time.After(batchSleepDuration):
			// Continue to next batch
		}
	}

	duration := time.Since(startTime)

	// Always log cleanup execution (Debug level for 0 rows, Info for >0) for debugging and monitoring purposes
	if totalDeleted == 0 {
		s.logger.Debug("Cleanup completed - no expired keys found",
			slog.Int64("rows_deleted", 0),
			slog.Int("batches_completed", batchCount),
			slog.Duration("duration", duration),
			slog.String("status", "success"))
	} else {
		s.logger.Info("Cleaned up expired idempotency keys",
			slog.Int64("rows_deleted", totalDeleted),
			slog.Int("batches_completed", batchCount),
			slog.Duration("duration", duration),
			slog.String("status", "success"))
	}
}

// extractDataQualityAssertions extracts test results from dataQualityAssertions facets
// in input datasets and stores them in the test_results table.
//
// This enables correlation between test failures and job runs via the OpenLineage
// dataQualityAssertions facet (per OpenLineage specification).
//
// Facet location: inputs[].inputFacets.dataQualityAssertions
//
// Facet structure (OpenLineage spec):
//
//	{
//	  "_producer": "https://github.com/...",
//	  "_schemaURL": "https://openlineage.io/spec/facets/...",
//	  "assertions": [
//	    {"assertion": "test_name", "success": true/false, "column": "optional"}
//	  ]
//	}
//
// Behavior:
//   - Non-blocking: Errors are logged but don't fail the event storage
//   - Same transaction: Test results are stored atomically with the event
//   - Maps success=true to "passed", success=false to "failed"
//   - Stores optional column in metadata.column
//
// Parameters:
//   - ctx: Context for cancellation
//   - tx: Transaction to use (same as event storage for atomicity)
//   - event: OpenLineage event containing input datasets with facets
//
//nolint:gocognit,funlen,cyclop // Parsing untyped OpenLineage facets requires sequential type assertions
func (s *LineageStore) extractDataQualityAssertions(
	ctx context.Context,
	tx *sql.Tx,
	event *ingestion.RunEvent,
) {
	jobRunID := event.JobRunID()
	eventTime := event.EventTime

	for _, input := range event.Inputs {
		// Check for dataQualityAssertions facet
		facet, ok := input.InputFacets["dataQualityAssertions"]
		if !ok {
			continue
		}

		// Type assert facet to map
		facetMap, ok := facet.(map[string]interface{})
		if !ok {
			s.logger.Warn("dataQualityAssertions facet is not a map",
				slog.String("job_run_id", jobRunID),
				slog.String("dataset_urn", input.URN()),
			)

			continue
		}

		// Extract assertions array
		assertionsRaw, ok := facetMap["assertions"]
		if !ok {
			s.logger.Warn("dataQualityAssertions facet missing assertions field",
				slog.String("job_run_id", jobRunID),
				slog.String("dataset_urn", input.URN()),
			)

			continue
		}

		assertions, ok := assertionsRaw.([]interface{})
		if !ok {
			s.logger.Warn("dataQualityAssertions assertions is not an array",
				slog.String("job_run_id", jobRunID),
				slog.String("dataset_urn", input.URN()),
			)

			continue
		}

		// Process each assertion
		for _, assertionRaw := range assertions {
			assertion, ok := assertionRaw.(map[string]interface{})
			if !ok {
				s.logger.Warn("assertion is not a map",
					slog.String("job_run_id", jobRunID),
				)

				continue
			}

			// Extract required fields
			testName, _ := assertion["assertion"].(string)
			if testName == "" {
				s.logger.Warn("assertion missing name",
					slog.String("job_run_id", jobRunID),
				)

				continue
			}

			// Map success boolean to status (default to failed if missing/malformed - safer for correlation)
			status := ingestion.TestStatusFailed

			successVal, hasSuccess := assertion["success"]
			if !hasSuccess {
				s.logger.Warn("assertion missing success field, defaulting to failed",
					slog.String("job_run_id", jobRunID),
					slog.String("test_name", testName),
				)
			} else if success, ok := successVal.(bool); ok && success {
				status = ingestion.TestStatusPassed
			} else if !ok {
				s.logger.Warn("assertion success is not a boolean, defaulting to failed",
					slog.String("job_run_id", jobRunID),
					slog.String("test_name", testName),
				)
			}

			// Extract optional column into metadata
			var metadata map[string]interface{}
			if column, ok := assertion["column"].(string); ok && column != "" {
				metadata = map[string]interface{}{"column": column}
			}

			// Store the test result
			if err := s.storeTestResult(ctx, tx, &ingestion.TestResult{
				TestName:   testName,
				TestType:   "dataQualityAssertion",
				DatasetURN: input.URN(),
				JobRunID:   jobRunID,
				Status:     status,
				Metadata:   metadata,
				ExecutedAt: eventTime,
			}); err != nil {
				s.logger.Warn("failed to store test result from facet",
					slog.String("job_run_id", jobRunID),
					slog.String("test_name", testName),
					slog.String("error", err.Error()),
				)
				// Non-blocking: continue processing other assertions
			}
		}
	}
}

// storeTestResult stores a single test result within an existing transaction.
// Used by extractDataQualityAssertions to store test results atomically with event storage.
//
// Behavior:
//   - Uses existing transaction (same as event storage for atomicity)
//   - Skips validation (facet data is already semi-validated)
//   - UPSERT on (test_name, dataset_urn, executed_at)
func (s *LineageStore) storeTestResult(
	ctx context.Context,
	tx *sql.Tx,
	testResult *ingestion.TestResult,
) error {
	// Marshal metadata to JSONB
	metadataJSON, err := marshalJSONB(testResult.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	query := `
		INSERT INTO test_results (
			test_name,
			test_type,
			dataset_urn,
			job_run_id,
			status,
			message,
			metadata,
			executed_at,
			duration_ms
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (test_name, dataset_urn, executed_at)
		DO UPDATE SET
			test_type = EXCLUDED.test_type,
			job_run_id = EXCLUDED.job_run_id,
			status = EXCLUDED.status,
			message = EXCLUDED.message,
			metadata = EXCLUDED.metadata,
			duration_ms = EXCLUDED.duration_ms,
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = tx.ExecContext(
		ctx,
		query,
		testResult.TestName,
		testResult.TestType,
		testResult.DatasetURN,
		testResult.JobRunID,
		testResult.Status.String(),
		testResult.Message,
		metadataJSON,
		testResult.ExecutedAt,
		testResult.DurationMs,
	)
	if err != nil {
		return fmt.Errorf("failed to insert test result: %w", err)
	}

	return nil
}

// marshalJSONB marshals a map to JSONB, returning NULL-safe value for database.
// Returns nil (SQL NULL) for nil/empty maps to avoid "invalid input syntax for type json" error.
func marshalJSONB(data map[string]interface{}) (sql.NullString, error) {
	if len(data) == 0 {
		return sql.NullString{Valid: false}, nil // SQL NULL
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return sql.NullString{Valid: false}, err
	}

	return sql.NullString{String: string(jsonBytes), Valid: true}, nil
}
