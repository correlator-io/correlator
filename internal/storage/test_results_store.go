package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/correlator-io/correlator/internal/ingestion"
)

// Sentinel errors for test result storage operations.
var (
	// ErrTestResultStoreFailed is returned when test result storage operation fails.
	ErrTestResultStoreFailed = errors.New("test result storage failed")

	// ErrTestResultFKViolation is returned when FK constraint is violated (missing dataset_urn or job_run_id).
	ErrTestResultFKViolation = errors.New("foreign key violation: dataset_urn or job_run_id does not exist")
)

// StoreTestResult stores a single test result with UPSERT behavior.
//
// Returns (stored, duplicate, error) where:
//   - stored=true: Test result was successfully stored or updated in the database
//   - duplicate=true: Test result already existed and was updated (UPSERT behavior)
//   - error: Storage operation failed (FK violation, validation error, etc.)
//
// UPSERT behavior:
//   - Unique key: (test_name, dataset_urn, executed_at)
//   - On conflict: Updates existing row with new values (status, message, metadata, etc.)
//   - Returns (true, true, nil) for updates → HTTP 200 OK
//   - Returns (true, false, nil) for inserts → HTTP 200 OK
//
// FK constraints:
//   - dataset_urn must exist in datasets table (DEFERRABLE INITIALLY DEFERRED)
//   - job_run_id must exist in job_runs table (DEFERRABLE INITIALLY DEFERRED)
//   - Violations return ErrTestResultFKViolation with context
func (s *LineageStore) StoreTestResult(
	ctx context.Context,
	testResult *ingestion.TestResult,
) (bool, bool, error) {
	startTime := time.Now()

	// Validate domain model before storage
	if err := testResult.Validate(); err != nil {
		s.logger.Error("Test result validation failed",
			"error", err,
			"test_name", testResult.TestName,
			"duration_ms", time.Since(startTime).Milliseconds(),
		)

		return false, false, fmt.Errorf("%w: %w", ErrTestResultStoreFailed, err)
	}

	// SQL: INSERT test result with UPSERT on conflict
	// RETURNING (xmax = 0) detects INSERT vs UPDATE:
	//   - xmax = 0: New row inserted (no existing row modified)
	//   - xmax != 0: Existing row updated (UPSERT occurred)
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
		RETURNING (xmax = 0) AS inserted
	`

	// Marshal JSONB field
	metadataJSON, err := marshalJSONB(testResult.Metadata)
	if err != nil {
		return false, false, fmt.Errorf("%w: failed to marshal metadata: %w", ErrTestResultStoreFailed, err)
	}

	// Execute INSERT/UPDATE and capture whether it was an insert or update
	var inserted bool

	err = s.conn.DB.QueryRowContext(ctx, query,
		testResult.TestName,
		testResult.TestType,
		testResult.DatasetURN,
		testResult.JobRunID,
		string(testResult.Status),
		testResult.Message,
		metadataJSON,
		testResult.ExecutedAt,
		nullableInt(testResult.DurationMs),
	).Scan(&inserted)
	if err != nil {
		// Check for FK violation
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			if pqErr.Code == "23503" { // foreign_key_violation
				s.logger.Warn("Test result FK constraint violation",
					"error", pqErr.Message,
					"constraint", pqErr.Constraint,
					"test_name", testResult.TestName,
					"dataset_urn", testResult.DatasetURN,
					"job_run_id", testResult.JobRunID,
					"duration_ms", time.Since(startTime).Milliseconds(),
				)

				return false, false, fmt.Errorf("%w: %s", ErrTestResultFKViolation, pqErr.Message)
			}
		}

		s.logger.Error("Test result storage failed",
			"error", err,
			"test_name", testResult.TestName,
			"duration_ms", time.Since(startTime).Milliseconds(),
		)

		return false, false, fmt.Errorf("%w: %w", ErrTestResultStoreFailed, err)
	}

	// Log with operation type (insert vs update)
	operation := "inserted"
	if !inserted {
		operation = "updated"
	}

	s.logger.Info("Test result stored successfully",
		"test_name", testResult.TestName,
		"dataset_urn", testResult.DatasetURN,
		"job_run_id", testResult.JobRunID,
		"status", testResult.Status,
		"operation", operation,
		"duration_ms", time.Since(startTime).Milliseconds(),
	)

	// Return: stored=true (success), duplicate=!inserted (true if update, false if insert)
	return true, !inserted, nil
}

// StoreTestResults stores multiple test results with per-result transaction pattern.
//
// Returns results for each test result to support 207 Multi-Status responses.
// Uses per-result transactions (NOT a single batch transaction) to enable
// partial success: one bad test result doesn't prevent others from being stored.
//
// Design: Follows same pattern as StoreEvents() for consistency.
func (s *LineageStore) StoreTestResults(
	ctx context.Context,
	testResults []*ingestion.TestResult,
) ([]*ingestion.TestResultStoreResult, error) {
	startTime := time.Now()
	results := make([]*ingestion.TestResultStoreResult, len(testResults))

	for i, testResult := range testResults {
		stored, duplicate, err := s.StoreTestResult(ctx, testResult)

		results[i] = &ingestion.TestResultStoreResult{
			TestResult: testResult,
			Stored:     stored,
			Duplicate:  duplicate,
			Error:      err,
		}
	}

	// Log batch summary
	successCount := 0
	duplicateCount := 0
	errorCount := 0

	for _, result := range results {
		switch {
		case result.Stored:
			successCount++
		case result.Duplicate:
			duplicateCount++
		case result.Error != nil:
			errorCount++
		}
	}

	s.logger.Info("Batch test results storage complete",
		"total", len(testResults),
		"stored", successCount,
		"duplicates", duplicateCount,
		"errors", errorCount,
		"duration_ms", time.Since(startTime).Milliseconds(),
	)

	return results, nil
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

// nullableInt returns sql.NullInt32 for optional integer fields.
// Returns NULL if value is zero (Go zero value = DB NULL).
//
//nolint:gosec
func nullableInt(value int) sql.NullInt32 {
	if value == 0 {
		return sql.NullInt32{Valid: false}
	}

	return sql.NullInt32{Int32: int32(value), Valid: true}
}
