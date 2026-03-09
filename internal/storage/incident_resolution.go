package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/correlator-io/correlator/internal/correlation"
)

// Sentinel errors for resolution store operations.
var (
	// ErrResolutionNotFound is returned when no resolution record exists for the given test result.
	ErrResolutionNotFound = errors.New("incident resolution not found")

	// ErrInvalidResolutionTransition is returned when a status transition violates the state machine.
	ErrInvalidResolutionTransition = errors.New("invalid resolution status transition")

	// ErrIncidentNotFound is returned when the test result ID does not exist in the incident view.
	ErrIncidentNotFound = errors.New("incident not found")
)

// GetResolution returns the current resolution state for an incident.
// Returns nil (no error) if no resolution row exists (incident is implicitly open).
func (s *LineageStore) GetResolution(ctx context.Context, testResultID int64) (*correlation.IncidentResolution, error) {
	const query = `
		SELECT id, test_result_id, status,
		       COALESCE(resolved_by, ''), COALESCE(resolution_reason, ''), COALESCE(resolution_note, ''),
		       resolved_by_test_result_id, mute_expires_at,
		       created_at, updated_at
		FROM incident_resolutions
		WHERE test_result_id = $1`

	var r correlation.IncidentResolution

	var resolvedByTRID sql.NullInt64

	var muteExpiresAt sql.NullTime

	err := s.conn.QueryRowContext(ctx, query, testResultID).Scan(
		&r.ID, &r.TestResultID, &r.Status,
		&r.ResolvedBy, &r.ResolutionReason, &r.ResolutionNote,
		&resolvedByTRID, &muteExpiresAt,
		&r.CreatedAt, &r.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil //nolint:nilnil // No resolution row = implicitly open, not an error
	}

	if err != nil {
		return nil, fmt.Errorf("get resolution: %w", err)
	}

	if resolvedByTRID.Valid {
		r.ResolvedByTestResultID = &resolvedByTRID.Int64
	}

	if muteExpiresAt.Valid {
		r.MuteExpiresAt = &muteExpiresAt.Time
	}

	return &r, nil
}

// SetResolution creates or updates the resolution state for an incident.
// Validates status transitions before applying the change.
func (s *LineageStore) SetResolution(
	ctx context.Context,
	testResultID int64,
	req correlation.ResolutionRequest,
	resolvedBy string,
) (*correlation.IncidentResolution, error) {
	existing, err := s.GetResolution(ctx, testResultID)
	if err != nil {
		return nil, err
	}

	currentStatus := correlation.ResolutionOpen
	if existing != nil {
		currentStatus = existing.Status
	}

	if !currentStatus.CanTransitionTo(req.Status) {
		return nil, fmt.Errorf("%w: %s → %s", ErrInvalidResolutionTransition, currentStatus, req.Status)
	}

	var muteExpiresAt *time.Time
	if req.Status == correlation.ResolutionMuted && req.MuteDays > 0 {
		t := time.Now().Add(time.Duration(req.MuteDays) * 24 * time.Hour)
		muteExpiresAt = &t
	}

	upsert := `
		INSERT INTO incident_resolutions (
		    test_result_id, status, resolved_by, resolution_reason, resolution_note, mute_expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (test_result_id) DO UPDATE SET
			status = EXCLUDED.status,
			resolved_by = EXCLUDED.resolved_by,
			resolution_reason = EXCLUDED.resolution_reason,
			resolution_note = EXCLUDED.resolution_note,
			mute_expires_at = EXCLUDED.mute_expires_at
		RETURNING id, test_result_id, status,
		          COALESCE(resolved_by, ''), COALESCE(resolution_reason, ''), COALESCE(resolution_note, ''),
		          resolved_by_test_result_id, mute_expires_at,
		          created_at, updated_at`

	var r correlation.IncidentResolution

	var resolvedByTRID sql.NullInt64

	var muteExp sql.NullTime

	err = s.conn.QueryRowContext(ctx, upsert,
		testResultID, req.Status, resolvedBy, req.Reason, req.Note, muteExpiresAt,
	).Scan(
		&r.ID, &r.TestResultID, &r.Status,
		&r.ResolvedBy, &r.ResolutionReason, &r.ResolutionNote,
		&resolvedByTRID, &muteExp,
		&r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("set resolution: %w", err)
	}

	if resolvedByTRID.Valid {
		r.ResolvedByTestResultID = &resolvedByTRID.Int64
	}

	if muteExp.Valid {
		r.MuteExpiresAt = &muteExp.Time
	}

	return &r, nil
}

// AutoResolveIncidents finds open/acknowledged incidents matching the given (testName, datasetURN)
// where the failure is older than gracePeriod, and auto-resolves them.
func (s *LineageStore) AutoResolveIncidents(
	ctx context.Context,
	testName string,
	datasetURN string,
	passingTestResultID int64,
	gracePeriod time.Duration,
) (int, error) {
	// Find open/acknowledged incidents for this (test_name, dataset_urn) whose failure
	// occurred more than gracePeriod ago. An incident with no resolution row is implicitly open.
	//
	// The anti-flapping grace period prevents auto-resolve when a test flaps quickly
	// (fails then passes within the window). Only stable passes trigger resolution.
	const query = `
		WITH eligible_incidents AS (
			SELECT tr.id AS test_result_id
			FROM test_results tr
			LEFT JOIN incident_resolutions ir ON tr.id = ir.test_result_id
			WHERE tr.test_name = $1
			  AND tr.dataset_urn = $2
			  AND tr.status IN ('failed', 'error')
			  AND (ir.status IS NULL OR ir.status IN ('open', 'acknowledged'))
			  AND tr.executed_at < NOW() - $3::interval
		)
		INSERT INTO incident_resolutions (
		    test_result_id, status, resolved_by, resolution_reason, resolved_by_test_result_id)
		SELECT test_result_id, 'resolved', 'auto', 'auto_pass', $4
		FROM eligible_incidents
		ON CONFLICT (test_result_id) DO UPDATE SET
			status = 'resolved',
			resolved_by = 'auto',
			resolution_reason = 'auto_pass',
			resolved_by_test_result_id = EXCLUDED.resolved_by_test_result_id
		WHERE incident_resolutions.status IN ('open', 'acknowledged')`

	gracePeriodStr := fmt.Sprintf("%d seconds", int(gracePeriod.Seconds()))

	result, err := s.conn.ExecContext(ctx, query, testName, datasetURN, gracePeriodStr, passingTestResultID)
	if err != nil {
		return 0, fmt.Errorf("auto-resolve on pass: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("auto-resolve rows affected: %w", err)
	}

	if rowsAffected > 0 {
		slog.Info("auto-resolved incidents on pass",
			slog.String("test_name", testName),
			slog.String("dataset_urn", datasetURN),
			slog.Int64("resolved_count", rowsAffected),
			slog.Int64("passing_test_result_id", passingTestResultID),
		)
	}

	return int(rowsAffected), nil
}

// autoResolvePassingTests triggers auto-resolve for each passing test result
// stored during ingestion. Runs after the transaction commits.
// Errors are logged but do not fail the ingestion — resolution is best-effort.
func (s *LineageStore) autoResolvePassingTests(ctx context.Context, passing []passingTestInfo) {
	if len(passing) == 0 {
		return
	}

	for _, pt := range passing {
		count, err := s.AutoResolveIncidents(ctx, pt.testName, pt.datasetURN, pt.testResultID, autoResolveGracePeriod)
		if err != nil {
			s.logger.Warn("auto-resolve failed for passing test",
				slog.String("test_name", pt.testName),
				slog.String("dataset_urn", pt.datasetURN),
				slog.Int64("test_result_id", pt.testResultID),
				slog.String("error", err.Error()),
			)

			continue
		}

		if count > 0 {
			s.logger.Info("auto-resolved incidents on ingestion",
				slog.String("test_name", pt.testName),
				slog.String("dataset_urn", pt.datasetURN),
				slog.Int("resolved_count", count),
			)
		}
	}
}
