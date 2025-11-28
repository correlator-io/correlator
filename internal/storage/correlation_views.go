package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/correlator-io/correlator/internal/correlation"
)

// Sentinel errors for correlation view operations.
var (
	// ErrViewRefreshFailed is returned when materialized view refresh fails.
	ErrViewRefreshFailed = errors.New("materialized view refresh failed")

	// ErrCorrelationQueryFailed is returned when correlation query fails.
	ErrCorrelationQueryFailed = errors.New("correlation query failed")
)

const statusFailed = "failed"

// RefreshViews implements correlation.Store.
// Refreshes all correlation materialized views in dependency order.
//
// This method calls the PostgreSQL function refresh_correlation_views() which:
//   - Refreshes incident_correlation_view (failed/error tests correlated to job runs)
//   - Refreshes lineage_impact_analysis (recursive downstream impact analysis)
//   - Refreshes recent_incidents_summary (7-day rolling window aggregation)
//   - Uses CONCURRENTLY for zero-downtime updates (~650ms-2s, no locks)
//
// Performance characteristics:
//   - Typical duration: 650ms-2s (depends on data volume)
//   - No table locks (CONCURRENTLY refresh)
//   - Safe to call frequently (e.g., every 5 minutes)
//
// Returns error if:
//   - Context is cancelled
//   - Database connection fails
//   - Any view refresh fails
func (s *LineageStore) RefreshViews(ctx context.Context) error {
	start := time.Now()

	s.logger.Debug("Starting correlation views refresh")

	_, err := s.conn.ExecContext(ctx, `SELECT refresh_correlation_views()`)
	if err != nil {
		s.logger.Error("Failed to refresh correlation views",
			slog.Any("error", err),
			slog.Duration("duration", time.Since(start)))

		return fmt.Errorf("%w: %w", ErrViewRefreshFailed, err)
	}

	duration := time.Since(start)
	s.logger.Info("Refreshed correlation views",
		slog.Duration("duration", duration))

	// Warn if refresh is slow (>2s indicates performance issue)
	if duration > 2*time.Second {
		s.logger.Warn("Slow correlation view refresh detected",
			slog.Duration("duration", duration),
			slog.String("recommendation", "Consider adding indexes or optimizing view queries"))
	}

	return nil
}

// QueryIncidents implements correlation.Store.
// Queries the incident_correlation_view with optional filters.
//
// This method provides type-safe access to the correlation view, which links
// test failures to the job runs that produced the failing datasets.
//
// Parameters:
//   - filter: Optional filter (nil = no filtering, returns all incidents)
//
// Returns:
//   - Slice of Incident results (empty slice if no matches)
//   - Error if query fails or context is cancelled
//
// Performance:
//   - View is pre-materialized (fast queries, typically 10-50ms)
//   - Uses indexes for filtered queries
//   - Call RefreshViews() to update data
func (s *LineageStore) QueryIncidents(
	ctx context.Context,
	filter *correlation.IncidentFilter,
) ([]correlation.Incident, error) {
	start := time.Now()

	query, args := buildIncidentCorrelationQuery(filter)

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		s.logger.Error("Failed to query incident correlation",
			slog.Any("error", err),
			slog.Duration("duration", time.Since(start)))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []correlation.Incident

	for rows.Next() {
		var r correlation.Incident

		err := rows.Scan(
			&r.TestResultID, &r.TestName, &r.TestType, &r.TestStatus, &r.TestMessage,
			&r.TestExecutedAt, &r.TestDurationMs,
			&r.DatasetURN, &r.DatasetName, &r.DatasetNS,
			&r.JobRunID, &r.OpenLineageRunID, &r.JobName, &r.JobNamespace, &r.JobStatus, &r.JobEventType,
			&r.JobStartedAt, &r.JobCompletedAt,
			&r.ProducerName, &r.ProducerVersion,
			&r.LineageEdgeID, &r.LineageEdgeType, &r.LineageCreatedAt,
		)
		if err != nil {
			s.logger.Error("Failed to scan incident correlation row",
				slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating incident correlation rows",
			slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	duration := time.Since(start)
	s.logger.Info("Queried incident correlation",
		slog.Duration("duration", duration),
		slog.Int("result_count", len(results)),
		slog.Bool("filtered", filter != nil))

	// Warn if query is slow (>500ms)
	if duration > 500*time.Millisecond {
		s.logger.Warn("Slow incident correlation query detected",
			slog.Duration("duration", duration),
			slog.Int("result_count", len(results)))
	}

	return results, nil
}

// buildIncidentCorrelationQuery constructs SQL query with WHERE clause based on filter.
// Returns (query, args) for use with QueryContext.
func buildIncidentCorrelationQuery(filter *correlation.IncidentFilter) (string, []interface{}) {
	baseQuery := `
		SELECT
			test_result_id, test_name, test_type, test_status, test_message,
			test_executed_at, test_duration_ms,
			dataset_urn, dataset_name, dataset_namespace,
			job_run_id, openlineage_run_id, job_name, job_namespace, job_status, job_event_type,
			job_started_at, job_completed_at,
			producer_name, producer_version,
			lineage_edge_id, lineage_edge_type, lineage_created_at
		FROM incident_correlation_view
	`

	if filter == nil {
		return baseQuery + " ORDER BY test_executed_at DESC", nil
	}

	var conditions []string

	var args []interface{}

	paramIndex := 1

	if filter.TestStatus != nil {
		conditions = append(conditions, fmt.Sprintf("test_status = $%d", paramIndex))
		args = append(args, *filter.TestStatus)
		paramIndex++
	}

	if filter.JobStatus != nil {
		conditions = append(conditions, fmt.Sprintf("job_status = $%d", paramIndex))
		args = append(args, *filter.JobStatus)
		paramIndex++
	}

	if filter.ProducerName != nil {
		conditions = append(conditions, fmt.Sprintf("producer_name = $%d", paramIndex))
		args = append(args, *filter.ProducerName)
		paramIndex++
	}

	if filter.DatasetURN != nil {
		conditions = append(conditions, fmt.Sprintf("dataset_urn = $%d", paramIndex))
		args = append(args, *filter.DatasetURN)
		paramIndex++
	}

	if filter.JobRunID != nil {
		conditions = append(conditions, fmt.Sprintf("job_run_id = $%d", paramIndex))
		args = append(args, *filter.JobRunID)
		paramIndex++
	}

	if filter.Tool != nil {
		// Filter by tool extracted from canonical job_run_id
		// Format: "dbt:abc-123" → matches "dbt:%"
		conditions = append(conditions, fmt.Sprintf("job_run_id LIKE $%d", paramIndex))
		args = append(args, *filter.Tool+":%")
		paramIndex++
	}

	if filter.TestExecutedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("test_executed_at > $%d", paramIndex))
		args = append(args, *filter.TestExecutedAfter)
		paramIndex++
	}

	if filter.TestExecutedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("test_executed_at < $%d", paramIndex))
		args = append(args, *filter.TestExecutedBefore)
	}

	if len(conditions) > 0 {
		baseQuery += " WHERE " + strings.Join(conditions, " AND ")
	}

	baseQuery += " ORDER BY test_executed_at DESC"

	return baseQuery, args
}

// QueryLineageImpact implements correlation.Store.
// Queries the lineage_impact_analysis view for downstream impact analysis.
//
// This method performs recursive lineage traversal to find all datasets affected
// by a specific job run, up to the specified depth.
//
// Parameters:
//   - jobRunID: Job run ID to analyze downstream impact for
//   - maxDepth: Maximum recursion depth (0 = all depths, -1 = direct outputs only, >0 = depth limit)
//
// Returns:
//   - Slice of ImpactResult sorted by depth (empty slice if no downstream datasets)
//   - Error if query fails or context is cancelled
//
// Performance:
//   - View is pre-materialized with recursive CTE (fast queries)
//   - Typical query time: 5-20ms
//   - Results limited by maxDepth parameter
func (s *LineageStore) QueryLineageImpact(
	ctx context.Context,
	jobRunID string,
	maxDepth int,
) ([]correlation.ImpactResult, error) {
	start := time.Now()

	query := `
		SELECT job_run_id, dataset_urn, dataset_name, depth
		FROM lineage_impact_analysis
		WHERE job_run_id = $1
	`
	args := []interface{}{jobRunID}

	// maxDepth -1 means only direct outputs (depth 0)
	if maxDepth == -1 {
		query += " AND depth = 0"
	} else if maxDepth > 0 {
		query += " AND depth <= $2"

		args = append(args, maxDepth)
	}

	query += " ORDER BY depth, dataset_urn"

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		s.logger.Error("Failed to query lineage impact",
			slog.Any("error", err),
			slog.String("job_run_id", jobRunID),
			slog.Int("max_depth", maxDepth))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []correlation.ImpactResult

	for rows.Next() {
		var r correlation.ImpactResult

		err := rows.Scan(&r.JobRunID, &r.DatasetURN, &r.DatasetName, &r.Depth)
		if err != nil {
			s.logger.Error("Failed to scan lineage impact row",
				slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating lineage impact rows",
			slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried lineage impact",
		slog.Duration("duration", time.Since(start)),
		slog.String("job_run_id", jobRunID),
		slog.Int("max_depth", maxDepth),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryRecentIncidents implements correlation.Store.
// Queries the recent_incidents_summary view for 7-day incident overview.
//
// This method returns aggregated incident statistics per job run for the last 7 days.
//
// Parameters:
//   - limit: Maximum number of results to return (0 = no limit)
//
// Returns:
//   - Slice of RecentIncidentSummary sorted by most recent failure first
//   - Empty slice if no incidents in 7-day window
//   - Error if query fails or context is cancelled
//
// Performance:
//   - View is pre-materialized with time-window filtering
//   - Typical query time: 5-15ms
//   - 7-day window filter applied at view level (uses database NOW())
func (s *LineageStore) QueryRecentIncidents(
	ctx context.Context,
	limit int,
) ([]correlation.RecentIncidentSummary, error) {
	start := time.Now()

	query := `
		SELECT
			job_run_id, job_name, job_namespace, job_status, producer_name,
			failed_test_count, affected_dataset_count,
			failed_test_names, affected_dataset_urns,
			first_test_failure_at, last_test_failure_at,
			job_started_at, job_completed_at,
			downstream_affected_count
		FROM recent_incidents_summary
		ORDER BY last_test_failure_at DESC
	`

	var args []interface{}

	if limit > 0 {
		query += " LIMIT $1"

		args = append(args, limit)
	}

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		s.logger.Error("Failed to query recent incidents",
			slog.Any("error", err),
			slog.Int("limit", limit))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []correlation.RecentIncidentSummary

	for rows.Next() {
		var r correlation.RecentIncidentSummary

		err := rows.Scan(
			&r.JobRunID, &r.JobName, &r.JobNamespace, &r.JobStatus, &r.ProducerName,
			&r.FailedTestCount, &r.AffectedDatasetCount,
			pq.Array(&r.FailedTestNames), pq.Array(&r.AffectedDatasetURNs),
			&r.FirstTestFailureAt, &r.LastTestFailureAt,
			&r.JobStartedAt, &r.JobCompletedAt,
			&r.DownstreamAffectedCount,
		)
		if err != nil {
			s.logger.Error("Failed to scan recent incidents row",
				slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating recent incidents rows",
			slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried recent incidents",
		slog.Duration("duration", time.Since(start)),
		slog.Int("limit", limit),
		slog.Int("result_count", len(results)))

	return results, nil
}
