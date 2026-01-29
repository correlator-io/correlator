package storage

import (
	"context"
	"database/sql"
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
// Queries the incident_correlation_view with optional filters and pagination.
//
// This method provides type-safe access to the correlation view, which links
// test failures to the job runs that produced the failing datasets.
//
// Parameters:
//   - filter: Optional filter (nil = no filtering, returns all incidents)
//   - pagination: Optional pagination (nil = returns all results)
//
// Returns:
//   - IncidentQueryResult with incidents and total count
//   - Error if query fails or context is cancelled
//
// Performance:
//   - View is pre-materialized (fast queries, typically 10-50ms)
//   - Uses COUNT(*) OVER() window function for efficient pagination
//   - Uses indexes for filtered queries
//   - Call RefreshViews() to update data
func (s *LineageStore) QueryIncidents(
	ctx context.Context,
	filter *correlation.IncidentFilter,
	pagination *correlation.Pagination,
) (*correlation.IncidentQueryResult, error) {
	start := time.Now()

	query, args := buildIncidentCorrelationQuery(filter, pagination)

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

	var total int

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
			&total,
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
		slog.Int("total", total),
		slog.Bool("filtered", filter != nil),
		slog.Bool("paginated", pagination != nil))

	// Warn if query is slow (>500ms)
	if duration > 500*time.Millisecond {
		s.logger.Warn("Slow incident correlation query detected",
			slog.Duration("duration", duration),
			slog.Int("result_count", len(results)))
	}

	return &correlation.IncidentQueryResult{
		Incidents: results,
		Total:     total,
	}, nil
}

// buildIncidentCorrelationQuery constructs SQL query with WHERE clause based on filter.
// Uses COUNT(*) OVER() window function for efficient pagination.
// Returns (query, args) for use with QueryContext.
func buildIncidentCorrelationQuery(
	filter *correlation.IncidentFilter,
	pagination *correlation.Pagination,
) (string, []interface{}) {
	// Use COUNT(*) OVER() to get total count in the same query
	baseQuery := `
		SELECT
			test_result_id, test_name, test_type, test_status, test_message,
			test_executed_at, test_duration_ms,
			dataset_urn, dataset_name, dataset_namespace,
			job_run_id, openlineage_run_id, job_name, job_namespace, job_status, job_event_type,
			job_started_at, job_completed_at,
			producer_name, producer_version,
			lineage_edge_id, lineage_edge_type, lineage_created_at,
			COUNT(*) OVER() AS total_count
		FROM incident_correlation_view
	`

	conditions, args, paramIndex := buildFilterConditions(filter)

	if len(conditions) > 0 {
		baseQuery += " WHERE " + strings.Join(conditions, " AND ")
	}

	baseQuery += " ORDER BY test_executed_at DESC"

	// Add pagination (LIMIT/OFFSET)
	if pagination != nil {
		baseQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", paramIndex, paramIndex+1)

		args = append(args, pagination.Limit, pagination.Offset)
	}

	return baseQuery, args
}

// buildFilterConditions extracts filter conditions from IncidentFilter.
// Returns (conditions, args, nextParamIndex).
func buildFilterConditions(filter *correlation.IncidentFilter) ([]string, []interface{}, int) {
	if filter == nil {
		return nil, nil, 1
	}

	var conditions []string

	var args []interface{}

	paramIndex := 1

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
		paramIndex++
	}

	return conditions, args, paramIndex
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

// QueryIncidentByID implements correlation.Store.
// Queries a single incident by test_result_id from the incident_correlation_view.
//
// Parameters:
//   - testResultID: Test result ID (primary key)
//
// Returns:
//   - Pointer to Incident (nil if not found, no error)
//   - Error if query fails or context is cancelled
func (s *LineageStore) QueryIncidentByID(ctx context.Context, testResultID int64) (*correlation.Incident, error) {
	start := time.Now()

	query := `
		SELECT
			test_result_id, test_name, test_type, test_status, test_message,
			test_executed_at, test_duration_ms,
			dataset_urn, dataset_name, dataset_namespace,
			job_run_id, openlineage_run_id, job_name, job_namespace, job_status, job_event_type,
			job_started_at, job_completed_at,
			producer_name, producer_version,
			lineage_edge_id, lineage_edge_type, lineage_created_at
		FROM incident_correlation_view
		WHERE test_result_id = $1
		LIMIT 1
	`

	row := s.conn.QueryRowContext(ctx, query, testResultID)

	var r correlation.Incident

	err := row.Scan(
		&r.TestResultID, &r.TestName, &r.TestType, &r.TestStatus, &r.TestMessage,
		&r.TestExecutedAt, &r.TestDurationMs,
		&r.DatasetURN, &r.DatasetName, &r.DatasetNS,
		&r.JobRunID, &r.OpenLineageRunID, &r.JobName, &r.JobNamespace, &r.JobStatus, &r.JobEventType,
		&r.JobStartedAt, &r.JobCompletedAt,
		&r.ProducerName, &r.ProducerVersion,
		&r.LineageEdgeID, &r.LineageEdgeType, &r.LineageCreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.Info("Incident not found",
				slog.Duration("duration", time.Since(start)),
				slog.Int64("id", testResultID))

			return nil, nil //nolint:nilnil // Not found returns nil incident, not an error
		}

		s.logger.Error("Failed to query incident by ID",
			slog.Any("error", err),
			slog.Int64("id", testResultID))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried incident by ID",
		slog.Duration("duration", time.Since(start)),
		slog.Int64("id", testResultID))

	return &r, nil
}

// QueryDownstreamCounts implements correlation.Store.
// Returns downstream dataset counts for multiple job runs in a single query.
//
// This method is optimized for batch queries to avoid N+1 problem when
// displaying incident lists. It queries the lineage_impact_analysis view
// and counts distinct downstream datasets (depth > 0) per job run.
//
// Parameters:
//   - jobRunIDs: Slice of job run IDs to query counts for
//
// Returns:
//   - Map of job_run_id -> downstream_count (missing keys have 0 downstream)
//   - Error if query fails or context is cancelled
func (s *LineageStore) QueryDownstreamCounts(
	ctx context.Context,
	jobRunIDs []string,
) (map[string]int, error) {
	start := time.Now()

	// Return empty map for empty input (avoid unnecessary query)
	if len(jobRunIDs) == 0 {
		return map[string]int{}, nil
	}

	query := `
		SELECT job_run_id, COUNT(DISTINCT dataset_urn) as downstream_count
		FROM lineage_impact_analysis
		WHERE job_run_id = ANY($1)
		  AND depth > 0
		GROUP BY job_run_id
	`

	rows, err := s.conn.QueryContext(ctx, query, pq.Array(jobRunIDs))
	if err != nil {
		s.logger.Error("Failed to query downstream counts",
			slog.Any("error", err),
			slog.Int("job_run_count", len(jobRunIDs)))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	results := make(map[string]int)

	for rows.Next() {
		var jobRunID string

		var count int

		if err := rows.Scan(&jobRunID, &count); err != nil {
			s.logger.Error("Failed to scan downstream count row",
				slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results[jobRunID] = count
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating downstream count rows",
			slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried downstream counts",
		slog.Duration("duration", time.Since(start)),
		slog.Int("job_run_count", len(jobRunIDs)),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryDownstreamWithParents implements correlation.Store.
// Queries downstream datasets with parent URN relationships for tree visualization.
//
// This method performs a recursive traversal starting from the job's direct outputs,
// following input→output relationships through consuming jobs.
//
// Parameters:
//   - jobRunID: Job run ID to query downstream impact for
//   - maxDepth: Maximum recursion depth (typically 10)
//
// Returns:
//   - Slice of DownstreamResult with parent_urn (depth > 0 only)
//   - Empty slice if no downstream datasets
//   - Error if query fails or context is cancelled
//
// Performance:
//   - Uses recursive CTE (efficient in PostgreSQL)
//   - Typical query time: 5-30ms depending on graph size
//   - maxDepth prevents runaway recursion
func (s *LineageStore) QueryDownstreamWithParents(
	ctx context.Context,
	jobRunID string,
	maxDepth int,
) ([]correlation.DownstreamResult, error) {
	start := time.Now()

	// Recursive CTE to build downstream tree with parent relationships
	query := `
		WITH RECURSIVE downstream_tree AS (
			-- Base case: Direct outputs of the job (depth 0)
			SELECT
				le.dataset_urn,
				d.name AS dataset_name,
				0 AS depth,
				le.dataset_urn AS parent_urn
			FROM lineage_edges le
				JOIN datasets d ON le.dataset_urn = d.dataset_urn
			WHERE le.job_run_id = $1
			  AND le.edge_type = 'output'

			UNION ALL

			-- Recursive case: Find jobs that consume our datasets and their outputs
			SELECT
				le_out.dataset_urn,
				d.name,
				dt.depth + 1,
				dt.dataset_urn AS parent_urn
			FROM downstream_tree dt
				-- Find jobs that consume this dataset as input
				JOIN lineage_edges le_in ON dt.dataset_urn = le_in.dataset_urn
					AND le_in.edge_type = 'input'
				-- Find outputs of those consuming jobs
				JOIN lineage_edges le_out ON le_in.job_run_id = le_out.job_run_id
					AND le_out.edge_type = 'output'
				JOIN datasets d ON le_out.dataset_urn = d.dataset_urn
			WHERE dt.depth < $2
			  -- Prevent self-loops
			  AND le_out.dataset_urn != dt.dataset_urn
		)
		SELECT DISTINCT dataset_urn, dataset_name, depth, parent_urn
		FROM downstream_tree
		WHERE depth > 0
		ORDER BY depth, dataset_urn
	`

	rows, err := s.conn.QueryContext(ctx, query, jobRunID, maxDepth)
	if err != nil {
		s.logger.Error("Failed to query downstream with parents",
			slog.Any("error", err),
			slog.String("job_run_id", jobRunID),
			slog.Int("max_depth", maxDepth))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []correlation.DownstreamResult

	for rows.Next() {
		var r correlation.DownstreamResult

		if err := rows.Scan(&r.DatasetURN, &r.DatasetName, &r.Depth, &r.ParentURN); err != nil {
			s.logger.Error("Failed to scan downstream with parents row",
				slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating downstream with parents rows",
			slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried downstream with parents",
		slog.Duration("duration", time.Since(start)),
		slog.String("job_run_id", jobRunID),
		slog.Int("max_depth", maxDepth),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryOrphanNamespaces implements correlation.Store.
// Returns namespaces that appear in validation tests but have no corresponding
// data producer output edges.
//
// Orphan Detection Logic:
//   - Producer namespaces: Namespaces with lineage_edges where edge_type='output'
//   - Validator namespaces: Namespaces from test_results joined to datasets and job_runs
//   - Orphan = Validator namespace NOT IN Producer namespaces
//
// This identifies namespace aliasing issues where different tools use different formats:
//   - GE might emit: "postgres_prod"
//   - dbt might emit: "postgresql://prod-db:5432/mydb"
//
// Returns:
//   - Slice of OrphanNamespace sorted by event_count DESC (most impactful first)
//   - Empty slice if no orphan namespaces exist (healthy state)
//   - Error if query fails or context is cancelled
//
// Performance:
//   - Queries test_results, datasets, lineage_edges, job_runs tables
//   - Filters out empty namespaces
//   - Typical query time: 10-100ms depending on data volume
func (s *LineageStore) QueryOrphanNamespaces(ctx context.Context) ([]correlation.OrphanNamespace, error) {
	start := time.Now()

	// Orphan detection query using CTEs
	query := `
		WITH producer_namespaces AS (
			-- Namespaces that have output lineage edges (data producers)
			SELECT DISTINCT d.namespace
			FROM lineage_edges le
			JOIN datasets d ON le.dataset_urn = d.dataset_urn
			WHERE le.edge_type = 'output'
			  AND d.namespace != ''
		),
		validator_namespaces AS (
			-- Namespaces from test results (validators)
			SELECT
				d.namespace,
				jr.producer_name,
				MAX(tr.executed_at) AS last_seen,
				COUNT(*) AS event_count
			FROM test_results tr
			JOIN datasets d ON tr.dataset_urn = d.dataset_urn
			JOIN job_runs jr ON tr.job_run_id = jr.job_run_id
			WHERE d.namespace != ''
			GROUP BY d.namespace, jr.producer_name
		)
		-- Orphans: validator namespaces not in producer namespaces
		SELECT
			vn.namespace,
			vn.producer_name,
			vn.last_seen,
			vn.event_count
		FROM validator_namespaces vn
		WHERE vn.namespace NOT IN (SELECT namespace FROM producer_namespaces)
		ORDER BY vn.event_count DESC, vn.namespace, vn.producer_name
	`

	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		s.logger.Error("Failed to query orphan namespaces",
			slog.Any("error", err),
			slog.Duration("duration", time.Since(start)))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []correlation.OrphanNamespace

	for rows.Next() {
		var r correlation.OrphanNamespace

		if err := rows.Scan(&r.Namespace, &r.Producer, &r.LastSeen, &r.EventCount); err != nil {
			s.logger.Error("Failed to scan orphan namespace row", slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating orphan namespace rows", slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried orphan namespaces",
		slog.Duration("duration", time.Since(start)),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryCorrelationHealth implements correlation.Store.
// Returns overall correlation health metrics including correlation rate,
// total datasets, and orphan namespaces.
//
// Correlation Rate Calculation:
//   - Numerator: Incidents where dataset namespace has producer output edges
//   - Denominator: All incidents from incident_correlation_view
//   - If denominator = 0, returns 1.0 (no incidents = healthy)
//
// Returns:
//   - Pointer to Health with metrics
//   - Error if query fails or context is cancelled
//
// Performance:
//   - Calls QueryOrphanNamespaces internally
//   - Queries incident_correlation_view for rate calculation
//   - Typical query time: 50-200ms
func (s *LineageStore) QueryCorrelationHealth(ctx context.Context) (*correlation.Health, error) {
	start := time.Now()

	// Query orphan namespaces first
	orphans, err := s.QueryOrphanNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	// Build set of orphan namespaces for efficient lookup
	orphanNSSet := make(map[string]bool)
	for _, o := range orphans {
		orphanNSSet[o.Namespace] = true
	}

	// Query correlation rate and total datasets
	query := `
		WITH all_failed_tests AS (
			-- Total failed/error tests across all datasets
			SELECT COUNT(*) AS total_incidents
			FROM test_results
			WHERE status IN ('failed', 'error')
		),
		correlated_incidents AS (
			-- Incidents that have lineage correlation (in the view)
			SELECT COUNT(*) AS correlated_count
			FROM incident_correlation_view
		),
		dataset_stats AS (
			SELECT COUNT(DISTINCT dataset_urn) AS total_datasets
			FROM test_results
		)
		SELECT
			COALESCE(a.total_incidents, 0) AS total_incidents,
			COALESCE(c.correlated_count, 0) AS correlated_incidents,
			COALESCE(d.total_datasets, 0) AS total_datasets
		FROM all_failed_tests a, correlated_incidents c, dataset_stats d
	`

	var totalIncidents, correlatedIncidents, totalDatasets int

	err = s.conn.QueryRowContext(ctx, query).Scan(&totalIncidents, &correlatedIncidents, &totalDatasets)
	if err != nil {
		// Handle case where no data exists (query returns no rows)
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.Info("Queried correlation health (empty state)",
				slog.Duration("duration", time.Since(start)))

			return &correlation.Health{
				CorrelationRate:  1.0, // No incidents = healthy
				TotalDatasets:    0,
				OrphanNamespaces: orphans,
			}, nil
		}

		s.logger.Error("Failed to query correlation health",
			slog.Any("error", err),
			slog.Duration("duration", time.Since(start)))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	// Calculate correlation rate (avoid division by zero)
	correlationRate := 1.0

	if totalIncidents > 0 {
		correlationRate = float64(correlatedIncidents) / float64(totalIncidents)
	}

	duration := time.Since(start)
	s.logger.Info("Queried correlation health",
		slog.Duration("duration", duration),
		slog.Float64("correlation_rate", correlationRate),
		slog.Int("total_incidents", totalIncidents),
		slog.Int("correlated_incidents", correlatedIncidents),
		slog.Int("total_datasets", totalDatasets),
		slog.Int("orphan_namespaces", len(orphans)))

	return &correlation.Health{
		CorrelationRate:  correlationRate,
		TotalDatasets:    totalDatasets,
		OrphanNamespaces: orphans,
	}, nil
}
