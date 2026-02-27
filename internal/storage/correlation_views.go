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

	"github.com/correlator-io/correlator/internal/canonicalization"
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

// refreshViews refreshes all correlation materialized views in dependency order.
//
// This is an internal method called by the debounced post-ingestion hook (notifyDataChanged).
// It calls the PostgreSQL function refresh_correlation_views() which:
//   - Refreshes incident_correlation_view (failed/error tests correlated to job runs)
//   - Refreshes lineage_impact_analysis (recursive downstream impact analysis)
//   - Refreshes recent_incidents_summary (7-day rolling window aggregation)
//   - Uses CONCURRENTLY for zero-downtime updates (~650ms-2s, no locks)
//
// Performance characteristics:
//   - Typical duration: 650ms-2s (depends on data volume)
//   - No table locks (CONCURRENTLY refresh)
//   - Safe to call frequently (e.g., every 5 minutes)
func (s *LineageStore) refreshViews(ctx context.Context) error {
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

// upsertBatchSize controls how many (raw_urn, canonical_urn) pairs are written
// per multi-value INSERT statement during refreshResolvedDatasets.
const upsertBatchSize = 500

// refreshResolvedDatasets populates the resolved_datasets lookup table.
//
// For every distinct dataset URN across datasets, test_results, and lineage_edges,
// it computes the canonical form via the alias resolver (identity if no resolver)
// and upserts the mappings. Stale rows (URNs no longer present in any source table)
// are deleted at the end of the transaction.
//
// Uses UPSERT + cleanup so the table is never empty mid-transaction — concurrent
// readers (e.g., a manual REFRESH MATERIALIZED VIEW CONCURRENTLY) always see a
// valid set of mappings.
//
// Performance: O(n) where n = distinct URN count. Typically <100ms for <10K URNs.
func (s *LineageStore) refreshResolvedDatasets(ctx context.Context) error {
	start := time.Now()

	// Collect all distinct URNs across source tables
	query := `
		SELECT DISTINCT urn FROM (
			SELECT dataset_urn AS urn FROM datasets
			UNION
			SELECT dataset_urn AS urn FROM test_results
			UNION
			SELECT dataset_urn AS urn FROM lineage_edges
		) all_urns
	`

	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query distinct URNs: %w", err)
	}

	defer func() { _ = rows.Close() }()

	var mappings []urnMapping

	for rows.Next() {
		var urn string
		if err := rows.Scan(&urn); err != nil {
			return fmt.Errorf("failed to scan URN: %w", err)
		}

		canonical := urn // identity by default
		if s.resolver != nil {
			canonical = s.resolver.Resolve(urn)
		}

		mappings = append(mappings, urnMapping{raw: urn, canonical: canonical})
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("URN iteration error: %w", err)
	}

	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() { _ = tx.Rollback() }()

	// UPSERT in batches — inserts new mappings and updates changed canonical URNs
	// without ever emptying the table.
	for i := 0; i < len(mappings); i += upsertBatchSize {
		end := i + upsertBatchSize
		if end > len(mappings) {
			end = len(mappings)
		}

		if err := s.upsertResolvedBatch(ctx, tx, mappings[i:end]); err != nil {
			return err
		}
	}

	// Remove stale rows whose raw_urn no longer appears in any source table.
	if err := s.deleteStaleResolved(ctx, tx, mappings); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit resolved_datasets: %w", err)
	}

	s.logger.Info("Refreshed resolved_datasets",
		slog.Duration("duration", time.Since(start)),
		slog.Int("mapping_count", len(mappings)))

	return nil
}

// upsertResolvedBatch writes a batch of URN mappings using a multi-value
// INSERT ... ON CONFLICT DO UPDATE. This keeps the number of SQL round-trips
// proportional to len(batch)/upsertBatchSize instead of len(batch).
//
//nolint:mnd
func (s *LineageStore) upsertResolvedBatch(
	ctx context.Context,
	tx *sql.Tx,
	batch []urnMapping,
) error {
	if len(batch) == 0 {
		return nil
	}

	var b strings.Builder
	b.WriteString("INSERT INTO resolved_datasets (raw_urn, canonical_urn) VALUES ")

	// Each urnMapping contributes two SQL parameters: raw_urn and canonical_urn Pre-allocating len(batch)*2 avoids
	// repeated slice growth during the loop
	args := make([]interface{}, 0, len(batch)*2)

	for i, m := range batch {
		if i > 0 {
			b.WriteString(", ")
		}

		paramBase := i*2 + 1 // add positional parameters ($1, $2, $3, ...)
		_, _ = fmt.Fprintf(&b, "($%d, $%d)", paramBase, paramBase+1)

		args = append(args, m.raw, m.canonical)
	}

	b.WriteString(" ON CONFLICT (raw_urn) DO UPDATE SET canonical_urn = EXCLUDED.canonical_urn")

	if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("failed to upsert resolved_datasets batch: %w", err)
	}

	return nil
}

// deleteStaleResolved removes rows from resolved_datasets whose raw_urn is not
// in the current set of mappings. This handles URNs that have been removed from
// all source tables (e.g., after a dataset deletion).
func (s *LineageStore) deleteStaleResolved(
	ctx context.Context,
	tx *sql.Tx,
	mappings []urnMapping,
) error {
	if len(mappings) == 0 {
		// No current URNs → delete everything
		if _, err := tx.ExecContext(ctx, "DELETE FROM resolved_datasets"); err != nil {
			return fmt.Errorf("failed to clear resolved_datasets: %w", err)
		}

		return nil
	}

	rawURNs := make([]string, len(mappings))
	for i, m := range mappings {
		rawURNs[i] = m.raw
	}

	if _, err := tx.ExecContext(ctx,
		"DELETE FROM resolved_datasets WHERE raw_urn != ALL($1)",
		pq.Array(rawURNs),
	); err != nil {
		return fmt.Errorf("failed to delete stale resolved_datasets: %w", err)
	}

	return nil
}

// QueryIncidents implements correlation.Store.
// Queries the incident_correlation_view with optional filters and pagination.
//
// Cross-tool correlation is handled by the resolved_datasets lookup table:
// the view JOINs through canonical URNs, so pattern resolution is transparent.
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
//   - Typical query time: 10-50ms (view-based, pre-computed)
//   - Views are auto-refreshed via debounced post-ingestion hook
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
			&r.RunID, &r.JobName, &r.JobNamespace, &r.JobStatus, &r.JobEventType,
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
			run_id, job_name, job_namespace, job_status, job_event_type,
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

	if filter.RunID != nil {
		conditions = append(conditions, fmt.Sprintf("run_id = $%d", paramIndex))
		args = append(args, *filter.RunID)
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

// QueryIncidentByID implements correlation.Store.
// Queries a single incident by test_result_id from the incident_correlation_view.
//
// Cross-tool correlation is handled by the resolved_datasets lookup table:
// the view JOINs through canonical URNs, so pattern resolution is transparent.
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
			run_id, job_name, job_namespace, job_status, job_event_type,
			job_started_at, job_completed_at,
			producer_name, producer_version,
			lineage_edge_id, lineage_edge_type, lineage_created_at,
			parent_run_id, parent_job_name, parent_job_namespace,
			parent_job_status, parent_job_completed_at, parent_producer_name,
			root_parent_run_id, root_parent_job_name, root_parent_job_namespace,
			root_parent_job_status, root_parent_job_completed_at, root_parent_producer_name
		FROM incident_correlation_view
		WHERE test_result_id = $1
		LIMIT 1
	`

	row := s.conn.QueryRowContext(ctx, query, testResultID)

	var r correlation.Incident

	var parentRunID, parentJobName, parentJobNamespace, parentJobStatus, parentProducerName sql.NullString

	var parentJobCompletedAt, rootParentJobCompletedAt sql.NullTime

	var rootParentRunID, rootParentJobName, rootParentJobNamespace sql.NullString

	var rootParentJobStatus, rootParentProducerName sql.NullString

	err := row.Scan(
		&r.TestResultID, &r.TestName, &r.TestType, &r.TestStatus, &r.TestMessage,
		&r.TestExecutedAt, &r.TestDurationMs,
		&r.DatasetURN, &r.DatasetName, &r.DatasetNS,
		&r.RunID, &r.JobName, &r.JobNamespace, &r.JobStatus, &r.JobEventType,
		&r.JobStartedAt, &r.JobCompletedAt,
		&r.ProducerName, &r.ProducerVersion,
		&r.LineageEdgeID, &r.LineageEdgeType, &r.LineageCreatedAt,
		&parentRunID, &parentJobName, &parentJobNamespace,
		&parentJobStatus, &parentJobCompletedAt, &parentProducerName,
		&rootParentRunID, &rootParentJobName, &rootParentJobNamespace,
		&rootParentJobStatus, &rootParentJobCompletedAt, &rootParentProducerName,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.Info("Incident not found in view",
				slog.Duration("duration", time.Since(start)),
				slog.Int64("id", testResultID))

			return nil, nil //nolint:nilnil // Not found returns nil incident, not an error
		}

		s.logger.Error("Failed to query incident by ID from view",
			slog.Any("error", err),
			slog.Int64("id", testResultID))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	// Map nullable parent fields
	r.ParentRunID = parentRunID.String
	r.ParentJobName = parentJobName.String
	r.ParentJobNamespace = parentJobNamespace.String
	r.ParentJobStatus = parentJobStatus.String
	r.ParentProducerName = parentProducerName.String

	if parentJobCompletedAt.Valid {
		r.ParentJobCompletedAt = &parentJobCompletedAt.Time
	}

	// Map nullable root parent fields
	r.RootParentRunID = rootParentRunID.String
	r.RootParentJobName = rootParentJobName.String
	r.RootParentJobNamespace = rootParentJobNamespace.String
	r.RootParentJobStatus = rootParentJobStatus.String
	r.RootParentProducerName = rootParentProducerName.String

	if rootParentJobCompletedAt.Valid {
		r.RootParentJobCompletedAt = &rootParentJobCompletedAt.Time
	}

	s.logger.Info("Queried incident by ID from view",
		slog.Duration("duration", time.Since(start)),
		slog.Int64("id", testResultID))

	return &r, nil
}

// QueryDownstreamCounts implements correlation.Store.
// Returns downstream dataset counts for multiple run IDs in a single query.
//
// This method is optimized for batch queries to avoid N+1 problem when
// displaying incident lists. It queries the lineage_impact_analysis view
// and counts distinct downstream datasets (depth > 0) per run.
//
// Parameters:
//   - runIDs: Slice of run IDs to query counts for
//
// Returns:
//   - Map of run_id -> downstream_count (missing keys have 0 downstream)
//   - Error if query fails or context is cancelled
func (s *LineageStore) QueryDownstreamCounts(
	ctx context.Context,
	runIDs []string,
) (map[string]int, error) {
	start := time.Now()

	// Return empty map for empty input (avoid unnecessary query)
	if len(runIDs) == 0 {
		return map[string]int{}, nil
	}

	query := `
		SELECT run_id, COUNT(DISTINCT dataset_urn) as downstream_count
		FROM lineage_impact_analysis
		WHERE run_id = ANY($1)
		  AND depth > 0
		GROUP BY run_id
	`

	rows, err := s.conn.QueryContext(ctx, query, pq.Array(runIDs))
	if err != nil {
		s.logger.Error("Failed to query downstream counts",
			slog.Any("error", err),
			slog.Int("run_id_count", len(runIDs)))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	results := make(map[string]int)

	for rows.Next() {
		var runID string

		var count int

		if err := rows.Scan(&runID, &count); err != nil {
			s.logger.Error("Failed to scan downstream count row",
				slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results[runID] = count
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating downstream count rows",
			slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried downstream counts",
		slog.Duration("duration", time.Since(start)),
		slog.Int("run_id_count", len(runIDs)),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryDownstreamWithParents implements correlation.Store.
// Queries downstream datasets with parent URN relationships for tree visualization.
//
// This method performs a recursive traversal starting from the job's direct outputs,
// following input->output relationships through consuming jobs.
//
// Parameters:
//   - runID: Run ID to query downstream impact for
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
	runID string,
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
				le.dataset_urn AS parent_urn,
				COALESCE(jr.producer_name, '') AS producer
			FROM lineage_edges le
				JOIN datasets d ON le.dataset_urn = d.dataset_urn
				LEFT JOIN job_runs jr ON le.run_id = jr.run_id
			WHERE le.run_id = $1
			  AND le.edge_type = 'output'

			UNION ALL

			-- Recursive case: Find jobs that consume our datasets and their outputs
			SELECT
				le_out.dataset_urn,
				d.name,
				dt.depth + 1,
				dt.dataset_urn AS parent_urn,
				COALESCE(jr.producer_name, '') AS producer
			FROM downstream_tree dt
				-- Find jobs that consume this dataset as input
				JOIN lineage_edges le_in ON dt.dataset_urn = le_in.dataset_urn
					AND le_in.edge_type = 'input'
				-- Find outputs of those consuming jobs
				JOIN lineage_edges le_out ON le_in.run_id = le_out.run_id
					AND le_out.edge_type = 'output'
				JOIN datasets d ON le_out.dataset_urn = d.dataset_urn
				LEFT JOIN job_runs jr ON le_out.run_id = jr.run_id
			WHERE dt.depth < $2
			  -- Prevent self-loops
			  AND le_out.dataset_urn != dt.dataset_urn
		)
		SELECT DISTINCT dataset_urn, dataset_name, depth, parent_urn, producer
		FROM downstream_tree
		WHERE depth > 0
		ORDER BY depth, dataset_urn
	`

	rows, err := s.conn.QueryContext(ctx, query, runID, maxDepth)
	if err != nil {
		s.logger.Error("Failed to query downstream with parents",
			slog.Any("error", err),
			slog.String("run_id", runID),
			slog.Int("max_depth", maxDepth))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []correlation.DownstreamResult

	for rows.Next() {
		var r correlation.DownstreamResult

		if err := rows.Scan(&r.DatasetURN, &r.DatasetName, &r.Depth, &r.ParentURN, &r.Producer); err != nil {
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
		slog.String("run_id", runID),
		slog.Int("max_depth", maxDepth),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryUpstreamWithChildren implements correlation.Store.
// Queries upstream datasets with child URN relationships for tree visualization.
//
// This is the inverse of QueryDownstreamWithParents:
//   - Downstream: follows output->input->output chain forward (consumers)
//   - Upstream: follows input->output->input chain backward (producers)
//
// Parameters:
//   - datasetURN: The root dataset URN (childURN for depth=1 results)
//   - runID: Run ID that produced the root dataset
//   - maxDepth: Maximum recursion depth (typically 3-10)
//
// Returns:
//   - Slice of UpstreamResult with child_urn for tree building
//   - Empty slice if job has no inputs
//   - Error if query fails or context is cancelled
//
// Performance:
//   - Uses recursive CTE (efficient in PostgreSQL)
//   - Joins job_runs to get producer information
//   - Typical query time: 5-30ms depending on graph size
//   - maxDepth prevents runaway recursion
func (s *LineageStore) QueryUpstreamWithChildren(
	ctx context.Context,
	datasetURN string,
	runID string,
	maxDepth int,
) ([]correlation.UpstreamResult, error) {
	start := time.Now()

	query := `
		WITH RECURSIVE upstream_tree AS (
			SELECT
				le.dataset_urn,
				d.name AS dataset_name,
				1 AS depth,
				$1::text AS child_urn,
				COALESCE(jr.producer_name, '') AS producer
			FROM lineage_edges le
				JOIN datasets d ON le.dataset_urn = d.dataset_urn
				LEFT JOIN lineage_edges le_prod ON le.dataset_urn = le_prod.dataset_urn
					AND le_prod.edge_type = 'output'
				LEFT JOIN job_runs jr ON le_prod.run_id = jr.run_id
			WHERE le.run_id = $2
			  AND le.edge_type = 'input'

			UNION ALL

			SELECT
				le_in.dataset_urn,
				d.name,
				ut.depth + 1,
				ut.dataset_urn AS child_urn,
				COALESCE(jr.producer_name, '') AS producer
			FROM upstream_tree ut
				JOIN lineage_edges le_out ON ut.dataset_urn = le_out.dataset_urn
					AND le_out.edge_type = 'output'
				JOIN lineage_edges le_in ON le_out.run_id = le_in.run_id
					AND le_in.edge_type = 'input'
				JOIN datasets d ON le_in.dataset_urn = d.dataset_urn
				LEFT JOIN lineage_edges le_prod ON le_in.dataset_urn = le_prod.dataset_urn
					AND le_prod.edge_type = 'output'
				LEFT JOIN job_runs jr ON le_prod.run_id = jr.run_id
			WHERE ut.depth < $3
			  AND le_in.dataset_urn != ut.dataset_urn
		)
		SELECT DISTINCT dataset_urn, dataset_name, depth, child_urn, producer
		FROM upstream_tree
		ORDER BY depth, dataset_urn
	`

	rows, err := s.conn.QueryContext(ctx, query, datasetURN, runID, maxDepth)
	if err != nil {
		s.logger.Error("Failed to query upstream with children",
			slog.Any("error", err),
			slog.String("dataset_urn", datasetURN),
			slog.String("run_id", runID),
			slog.Int("max_depth", maxDepth))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []correlation.UpstreamResult

	for rows.Next() {
		var r correlation.UpstreamResult

		if err := rows.Scan(&r.DatasetURN, &r.DatasetName, &r.Depth, &r.ChildURN, &r.Producer); err != nil {
			s.logger.Error("Failed to scan upstream with children row",
				slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating upstream with children rows",
			slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried upstream with children",
		slog.Duration("duration", time.Since(start)),
		slog.String("dataset_urn", datasetURN),
		slog.String("run_id", runID),
		slog.Int("max_depth", maxDepth),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryOrphanDatasets implements correlation.Store.
// Returns datasets that have test results but no corresponding data producer output edges.
//
// Cross-tool correlation is handled by the resolved_datasets lookup table:
// the SQL query JOINs through canonical URNs, so datasets that resolve to
// produced datasets are automatically excluded.
//
// Orphan Detection Logic:
//   - Orphan = Dataset with test results where canonical URN has no producer output edge
//
// Likely Match Algorithm:
//   - Extract table name from orphan URN using canonicalization.ExtractTableName()
//   - Extract table name from each produced dataset URN
//   - If exact match found, set LikelyMatch with Confidence=1.0
//
// Returns:
//   - Slice of OrphanDataset sorted by test_count DESC (most impactful first)
//   - Each orphan includes LikelyMatch if a candidate was found
//   - Empty slice if no orphan datasets exist (healthy state)
//   - Error if query fails or context is cancelled
//
// Performance:
//   - Queries test_results and lineage_edges via resolved_datasets JOINs
//   - Table name extraction done in Go (not SQL) for flexibility
//   - Typical query time: 20-100ms depending on data volume
func (s *LineageStore) QueryOrphanDatasets(ctx context.Context) ([]correlation.OrphanDataset, error) {
	start := time.Now()

	orphans, err := s.findTrueOrphans(ctx)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	s.logger.Info("Queried orphan datasets",
		slog.Duration("duration", duration),
		slog.Int("orphan_count", len(orphans)))

	return orphans, nil
}

// queryTestedDatasetsWithoutProducer queries datasets with test results but no output edges.
// Uses resolved_datasets for canonical URN matching (cross-tool correlation).
func (s *LineageStore) queryTestedDatasetsWithoutProducer(ctx context.Context) ([]correlation.OrphanDataset, error) {
	query := `
		WITH produced_canonical AS (
			SELECT DISTINCT rd.canonical_urn
			FROM lineage_edges le
			JOIN resolved_datasets rd ON le.dataset_urn = rd.raw_urn
			WHERE le.edge_type = 'output'
		),
		tested_datasets AS (
			SELECT
				tr.dataset_urn,
				COUNT(*) AS test_count,
				MAX(tr.executed_at) AS last_seen
			FROM test_results tr
			GROUP BY tr.dataset_urn
		)
		SELECT td.dataset_urn, td.test_count, td.last_seen
		FROM tested_datasets td
		JOIN resolved_datasets rd ON td.dataset_urn = rd.raw_urn
		WHERE rd.canonical_urn NOT IN (SELECT canonical_urn FROM produced_canonical)
		ORDER BY td.test_count DESC, td.dataset_urn
	`

	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		s.logger.Error("Failed to query orphan datasets", slog.Any("error", err))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var orphans []correlation.OrphanDataset

	for rows.Next() {
		var o correlation.OrphanDataset

		if err := rows.Scan(&o.DatasetURN, &o.TestCount, &o.LastSeen); err != nil {
			s.logger.Error("Failed to scan orphan dataset row", slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		orphans = append(orphans, o)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating orphan dataset rows", slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return orphans, nil
}

// buildTableNameToProducedURNIndex queries produced datasets and indexes them by extracted table name.
// Results are ordered for deterministic first-match-wins behavior.
func (s *LineageStore) buildTableNameToProducedURNIndex(ctx context.Context) (map[string]string, error) {
	query := `
		SELECT DISTINCT dataset_urn
		FROM lineage_edges
		WHERE edge_type = 'output'
		ORDER BY dataset_urn
	`

	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		s.logger.Error("Failed to query produced datasets", slog.Any("error", err))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	tableNameToProduced := make(map[string]string)

	for rows.Next() {
		var producedURN string

		if err := rows.Scan(&producedURN); err != nil {
			s.logger.Error("Failed to scan produced dataset row", slog.Any("error", err))

			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		tableName := canonicalization.ExtractTableName(producedURN)
		if tableName != "" {
			// First match wins (deterministic ordering from query)
			if _, exists := tableNameToProduced[tableName]; !exists {
				tableNameToProduced[tableName] = producedURN
			}
		}
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Error iterating produced dataset rows", slog.Any("error", err))

		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return tableNameToProduced, nil
}

// QueryCorrelationHealth implements correlation.Store.
// Returns overall correlation health metrics including correlation rate,
// dataset counts, orphan datasets, and suggested patterns.
//
// Cross-tool correlation is handled by the resolved_datasets lookup table:
// all SQL queries JOIN through canonical URNs for transparent correlation.
//
// Correlation Rate Calculation:
//   - Numerator: Distinct failed-test datasets with a producer output edge (via canonical URN)
//   - Denominator: All distinct failed-test datasets
//   - If denominator = 0, returns 1.0 (no incidents = healthy)
//
// Returns:
//   - Pointer to Health with metrics, orphans, and suggested patterns
//   - Error if query fails or context is cancelled
//
// Performance:
//   - Queries produced datasets once (shared across orphan detection and rate calculation)
//   - Typical query time: 50-200ms
func (s *LineageStore) QueryCorrelationHealth(ctx context.Context) (*correlation.Health, error) {
	start := time.Now()

	// Query orphan datasets
	orphans, err := s.findTrueOrphans(ctx)
	if err != nil {
		return nil, err
	}

	// Generate pattern suggestions from orphans
	suggestedPatterns := correlation.SuggestPatterns(orphans)

	// Query health statistics
	stats, err := s.queryHealthStats(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.Info("Queried correlation health (empty state)",
				slog.Duration("duration", time.Since(start)))

			return &correlation.Health{
				CorrelationRate:    1.0, // No incidents = healthy
				TotalDatasets:      0,
				ProducedDatasets:   0,
				CorrelatedDatasets: 0,
				OrphanDatasets:     orphans,
				SuggestedPatterns:  suggestedPatterns,
			}, nil
		}

		s.logger.Error("Failed to query correlation health",
			slog.Any("error", err),
			slog.Duration("duration", time.Since(start)))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	correlationRate := s.calculateCorrelationRate(stats)

	duration := time.Since(start)
	s.logger.Info("Queried correlation health",
		slog.Duration("duration", duration),
		slog.Float64("correlation_rate", correlationRate),
		slog.Int("total_datasets", stats.totalDatasets),
		slog.Int("produced_datasets", stats.producedDatasets),
		slog.Int("correlated_datasets", stats.correlatedDatasets),
		slog.Int("orphan_datasets", len(orphans)),
		slog.Int("suggested_patterns", len(suggestedPatterns)))

	return &correlation.Health{
		CorrelationRate:    correlationRate,
		TotalDatasets:      stats.totalDatasets,
		ProducedDatasets:   stats.producedDatasets,
		CorrelatedDatasets: stats.correlatedDatasets,
		OrphanDatasets:     orphans,
		SuggestedPatterns:  suggestedPatterns,
	}, nil
}

// findTrueOrphans returns orphan datasets enriched with likely matches.
// Cross-tool correlation is handled in SQL via resolved_datasets JOINs.
func (s *LineageStore) findTrueOrphans(ctx context.Context) ([]correlation.OrphanDataset, error) {
	tableNameToProducedURNIndex, err := s.buildTableNameToProducedURNIndex(ctx)
	if err != nil {
		return nil, err
	}

	return s.findUnresolvedOrphanDatasets(ctx, tableNameToProducedURNIndex)
}

// findUnresolvedOrphanDatasets returns true orphan datasets enriched with likely matches.
func (s *LineageStore) findUnresolvedOrphanDatasets(
	ctx context.Context,
	tableNameToProducedURNIndex map[string]string,
) ([]correlation.OrphanDataset, error) {
	// Query orphan datasets (datasets with test results but no output edges)
	orphans, err := s.queryTestedDatasetsWithoutProducer(ctx)
	if err != nil {
		return nil, err
	}

	if len(orphans) == 0 {
		return orphans, nil
	}

	// Enrich orphans with likely matches based on table name
	filteredOrphans := make([]correlation.OrphanDataset, 0, len(orphans))

	for _, o := range orphans {
		// Try to find likely match by table name
		orphanTableName := canonicalization.ExtractTableName(o.DatasetURN)
		if orphanTableName != "" {
			if producedURN, found := tableNameToProducedURNIndex[orphanTableName]; found {
				o.LikelyMatch = &correlation.DatasetMatch{
					DatasetURN:  producedURN,
					Confidence:  1.0,
					MatchReason: "exact_table_name",
				}
			}
		}

		filteredOrphans = append(filteredOrphans, o)
	}

	return filteredOrphans, nil
}

// calculateCorrelationRate computes the correlation rate for the health endpoint.
//
// Cross-tool correlation is handled by the resolved_datasets lookup table:
// the health stats query JOINs through canonical URNs, so all correlation
// (including cross-tool) is computed in SQL.
//
// Formula: correlated_failed_datasets / total_failed_datasets
//
// Parameters:
//   - stats: Pre-queried health statistics from queryHealthStats()
//
// Returns:
//   - Correlation rate between 0.0 and 1.0 (1.0 = all failed test datasets can be traced to a producer)
func (s *LineageStore) calculateCorrelationRate(stats *healthStats) float64 {
	return s.calculateCorrelationRateFromHealthStats(stats)
}

// calculateCorrelationRateFromHealthStats computes correlation rate using pre-computed database statistics.
//
// Uses counts already computed by queryHealthStats() via resolved_datasets JOINs,
// which handle cross-tool correlation transparently in SQL.
//
// Formula: correlatedFailedTestedDatasets / totalFailedTestedDatasets
//
// Parameters:
//   - stats: Pre-queried health statistics containing distinct dataset counts
//
// Returns:
//   - Correlation rate between 0.0 and 1.0
//   - Returns 1.0 if no failed tests exist (healthy state)
func (s *LineageStore) calculateCorrelationRateFromHealthStats(stats *healthStats) float64 {
	if stats.totalFailedTestedDatasets > 0 {
		return float64(stats.correlatedFailedTestedDatasets) / float64(stats.totalFailedTestedDatasets)
	}

	return 1.0 // No failed tests = healthy
}

// QueryOrchestrationChain walks the parent_run_id chain from a given run
// up to the root orchestrator using a recursive CTE.
// Returns the ancestor chain ordered from root (index 0) to immediate parent (last).
// The starting job itself is excluded from the result.
func (s *LineageStore) QueryOrchestrationChain(
	ctx context.Context,
	runID string,
	maxDepth int,
) ([]correlation.OrchestrationNode, error) {
	start := time.Now()

	query := `
		WITH RECURSIVE chain AS (
			SELECT
				jr.run_id,
				jr.job_name,
				jr.job_namespace,
				jr.producer_name,
				jr.current_state,
				jr.parent_run_id,
				1 AS depth
			FROM job_runs jr
			WHERE jr.run_id = (
				SELECT parent_run_id FROM job_runs WHERE run_id = $1
			)

			UNION ALL

			SELECT
				jr.run_id,
				jr.job_name,
				jr.job_namespace,
				jr.producer_name,
				jr.current_state,
				jr.parent_run_id,
				c.depth + 1
			FROM job_runs jr
			JOIN chain c ON jr.run_id = c.parent_run_id
			WHERE c.depth < $2
		)
		SELECT run_id, job_name, job_namespace, producer_name, current_state, depth
		FROM chain
		ORDER BY depth DESC
	`

	rows, err := s.conn.QueryContext(ctx, query, runID, maxDepth)
	if err != nil {
		s.logger.Error("Failed to query orchestration chain",
			slog.Any("error", err),
			slog.String("run_id", runID))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var chain []correlation.OrchestrationNode

	for rows.Next() {
		var node correlation.OrchestrationNode

		var depth int

		if err := rows.Scan(
			&node.RunID, &node.JobName, &node.JobNamespace,
			&node.ProducerName, &node.Status, &depth,
		); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		chain = append(chain, node)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	s.logger.Info("Queried orchestration chain",
		slog.Duration("duration", time.Since(start)),
		slog.String("run_id", runID),
		slog.Int("chain_length", len(chain)))

	return chain, nil
}

// queryHealthStats queries database for health statistics.
// All metrics use DISTINCT dataset_urn counts for accurate correlation rate calculation.
func (s *LineageStore) queryHealthStats(ctx context.Context) (*healthStats, error) {
	query := `
		WITH failed_tested_datasets AS (
			-- Distinct datasets with failed/error tests (denominator for correlation rate)
			SELECT COUNT(DISTINCT dataset_urn) AS total_count
			FROM test_results
			WHERE status IN ('failed', 'error')
		),
		correlated_failed_datasets AS (
			-- Distinct datasets with failed tests AND producer output edges (via canonical URN resolution)
			SELECT COUNT(DISTINCT tr.dataset_urn) AS correlated_count
			FROM test_results tr
			JOIN resolved_datasets rd ON tr.dataset_urn = rd.raw_urn
			WHERE tr.status IN ('failed', 'error')
			AND EXISTS (
				SELECT 1 FROM resolved_datasets rd2
				JOIN lineage_edges le ON le.dataset_urn = rd2.raw_urn
				WHERE rd2.canonical_urn = rd.canonical_urn AND le.edge_type = 'output'
			)
		),
		all_tested_datasets AS (
			-- Distinct datasets with any test results
			SELECT COUNT(DISTINCT dataset_urn) AS total_datasets
			FROM test_results
		),
		produced_datasets AS (
			-- Distinct datasets with output edges
			SELECT COUNT(DISTINCT dataset_urn) AS produced_count
			FROM lineage_edges
			WHERE edge_type = 'output'
		),
		correlated_datasets AS (
			-- Distinct datasets with both tests (any status) AND output edges (via canonical URN resolution)
			SELECT COUNT(DISTINCT tr.dataset_urn) AS correlated_count
			FROM test_results tr
			JOIN resolved_datasets rd ON tr.dataset_urn = rd.raw_urn
			WHERE EXISTS (
				SELECT 1 FROM resolved_datasets rd2
				JOIN lineage_edges le ON le.dataset_urn = rd2.raw_urn
				WHERE rd2.canonical_urn = rd.canonical_urn AND le.edge_type = 'output'
			)
		)
		SELECT
			COALESCE(ftd.total_count, 0),
			COALESCE(cfd.correlated_count, 0),
			COALESCE(atd.total_datasets, 0),
			COALESCE(pd.produced_count, 0),
			COALESCE(cd.correlated_count, 0)
		FROM failed_tested_datasets ftd, correlated_failed_datasets cfd,
		     all_tested_datasets atd, produced_datasets pd, correlated_datasets cd
	`

	var stats healthStats

	err := s.conn.QueryRowContext(ctx, query).Scan(
		&stats.totalFailedTestedDatasets, &stats.correlatedFailedTestedDatasets, &stats.totalDatasets,
		&stats.producedDatasets, &stats.correlatedDatasets,
	)

	return &stats, err
}
