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
// When a pattern resolver is configured (via WithAliasResolver), this method
// applies pattern-based URN resolution to correlate test failures across
// different URN formats. Without a resolver, only exact URN matches are found.
//
// Parameters:
//   - filter: Optional filter (nil = no filtering, returns all incidents)
//   - pagination: Optional pagination (nil = returns all results)
//
// Returns:
//   - IncidentQueryResult with incidents and total count
//   - Error if query fails or context is cancelled
//
// Pattern Resolution:
//   - If resolver is configured, test result dataset_urns are resolved to canonical form
//   - Resolved URNs are used to find producing job runs
//   - Incidents are returned with canonical dataset URNs
//
// Performance:
//   - View-based query (no patterns): typically 10-50ms
//   - Pattern-resolved query: typically 50-200ms (additional lookups)
//   - Call RefreshViews() to update data
func (s *LineageStore) QueryIncidents(
	ctx context.Context,
	filter *correlation.IncidentFilter,
	pagination *correlation.Pagination,
) (*correlation.IncidentQueryResult, error) {
	// Use pattern-resolved query if resolver is configured with patterns
	if s.resolver != nil && s.resolver.GetPatternCount() > 0 {
		return s.queryIncidentsWithPatternResolution(ctx, filter, pagination)
	}

	// Fall back to view-based query (exact match only)
	return s.queryIncidentsFromView(ctx, filter, pagination)
}

// queryIncidentsFromView queries the incident_correlation_view for exact URN matches.
// This is the original implementation, used when no pattern resolver is configured.
func (s *LineageStore) queryIncidentsFromView(
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

// queryIncidentsWithPatternResolution queries incidents with pattern-based URN resolution.
// This enables correlation across different URN formats (e.g., GE → dbt).
//
// Algorithm (memory-efficient two-phase approach):
//  1. Query produced dataset URNs (for correlation filtering)
//  2. Query correlated test IDs (IDs only, not full data) - O(n) small integers
//  3. Apply pagination to ID list
//  4. Query full incident data only for paginated IDs - O(page_size) records
//
// This approach bounds memory usage regardless of total incident count.
func (s *LineageStore) queryIncidentsWithPatternResolution(
	ctx context.Context,
	filter *correlation.IncidentFilter,
	pagination *correlation.Pagination,
) (*correlation.IncidentQueryResult, error) {
	start := time.Now()

	// Step 1: Query produced URN set (needed for correlation filtering)
	producedURNSet, err := s.queryProducedURNSet(ctx)
	if err != nil {
		return nil, err
	}

	if len(producedURNSet) == 0 {
		s.logger.Info("No produced datasets found")

		return &correlation.IncidentQueryResult{Incidents: []correlation.Incident{}, Total: 0}, nil
	}

	// Step 2: Query correlated test IDs (lightweight - just IDs and URNs)
	correlatedTests, err := s.queryCorrelatedTestIDs(ctx, filter, producedURNSet)
	if err != nil {
		return nil, err
	}

	total := len(correlatedTests)
	if total == 0 {
		s.logger.Info("No correlated test results found")

		return &correlation.IncidentQueryResult{Incidents: []correlation.Incident{}, Total: 0}, nil
	}

	// Step 3: Apply pagination to test ID list
	paginatedTests := applyTestIDPagination(correlatedTests, pagination)
	if len(paginatedTests) == 0 {
		return &correlation.IncidentQueryResult{Incidents: []correlation.Incident{}, Total: total}, nil
	}

	// Step 4: Query full incident data only for paginated test IDs
	incidents, err := s.buildIncidentsForTests(ctx, paginatedTests)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	s.logger.Info("Queried incidents with pattern resolution",
		slog.Duration("duration", duration),
		slog.Int("result_count", len(incidents)),
		slog.Int("total", total),
		slog.Int("pattern_count", s.resolver.GetPatternCount()),
		slog.Bool("filtered", filter != nil),
		slog.Bool("paginated", pagination != nil))

	return &correlation.IncidentQueryResult{
		Incidents: incidents,
		Total:     total,
	}, nil
}

// correlatedTestInfo holds minimal info for a correlated test (used in phase 1).
type correlatedTestInfo struct {
	ID          int64
	ResolvedURN string
}

// queryCorrelatedTestIDs returns IDs of failed tests that correlate via pattern resolution.
// This is a lightweight query that only loads IDs and URNs, not full test data.
func (s *LineageStore) queryCorrelatedTestIDs(
	ctx context.Context,
	filter *correlation.IncidentFilter,
	producedURNSet map[string]bool,
) ([]correlatedTestInfo, error) {
	// Query failed test IDs and their dataset URNs
	query := `
		SELECT tr.id, tr.dataset_urn
		FROM test_results tr
		WHERE tr.status IN ('failed', 'error')
	`

	var args []any

	paramIndex := 1

	if filter != nil {
		if filter.TestExecutedAfter != nil {
			query += fmt.Sprintf(" AND tr.executed_at > $%d", paramIndex)
			paramIndex++

			args = append(args, *filter.TestExecutedAfter)
		}

		if filter.TestExecutedBefore != nil {
			query += fmt.Sprintf(" AND tr.executed_at < $%d", paramIndex)

			args = append(args, *filter.TestExecutedBefore)
		}
	}

	query += " ORDER BY tr.executed_at DESC"

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	// Filter to correlated tests (those whose resolved URN is in producedURNSet)
	var results []correlatedTestInfo

	for rows.Next() {
		var id int64

		var datasetURN string

		if err := rows.Scan(&id, &datasetURN); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		resolved := s.resolver.Resolve(datasetURN)
		if producedURNSet[resolved] {
			results = append(results, correlatedTestInfo{ID: id, ResolvedURN: resolved})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return results, nil
}

// applyTestIDPagination applies pagination to correlated test info list.
func applyTestIDPagination(
	tests []correlatedTestInfo,
	pagination *correlation.Pagination,
) []correlatedTestInfo {
	if pagination == nil {
		return tests
	}

	start := pagination.Offset
	end := pagination.Offset + pagination.Limit

	switch {
	case start > len(tests):
		return []correlatedTestInfo{}
	case end > len(tests):
		return tests[start:]
	default:
		return tests[start:end]
	}
}

// buildIncidentsForTests builds full Incident objects for specific test IDs.
func (s *LineageStore) buildIncidentsForTests(
	ctx context.Context,
	tests []correlatedTestInfo,
) ([]correlation.Incident, error) {
	if len(tests) == 0 {
		return []correlation.Incident{}, nil
	}

	// Collect test IDs and resolved URNs
	testIDs := make([]int64, len(tests))
	resolvedURNByID := make(map[int64]string, len(tests))

	uniqueResolvedURNs := make([]string, 0, len(tests))
	seenURNs := make(map[string]bool)

	for i, t := range tests {
		testIDs[i] = t.ID
		resolvedURNByID[t.ID] = t.ResolvedURN

		if !seenURNs[t.ResolvedURN] {
			uniqueResolvedURNs = append(uniqueResolvedURNs, t.ResolvedURN)
			seenURNs[t.ResolvedURN] = true
		}
	}

	// Query producer job info for resolved URNs
	producerInfo, err := s.queryProducerJobsForDatasets(ctx, uniqueResolvedURNs)
	if err != nil {
		return nil, err
	}

	// Query full test result data for the specific IDs
	testResults, err := s.queryTestResultsByIDs(ctx, testIDs)
	if err != nil {
		return nil, err
	}

	// Build incidents
	incidents := make([]correlation.Incident, 0, len(tests))

	for _, tr := range testResults {
		resolved := resolvedURNByID[tr.ID]

		producer, hasProducer := producerInfo[resolved]
		if !hasProducer {
			continue
		}

		incident := s.buildIncidentFromTestAndProducer(tr, resolved, producer)
		incidents = append(incidents, incident)
	}

	return incidents, nil
}

// queryTestResultsByIDs queries full test result data for specific IDs.
func (s *LineageStore) queryTestResultsByIDs(
	ctx context.Context,
	testIDs []int64,
) ([]failedTestResult, error) {
	query := `
		SELECT
			tr.id, tr.test_name, tr.test_type, tr.dataset_urn, tr.job_run_id,
			tr.status, tr.message, tr.executed_at, tr.duration_ms
		FROM test_results tr
		WHERE tr.id = ANY($1)
		ORDER BY tr.executed_at DESC
	`

	rows, err := s.conn.QueryContext(ctx, query, pq.Array(testIDs))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []failedTestResult

	for rows.Next() {
		var r failedTestResult

		if err := rows.Scan(
			&r.ID, &r.TestName, &r.TestType, &r.DatasetURN, &r.JobRunID,
			&r.Status, &r.Message, &r.ExecutedAt, &r.DurationMs,
		); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return results, nil
}

// buildIncidentFromTestAndProducer constructs an Incident from test result and producer info.
func (s *LineageStore) buildIncidentFromTestAndProducer(
	tr failedTestResult,
	resolvedURN string,
	producer producerJobInfo,
) correlation.Incident {
	incident := correlation.Incident{
		// Test result fields
		TestResultID:   tr.ID,
		TestName:       tr.TestName,
		TestType:       tr.TestType,
		TestStatus:     tr.Status,
		TestMessage:    tr.Message.String,
		TestExecutedAt: tr.ExecutedAt,
		TestDurationMs: tr.DurationMs.Int64,
		// Dataset fields (use canonical/resolved URN)
		DatasetURN:  resolvedURN,
		DatasetName: producer.DatasetName,
		DatasetNS:   producer.DatasetNamespace,
		// Job run fields
		JobRunID:         producer.JobRunID,
		OpenLineageRunID: producer.RunID,
		JobName:          producer.JobName,
		JobNamespace:     producer.JobNamespace,
		JobStatus:        producer.JobStatus,
		JobEventType:     producer.EventType,
		JobStartedAt:     producer.StartedAt.Time,
		ProducerName:     producer.ProducerName,
		// Lineage edge fields
		LineageEdgeID:    producer.EdgeID,
		LineageEdgeType:  producer.EdgeType,
		LineageCreatedAt: producer.EdgeCreatedAt,
	}

	// Set nullable fields
	if producer.CompletedAt.Valid {
		incident.JobCompletedAt = &producer.CompletedAt.Time
	}

	if producer.ProducerVersion.Valid {
		incident.ProducerVersion = &producer.ProducerVersion.String
	}

	return incident
}

// failedTestResult holds a failed/error test result for pattern resolution.
type failedTestResult struct {
	ID         int64
	TestName   string
	TestType   string
	DatasetURN string
	JobRunID   string
	Status     string
	Message    sql.NullString
	ExecutedAt time.Time
	DurationMs sql.NullInt64
}

// queryFailedTestResults queries all test results with failed/error status.
func (s *LineageStore) queryFailedTestResults(
	ctx context.Context,
	filter *correlation.IncidentFilter,
) ([]failedTestResult, error) {
	query := `
		SELECT
			tr.id, tr.test_name, tr.test_type, tr.dataset_urn, tr.job_run_id,
			tr.status, tr.message, tr.executed_at, tr.duration_ms
		FROM test_results tr
		WHERE tr.status IN ('failed', 'error')
	`

	var args []any

	paramIndex := 1

	// Apply time filters
	if filter != nil {
		if filter.TestExecutedAfter != nil {
			query += fmt.Sprintf(" AND tr.executed_at > $%d", paramIndex)
			paramIndex++

			args = append(args, *filter.TestExecutedAfter)
		}

		if filter.TestExecutedBefore != nil {
			query += fmt.Sprintf(" AND tr.executed_at < $%d", paramIndex)

			args = append(args, *filter.TestExecutedBefore)
		}
	}

	query += " ORDER BY tr.executed_at DESC"

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		s.logger.Error("Failed to query failed test results", slog.Any("error", err))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	var results []failedTestResult

	for rows.Next() {
		var r failedTestResult

		if err := rows.Scan(
			&r.ID, &r.TestName, &r.TestType, &r.DatasetURN, &r.JobRunID,
			&r.Status, &r.Message, &r.ExecutedAt, &r.DurationMs,
		); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return results, nil
}

// producerJobInfo holds information about a job that produces a dataset.
type producerJobInfo struct {
	JobRunID         string
	RunID            string
	JobName          string
	JobNamespace     string
	JobStatus        string
	EventType        string
	StartedAt        sql.NullTime
	CompletedAt      sql.NullTime
	ProducerName     string
	ProducerVersion  sql.NullString
	DatasetName      string
	DatasetNamespace string
	EdgeID           int64
	EdgeType         string
	EdgeCreatedAt    time.Time
}

// queryProducerJobsForDatasets queries producer job info for a list of dataset URNs.
// Returns a map of dataset_urn → producer info.
func (s *LineageStore) queryProducerJobsForDatasets(
	ctx context.Context,
	datasetURNs []string,
) (map[string]producerJobInfo, error) {
	if len(datasetURNs) == 0 {
		return map[string]producerJobInfo{}, nil
	}

	query := `
		SELECT
			le.dataset_urn,
			jr.job_run_id, jr.run_id, jr.job_name, jr.job_namespace,
			jr.current_state, jr.event_type, jr.started_at, jr.completed_at,
			jr.producer_name, jr.producer_version,
			d.name, d.namespace,
			le.id, le.edge_type, le.created_at
		FROM lineage_edges le
		JOIN job_runs jr ON jr.job_run_id = le.job_run_id
		JOIN datasets d ON d.dataset_urn = le.dataset_urn
		WHERE le.edge_type = 'output'
		  AND le.dataset_urn = ANY($1)
	`

	rows, err := s.conn.QueryContext(ctx, query, pq.Array(datasetURNs))
	if err != nil {
		s.logger.Error("Failed to query producer jobs", slog.Any("error", err))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	results := make(map[string]producerJobInfo)

	for rows.Next() {
		var datasetURN string

		var info producerJobInfo

		if err := rows.Scan(
			&datasetURN,
			&info.JobRunID, &info.RunID, &info.JobName, &info.JobNamespace,
			&info.JobStatus, &info.EventType, &info.StartedAt, &info.CompletedAt,
			&info.ProducerName, &info.ProducerVersion,
			&info.DatasetName, &info.DatasetNamespace,
			&info.EdgeID, &info.EdgeType, &info.EdgeCreatedAt,
		); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		// First match wins (consistent with table name matching behavior)
		if _, exists := results[datasetURN]; !exists {
			results[datasetURN] = info
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return results, nil
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

// QueryOrphanDatasets implements correlation.Store.
// Returns datasets that have test results but no corresponding data producer output edges.
//
// When a pattern resolver is configured (via WithAliasResolver), datasets that resolve
// to produced datasets via patterns are NOT considered orphans. This reflects the
// effective correlation state after pattern resolution is applied.
//
// Orphan Detection Logic:
//   - Orphan = Dataset with test results where:
//     a) Dataset URN is NOT in produced datasets, AND
//     b) Resolved URN (via patterns) is also NOT in produced datasets
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
//   - Queries test_results and lineage_edges tables
//   - Table name extraction done in Go (not SQL) for flexibility
//   - Pattern resolution adds minimal overhead (in-memory regex)
//   - Typical query time: 20-100ms depending on data volume
func (s *LineageStore) QueryOrphanDatasets(ctx context.Context) ([]correlation.OrphanDataset, error) {
	start := time.Now()

	// Query produced datasets (needed for orphan detection and matching)
	producedByTableName, err := s.queryProducedDatasetsByTableName(ctx)
	if err != nil {
		return nil, err
	}

	// Build URN set for pattern resolution
	producedURNSet := make(map[string]bool)
	for _, urn := range producedByTableName {
		producedURNSet[urn] = true
	}

	// Delegate to internal function
	orphans, err := s.queryOrphanDatasetsWithProduced(ctx, producedByTableName, producedURNSet)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	s.logger.Info("Queried orphan datasets",
		slog.Duration("duration", duration),
		slog.Int("orphan_count", len(orphans)),
		slog.Int("produced_count", len(producedByTableName)))

	return orphans, nil
}

// queryTestedDatasetsWithoutProducer queries datasets with test results but no output edges.
func (s *LineageStore) queryTestedDatasetsWithoutProducer(ctx context.Context) ([]correlation.OrphanDataset, error) {
	query := `
		WITH produced_datasets AS (
			SELECT DISTINCT dataset_urn
			FROM lineage_edges
			WHERE edge_type = 'output'
		),
		tested_datasets AS (
			SELECT
				dataset_urn,
				COUNT(*) AS test_count,
				MAX(executed_at) AS last_seen
			FROM test_results
			GROUP BY dataset_urn
		)
		SELECT td.dataset_urn, td.test_count, td.last_seen
		FROM tested_datasets td
		WHERE td.dataset_urn NOT IN (SELECT dataset_urn FROM produced_datasets)
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

// queryProducedURNSet returns a set of all produced dataset URNs.
// Used for efficient correlation filtering in pattern resolution.
func (s *LineageStore) queryProducedURNSet(ctx context.Context) (map[string]bool, error) {
	query := `
		SELECT DISTINCT dataset_urn
		FROM lineage_edges
		WHERE edge_type = 'output'
		ORDER BY dataset_urn
	`

	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		s.logger.Error("Failed to query produced URN set", slog.Any("error", err))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	result := make(map[string]bool)

	for rows.Next() {
		var urn string

		if err := rows.Scan(&urn); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		result[urn] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return result, nil
}

// queryProducedDatasetsByTableName queries produced datasets and indexes them by extracted table name.
// Results are ordered for deterministic first-match-wins behavior.
func (s *LineageStore) queryProducedDatasetsByTableName(ctx context.Context) (map[string]string, error) {
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
// When a pattern resolver is configured, the correlation rate reflects
// pattern-resolved correlations (incidents that correlate via patterns are counted).
//
// Correlation Rate Calculation:
//   - Numerator: Incidents where dataset resolves (via pattern or exact) to producer output
//   - Denominator: All failed test results
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

	// Query produced datasets ONCE (used by both orphan detection and correlation rate)
	producedByTableName, err := s.queryProducedDatasetsByTableName(ctx)
	if err != nil {
		return nil, err
	}

	// Build URN set for pattern resolution
	producedURNSet := make(map[string]bool)
	for _, urn := range producedByTableName {
		producedURNSet[urn] = true
	}

	// Query orphan datasets (pass pre-queried data to avoid duplicate query)
	orphans, err := s.queryOrphanDatasetsWithProduced(ctx, producedByTableName, producedURNSet)
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

	// Calculate correlation rate (pass pre-queried data to avoid duplicate query)
	correlationRate := s.calculateCorrelationRateWithProduced(ctx, stats, producedURNSet)

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

// queryOrphanDatasetsWithProduced is an internal version that accepts pre-queried produced data.
func (s *LineageStore) queryOrphanDatasetsWithProduced(
	ctx context.Context,
	producedByTableName map[string]string,
	producedURNSet map[string]bool,
) ([]correlation.OrphanDataset, error) {
	// Query orphan datasets (datasets with test results but no output edges)
	orphans, err := s.queryTestedDatasetsWithoutProducer(ctx)
	if err != nil {
		return nil, err
	}

	if len(orphans) == 0 {
		return orphans, nil
	}

	// Filter orphans that resolve to produced datasets via patterns
	filteredOrphans := make([]correlation.OrphanDataset, 0, len(orphans))

	for _, o := range orphans {
		// Check if this orphan resolves to a produced dataset via patterns
		if s.resolver != nil && s.resolver.GetPatternCount() > 0 {
			resolved := s.resolver.Resolve(o.DatasetURN)
			if producedURNSet[resolved] {
				continue // Resolved via pattern - not an orphan
			}
		}

		// Try to find likely match by table name
		orphanTableName := canonicalization.ExtractTableName(o.DatasetURN)
		if orphanTableName != "" {
			if producedURN, found := producedByTableName[orphanTableName]; found {
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

// calculateCorrelationRateWithProduced calculates rate using pre-queried produced data.
func (s *LineageStore) calculateCorrelationRateWithProduced(
	ctx context.Context,
	stats *healthStats,
	producedURNSet map[string]bool,
) float64 {
	// Use pattern-resolved rate if patterns are configured
	if s.resolver != nil && s.resolver.GetPatternCount() > 0 {
		rate, err := s.calculatePatternResolvedRateWithProduced(ctx, stats.totalIncidents, producedURNSet)
		if err != nil {
			s.logger.Error("Failed to calculate pattern-resolved correlation rate",
				slog.Any("error", err))

			return s.calculateDatabaseCorrelationRate(stats)
		}

		return rate
	}

	return s.calculateDatabaseCorrelationRate(stats)
}

// calculateDatabaseCorrelationRate calculates correlation rate from database stats.
func (s *LineageStore) calculateDatabaseCorrelationRate(stats *healthStats) float64 {
	if stats.totalIncidents > 0 {
		return float64(stats.correlatedIncidents) / float64(stats.totalIncidents)
	}

	return 1.0 // No incidents = healthy
}

// calculatePatternResolvedRateWithProduced calculates pattern-resolved rate using pre-queried data.
func (s *LineageStore) calculatePatternResolvedRateWithProduced(
	ctx context.Context,
	totalIncidents int,
	producedURNSet map[string]bool,
) (float64, error) {
	if totalIncidents == 0 {
		return 1.0, nil
	}

	// Get all failed test results
	failedTests, err := s.queryFailedTestResults(ctx, nil)
	if err != nil {
		return 0, err
	}

	if len(failedTests) == 0 {
		return 1.0, nil
	}

	// Count correlated incidents
	correlated := 0

	for _, tr := range failedTests {
		resolved := s.resolver.Resolve(tr.DatasetURN)
		if producedURNSet[resolved] {
			correlated++
		}
	}

	return float64(correlated) / float64(len(failedTests)), nil
}

// healthStats holds correlation health statistics.
type healthStats struct {
	totalIncidents      int
	correlatedIncidents int
	totalDatasets       int
	producedDatasets    int
	correlatedDatasets  int
}

// queryHealthStats queries database for health statistics.
func (s *LineageStore) queryHealthStats(ctx context.Context) (*healthStats, error) {
	query := `
		WITH all_failed_tests AS (
			SELECT COUNT(*) AS total_incidents
			FROM test_results
			WHERE status IN ('failed', 'error')
		),
		correlated_incidents AS (
			SELECT COUNT(*) AS correlated_count
			FROM incident_correlation_view
		),
		tested_datasets AS (
			SELECT COUNT(DISTINCT dataset_urn) AS total_datasets
			FROM test_results
		),
		produced_datasets AS (
			SELECT COUNT(DISTINCT dataset_urn) AS produced_count
			FROM lineage_edges
			WHERE edge_type = 'output'
		),
		correlated_datasets AS (
			SELECT COUNT(DISTINCT tr.dataset_urn) AS correlated_count
			FROM test_results tr
			WHERE EXISTS (
				SELECT 1 FROM lineage_edges le
				WHERE le.dataset_urn = tr.dataset_urn AND le.edge_type = 'output'
			)
		)
		SELECT
			COALESCE(a.total_incidents, 0),
			COALESCE(c.correlated_count, 0),
			COALESCE(t.total_datasets, 0),
			COALESCE(p.produced_count, 0),
			COALESCE(cd.correlated_count, 0)
		FROM all_failed_tests a, correlated_incidents c, tested_datasets t, produced_datasets p, correlated_datasets cd
	`

	var stats healthStats

	err := s.conn.QueryRowContext(ctx, query).Scan(
		&stats.totalIncidents, &stats.correlatedIncidents, &stats.totalDatasets,
		&stats.producedDatasets, &stats.correlatedDatasets,
	)

	return &stats, err
}
