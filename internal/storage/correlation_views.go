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
//  1. Get produced dataset URNs (for correlation filtering)
//  2. Find correlated failed tests (IDs only, not full data) - O(n) small integers
//  3. Paginate the correlated tests
//  4. Build full incidents only for paginated tests - O(page_size) records
//
// This approach bounds memory usage regardless of total incident count.
func (s *LineageStore) queryIncidentsWithPatternResolution(
	ctx context.Context,
	filter *correlation.IncidentFilter,
	pagination *correlation.Pagination,
) (*correlation.IncidentQueryResult, error) {
	start := time.Now()

	producedURNs, err := s.getProducedDatasetURNs(ctx)
	if err != nil {
		return nil, err
	}

	if len(producedURNs) == 0 {
		s.logger.Info("No produced datasets found")

		return &correlation.IncidentQueryResult{Incidents: []correlation.Incident{}, Total: 0}, nil
	}

	correlatedTests, err := s.findCorrelatedFailedTestsWithPatternResolution(ctx, filter, producedURNs)
	if err != nil {
		return nil, err
	}

	totalCount := len(correlatedTests)
	if totalCount == 0 {
		s.logger.Info("No correlated test results found")

		return &correlation.IncidentQueryResult{Incidents: []correlation.Incident{}, Total: 0}, nil
	}

	pageOfTests := paginateCorrelatedTests(correlatedTests, pagination)
	if len(pageOfTests) == 0 {
		return &correlation.IncidentQueryResult{Incidents: []correlation.Incident{}, Total: totalCount}, nil
	}

	incidents, err := s.buildIncidentsFromCorrelatedTests(ctx, pageOfTests)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	s.logger.Info("Queried incidents with pattern resolution",
		slog.Duration("duration", duration),
		slog.Int("result_count", len(incidents)),
		slog.Int("total", totalCount),
		slog.Int("pattern_count", s.resolver.GetPatternCount()),
		slog.Bool("filtered", filter != nil),
		slog.Bool("paginated", pagination != nil))

	return &correlation.IncidentQueryResult{
		Incidents: incidents,
		Total:     totalCount,
	}, nil
}

// findCorrelatedFailedTestsWithPatternResolution finds failed tests that correlate to produced datasets.
// Applies pattern resolution to each test's dataset URN before checking against produced URNs.
func (s *LineageStore) findCorrelatedFailedTestsWithPatternResolution(
	ctx context.Context,
	filter *correlation.IncidentFilter,
	producedURNs map[string]bool,
) ([]correlatedTest, error) {
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

	var correlated []correlatedTest

	for rows.Next() {
		var testID int64

		var datasetURN string

		if err := rows.Scan(&testID, &datasetURN); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		resolvedURN := s.resolver.Resolve(datasetURN)
		if producedURNs[resolvedURN] {
			correlated = append(correlated, correlatedTest{TestID: testID, ResolvedURN: resolvedURN})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return correlated, nil
}

// paginateCorrelatedTests returns a slice of correlated tests for the requested page.
func paginateCorrelatedTests(
	tests []correlatedTest,
	pagination *correlation.Pagination,
) []correlatedTest {
	if pagination == nil {
		return tests
	}

	start := pagination.Offset
	end := pagination.Offset + pagination.Limit

	switch {
	case start > len(tests):
		return []correlatedTest{}
	case end > len(tests):
		return tests[start:]
	default:
		return tests[start:end]
	}
}

// buildIncidentsFromCorrelatedTests builds full Incident objects from correlated test info.
// Fetches test results and producer job details, then assembles complete incidents.
func (s *LineageStore) buildIncidentsFromCorrelatedTests(
	ctx context.Context,
	correlatedTests []correlatedTest,
) ([]correlation.Incident, error) {
	if len(correlatedTests) == 0 {
		return []correlation.Incident{}, nil
	}

	// Extract test IDs and build resolved URN lookup
	testIDs := make([]int64, len(correlatedTests))
	resolvedURNByTestID := make(map[int64]string, len(correlatedTests))

	uniqueResolvedURNs := make([]string, 0, len(correlatedTests))
	seenURNs := make(map[string]bool)

	for i, ct := range correlatedTests {
		testIDs[i] = ct.TestID
		resolvedURNByTestID[ct.TestID] = ct.ResolvedURN

		if !seenURNs[ct.ResolvedURN] {
			uniqueResolvedURNs = append(uniqueResolvedURNs, ct.ResolvedURN)
			seenURNs[ct.ResolvedURN] = true
		}
	}

	producersByURN, err := s.getProducerJobsByDatasetURN(ctx, uniqueResolvedURNs)
	if err != nil {
		return nil, err
	}

	testResults, err := s.getTestResultsByIDs(ctx, testIDs)
	if err != nil {
		return nil, err
	}

	// Assemble incidents
	incidents := make([]correlation.Incident, 0, len(correlatedTests))

	for _, testResult := range testResults {
		resolvedURN := resolvedURNByTestID[testResult.ID]

		producer, found := producersByURN[resolvedURN]
		if !found {
			continue
		}

		incident := s.assembleIncident(testResult, resolvedURN, producer)
		incidents = append(incidents, incident)
	}

	return incidents, nil
}

// getTestResultsByIDs queries full test result data for specific IDs.
func (s *LineageStore) getTestResultsByIDs(
	ctx context.Context,
	testIDs []int64,
) ([]failedTestResult, error) {
	query := `
		SELECT
			tr.id, tr.test_name, tr.test_type, tr.dataset_urn, tr.job_run_id,
			tr.status, tr.message, tr.executed_at, tr.duration_ms, tr.producer_name
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
			&r.Status, &r.Message, &r.ExecutedAt, &r.DurationMs, &r.ProducerName,
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

// assembleIncident constructs an Incident from a test result and its producer job info.
func (s *LineageStore) assembleIncident(
	testResult failedTestResult,
	resolvedURN string,
	producer producerJobInfo,
) correlation.Incident {
	incident := correlation.Incident{
		// Test result fields
		TestResultID:     testResult.ID,
		TestName:         testResult.TestName,
		TestType:         testResult.TestType,
		TestStatus:       testResult.Status,
		TestMessage:      testResult.Message.String,
		TestExecutedAt:   testResult.ExecutedAt,
		TestDurationMs:   testResult.DurationMs.Int64,
		TestProducerName: testResult.ProducerName,
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
	ID           int64
	TestName     string
	TestType     string
	DatasetURN   string
	JobRunID     string
	Status       string
	Message      sql.NullString
	ExecutedAt   time.Time
	DurationMs   sql.NullInt64
	ProducerName string
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

// getProducerJobsByDatasetURN fetches producer job info for a list of dataset URNs.
// Returns a map of datasetURN → producerJobInfo for efficient lookup.
func (s *LineageStore) getProducerJobsByDatasetURN(
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

	producersByURN := make(map[string]producerJobInfo)

	for rows.Next() {
		var datasetURN string

		var producer producerJobInfo

		if err := rows.Scan(
			&datasetURN,
			&producer.JobRunID, &producer.RunID, &producer.JobName, &producer.JobNamespace,
			&producer.JobStatus, &producer.EventType, &producer.StartedAt, &producer.CompletedAt,
			&producer.ProducerName, &producer.ProducerVersion,
			&producer.DatasetName, &producer.DatasetNamespace,
			&producer.EdgeID, &producer.EdgeType, &producer.EdgeCreatedAt,
		); err != nil {
			return nil, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		// First match wins (deterministic behavior)
		if _, exists := producersByURN[datasetURN]; !exists {
			producersByURN[datasetURN] = producer
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return producersByURN, nil
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
// Queries a single incident by test_result_id.
//
// When a pattern resolver is configured (via WithAliasResolver), this method
// applies pattern-based URN resolution to correlate test failures across
// different URN formats. Without a resolver, only exact URN matches are found.
//
// Parameters:
//   - testResultID: Test result ID (primary key)
//
// Returns:
//   - Pointer to Incident (nil if not found, no error)
//   - Error if query fails or context is cancelled
func (s *LineageStore) QueryIncidentByID(ctx context.Context, testResultID int64) (*correlation.Incident, error) {
	if s.resolver != nil && s.resolver.GetPatternCount() > 0 {
		return s.queryIncidentByIDWithPatternResolution(ctx, testResultID)
	}

	// Fall back to view-based query (exact match only)
	return s.queryIncidentByIDFromView(ctx, testResultID)
}

// queryIncidentByIDFromView queries a single incident from the incident_correlation_view.
// This is the original implementation, used when no pattern resolver is configured.
func (s *LineageStore) queryIncidentByIDFromView(
	ctx context.Context,
	testResultID int64,
) (*correlation.Incident, error) {
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

	s.logger.Info("Queried incident by ID from view",
		slog.Duration("duration", time.Since(start)),
		slog.Int64("id", testResultID))

	return &r, nil
}

// queryIncidentByIDWithPatternResolution queries a single incident with pattern-based URN resolution.
// This enables correlation across different URN formats (e.g., GE → dbt).
//
// Algorithm:
//  1. Get test result by ID
//  2. Check if status is failed/error (only incidents are correlatable)
//  3. Apply pattern resolution to dataset URN
//  4. Look up producer job using resolved URN
//  5. Assemble incident with test data + producer data
func (s *LineageStore) queryIncidentByIDWithPatternResolution(
	ctx context.Context,
	testResultID int64,
) (*correlation.Incident, error) {
	start := time.Now()

	testResult, err := s.getTestResultByID(ctx, testResultID)
	if err != nil {
		return nil, err
	}

	if testResult == nil {
		s.logger.Info("Test result not found",
			slog.Duration("duration", time.Since(start)),
			slog.Int64("id", testResultID))

		return nil, nil //nolint:nilnil // Not found returns nil incident, not an error
	}

	if testResult.Status != statusFailed && testResult.Status != "error" {
		s.logger.Info("Test result is not failed/error, not an incident",
			slog.Duration("duration", time.Since(start)),
			slog.Int64("id", testResultID),
			slog.String("status", testResult.Status))

		return nil, nil //nolint:nilnil // Not an incident
	}

	resolvedURN := s.resolver.Resolve(testResult.DatasetURN)

	producersByURN, err := s.getProducerJobsByDatasetURN(ctx, []string{resolvedURN})
	if err != nil {
		return nil, err
	}

	producer, found := producersByURN[resolvedURN]
	if !found {
		s.logger.Info("No producer found for resolved URN",
			slog.Duration("duration", time.Since(start)),
			slog.Int64("id", testResultID),
			slog.String("original_urn", testResult.DatasetURN),
			slog.String("resolved_urn", resolvedURN))

		return nil, nil //nolint:nilnil // No producer = not correlatable
	}

	incident := s.assembleIncident(*testResult, resolvedURN, producer)

	s.logger.Info("Queried incident by ID with pattern resolution",
		slog.Duration("duration", time.Since(start)),
		slog.Int64("id", testResultID),
		slog.String("original_urn", testResult.DatasetURN),
		slog.String("resolved_urn", resolvedURN),
		slog.String("producer", producer.ProducerName))

	return &incident, nil
}

// getTestResultByID queries a single test result by ID.
// Returns nil if not found (not an error).
func (s *LineageStore) getTestResultByID(ctx context.Context, testResultID int64) (*failedTestResult, error) {
	query := `
		SELECT
			tr.id, tr.test_name, tr.test_type, tr.dataset_urn, tr.job_run_id,
			tr.status, tr.message, tr.executed_at, tr.duration_ms, tr.producer_name
		FROM test_results tr
		WHERE tr.id = $1
	`

	row := s.conn.QueryRowContext(ctx, query, testResultID)

	var r failedTestResult

	err := row.Scan(
		&r.ID, &r.TestName, &r.TestType, &r.DatasetURN, &r.JobRunID,
		&r.Status, &r.Message, &r.ExecutedAt, &r.DurationMs, &r.ProducerName,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil //nolint:nilnil // Not found returns nil, not an error
		}

		s.logger.Error("Failed to query test result by ID",
			slog.Any("error", err),
			slog.Int64("id", testResultID))

		return nil, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

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
				le.dataset_urn AS parent_urn,
				COALESCE(jr.producer_name, '') AS producer
			FROM lineage_edges le
				JOIN datasets d ON le.dataset_urn = d.dataset_urn
				LEFT JOIN job_runs jr ON le.job_run_id = jr.job_run_id
			WHERE le.job_run_id = $1
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
				JOIN lineage_edges le_out ON le_in.job_run_id = le_out.job_run_id
					AND le_out.edge_type = 'output'
				JOIN datasets d ON le_out.dataset_urn = d.dataset_urn
				LEFT JOIN job_runs jr ON le_out.job_run_id = jr.job_run_id
			WHERE dt.depth < $2
			  -- Prevent self-loops
			  AND le_out.dataset_urn != dt.dataset_urn
		)
		SELECT DISTINCT dataset_urn, dataset_name, depth, parent_urn, producer
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
		slog.String("job_run_id", jobRunID),
		slog.Int("max_depth", maxDepth),
		slog.Int("result_count", len(results)))

	return results, nil
}

// QueryUpstreamWithChildren implements correlation.Store.
// Queries upstream datasets with child URN relationships for tree visualization.
//
// This is the inverse of QueryDownstreamWithParents:
//   - Downstream: follows output→input→output chain forward (consumers)
//   - Upstream: follows input→output→input chain backward (producers)
//
// Parameters:
//   - datasetURN: The root dataset URN (childURN for depth=1 results)
//   - jobRunID: Job run ID that produced the root dataset
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
	jobRunID string,
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
				LEFT JOIN job_runs jr ON le_prod.job_run_id = jr.job_run_id
			WHERE le.job_run_id = $2
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
				JOIN lineage_edges le_in ON le_out.job_run_id = le_in.job_run_id
					AND le_in.edge_type = 'input'
				JOIN datasets d ON le_in.dataset_urn = d.dataset_urn
				LEFT JOIN lineage_edges le_prod ON le_in.dataset_urn = le_prod.dataset_urn
					AND le_prod.edge_type = 'output'
				LEFT JOIN job_runs jr ON le_prod.job_run_id = jr.job_run_id
			WHERE ut.depth < $3
			  AND le_in.dataset_urn != ut.dataset_urn
		)
		SELECT DISTINCT dataset_urn, dataset_name, depth, child_urn, producer
		FROM upstream_tree
		ORDER BY depth, dataset_urn
	`

	rows, err := s.conn.QueryContext(ctx, query, datasetURN, jobRunID, maxDepth)
	if err != nil {
		s.logger.Error("Failed to query upstream with children",
			slog.Any("error", err),
			slog.String("dataset_urn", datasetURN),
			slog.String("job_run_id", jobRunID),
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

	orphans, _, err := s.findTrueOrphans(ctx)
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

// getProducedDatasetURNs returns a set of all produced dataset URNs.
// Used for efficient correlation filtering in pattern resolution.
func (s *LineageStore) getProducedDatasetURNs(ctx context.Context) (map[string]bool, error) {
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

	// Query orphan datasets and "known" producers
	orphans, knownProducedURNs, err := s.findTrueOrphans(ctx)
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

	correlationRate := s.calculateCorrelationRate(ctx, stats, knownProducedURNs)

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

// findTrueOrphans filters down to "real" orphans after pattern resolution.
func (s *LineageStore) findTrueOrphans(ctx context.Context) ([]correlation.OrphanDataset, map[string]bool, error) {
	tableNameToProducedURNIndex, err := s.buildTableNameToProducedURNIndex(ctx)
	if err != nil {
		return nil, nil, err
	}

	// these are the "known" producers
	knownProducedURNs := make(map[string]bool)
	for _, urn := range tableNameToProducedURNIndex {
		knownProducedURNs[urn] = true
	}

	orphans, err := s.findUnresolvedOrphanDatasets(ctx, tableNameToProducedURNIndex, knownProducedURNs)

	return orphans, knownProducedURNs, err
}

// findUnresolvedOrphanDatasets returns true orphan datasets.
// It filters out pattern-resolved matches and enriching with likely matches.
func (s *LineageStore) findUnresolvedOrphanDatasets(
	ctx context.Context,
	tableNameToProducedURNIndex map[string]string,
	knownProducedURNs map[string]bool,
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
			if knownProducedURNs[resolved] {
				continue // Resolved via pattern - not an orphan
			}
		}

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
// This is the main entry point for correlation rate calculation. It selects the
// appropriate strategy based on whether a pattern resolver is configured:
//   - With patterns: Uses calculateCorrelationRateWithPatternResolution (query-time URN resolution)
//   - Without patterns: Uses calculateCorrelationRateFromHealthStats (pre-computed database stats)
//
// Formula: correlated_failed_datasets / total_failed_datasets
//
// Parameters:
//   - ctx: Context for cancellation
//   - stats: Pre-queried health statistics from queryHealthStats()
//   - knownProducedURNs: Set of test dataset URNs with producer output edges
//
// Returns:
//   - Correlation rate between 0.0 and 1.0 (1.0 = all failed test datasets can be traced to a producer)
func (s *LineageStore) calculateCorrelationRate(
	ctx context.Context,
	stats *healthStats,
	knownProducedURNs map[string]bool,
) float64 {
	if s.resolver != nil && s.resolver.GetPatternCount() > 0 {
		rate, err := s.calculateCorrelationRateWithPatternResolution(ctx, stats.totalFailedTestedDatasets, knownProducedURNs)
		if err != nil {
			s.logger.Error("Failed to calculate pattern-resolved correlation rate",
				slog.Any("error", err))

			return s.calculateCorrelationRateFromHealthStats(stats)
		}

		return rate
	}

	return s.calculateCorrelationRateFromHealthStats(stats)
}

// calculateCorrelationRateFromHealthStats computes correlation rate using pre-computed database statistics.
//
// This is the fallback strategy when no pattern resolver is configured. It uses counts
// already computed by queryHealthStats(), avoiding additional database queries.
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

// calculateCorrelationRateWithPatternResolution computes correlation rate using query-time URN resolution.
//
// This strategy is used when a pattern resolver is configured. It queries all distinct
// dataset URNs with failed tests, applies pattern resolution to each URN, and checks
// if the resolved URN exists in the set of known produced datasets.
//
// This approach enables correlation across different URN formats (e.g., GE's "demo_postgres/customers"
// resolving to dbt's "postgresql://demo/marts.customers" via configured patterns).
//
// Formula: correlated_failed_datasets / total_failed_datasets
// Where correlated = resolved URN exists in knownProducedURNs
//
// Parameters:
//   - ctx: Context for cancellation
//   - totalFailedTestedDatasets: Count of distinct datasets with failed tests (denominator)
//   - knownProducedURNs: Set of dataset URNs with producer output edges
//
// Returns:
//   - Correlation rate between 0.0 and 1.0
//   - Returns 1.0 if no failed tests exist (healthy state)
//   - Error if database query fails
func (s *LineageStore) calculateCorrelationRateWithPatternResolution(
	ctx context.Context,
	totalTestedDatasets int,
	producedURNSet map[string]bool,
) (float64, error) {
	if totalTestedDatasets == 0 {
		return 1.0, nil
	}

	// Query distinct dataset URNs with failed tests
	query := `
		SELECT DISTINCT dataset_urn
		FROM test_results
		WHERE status IN ('failed', 'error')
	`

	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrCorrelationQueryFailed, err)
	}

	defer func() {
		_ = rows.Close()
	}()

	// Count distinct datasets that resolve to produced datasets
	correlatedCount := 0

	for rows.Next() {
		var datasetURN string

		if err := rows.Scan(&datasetURN); err != nil {
			return 0, fmt.Errorf("%w: failed to scan row: %w", ErrCorrelationQueryFailed, err)
		}

		resolved := s.resolver.Resolve(datasetURN)
		if producedURNSet[resolved] {
			correlatedCount++
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("%w: row iteration error: %w", ErrCorrelationQueryFailed, err)
	}

	return float64(correlatedCount) / float64(totalTestedDatasets), nil
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
			-- Distinct datasets with failed tests AND producer output edges (numerator)
			SELECT COUNT(DISTINCT tr.dataset_urn) AS correlated_count
			FROM test_results tr
			WHERE tr.status IN ('failed', 'error')
			AND EXISTS (
				SELECT 1 FROM lineage_edges le
				WHERE le.dataset_urn = tr.dataset_urn AND le.edge_type = 'output'
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
			-- Distinct datasets with both tests (any status) AND output edges
			SELECT COUNT(DISTINCT tr.dataset_urn) AS correlated_count
			FROM test_results tr
			WHERE EXISTS (
				SELECT 1 FROM lineage_edges le
				WHERE le.dataset_urn = tr.dataset_urn AND le.edge_type = 'output'
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
