// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import "context"

// Store defines the read interface for correlation queries.
//
// This interface is intentionally separate from ingestion.Store to follow
// Interface Segregation Principle - clients only depend on methods they need.
//
// Design rationale:
//   - ingestion.Store: Write-only interface (StoreEvent)
//   - correlation.Store: Read-only interface (query methods)
//   - storage.LineageStore: Implements BOTH interfaces
//
// This separation enables:
//   - UI API handlers to depend only on correlation.Store (read-only)
//   - Ingestion handlers to depend only on ingestion.Store (write-only)
//   - Future CQRS pattern (separate read/write stores) without breaking clients
//
// Implemented by: storage.LineageStore.
type Store interface {
	// RefreshViews refreshes all correlation materialized views in dependency order.
	//
	// This method calls the PostgreSQL function refresh_correlation_views() which:
	//   - Refreshes incident_correlation_view (failed/error tests correlated to job runs)
	//   - Refreshes lineage_impact_analysis (recursive downstream impact)
	//   - Refreshes recent_incidents_summary (7-day rolling window aggregation)
	//   - Uses CONCURRENTLY for zero-downtime updates (~650ms-2s, no locks)
	//
	// Should be called:
	//   - After bulk data ingestion
	//   - On schedule (e.g., every 5 minutes)
	//   - Before serving correlation queries (if data freshness critical)
	//
	// Returns error if refresh fails or context is cancelled.
	RefreshViews(ctx context.Context) error

	// QueryIncidents queries the incident_correlation_view with optional filters and pagination.
	//
	// This view correlates test failures to the job runs that produced the failing datasets enabling 2-click navigation
	// to root cause. The view already filters to only failed/error tests at the database level.
	//
	// Parameters:
	//   - filter: Optional filter (nil = no filtering, returns all incidents)
	//   - pagination: Optional pagination (nil = returns all results, no limit)
	//
	// Returns:
	//   - IncidentQueryResult containing incidents and total count
	//   - Error if query fails or context is cancelled
	//
	// Performance:
	//   - View is pre-materialized (fast queries, ~10-50ms typical)
	//   - Uses COUNT(*) OVER() window function for efficient pagination
	//   - Uses indexes: incident_correlation_view_pk, idx_incident_correlation_view_job_run_id
	//   - Refresh latency: Call RefreshViews() to update data.
	QueryIncidents(ctx context.Context, filter *IncidentFilter, pagination *Pagination) (*IncidentQueryResult, error)

	// QueryLineageImpact queries the lineage_impact_analysis view for downstream impact.
	//
	// This view performs recursive lineage traversal to find all datasets and jobs
	// affected by a specific job run, up to 10 levels deep.
	//
	// Parameters:
	//   - jobRunID: Job run ID to analyze downstream impact for
	//   - maxDepth: Maximum recursion depth (0 = unlimited, -1 = direct outputs only, >0 = depth limit)
	//
	// Returns:
	//   - Slice of ImpactResult sorted by depth (empty slice if no downstream datasets)
	//   - Error if query fails or context is cancelled
	//
	// Example:
	//   // Get all downstream datasets (unlimited depth)
	//   impact, err := store.QueryLineageImpact(ctx, "job-123", 0)
	//
	//   // Get only direct outputs (depth 0)
	//   directOutputs, err := store.QueryLineageImpact(ctx, "job-123", -1)
	//
	//   // Get up to 3 levels downstream
	//   impact, err := store.QueryLineageImpact(ctx, "job-123", 3).
	QueryLineageImpact(ctx context.Context, jobRunID string, maxDepth int) ([]ImpactResult, error)

	// QueryRecentIncidents queries the recent_incidents_summary view for 7-day overview.
	//
	// This view aggregates test failures per job run for the last 7 days, providing:
	//   - Failed test counts per job run
	//   - Affected dataset counts
	//   - Downstream impact estimates
	//   - Time-windowed filtering (NOW() - 7 days)
	//
	// Parameters:
	//   - limit: Maximum number of results to return (0 = no limit)
	//
	// Returns:
	//   - Slice of RecentIncidentSummary sorted by most recent failure first
	//   - Empty slice if no incidents in 7-day window
	//   - Error if query fails or context is cancelled
	//
	// Example:
	//   // Get top 10 recent incidents
	//   incidents, err := store.QueryRecentIncidents(ctx, 10)
	//
	//   // Get all incidents (no limit)
	//   allIncidents, err := store.QueryRecentIncidents(ctx, 0).
	QueryRecentIncidents(ctx context.Context, limit int) ([]RecentIncidentSummary, error)

	// QueryIncidentByID queries a single incident by test_result_id.
	//
	// This is used by the incident detail endpoint to fetch full incident information.
	//
	// Parameters:
	//   - testResultID: Test result ID (primary key)
	//
	// Returns:
	//   - Pointer to Incident (nil if not found, no error)
	//   - Error if query fails or context is cancelled
	QueryIncidentByID(ctx context.Context, testResultID int64) (*Incident, error)

	// QueryDownstreamCounts returns downstream dataset counts for multiple job runs.
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
	QueryDownstreamCounts(ctx context.Context, jobRunIDs []string) (map[string]int, error)

	// QueryDownstreamWithParents queries downstream datasets with parent URN relationships.
	// This enables the frontend to build a lineage tree visualization.
	//
	// The query performs a recursive traversal starting from the job's direct outputs,
	// following input→output relationships through consuming jobs.
	//
	// Parameters:
	//   - jobRunID: Job run ID to query downstream impact for
	//   - maxDepth: Maximum recursion depth (typically 10)
	//
	// Returns:
	//   - Slice of DownstreamResult with parent_urn for tree building (depth > 0 only)
	//   - Empty slice if no downstream datasets
	//   - Error if query fails or context is cancelled
	//
	// Note: Results exclude depth=0 (direct outputs) since those are the starting point,
	// not downstream. Use QueryLineageImpact with maxDepth=-1 to get direct outputs.
	QueryDownstreamWithParents(ctx context.Context, jobRunID string, maxDepth int) ([]DownstreamResult, error)

	// QueryUpstreamWithChildren queries upstream datasets with child URN relationships.
	// This enables the frontend to build a lineage tree visualization showing data provenance.
	//
	// This is the inverse of QueryDownstreamWithParents:
	//   - Downstream: "What datasets are affected if this job fails?" (follows consumers)
	//   - Upstream: "What datasets were consumed to produce this output?" (follows producers)
	//
	// The query performs a recursive traversal starting from the job's direct inputs,
	// following output→input relationships through producing jobs backward.
	//
	// Parameters:
	//   - datasetURN: The root dataset URN (typically the tested dataset from the incident).
	//     This becomes the childURN for depth=1 results, anchoring the tree.
	//   - jobRunID: Job run ID that produced the root dataset
	//   - maxDepth: Maximum recursion depth (typically 3-10)
	//
	// Returns:
	//   - Slice of UpstreamResult with child_urn for tree building
	//   - Depth=1: Direct inputs to the job, childURN = datasetURN (the root)
	//   - Depth=2+: Upstream of those inputs, childURN = previous level's dataset
	//   - Each result includes the producer tool that created that upstream dataset
	//   - Empty slice if no upstream datasets (job has no inputs)
	//   - Error if query fails or context is cancelled
	//
	// The ChildURN field represents the "feeds into" relationship:
	// the upstream dataset was consumed to produce the child dataset.
	//
	// Example: If job transforms staging.customers → marts.customers (tested):
	//   - UpstreamResult{URN: "staging.customers", Depth: 1, ChildURN: "marts.customers"}
	//   - UpstreamResult{URN: "raw_customers", Depth: 2, ChildURN: "staging.customers"}
	//
	// Performance:
	//   - Uses recursive CTE (efficient in PostgreSQL)
	//   - Joins job_runs table to get producer information
	//   - Typical query time: 5-30ms depending on graph size
	//   - maxDepth prevents runaway recursion
	QueryUpstreamWithChildren(
		ctx context.Context, datasetURN string, jobRunID string, maxDepth int,
	) ([]UpstreamResult, error)

	// QueryOrphanDatasets returns datasets that have test results but no corresponding
	// data producer output edges.
	//
	// Orphan Detection Logic:
	//   - Produced datasets: Datasets with lineage_edges where edge_type='output'
	//   - Tested datasets: Datasets with test_results
	//   - Orphan = Tested dataset NOT IN Produced datasets
	//
	// This identifies Entity Resolution issues where different tools emit different
	// URN formats for the same logical dataset:
	//   - GE might emit: "demo_postgres/customers"
	//   - dbt might emit: "postgresql://demo/marts.customers"
	//
	// For each orphan, the method attempts to find a likely match among produced
	// datasets by comparing extracted table names.
	//
	// Returns:
	//   - Slice of OrphanDataset sorted by test_count DESC (most impactful first)
	//   - Each orphan includes LikelyMatch if a candidate was found
	//   - Empty slice if no orphan datasets exist (healthy state)
	//   - Error if query fails or context is cancelled
	//
	// Performance:
	//   - Queries test_results, lineage_edges tables
	//   - Table name extraction done in Go (not SQL)
	//   - Typical query time: 20-100ms depending on data volume
	//
	// Used by:
	//   - GET /api/v1/health/correlation endpoint
	//   - Pattern suggestion algorithm
	QueryOrphanDatasets(ctx context.Context) ([]OrphanDataset, error)

	// QueryCorrelationHealth returns overall correlation health metrics.
	//
	// This aggregates:
	//   - Correlation rate (correlated incidents / total incidents)
	//   - Total distinct datasets with test results
	//   - List of orphan namespaces requiring configuration
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
	//
	// Used by:
	//   - GET /api/v1/health/correlation endpoint
	QueryCorrelationHealth(ctx context.Context) (*Health, error)
}
