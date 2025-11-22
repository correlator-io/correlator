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

	// QueryIncidents queries the incident_correlation_view with optional filters.
	//
	// This view correlates test failures to the job runs that produced the failing datasets enabling 2-click navigation
	//  to root cause.
	//
	// Parameters:
	//   - filter: Optional filter (nil = no filtering, returns all incidents).
	//
	// Returns:
	//   - Slice of Incident results (empty slice if no matches)
	//   - Error if query fails or context is cancelled
	//
	// Performance:
	//   - View is pre-materialized (fast queries, ~10-50ms typical)
	//   - Uses indexes: incident_correlation_view_pk, idx_incident_correlation_view_job_run_id
	//   - Refresh latency: Call RefreshViews() to update data.
	QueryIncidents(ctx context.Context, filter *IncidentFilter) ([]Incident, error)

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
}
