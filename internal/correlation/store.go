// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import (
	"context"
	"time"
)

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
	//   - Uses indexes: incident_correlation_view_pk, idx_incident_correlation_view_run_id
	//   - Views are auto-refreshed via debounced post-ingestion hook on LineageStore.
	QueryIncidents(ctx context.Context, filter *IncidentFilter, pagination *Pagination) (*IncidentQueryResult, error)

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
	//   - runIDs: Slice of job run IDs to query counts for
	//
	// Returns:
	//   - Map of run_id -> downstream_count (missing keys have 0 downstream)
	//   - Error if query fails or context is cancelled
	QueryDownstreamCounts(ctx context.Context, runIDs []string) (map[string]int, error)

	// QueryDownstreamWithParents queries downstream datasets with parent URN relationships.
	// This enables the frontend to build a lineage tree visualization.
	//
	// The query performs a recursive traversal starting from the job's direct outputs,
	// following input→output relationships through consuming jobs.
	//
	// Parameters:
	//   - runID: Job run ID to query downstream impact for
	//   - maxDepth: Maximum recursion depth (typically 10)
	//
	// Returns:
	//   - Slice of DownstreamResult with parent_urn for tree building (depth > 0 only)
	//   - Empty slice if no downstream datasets
	//   - Error if query fails or context is cancelled
	//
	// Note: Results exclude depth=0 (direct outputs) since those are the starting point, not downstream.
	QueryDownstreamWithParents(ctx context.Context, runID string, maxDepth int) ([]DownstreamResult, error)

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
	//   - runID: Job run ID that produced the root dataset
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
		ctx context.Context, datasetURN string, runID string, maxDepth int,
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

	// QueryOrchestrationChain walks the parent_run_id chain from a given job run
	// up to the root orchestrator. Returns the ancestor chain ordered from root to
	// the immediate parent (excludes the starting job itself).
	//
	// Parameters:
	//   - runID: The job run ID to walk up from
	//   - maxDepth: Safety limit to prevent infinite loops (typically 10)
	//
	// Returns:
	//   - Slice of OrchestrationNode from root (index 0) to immediate parent (last)
	//   - Empty slice if job has no parent
	//   - Error if query fails or context is cancelled
	QueryOrchestrationChain(ctx context.Context, runID string, maxDepth int) ([]OrchestrationNode, error)

	// QueryIncidentCounts returns the number of active, resolved, and muted incidents.
	// Resolved/muted counts are scoped to the given windowDays (server default: 30).
	// Active incidents are always counted regardless of window.
	QueryIncidentCounts(ctx context.Context, windowDays int) (*IncidentCounts, error)

	// QueryOtherAttempts returns sibling retry attempts for an incident, excluding the
	// current incident itself. Uses (test_name, dataset_urn, root_parent_run_id) grouping.
	// Returns nil if the incident has no root_parent_run_id or no siblings exist.
	QueryOtherAttempts(ctx context.Context, testResultID int64) ([]RunRetryAttempt, error)
}

// ResolutionStore defines write operations for incident resolution lifecycle.
//
// Separated from the read-only Store to follow Interface Segregation:
//   - API handlers for PATCH /incidents/{id}/status depend on ResolutionStore
//   - Ingestion path for auto-resolve depends on ResolutionStore
//   - List/detail API handlers depend on Store (read-only, resolution status comes via JOIN)
//
// Implemented by: storage.LineageStore.
type ResolutionStore interface {
	// GetResolution returns the current resolution state for an incident.
	// Returns nil (no error) if no resolution row exists (incident is implicitly open).
	GetResolution(ctx context.Context, testResultID int64) (*IncidentResolution, error)

	// SetResolution creates or updates the resolution state for an incident.
	// Validates status transitions: open→acknowledged/resolved/muted, acknowledged→resolved/muted.
	// Terminal states (resolved, muted) cannot be transitioned away from.
	//
	// For auto-resolve: pass resolvedBy="auto", reason="auto_pass", and the passing testResultID.
	// For manual actions: pass the client_id as resolvedBy.
	SetResolution(
		ctx context.Context,
		testResultID int64,
		req ResolutionRequest,
		resolvedBy string,
	) (*IncidentResolution, error)

	// AutoResolveIncidents finds open/acknowledged incidents matching the given (testName, datasetURN)
	// where the failing test was executed more than gracePeriod ago, and auto-resolves them.
	//
	// Anti-flapping: Only resolves incidents whose failure is older than gracePeriod (default 1 hour).
	// This prevents resolution noise from intermittent test passes.
	//
	// Parameters:
	//   - testName: The test name that just passed
	//   - datasetURN: The dataset URN the test ran against
	//   - passingTestResultID: The test_result_id of the passing test (stored as evidence)
	//   - gracePeriod: Minimum time since failure before auto-resolve is allowed
	//
	// Returns:
	//   - Number of incidents auto-resolved
	//   - Error if query fails
	AutoResolveIncidents(
		ctx context.Context,
		testName string,
		datasetURN string,
		passingTestResultID int64,
		gracePeriod time.Duration,
	) (int, error)

	// CascadeResolutionToSiblings applies the same resolution to all sibling
	// retry attempts in the same group (test_name, dataset_urn, test_root_parent_run_id).
	// Siblings that cannot transition (already in a terminal state) are skipped.
	// Returns the number of siblings updated.
	CascadeResolutionToSiblings(
		ctx context.Context,
		testResultID int64,
		req ResolutionRequest,
		resolvedBy string,
	) (int, error)
}
