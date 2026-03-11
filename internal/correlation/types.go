// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import "time"

// ResolutionStatus constants define the valid resolution states.
const (
	ResolutionOpen         ResolutionStatus = "open"
	ResolutionAcknowledged ResolutionStatus = "acknowledged"
	ResolutionResolved     ResolutionStatus = "resolved"
	ResolutionMuted        ResolutionStatus = "muted"
)

// validResolutionStatuses is the set of all valid resolution states.
//
//nolint:gochecknoglobals // Package-level lookup table, read-only after init.
var validResolutionStatuses = map[ResolutionStatus]bool{
	ResolutionOpen:         true,
	ResolutionAcknowledged: true,
	ResolutionResolved:     true,
	ResolutionMuted:        true,
}

// IsValidResolutionStatus checks whether the given status is a valid resolution state.
func IsValidResolutionStatus(s ResolutionStatus) bool {
	return validResolutionStatuses[s]
}

// validTransitions defines the allowed state transitions for incident resolution.
// Resolved and muted are terminal states in alpha (no reopening).
//
//nolint:gochecknoglobals // Package-level lookup table, read-only after init.
var validTransitions = map[ResolutionStatus][]ResolutionStatus{
	ResolutionOpen:         {ResolutionAcknowledged, ResolutionResolved, ResolutionMuted},
	ResolutionAcknowledged: {ResolutionResolved, ResolutionMuted},
}

// ResolutionStatusFilter constants define the valid status filter values for listing incidents.
const (
	StatusFilterActive   ResolutionStatusFilter = "active" // open + acknowledged
	StatusFilterResolved ResolutionStatusFilter = "resolved"
	StatusFilterMuted    ResolutionStatusFilter = "muted"
	StatusFilterAll      ResolutionStatusFilter = "all"
)

// validStatusFilters is the set of all valid status filter values.
//
//nolint:gochecknoglobals // Package-level lookup table, read-only after init.
var validStatusFilters = map[ResolutionStatusFilter]bool{
	StatusFilterActive:   true,
	StatusFilterResolved: true,
	StatusFilterMuted:    true,
	StatusFilterAll:      true,
}

// IsValidResolutionStatusFilter checks whether the given filter is a valid status filter.
func IsValidResolutionStatusFilter(f ResolutionStatusFilter) bool {
	return validStatusFilters[f]
}

type (
	// Incident represents a single row from the incident_correlation_view materialized view.
	//
	// This domain type maps to the materialized view schema and contains all fields needed
	// for correlating test failures to job runs that produced the failing datasets.
	//
	// Fields:
	//   - TestResultID: Primary key of the test result
	//   - TestName: Name of the test (e.g., "not_null_customers_customer_id")
	//   - TestType: Type of test (e.g., "not_null", "unique", "freshness")
	//   - TestStatus: Status of test execution (e.g., "passed", "failed", "error")
	//   - TestMessage: Detailed failure message (e.g., "Found 3 null values")
	//   - TestExecutedAt: When the test was executed
	//   - TestDurationMs: Test execution time in milliseconds
	//   - DatasetURN: URN of the dataset that was tested
	//   - DatasetName: Human-readable dataset name
	//   - DatasetNamespace: Dataset namespace (schema/database)
	//   - RunID: OpenLineage run UUID of the job that produced this dataset
	//   - JobName: Name of the job (e.g., "transform_customers")
	//   - JobNamespace: Job namespace (e.g., "dbt_prod")
	//   - JobStatus: Job execution status (e.g., "COMPLETE", "FAIL")
	//   - JobStartedAt: When the job started
	//   - JobCompletedAt: When the job completed (nil if still running)
	//   - TestProducerName: Tool that ran the validation (e.g., "great_expectations", "dbt")
	//   - JobProducerName: Tool that generated the lineage event (e.g., "dbt", "airflow")
	//   - JobEventType: OpenLineage event type (e.g., "COMPLETE", "FAIL")
	//   - ParentRunID: Parent run UUID (empty if no parent)
	//   - ParentJobName: Parent job name (e.g., "jaffle_shop.build")
	//   - ParentJobStatus: Parent job status (e.g., "COMPLETE", "FAIL")
	//   - ParentJobCompletedAt: Parent job completion timestamp (nil if no parent or still running)
	//
	// Used by:
	//   - correlation.Store.QueryIncidents() - Returns this type
	//   - API handlers - Should convert to response types
	Incident struct {
		TestResultID     int64
		TestName         string
		TestType         string
		TestStatus       string
		TestMessage      string
		TestExecutedAt   time.Time
		TestDurationMs   int64
		TestProducerName string
		DatasetURN       string
		DatasetName      string
		DatasetNS        string
		RunID            string
		JobName          string
		JobNamespace     string
		JobStatus        string
		JobStartedAt     time.Time
		JobCompletedAt   *time.Time
		JobProducerName  string
		JobEventType     string
		// Parent job fields (from OpenLineage ParentRunFacet)
		ParentRunID          string     // Parent run UUID (empty if no parent)
		ParentJobName        string     // Parent job name (e.g., "jaffle_shop.build")
		ParentJobNamespace   string     // Parent job namespace (e.g., "dbt")
		ParentJobStatus      string     // Parent job status (e.g., "COMPLETE", "FAIL")
		ParentJobCompletedAt *time.Time // Parent job completion timestamp
		ParentProducerName   string     // Parent producer name (e.g., "correlator-dbt")
		// Root parent job fields (from OpenLineage ParentRunFacet root)
		RootParentRunID          string     // Root parent run UUID (empty if no root)
		RootParentJobName        string     // Root parent job name (e.g., "demo_pipeline")
		RootParentJobNamespace   string     // Root parent job namespace (e.g., "airflow")
		RootParentJobStatus      string     // Root parent job status
		RootParentJobCompletedAt *time.Time // Root parent job completion timestamp
		RootParentProducerName   string     // Root parent producer (e.g., "airflow")
		// Test run's root parent — the orchestrator retry group key.
		// May differ from RootParentRunID when test runner (GE) and producer (dbt) are separate tasks.
		TestRootParentRunID string
		// Resolution fields (joined from incident_resolutions, NULL = implicitly open)
		ResolutionStatus ResolutionStatus // "open" if no row in incident_resolutions
		ResolvedBy       string           // "auto" or client_id (empty if open)
		ResolutionReason string           // "auto_pass", "manual", etc. (empty if open)
		MuteExpiresAt    *time.Time       // Only for muted status
		ResolvedAt       *time.Time       // When the status was last changed
		// Run retry context (computed via window functions, only populated in list queries)
		RunRetryContext *RunRetryContext
	}

	// OrchestrationNode represents one level in the orchestration chain.
	// Used to build the full hierarchy from root orchestrator to producing job.
	OrchestrationNode struct {
		RunID        string
		JobName      string
		JobNamespace string
		ProducerName string
		Status       string
	}

	// IncidentFilter provides filtering options for querying incident_correlation_view.
	//
	// All fields are optional (pointer types). If a field is nil, it won't be used in the query.
	// Multiple filters are combined with AND logic.
	//
	// Note: TestStatus is NOT included because the incident_correlation_view already filters
	// to only failed/error tests at the database level (WHERE status IN ('failed', 'error')).
	//
	// Fields:
	//   - JobStatus: Filter by job status (e.g., "COMPLETE", "FAIL")
	//   - JobProducerName: Filter by job producer (e.g., "dbt", "airflow")
	//   - DatasetURN: Filter by specific dataset URN
	//   - RunID: Filter by specific run UUID
	//   - TestExecutedAfter: Filter tests executed after this timestamp
	//   - TestExecutedBefore: Filter tests executed before this timestamp
	//
	// Example:
	//
	//	// Find all incidents from dbt jobs in the last 24 hours
	//	filter := &correlation.IncidentFilter{
	//	    JobProducerName: strPtr("dbt"),
	//	    TestExecutedAfter: timePtr(time.Now().Add(-24 * time.Hour)),
	//	}
	//	result, err := store.QueryIncidents(ctx, filter, nil)
	IncidentFilter struct {
		JobStatus          *string
		JobProducerName    *string
		DatasetURN         *string
		RunID              *string
		TestExecutedAfter  *time.Time
		TestExecutedBefore *time.Time
		// Resolution lifecycle filters
		StatusFilter ResolutionStatusFilter // "active" (default), "resolved", "muted", "all"
		WindowDays   int                    // Time window in days for historical views (0 = no window)
	}

	// Pagination specifies pagination parameters for list queries.
	//
	// Fields:
	//   - Limit: Maximum number of results to return (required, typically 1-100)
	//   - Offset: Number of results to skip (default 0)
	Pagination struct {
		Limit  int
		Offset int
	}

	// IncidentQueryResult contains paginated incident query results.
	//
	// Fields:
	//   - Incidents: Slice of incidents for the requested page
	//   - Total: Total count of incidents matching the filter (before pagination)
	IncidentQueryResult struct {
		Incidents []Incident
		Total     int
	}

	// DownstreamResult represents a downstream dataset with parent relationship.
	// This type is used for building lineage tree visualizations in the UI.
	//
	// Fields:
	//   - DatasetURN: Unique resource name of the downstream dataset
	//   - DatasetName: Human-readable dataset name
	//   - Depth: Number of hops from the original producing job (0 = direct output)
	//   - ParentURN: URN of the parent dataset in the lineage tree
	//
	// The ParentURN field enables the frontend to build a tree structure from
	// the flat list of results using React Flow or similar visualization libraries.
	DownstreamResult struct {
		DatasetURN  string
		DatasetName string
		Depth       int
		ParentURN   string
		Producer    string
	}

	// UpstreamResult represents an upstream dataset with child relationship.
	// This type is used for building lineage tree visualizations showing data provenance.
	//
	// Upstream traversal answers: "What datasets were consumed to produce this dataset?"
	// This is the inverse of downstream traversal (DownstreamResult).
	//
	// Fields:
	//   - DatasetURN: Unique resource name of the upstream dataset (input to some job)
	//   - DatasetName: Human-readable dataset name
	//   - Depth: Number of hops upstream from the starting job (1 = direct input, 2+ = further back)
	//   - ChildURN: URN of the dataset that this upstream dataset feeds into
	//   - Producer: Tool that produced this upstream dataset (e.g., "dbt", "airflow")
	//
	// The ChildURN field enables the frontend to build a tree structure from
	// the flat list of results. It represents the "feeds into" relationship:
	// this upstream dataset was consumed to produce the child dataset.
	//
	// Example lineage: raw_data → staging_data → mart_data
	//   - UpstreamResult{URN: "staging_data", Depth: 1, ChildURN: "mart_data", Producer: "dbt"}
	//   - UpstreamResult{URN: "raw_data", Depth: 2, ChildURN: "staging_data", Producer: "dbt"}
	UpstreamResult struct {
		DatasetURN  string
		DatasetName string
		Depth       int
		ChildURN    string
		Producer    string
	}

	// OrphanNamespace represents a namespace that appears in validation tests
	// but has no corresponding data producer output edges.
	//
	// This indicates a namespace aliasing issue where validators (Great Expectations, Soda)
	// emit events with a different namespace format than data producers (dbt, Airflow).
	//
	// Fields:
	//   - Namespace: The orphan namespace string (e.g., "postgres_prod")
	//   - Producer: Tool that emitted validation events (e.g., "great_expectations", "soda")
	//   - LastSeen: Most recent event timestamp for this namespace
	//   - EventCount: Number of test results in this namespace
	//   - SuggestedAlias: Potential matching producer namespace (nil for MVP)
	//
	// Example:
	//
	//	Great Expectations emits tests for namespace "postgres_prod"
	//	dbt emits output edges for namespace "postgresql://prod-db:5432/mydb"
	//	→ OrphanNamespace{Namespace: "postgres_prod", Producer: "great_expectations", ...}
	//
	// Resolution: Configure namespace alias in correlator.yaml to map "postgres_prod"
	// to "postgresql://prod-db:5432/mydb".
	//
	// Used by:
	//   - correlation.Store.QueryOrphanNamespaces() - Returns this type
	//   - Correlation Health API - GET /api/v1/health/correlation
	//   - UI Correlation Health page - Shows orphan namespaces needing configuration
	OrphanNamespace struct {
		Namespace      string
		Producer       string
		LastSeen       time.Time
		EventCount     int
		SuggestedAlias *string
	}

	// Health represents overall correlation system health metrics.
	//
	// This type aggregates correlation statistics to help users identify
	// configuration issues that prevent cross-tool correlation.
	//
	// Fields:
	//   - CorrelationRate: Ratio of correlated tested datasets to total tested datasets (0.0-1.0)
	//   - TotalDatasets: Count of distinct datasets with test results (any status)
	//   - ProducedDatasets: Count of distinct datasets with producer output edges
	//   - CorrelatedDatasets: Count of distinct datasets with both tests AND output edges
	//   - OrphanDatasets: List of datasets requiring pattern configuration
	//   - SuggestedPatterns: Auto-generated patterns to resolve orphan datasets
	//
	// Correlation Rate Calculation:
	//
	//	correlation_rate = correlated_tested_datasets / total_tested_datasets
	//
	// Where:
	//   - correlated_tested_datasets = distinct datasets with failed tests AND producer output edges
	//   - total_tested_datasets = distinct datasets with failed/error test results
	//   - If total_tested_datasets = 0, returns 1.0 (no failed tests = healthy)
	//
	// Used by:
	//   - correlation.Store.QueryCorrelationHealth() - Returns this type
	//   - Correlation Health API - GET /api/v1/health/correlation
	//   - UI Correlation Health page - Shows overall system health
	Health struct {
		CorrelationRate    float64
		TotalDatasets      int
		ProducedDatasets   int
		CorrelatedDatasets int
		OrphanDatasets     []OrphanDataset
		SuggestedPatterns  []SuggestedPattern
	}

	// OrphanDataset represents a dataset with test results but no corresponding
	// data producer output edges. This is the dataset-level equivalent of OrphanNamespace,
	// providing finer granularity for correlation diagnostics.
	//
	// Unlike OrphanNamespace which groups by namespace, OrphanDataset tracks individual
	// dataset URNs, enabling:
	//   - Precise identification of uncorrelated test results
	//   - Automatic matching to likely producer datasets via table name extraction
	//   - Pattern suggestion for resolving Entity Resolution issues
	//
	// Fields:
	//   - DatasetURN: The orphan dataset URN (e.g., "demo_postgres/customers")
	//   - TestCount: Number of test results for this dataset
	//   - LastSeen: Most recent test execution timestamp
	//   - Producer: Dominant test producer for this dataset (e.g., "great_expectations")
	//   - LikelyMatch: Candidate producer dataset match (nil if no match found)
	//
	// Example:
	//
	//	GE emits tests for "demo_postgres/customers"
	//	dbt produces "postgresql://demo/marts.customers"
	//	→ OrphanDataset{
	//	    DatasetURN: "demo_postgres/customers",
	//	    LikelyMatch: &DatasetMatch{
	//	        DatasetURN: "postgresql://demo/marts.customers",
	//	        Confidence: 1.0,
	//	        MatchReason: "exact_table_name",
	//	    },
	//	  }
	//
	// Used by:
	//   - correlation.Store.DetectOrphanDatasets() - Returns this type
	//   - Pattern suggestion algorithm - Uses LikelyMatch to generate patterns
	//   - Correlation Health API - Future enhancement to orphan_datasets field
	OrphanDataset struct {
		DatasetURN  string
		TestCount   int
		LastSeen    time.Time
		Producer    string // Dominant test producer (e.g., "great_expectations", "dbt")
		LikelyMatch *DatasetMatch
	}

	// DatasetMatch represents a candidate match between an orphan dataset and a
	// produced dataset. Used for automatic pattern suggestion.
	//
	// Fields:
	//   - DatasetURN: The producer dataset URN that potentially matches the orphan
	//   - Confidence: Match confidence score (0.0 to 1.0)
	//     - 1.0: Exact table name match (e.g., both extract to "customers")
	//     - 0.0: No match found
	//   - MatchReason: Human-readable explanation of why this match was suggested
	//     - "exact_table_name": Table names extracted from both URNs are identical
	//     - "no_match": No matching producer dataset found
	//   - Producer: Tool that produces the matched dataset (e.g., "dbt", "airflow")
	//
	// Example:
	//
	//	Orphan: "demo_postgres/customers" → table name: "customers"
	//	Producer: "postgresql://demo/marts.customers" → table name: "customers"
	//	→ DatasetMatch{Confidence: 1.0, MatchReason: "exact_table_name"}
	DatasetMatch struct {
		DatasetURN  string
		Confidence  float64
		MatchReason string
		Producer    string // Producer of the matched dataset (e.g., "dbt", "airflow")
	}

	// IncidentResolution represents the resolution state of a single incident.
	// Maps to the incident_resolutions table. An incident with no resolution row
	// is implicitly "open".
	IncidentResolution struct {
		ID                     int64
		TestResultID           int64
		Status                 ResolutionStatus
		ResolvedBy             string // "auto" or client_id
		ResolutionReason       string // "auto_pass", "manual", "false_positive", "expected"
		ResolutionNote         string
		ResolvedByTestResultID *int64     // For auto-resolve: the passing test result
		MuteExpiresAt          *time.Time // Only for muted status
		CreatedAt              time.Time
		UpdatedAt              time.Time
	}

	// ResolutionStatus represents the resolution state of an incident.
	ResolutionStatus string

	// ResolutionRequest represents a manual status change request from the API.
	ResolutionRequest struct {
		Status   ResolutionStatus
		Reason   string // "manual", "false_positive", "expected"
		Note     string
		MuteDays int // Only for muted; default 30
	}

	// ResolutionStatusFilter represents the status filter for listing incidents.
	ResolutionStatusFilter string

	// IncidentCounts holds the count of incidents by resolution status.
	// Used by GET /api/v1/incidents/counts.
	IncidentCounts struct {
		Active   int
		Resolved int
		Muted    int
	}

	// RunRetryContext provides retry metadata for an incident whose test ran
	// multiple times under the same orchestrator run (e.g., Airflow retry).
	// Nil when no retries exist (total_attempts == 1).
	RunRetryContext struct {
		TotalAttempts  int    // Total number of attempts for this (test_name, dataset_urn, root_parent_run_id)
		CurrentAttempt int    // Ordinal position of this attempt (1-based)
		AllFailed      bool   // True if every attempt in the group has status IN ('failed', 'error')
		RootRunID      string // The root_parent_run_id grouping key
		// OtherAttempts is only populated on detail responses, not list.
		OtherAttempts []RunRetryAttempt
	}

	// RunRetryAttempt represents one sibling attempt in a retry group (excludes current incident).
	RunRetryAttempt struct {
		IncidentID       string
		Attempt          int
		TestStatus       string
		ExecutedAt       time.Time
		JobRunID         string
		ResolutionStatus ResolutionStatus
	}
)

// IsTerminal returns true if the status is immutable (no further transitions allowed in alpha).
func (s ResolutionStatus) IsTerminal() bool {
	return s == ResolutionResolved || s == ResolutionMuted
}

// CanTransitionTo checks whether transitioning from the current status to the target is allowed.
func (s ResolutionStatus) CanTransitionTo(target ResolutionStatus) bool {
	allowed, ok := validTransitions[s]
	if !ok {
		return false
	}

	for _, a := range allowed {
		if a == target {
			return true
		}
	}

	return false
}
