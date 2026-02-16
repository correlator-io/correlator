// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import "time"

type (
	// ImpactResult represents a single row from the lineage_impact_analysis materialized view.
	//
	// This is a domain type representing the business concept of "downstream impact analysis".
	//
	// Fields:
	//   - JobRunID: Canonical ID of the job run that produced this dataset
	//   - DatasetURN: Unique resource name of the dataset (e.g., "urn:postgres:warehouse:public.customers")
	//   - DatasetName: Human-readable dataset name (e.g., "customers")
	//   - Depth: How many hops downstream from the original job run (0 = direct output, 1+ = downstream)
	//
	// Example:
	//
	//	If job1 produces dataset_a, which is consumed by job2 that produces dataset_b:
	//	- ImpactResult{JobRunID: "job1", DatasetURN: "dataset_a", Depth: 0}  // job1's direct output
	//	- ImpactResult{JobRunID: "job2", DatasetURN: "dataset_b", Depth: 1}  // 1 hop downstream from job1
	//
	// Used by:
	//   - Correlation engine to calculate blast radius of job failures
	//   - UI to visualize downstream impact in lineage graph
	//   - Alerting system to determine affected teams/datasets
	ImpactResult struct {
		JobRunID    string
		DatasetURN  string
		DatasetName string
		Depth       int
	}

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
	//   - JobRunID: Canonical ID of the job run that produced this dataset
	//   - JobName: Name of the job (e.g., "transform_customers")
	//   - JobNamespace: Job namespace (e.g., "dbt_prod")
	//   - JobStatus: Job execution status (e.g., "COMPLETE", "FAIL")
	//   - JobStartedAt: When the job started
	//   - JobCompletedAt: When the job completed (nil if still running)
	//   - ProducerName: Tool that generated the lineage event (e.g., "dbt", "airflow")
	//   - ProducerVersion: Version of the producer tool (nullable)
	//   - LineageEdgeID: Primary key of the lineage edge
	//   - LineageEdgeType: Type of lineage relationship ("input" or "output")
	//   - OpenLineageRunID: OpenLineage client-generated UUID for the run
	//   - JobEventType: OpenLineage event type (e.g., "COMPLETE", "FAIL")
	//   - LineageCreatedAt: When the lineage edge was created
	//   - ParentJobRunID: Canonical parent job run ID (empty if no parent)
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
		JobRunID         string
		JobName          string
		JobNamespace     string
		JobStatus        string
		JobStartedAt     time.Time
		JobCompletedAt   *time.Time
		ProducerName     string
		ProducerVersion  *string
		LineageEdgeID    int64
		LineageEdgeType  string
		OpenLineageRunID string
		JobEventType     string
		LineageCreatedAt time.Time
		// Parent job fields (from OpenLineage ParentRunFacet)
		ParentJobRunID       string     // Canonical parent job run ID (empty if no parent)
		ParentJobName        string     // Parent job name (e.g., "jaffle_shop.build")
		ParentJobStatus      string     // Parent job status (e.g., "COMPLETE", "FAIL")
		ParentJobCompletedAt *time.Time // Parent job completion timestamp
	}

	// RecentIncidentSummary represents a single row from the recent_incidents_summary materialized view.
	//
	// This domain type provides a 7-day aggregated view of test failures per job run.
	//
	// Fields:
	//   - JobRunID: Canonical ID of the job run
	//   - JobName: Name of the job
	//   - JobNamespace: Job namespace
	//   - JobStatus: Job execution status
	//   - ProducerName: Tool that generated the lineage event
	//   - FailedTestCount: Number of failed tests for this job run
	//   - AffectedDatasetCount: Number of distinct datasets with failed tests
	//   - FailedTestNames: Array of failed test names
	//   - AffectedDatasetURNs: Array of affected dataset URNs
	//   - FirstTestFailureAt: Timestamp of first test failure
	//   - LastTestFailureAt: Timestamp of most recent test failure
	//   - JobStartedAt: When the job started
	//   - JobCompletedAt: When the job completed (nil if still running)
	//   - DownstreamAffectedCount: Number of downstream datasets impacted
	//
	// Used by:
	//   - correlation.Store.QueryRecentIncidents() - Returns this type
	//   - Dashboard/UI - Should convert to response types
	RecentIncidentSummary struct {
		JobRunID                string
		JobName                 string
		JobNamespace            string
		JobStatus               string
		ProducerName            string
		FailedTestCount         int64
		AffectedDatasetCount    int64
		FailedTestNames         []string
		AffectedDatasetURNs     []string
		FirstTestFailureAt      time.Time
		LastTestFailureAt       time.Time
		JobStartedAt            time.Time
		JobCompletedAt          *time.Time
		DownstreamAffectedCount int64
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
	//   - ProducerName: Filter by producer (e.g., "dbt", "airflow")
	//   - DatasetURN: Filter by specific dataset URN
	//   - JobRunID: Filter by specific job run ID
	//   - Tool: Filter by tool extracted from canonical job_run_id (e.g., "dbt", "airflow", "spark")
	//   - TestExecutedAfter: Filter tests executed after this timestamp
	//   - TestExecutedBefore: Filter tests executed before this timestamp
	//
	// Tool vs ProducerName:
	//   - Tool: Filters by tool type from canonical ID format "tool:runID" (more reliable, uses LIKE pattern)
	//   - ProducerName: Filters by producer name extracted from producer URL (may vary by version)
	//
	// Example:
	//
	//	// Find all incidents from dbt jobs in the last 24 hours
	//	filter := &correlation.IncidentFilter{
	//	    Tool: strPtr("dbt"),  // Uses job_run_id LIKE 'dbt:%'
	//	    TestExecutedAfter: timePtr(time.Now().Add(-24 * time.Hour)),
	//	}
	//	result, err := store.QueryIncidents(ctx, filter, nil)
	IncidentFilter struct {
		JobStatus          *string
		ProducerName       *string
		DatasetURN         *string
		JobRunID           *string
		Tool               *string
		TestExecutedAfter  *time.Time
		TestExecutedBefore *time.Time
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
	}
)
