// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import "time"

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
type ImpactResult struct {
	JobRunID    string
	DatasetURN  string
	DatasetName string
	Depth       int
}

// FilterImpactResults filters impact analysis results by job_run_id and depth.
//
// This is a utility function for analyzing subsets of impact analysis data.
// It provides a simple way to filter results without writing loops.
//
// Parameters:
//   - results: Slice of ImpactResult to filter
//   - jobRunID: Filter by this job run ID (exact match)
//   - depth: Filter by this depth level (exact match)
//
// Returns:
//   - Filtered slice containing only results matching both criteria
//   - Empty slice if no matches
//
// Example:
//
//	// Get all direct outputs (depth 0) for a specific job
//	directOutputs := FilterImpactResults(allResults, "job-123", 0)
//
//	// Get all first-level downstream datasets (depth 1)
//	downstream := FilterImpactResults(allResults, "job-123", 1)
func FilterImpactResults(results []ImpactResult, jobRunID string, depth int) []ImpactResult {
	var filtered []ImpactResult

	for _, r := range results {
		if r.JobRunID == jobRunID && r.Depth == depth {
			filtered = append(filtered, r)
		}
	}

	return filtered
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
//
// Used by:
//   - correlation.Store.QueryIncidents() - Returns this type
//   - API handlers - Should convert to response types
type Incident struct {
	TestResultID     int64
	TestName         string
	TestType         string
	TestStatus       string
	TestMessage      string
	TestExecutedAt   time.Time
	TestDurationMs   int64
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
type RecentIncidentSummary struct {
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
// Fields:
//   - TestStatus: Filter by test status (e.g., "failed", "passed")
//   - JobStatus: Filter by job status (e.g., "COMPLETE", "FAIL")
//   - ProducerName: Filter by producer (e.g., "dbt", "airflow")
//   - DatasetURN: Filter by specific dataset URN
//   - JobRunID: Filter by specific job run ID
//   - TestExecutedAfter: Filter tests executed after this timestamp
//   - TestExecutedBefore: Filter tests executed before this timestamp
//
// Example:
//
//	// Find all failed tests from dbt jobs in the last 24 hours
//	filter := &correlation.IncidentFilter{
//	    TestStatus: strPtr("failed"),
//	    ProducerName: strPtr("dbt"),
//	    TestExecutedAfter: timePtr(time.Now().Add(-24 * time.Hour)),
//	}
//	incidents, err := store.QueryIncidents(ctx, filter)
type IncidentFilter struct {
	TestStatus         *string
	JobStatus          *string
	ProducerName       *string
	DatasetURN         *string
	JobRunID           *string
	TestExecutedAfter  *time.Time
	TestExecutedBefore *time.Time
}
