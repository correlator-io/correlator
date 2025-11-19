// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

// ImpactResult represents a single row from the lineage_impact_analysis materialized view.
// This view performs recursive downstream impact analysis to find all datasets and jobs
// affected by a job run failure.
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
// This is a utility function for analyzing subsets of impact analysis data.
//
// Parameters:
//   - results: Slice of ImpactResult to filter
//   - jobRunID: Filter by this job run ID (exact match)
//   - depth: Filter by this depth level (exact match)
//
// Returns:
//   - Filtered slice containing only results matching both criteria
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
