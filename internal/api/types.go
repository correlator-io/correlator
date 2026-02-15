// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"time"
)

type (
	// IncidentListResponse represents the response for GET /api/v1/incidents.
	// Contains a paginated list of incidents with metadata for pagination.
	IncidentListResponse struct {
		Incidents   []IncidentSummary `json:"incidents"`
		Total       int               `json:"total"`
		Limit       int               `json:"limit"`
		Offset      int               `json:"offset"`
		OrphanCount int               `json:"orphan_count"` //nolint:tagliatelle
	}

	// IncidentSummary represents a single incident in the list view.
	// This is a simplified view of an incident, optimized for list display.
	// Use GET /api/v1/incidents/{id} for full incident details.
	IncidentSummary struct {
		ID                  string    `json:"id"`
		TestName            string    `json:"test_name"`    //nolint:tagliatelle
		TestType            string    `json:"test_type"`    //nolint:tagliatelle
		TestStatus          string    `json:"test_status"`  //nolint:tagliatelle
		DatasetURN          string    `json:"dataset_urn"`  //nolint:tagliatelle
		DatasetName         string    `json:"dataset_name"` //nolint:tagliatelle
		Producer            string    `json:"producer"`
		JobName             string    `json:"job_name"`              //nolint:tagliatelle
		JobRunID            string    `json:"job_run_id"`            //nolint:tagliatelle
		DownstreamCount     int       `json:"downstream_count"`      //nolint:tagliatelle
		HasCorrelationIssue bool      `json:"has_correlation_issue"` //nolint:tagliatelle
		ExecutedAt          time.Time `json:"executed_at"`           //nolint:tagliatelle
	}

	// IncidentDetailResponse represents the response for GET /api/v1/incidents/{id}.
	// Contains full incident information including test details, dataset, job, and lineage.
	IncidentDetailResponse struct {
		ID                string              `json:"id"`
		Test              TestDetail          `json:"test"`
		Dataset           DatasetDetail       `json:"dataset"`
		Job               *JobDetail          `json:"job"` // nil if uncorrelated
		Upstream          []UpstreamDataset   `json:"upstream"`
		Downstream        []DownstreamDataset `json:"downstream"`
		CorrelationStatus string              `json:"correlation_status"` //nolint:tagliatelle
	}

	// TestDetail contains test information for incident detail view.
	TestDetail struct {
		Name       string    `json:"name"`
		Type       string    `json:"type"`
		Status     string    `json:"status"`
		Message    string    `json:"message"`
		ExecutedAt time.Time `json:"executed_at"` //nolint:tagliatelle
		DurationMs int64     `json:"duration_ms"` //nolint:tagliatelle
		Producer   string    `json:"producer"`
	}

	// DatasetDetail contains dataset information for incident detail view.
	DatasetDetail struct {
		URN       string `json:"urn"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}

	// JobDetail contains job information for incident detail view.
	// This is nil when the incident is uncorrelated (orphan namespace).
	JobDetail struct {
		Name        string     `json:"name"`
		Namespace   string     `json:"namespace"`
		RunID       string     `json:"run_id"` //nolint:tagliatelle
		Producer    string     `json:"producer"`
		Status      string     `json:"status"`
		StartedAt   time.Time  `json:"started_at"`             //nolint:tagliatelle
		CompletedAt *time.Time `json:"completed_at,omitempty"` //nolint:tagliatelle
	}

	// DownstreamDataset represents a downstream dataset in the lineage tree.
	// The ParentURN field enables frontend to build tree visualization.
	DownstreamDataset struct {
		URN       string `json:"urn"`
		Name      string `json:"name"`
		Depth     int    `json:"depth"`
		ParentURN string `json:"parentUrn"` // Parent dataset URN for tree building
		Producer  string `json:"producer"`  // Tool that produced this dataset (e.g., "dbt")
	}

	// UpstreamDataset represents an upstream dataset in the lineage tree.
	// This is the inverse of DownstreamDataset - showing data provenance.
	// The ChildURN field enables frontend to build tree visualization.
	UpstreamDataset struct {
		URN      string `json:"urn"`
		Name     string `json:"name"`
		Depth    int    `json:"depth"`
		ChildURN string `json:"childUrn"` // Child dataset URN (what this feeds into)
		Producer string `json:"producer"` // Tool that produced this dataset (e.g., "dbt")
	}

	// CorrelationHealthResponse represents the response for GET /api/v1/health/correlation.
	// Contains overall correlation system health metrics and orphan dataset details.
	CorrelationHealthResponse struct {
		CorrelationRate    float64                    `json:"correlation_rate"`    //nolint:tagliatelle
		TotalDatasets      int                        `json:"total_datasets"`      //nolint:tagliatelle
		ProducedDatasets   int                        `json:"produced_datasets"`   //nolint:tagliatelle
		CorrelatedDatasets int                        `json:"correlated_datasets"` //nolint:tagliatelle
		OrphanDatasets     []OrphanDatasetResponse    `json:"orphan_datasets"`     //nolint:tagliatelle
		SuggestedPatterns  []SuggestedPatternResponse `json:"suggested_patterns"`  //nolint:tagliatelle
	}

	// OrphanDatasetResponse represents a dataset that requires pattern configuration.
	// Orphan datasets have test results but no corresponding data producer output edges.
	OrphanDatasetResponse struct {
		DatasetURN  string                `json:"dataset_urn"`  //nolint:tagliatelle
		TestCount   int                   `json:"test_count"`   //nolint:tagliatelle
		LastSeen    time.Time             `json:"last_seen"`    //nolint:tagliatelle
		LikelyMatch *DatasetMatchResponse `json:"likely_match"` //nolint:tagliatelle
	}

	// DatasetMatchResponse represents a candidate match for an orphan dataset.
	DatasetMatchResponse struct {
		DatasetURN  string  `json:"dataset_urn"` //nolint:tagliatelle
		Confidence  float64 `json:"confidence"`
		MatchReason string  `json:"match_reason"` //nolint:tagliatelle
	}

	// SuggestedPatternResponse represents a pattern suggestion derived from orphan→match pairs.
	SuggestedPatternResponse struct {
		Pattern         string   `json:"pattern"`
		Canonical       string   `json:"canonical"`
		ResolvesCount   int      `json:"resolves_count"`   //nolint:tagliatelle
		OrphansResolved []string `json:"orphans_resolved"` //nolint:tagliatelle
	}
)

type (
	// LineageResponse represents OpenLineage-compliant batch response.
	// Spec: https://openlineage.io/apidocs/openapi/#tag/OpenLineage/operation/postEventBatch
	//
	// The response includes only failed events (OpenLineage spec) plus Correlator extensions
	// for observability (correlation_id, timestamp).
	//
	// Correlator Extensions (not in OpenLineage spec):
	//   - correlation_id: Request correlation ID for tracing
	//   - timestamp: Response generation time (ISO8601)
	LineageResponse struct {
		Status        string          `json:"status"`         // "success" or "error" (OpenLineage spec)
		Summary       ResponseSummary `json:"summary"`        // Event counts (received, successful, failed, retriable)
		FailedEvents  []FailedEvent   `json:"failed_events"`  //nolint: tagliatelle // Only failed events
		CorrelationID string          `json:"correlation_id"` //nolint: tagliatelle // Correlator extension
		Timestamp     string          `json:"timestamp"`      // Correlator extension
	}

	// ResponseSummary provides aggregate counts for batch processing.
	// This matches the OpenLineage spec format.
	ResponseSummary struct {
		Received     int `json:"received"`      // Total events in batch
		Successful   int `json:"successful"`    // Stored + duplicates (idempotent = success)
		Failed       int `json:"failed"`        // Events that failed validation or storage
		Retriable    int `json:"retriable"`     // Transient failures (network, timeout)
		NonRetriable int `json:"non_retriable"` //nolint: tagliatelle // Permanent failures (validation, missing fields)
	}

	// FailedEvent describes a single failed event in the batch.
	// OpenLineage spec only includes failed events in response (not successful).
	FailedEvent struct {
		Index     int    `json:"index"`     // Event index in original batch (0-based)
		Reason    string `json:"reason"`    // Human-readable failure reason
		Retriable bool   `json:"retriable"` // True if transient failure (can retry)
	}

	// LineageEvent model represents an event in the payload of an API request to ingest OpenLineage events.
	// This is separate from the domain model (ingestion.RunEvent) to decouple
	// the API contract from internal domain types.
	//
	// MVP Scope:
	//   - Producer URL format validation is a nice-to-have for post-MVP
	//   - SchemaURL validation is a nice-to-have for post-MVP
	//   - Current implementation prioritizes flexibility over strict validation
	LineageEvent struct {
		EventTime time.Time `json:"eventTime"`
		EventType string    `json:"eventType"`
		Producer  string    `json:"producer"`  // Optional, URL format validation deferred to post-MVP
		SchemaURL string    `json:"schemaURL"` //nolint: tagliatelle // Optional, validation deferred to post-MVP
		Run       Run       `json:"run"`
		Job       Job       `json:"job"`
		Inputs    []Dataset `json:"inputs,omitempty"`
		Outputs   []Dataset `json:"outputs,omitempty"`
	}

	// Run represents the run section of a LineageEvent.
	Run struct {
		ID     string                 `json:"runId"`
		Facets map[string]interface{} `json:"facets,omitempty"`
	}

	// Job represents the job section of a LineageEvent.
	Job struct {
		Namespace string                 `json:"namespace"`
		Name      string                 `json:"name"`
		Facets    map[string]interface{} `json:"facets,omitempty"`
	}

	// Dataset represents a dataset (input or output) in a LineageEvent.
	Dataset struct {
		Namespace    string                 `json:"namespace"`
		Name         string                 `json:"name"`
		Facets       map[string]interface{} `json:"facets,omitempty"`
		InputFacets  map[string]interface{} `json:"inputFacets,omitempty"`
		OutputFacets map[string]interface{} `json:"outputFacets,omitempty"`
	}
)

// CorrelationStatus constants for incident correlation state.
const (
	// CorrelationStatusCorrelated indicates the incident is fully correlated
	// with a data-producing job (dbt, Airflow, etc).
	CorrelationStatusCorrelated = "correlated"

	// CorrelationStatusOrphan indicates a namespace aliasing issue where
	// the dataset's namespace only appears from validation tools (GE, Soda)
	// but not from data producers (dbt, Airflow).
	CorrelationStatusOrphan = "orphan"

	// CorrelationStatusUnknown indicates the correlation status cannot be determined.
	CorrelationStatusUnknown = "unknown"
)
