// Package ingestion provides OpenLineage domain models for event ingestion.
// Spec: https://openlineage.io/docs/spec/object-model
package ingestion

import (
	"time"
)

type (
	// RunEvent represents an OpenLineage RunEvent (runtime lineage).
	// RunEvents describe the execution of a job and are emitted at runtime when jobs
	// start, run, or complete. Each RunEvent can include details about the Job, the Run,
	// and the input and output Datasets involved in the run.
	//
	// Spec: https://openlineage.io/docs/spec/object-model#job-run-state-update
	RunEvent struct {
		// EventTime is the timestamp when this event occurred (RFC3339 format).
		// Use for ordering events, not arrival time (handles out-of-order events).
		EventTime time.Time `json:"eventTime"`

		// EventType is the run state: START, RUNNING, COMPLETE, FAIL, ABORT, or OTHER.
		// Terminal states (COMPLETE, FAIL, ABORT) are idempotent.
		EventType EventType `json:"eventType"`

		// Producer identifies the tool that generated this event.
		// Format: URL with version (e.g., "https://github.com/dbt-labs/dbt-core/tree/1.5.0")
		Producer string `json:"producer"`

		// SchemaURL is the OpenLineage spec version URL.
		// Example: "https://openlineage.io/spec/2-0-2/OpenLineage.json"
		SchemaURL string `json:"schemaUrl"`

		// Run contains metadata about this specific run instance.
		Run Run `json:"run"`

		// Job contains metadata about the job definition.
		Job Job `json:"job"`

		// Inputs are datasets consumed by this run (optional).
		// Can be specified in START, COMPLETE, or both (events are accumulative).
		Inputs []Dataset `json:"inputs,omitempty"`

		// Outputs are datasets produced by this run (optional).
		// Typically specified in COMPLETE event.
		Outputs []Dataset `json:"outputs,omitempty"`
	}

	// EventType represents OpenLineage run states.
	// Spec: https://openlineage.io/docs/spec/run-cycle#run-states
	EventType string

	// Facets are extensible metadata common to inputs and outputs.
	// Standard facets: schema, dataSource, lifecycleStateChange, version
	// Spec: https://openlineage.io/docs/spec/facets/dataset-facets
	Facets map[string]interface{}

	// Run represents a single execution instance of a Job.
	// Each run has a uniquely identifiable runId (client-generated UUID).
	// The client is responsible for maintaining the runId between different run state updates.
	//
	// Spec: https://openlineage.io/docs/spec/object-model#run
	Run struct {
		// ID is a client-generated UUID that uniquely identifies this run.
		// Must be maintained throughout the run lifecycle (START → COMPLETE).
		// Recommended format: UUIDv7 (https://datatracker.ietf.org/doc/draft-ietf-uuidrev-rfc4122bis/)
		ID string `json:"runId"`

		// Facets are extensible metadata about this run instance.
		// Standard facets: nominalTime, parent, errorMessage, sql
		// Spec: https://openlineage.io/docs/spec/facets/run-facets
		Facets Facets `json:"facets"`
	}

	// Job represents a recurring data transformation process with inputs and outputs.
	// Examples: dbt model, Airflow task, Spark job, SQL query.
	//
	// Jobs are identified by a unique name within a namespace. They are expected to
	// evolve over time and their changes can be captured through run state updates.
	//
	// Spec: https://openlineage.io/docs/spec/object-model#job
	Job struct {
		// Namespace identifies the scheduler/orchestrator.
		// Format: Typically a URL (e.g., "airflow://production", "dbt://analytics")
		// Spec: https://openlineage.io/docs/spec/naming#job-naming
		Namespace string `json:"namespace"`

		// Name is unique within the namespace.
		// Examples: "daily_etl.load_orders" (Airflow), "transform_orders" (dbt)
		Name string `json:"name"`

		// Facets are extensible metadata about the job definition.
		// Standard facets: sourceCodeLocation, sourceCode, sql, jobType
		// Spec: https://openlineage.io/docs/spec/facets/job-facets
		Facets Facets `json:"facets"`
	}

	// Dataset represents an abstract data artifact: a table, file, topic, or directory.
	// Datasets have a unique name within a namespace derived from their physical location.
	//
	// The combined namespace and name should be enough to uniquely identify a dataset
	// within a data ecosystem.
	//
	// Spec: https://openlineage.io/docs/spec/object-model#dataset
	Dataset struct {
		// Namespace identifies the data source.
		// Format: {protocol}://{host}:{port} or {protocol}://{service_identifier}
		// Examples: "postgres://prod-db:5432", "s3://raw-data", "bigquery"
		// Spec: https://openlineage.io/docs/spec/naming#dataset-naming
		Namespace string `json:"namespace"`

		// Name is the hierarchical path to the dataset.
		// Examples: "analytics.public.orders" (PostgreSQL), "/orders/2025-10-18.parquet" (S3)
		Name string `json:"name"`

		// Facets are extensible metadata common to inputs and outputs.
		// Standard facets: schema, dataSource, lifecycleStateChange, version
		// Spec: https://openlineage.io/docs/spec/facets/dataset-facets
		Facets Facets `json:"facets,omitempty"`

		// InputFacets are metadata specific to input datasets.
		// Standard facets: dataQualityMetrics, dataQualityAssertions
		// Only populated when this dataset is an input.
		InputFacets Facets `json:"inputFacets,omitempty"`

		// OutputFacets are metadata specific to output datasets.
		// Standard facets: outputStatistics
		// Only populated when this dataset is an output.
		OutputFacets Facets `json:"outputFacets,omitempty"`
	}
)

const (
	// EventTypeStart indicates the beginning of a job execution.
	EventTypeStart EventType = "START"

	// EventTypeRunning provides additional information about a running job.
	EventTypeRunning EventType = "RUNNING"

	// EventTypeComplete signifies that execution of the job has concluded successfully.
	// Terminal state (idempotent).
	EventTypeComplete EventType = "COMPLETE"

	// EventTypeFail signifies that the job has failed.
	// Terminal state (idempotent).
	EventTypeFail EventType = "FAIL"

	// EventTypeAbort signifies that the job has been stopped abnormally.
	// Terminal state (idempotent).
	EventTypeAbort EventType = "ABORT"

	// EventTypeOther is used to send additional metadata outside standard run cycle.
	// Can be sent anytime, even before START.
	EventTypeOther EventType = "OTHER"
)

// ValidEventTypes returns all valid OpenLineage event types.
func ValidEventTypes() []EventType {
	return []EventType{
		EventTypeStart,
		EventTypeRunning,
		EventTypeComplete,
		EventTypeFail,
		EventTypeAbort,
		EventTypeOther,
	}
}

// IsValid checks if the EventType is a valid OpenLineage run state.
func (et EventType) IsValid() bool {
	for _, valid := range ValidEventTypes() {
		if et == valid {
			return true
		}
	}

	return false
}

// IsTerminal returns true if the event type is a terminal state.
// Terminal states (COMPLETE, FAIL, ABORT) are idempotent and cannot transition
// to other states.
//
// Spec: https://openlineage.io/docs/spec/run-cycle#run-states
func (et EventType) IsTerminal() bool {
	return et == EventTypeComplete || et == EventTypeFail || et == EventTypeAbort
}
