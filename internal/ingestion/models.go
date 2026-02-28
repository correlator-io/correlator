// Package ingestion provides OpenLineage domain models for event ingestion.
// Spec: https://openlineage.io/docs/spec/object-model
package ingestion

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/correlator-io/correlator/internal/canonicalization"
)

type (
	// RunEvent represents an OpenLineage RunEvent (runtime lineage) - Domain Model.
	// RunEvents describe the execution of a job and are emitted at runtime when jobs
	// start, run, or complete. Each RunEvent can include details about the Job, the Run,
	// and the input and output Datasets involved in the run.
	//
	// This is a pure domain model without JSON tags. The API layer uses LineageEventRequest
	// for JSON marshaling and maps to this domain type.
	//
	// Spec: https://openlineage.io/docs/spec/object-model#job-run-state-update
	RunEvent struct {
		// EventTime is the timestamp when this event occurred (RFC3339 format).
		// Use for ordering events, not arrival time (handles out-of-order events).
		EventTime time.Time

		// EventType is the run state: START, RUNNING, COMPLETE, FAIL, ABORT, or OTHER.
		// Terminal states (COMPLETE, FAIL, ABORT) are idempotent.
		EventType EventType

		// Producer identifies the tool that generated this event.
		// Format: URL with version (e.g., "https://github.com/dbt-labs/dbt-core/tree/1.5.0")
		Producer string

		// SchemaURL is the OpenLineage spec version URL.
		// Example: "https://openlineage.io/spec/2-0-2/OpenLineage.json"
		SchemaURL string

		// Run contains metadata about this specific run instance.
		Run Run

		// Job contains metadata about the job definition.
		Job Job

		// Inputs are datasets consumed by this run (optional).
		// Can be specified in START, COMPLETE, or both (events are accumulative).
		Inputs []Dataset

		// Outputs are datasets produced by this run (optional).
		// Typically specified in COMPLETE event.
		Outputs []Dataset
	}

	// EventType represents OpenLineage run states.
	// Spec: https://openlineage.io/docs/spec/run-cycle#run-states
	EventType string

	// Facets are extensible metadata common to inputs and outputs.
	// Standard facets: schema, dataSource, lifecycleStateChange, version
	// Spec: https://openlineage.io/docs/spec/facets/dataset-facets
	Facets map[string]interface{}

	// Run represents a single execution instance of a Job - Domain Model.
	// Each run has a uniquely identifiable runId (client-generated UUID).
	// The client is responsible for maintaining the runId between different run state updates.
	//
	// Spec: https://openlineage.io/docs/spec/object-model#run
	Run struct {
		// ID is a client-generated UUID that uniquely identifies this run.
		// Must be maintained throughout the run lifecycle (START → COMPLETE).
		// Recommended format: UUIDv7 (https://datatracker.ietf.org/doc/draft-ietf-uuidrev-rfc4122bis/)
		ID string

		// Facets are extensible metadata about this run instance.
		// Standard facets: nominalTime, parent, errorMessage, sql
		// Spec: https://openlineage.io/docs/spec/facets/run-facets
		Facets Facets
	}

	// Job represents a recurring data transformation process with inputs and outputs - Domain Model.
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
		Namespace string

		// Name is unique within the namespace.
		// Examples: "daily_etl.load_orders" (Airflow), "transform_orders" (dbt)
		Name string

		// Facets are extensible metadata about the job definition.
		// Standard facets: sourceCodeLocation, sourceCode, sql, jobType
		// Spec: https://openlineage.io/docs/spec/facets/job-facets
		Facets Facets
	}

	// Dataset represents an abstract data artifact: a table, file, topic, or directory - Domain Model.
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
		Namespace string

		// Name is the hierarchical path to the dataset.
		// Examples: "analytics.public.orders" (PostgreSQL), "/orders/2025-10-18.parquet" (S3)
		Name string

		// Facets are extensible metadata common to inputs and outputs.
		// Standard facets: schema, dataSource, lifecycleStateChange, version
		// Spec: https://openlineage.io/docs/spec/facets/dataset-facets
		Facets Facets

		// InputFacets are metadata specific to input datasets.
		// Standard facets: dataQualityMetrics, dataQualityAssertions
		// Only populated when this dataset is an input.
		InputFacets Facets

		// OutputFacets are metadata specific to output datasets.
		// Standard facets: outputStatistics
		// Only populated when this dataset is an output.
		OutputFacets Facets
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

// IdempotencyKey returns the idempotency key for this event.
//
// This key is used to detect duplicate events and prevent reprocessing.
// The key includes producer, job, run, eventTime, and eventType for uniqueness.
//
// Formula: SHA256(producer + job.namespace + job.name + run.runId + eventTime + eventType)
//
// Example:
//
//	event1 := RunEvent{...} // Same event sent twice
//	event2 := RunEvent{...} // Duplicate
//	event1.IdempotencyKey() == event2.IdempotencyKey()  // true (duplicate)
//
// Returns: 64-character lowercase hex string (SHA256 output).
func (e *RunEvent) IdempotencyKey() string {
	return canonicalization.GenerateIdempotencyKey(
		e.Producer,
		e.Job.Namespace,
		e.Job.Name,
		e.Run.ID,
		e.EventTime.Format("2006-01-02T15:04:05.999999999Z07:00"), // RFC3339Nano
		string(e.EventType),
	)
}

// URN returns the canonical URN for this dataset.
//
// Format: {namespace}/{name}
//
// Example:
//
//	dataset := Dataset{Namespace: "postgres://prod-db:5432", Name: "analytics.public.orders"}
//	dataset.URN()  // "postgres://prod-db:5432/analytics.public.orders"
//
// Returns: URN string.
func (d *Dataset) URN() string {
	return canonicalization.GenerateDatasetURN(d.Namespace, d.Name)
}

// ============================================================================
// Test Result Domain Models
// ============================================================================

type (
	// TestResult represents a data quality test execution result (domain model).
	// This is the domain representation extracted from dataQualityAssertions facets during OpenLineage event ingestion.
	//
	// Test results link test failures to the job runs that produced the tested datasets,
	// enabling incident correlation and root cause analysis.
	//
	// Design: Follows same pattern as RunEvent (pure domain model, not API contract).
	// Review Note: No constructor function - struct literals are idiomatic Go. Add constructor
	// only if needed for complex initialization/invariants.
	TestResult struct {
		// TestName uniquely identifies the test within the testing framework.
		// Examples: "test_column_not_null", "assert_unique_user_id", "row_count_threshold"
		// Max length: 750 chars (validated)
		TestName string

		// TestType categorizes the test for filtering and analysis.
		// Common values: "data_quality", "schema", "freshness", "volume", "distribution"
		// Review Note: No validation on TestType (no enum) - flexibility preferred over strictness
		// for MVP. Allows custom test types without breaking changes. API layer can apply
		// default if needed.
		TestType string

		// DatasetURN identifies the tested dataset (FK to datasets table).
		// Format: "<namespace>://<authority>/<path>"
		// Example: "postgres://prod-db:5432/analytics.users"
		// Must exist in datasets table (validated by storage layer).
		// Review Note: Basic URN validation (contains ":") is intentional. Complex validation
		// belongs in canonicalization package. Storage FK provides safety net.
		DatasetURN string

		// RunID is the OpenLineage run UUID (FK to job_runs table).
		// Format: UUID string (e.g., "550e8400-e29b-41d4-a716-446655440000")
		// CRITICAL: This is the correlation key linking test failures to producing jobs.
		RunID string

		// Status indicates the test outcome.
		// Valid values: "passed", "failed", "error", "skipped", "warning"
		// Only "failed" and "error" contribute to incident correlation.
		Status TestStatus

		// Message provides human-readable test failure details (optional).
		// Example: "Column 'user_id' contains 5 NULL values".
		Message string

		// Metadata stores test framework-specific context (optional, JSONB).
		// Structure varies by test framework:
		//   - dbt: {"assertion_type": "not_null", "column_name": "user_id", "row_count": 15743, "failed_rows": 0}
		//   - Great Expectations: {"expectation_type": "expect_column_values_to_be_unique", "unexpected_count": 2}
		//   - pytest: {"test_function": "test_user_validation", "assertions": [...]}
		//
		// Design: Single flexible field instead of expected_value/actual_value separation.
		// Rationale: Test frameworks don't have standardized expected/actual structure.
		// Different frameworks embed these concepts differently within their metadata.
		// A single flexible metadata field allows each framework to use its native format.
		Metadata map[string]interface{}

		// ExecutedAt is when the test was executed (not when ingested).
		// Used for temporal correlation and time-series analysis.
		ExecutedAt time.Time

		// DurationMs is the test execution time in milliseconds (optional).
		// Used for performance analysis and regression detection.
		DurationMs int

		// ProducerName identifies the tool that ran this test.
		// Examples: "great_expectations", "correlator-dbt", "correlator-ge", "soda"
		// Extracted from OpenLineage producer URL during ingestion.
		ProducerName string

		// ProducerVersion is the version of the testing tool.
		// Example: "0.18.0"
		ProducerVersion string

		// Facets stores the raw OpenLineage input facets from the validation event.
		// Preserves the complete facet blob (assertions, data quality metrics) for auditability.
		// Validator observations belong here, not in the datasets.facets column.
		Facets map[string]interface{}
	}

	// TestStatus represents valid test execution outcomes.
	// Only "failed" and "error" are considered incidents for correlation.
	TestStatus string
)

const (
	// TestStatusPassed indicates successful test validation.
	TestStatusPassed TestStatus = "passed"

	// TestStatusFailed indicates test assertion failed (data quality issue).
	TestStatusFailed TestStatus = "failed"

	// TestStatusError indicates test execution error (technical issue).
	TestStatusError TestStatus = "error"

	// TestStatusSkipped indicates test was not executed.
	TestStatusSkipped TestStatus = "skipped"

	// TestStatusWarning indicates test passed but with warnings.
	TestStatusWarning TestStatus = "warning"

	maxTestNameLength = 750
)

// Test result validation errors (static sentinel errors for errors.Is() checks).
var (
	// ErrTestNameEmpty indicates test_name is required.
	ErrTestNameEmpty = errors.New("test_name cannot be empty")

	// ErrTestNameTooLong indicates test_name exceeds max length (750 chars).
	ErrTestNameTooLong = errors.New("test_name cannot exceed 750 characters")

	// ErrDatasetURNEmpty indicates dataset_urn is required.
	ErrDatasetURNEmpty = errors.New("dataset_urn cannot be empty")

	// ErrDatasetURNInvalid indicates dataset_urn has invalid format.
	ErrDatasetURNInvalid = errors.New("dataset_urn must contain ':' separator")

	// ErrRunIDEmpty indicates run_id is required.
	ErrRunIDEmpty = errors.New("run_id cannot be empty")

	// ErrStatusInvalid indicates status is not a valid TestStatus.
	ErrStatusInvalid = errors.New("status must be one of: passed, failed, error, skipped, warning")

	// ErrExecutedAtZero indicates executed_at timestamp is required.
	ErrExecutedAtZero = errors.New("executed_at cannot be zero")

	// ErrDurationMsNegative indicates duration_ms cannot be negative.
	ErrDurationMsNegative = errors.New("duration_ms cannot be negative")
)

// Validate performs domain validation on the TestResult.
// Returns validation errors (not storage errors like FK violations).
//
// Validation rules:
//   - test_name: required, ≤750 chars
//   - dataset_urn: required, valid URN format (contains ":")
//   - run_id: required
//   - status: must be valid TestStatus
//   - executed_at: required (not zero)
//   - duration_ms: ≥0 if provided
//
// Storage-level validations (FK constraints, etc.) are handled by the storage layer.
func (tr *TestResult) Validate() error {
	// Validate test_name
	if strings.TrimSpace(tr.TestName) == "" {
		return ErrTestNameEmpty
	}

	if len(tr.TestName) > maxTestNameLength {
		return fmt.Errorf("%w: got %d characters", ErrTestNameTooLong, len(tr.TestName))
	}

	// Validate dataset_urn
	if strings.TrimSpace(tr.DatasetURN) == "" {
		return ErrDatasetURNEmpty
	}

	// Basic URN format check (must contain ":")
	if !strings.Contains(tr.DatasetURN, ":") {
		return fmt.Errorf("%w: '%s'", ErrDatasetURNInvalid, tr.DatasetURN)
	}

	// Validate run_id
	if strings.TrimSpace(tr.RunID) == "" {
		return ErrRunIDEmpty
	}

	// Validate status
	if !tr.Status.IsValid() {
		return fmt.Errorf("%w: got '%s'", ErrStatusInvalid, tr.Status)
	}

	// Validate executed_at
	if tr.ExecutedAt.IsZero() {
		return ErrExecutedAtZero
	}

	// Validate duration_ms
	if tr.DurationMs < 0 {
		return fmt.Errorf("%w: got %d", ErrDurationMsNegative, tr.DurationMs)
	}

	return nil
}

// String returns the string representation of TestStatus.
func (ts TestStatus) String() string {
	return string(ts)
}

// IsValid checks if the TestStatus is a valid enum value.
func (ts TestStatus) IsValid() bool {
	switch ts {
	case TestStatusPassed, TestStatusFailed, TestStatusError, TestStatusSkipped, TestStatusWarning:
		return true
	default:
		return false
	}
}

// IsIncident returns true if the test status represents an incident (failed or error).
// Only incident statuses are included in correlation analysis.
func (ts TestStatus) IsIncident() bool {
	return ts == TestStatusFailed || ts == TestStatusError
}
