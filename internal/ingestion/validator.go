// Package ingestion provides OpenLineage event validation.
package ingestion

import (
	"errors"
	"fmt"

	"github.com/correlator-io/correlator/internal/canonicalization"
)

// Sentinel errors for validation failures.
var (
	ErrNilEvent               = errors.New("event cannot be nil")
	ErrInvalidEventType       = errors.New("invalid eventType")
	ErrMissingEventTime       = errors.New("eventTime is required")
	ErrMissingProducer        = errors.New("producer is required")
	ErrMissingSchemaURL       = errors.New("schemaURL is required")
	ErrMissingRunID           = errors.New("run.runId is required")
	ErrMissingJobNamespace    = errors.New("job.namespace is required")
	ErrMissingJobName         = errors.New("job.name is required")
	ErrNilDataset             = errors.New("dataset cannot be nil")
	ErrDatasetMissingNamespace = errors.New("dataset.namespace is required")
	ErrDatasetMissingName     = errors.New("dataset.name is required")
	ErrDatasetInvalidURN      = errors.New("dataset URN format is invalid")
)

// Validator performs semantic validation of OpenLineage RunEvents.
// Validation strategy follows ADR 001: semantic validation (unmarshal + business rules)
// rather than formal JSON schema validation due to OpenLineage schema complexity.
//
// Performance: ~5µs per event validation (232K events/sec throughput)
// See: docs/adr/001-openlineage-validation-strategy.md.
type Validator struct{}

// NewValidator creates a new Validator instance.
func NewValidator() *Validator {
	return &Validator{}
}

// ValidateBaseEvent validates that a RunEvent contains all required OpenLineage fields in the BaseEvent as
// per OpenLineage v2 spec.
//
// Required fields (per OpenLineage v2 spec):
//   - eventTime: Must not be zero value
//   - producer: Must not be empty
//   - schemaURL: Must not be empty
//
// The required fields in the base event apply to RunEvent, JobEvent, DatasetEvent.
func (v *Validator) ValidateBaseEvent(event *RunEvent) error {
	// Handle nil event
	if event == nil {
		return ErrNilEvent
	}

	// Validate eventType (required, must be valid)
	if !event.EventType.IsValid() {
		return fmt.Errorf(
			"%w: %s (valid: START, RUNNING, COMPLETE, FAIL, ABORT, OTHER)",
			ErrInvalidEventType, event.EventType,
		)
	}

	// Validate eventTime (required)
	if event.EventTime.IsZero() {
		return ErrMissingEventTime
	}

	// Validate producer (required)
	if event.Producer == "" {
		return ErrMissingProducer
	}

	// Validate schemaURL (required)
	if event.SchemaURL == "" {
		return ErrMissingSchemaURL
	}

	return nil
}

// ValidateRunEvent validates that a RunEvent contains all required OpenLineage fields
// and satisfies business rules.
//
// Required fields (per OpenLineage v2 spec):
//   - eventTime: Must not be zero value
//   - eventType: Must be valid OpenLineage event type (START, RUNNING, COMPLETE, FAIL, ABORT, OTHER)
//   - producer: Must not be empty
//   - run.runId: Must not be empty
//   - job.namespace: Must not be empty
//   - job.name: Must not be empty
//
// Optional fields:
//   - inputs: May be empty or nil (especially for START/OTHER events)
//   - outputs: May be empty or nil
//   - facets: May be nil or contain unknown facets (extensibility)
//
// Returns nil if valid, error with descriptive message if validation fails.
func (v *Validator) ValidateRunEvent(event *RunEvent) error {
	// Validate the required fields in the base event specified in OpenLineage v2 spec
	if err := v.ValidateBaseEvent(event); err != nil {
		return err
	}

	// Validate run.runId (required)
	if event.Run.ID == "" {
		return ErrMissingRunID
	}

	// Validate job.namespace (required)
	if event.Job.Namespace == "" {
		return ErrMissingJobNamespace
	}

	// Validate job.name (required)
	if event.Job.Name == "" {
		return ErrMissingJobName
	}

	return nil
}

// ValidateDataset validates that a Dataset contains all required OpenLineage fields
// and satisfies URN format requirements.
//
// Validation includes:
//   - Required field validation (namespace, name must not be empty)
//   - Advanced URN format validation (round-trip parse check)
//
// Required fields (per OpenLineage v2 spec):
//   - namespace: Must not be empty (data source identifier)
//   - name: Must not be empty (dataset path/identifier)
//
// URN format validation:
//   - URN must contain "/" delimiter
//   - URN must parse correctly (namespace and name must be recoverable)
//   - Prevents malformed URNs like "namespace:" or "namespace//" from reaching database
//
// Examples of valid datasets:
//   - {Namespace: "postgres://prod-db:5432", Name: "analytics.public.orders"}
//   - {Namespace: "s3://bucket", Name: "/path/to/file.parquet"}
//   - {Namespace: "bigquery", Name: "project.dataset.table"}
//
// Returns nil if valid, error with descriptive message if validation fails.
func (v *Validator) ValidateDataset(dataset *Dataset) error {
	// Handle nil dataset
	if dataset == nil {
		return ErrNilDataset
	}

	// Validate namespace (required)
	if dataset.Namespace == "" {
		return ErrDatasetMissingNamespace
	}

	// Validate name (required)
	if dataset.Name == "" {
		return ErrDatasetMissingName
	}

	// Advanced URN format validation
	// Generate URN from dataset fields
	urn := canonicalization.GenerateDatasetURN(dataset.Namespace, dataset.Name)

	// Attempt to parse URN back - this validates format
	namespace, name, err := canonicalization.ParseDatasetURN(urn)
	if err != nil {
		return fmt.Errorf("%w: %w (namespace=%q, name=%q, urn=%q)",
			ErrDatasetInvalidURN, err, dataset.Namespace, dataset.Name, urn)
	}

	// Verify round-trip: parsed values should match original
	if namespace != dataset.Namespace || name != dataset.Name {
		return fmt.Errorf("%w: round-trip mismatch (original: namespace=%q name=%q, parsed: namespace=%q name=%q)",
			ErrDatasetInvalidURN, dataset.Namespace, dataset.Name, namespace, name)
	}

	return nil
}
