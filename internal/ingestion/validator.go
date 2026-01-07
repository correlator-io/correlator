// Package ingestion provides OpenLineage event validation.
package ingestion

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Sentinel errors for validation failures.
var (
	ErrNilEvent                = errors.New("event cannot be nil")
	ErrInvalidEventType        = errors.New("invalid eventType")
	ErrMissingEventTime        = errors.New("eventTime is required")
	ErrMissingProducer         = errors.New("producer is required")
	ErrMissingSchemaURL        = errors.New("schemaURL is required")
	ErrInvalidSchemaURL        = errors.New("schemaURL must be an OpenLineage spec URL")
	ErrMissingRunID            = errors.New("run.runId is required")
	ErrMissingJobNamespace     = errors.New("job.namespace is required")
	ErrMissingJobName          = errors.New("job.name is required")
	ErrNilDataset              = errors.New("dataset cannot be nil")
	ErrDatasetMissingNamespace = errors.New("dataset.namespace is required")
	ErrDatasetMissingName      = errors.New("dataset.name is required")
)

// openLineageSchemaURLPattern is a pre-compiled regex for validating OpenLineage schema URLs.
// This is compiled once at package initialization to avoid repeated compilation overhead
// during validation of incoming events.
//
// The pattern validates that the URL:
//   - Starts with https://openlineage.io/spec/
//   - Contains a version in X-Y-Z format (e.g., 2-0-2, 1-8-0)
//   - Ends with /OpenLineage.json
var openLineageSchemaURLPattern = regexp.MustCompile(`^https://openlineage\.io/spec/\d+-\d+-\d+/OpenLineage\.json$`)

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

	// Validate schemaURL format (must be an OpenLineage spec URL)
	// We accept all OpenLineage versions (1.x, 2.x, etc.) to support heterogeneous producers
	if !IsValidOpenLineageSchemaURL(event.SchemaURL) {
		return fmt.Errorf("%w, got: %s", ErrInvalidSchemaURL, event.SchemaURL)
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

// ValidateDataset validates that a Dataset contains all required OpenLineage fields.
//
// Validation rules:
//   - Dataset must not be nil
//   - Namespace must not be empty (data source identifier)
//   - Name must not be empty (dataset path/identifier)
//
// Required fields (per OpenLineage v2 spec):
//   - namespace: Data source identifier (e.g., "postgres://prod-db:5432", "s3://bucket", "bigquery")
//   - name: Dataset path/identifier (e.g., "analytics.public.orders", "/path/to/file.parquet")
//
// URN format validation is deferred to the storage layer when URNs are generated.
// This separation of concerns ensures:
//   - Validator validates OpenLineage spec compliance (required fields, structure)
//   - Canonicalization validates URN format (namespace normalization, parsing)
//   - Storage layer handles URN generation errors gracefully
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

	return nil
}

// ExtractOpenLineageVersion extracts the version string from an OpenLineage schemaURL.
// Returns empty string if the URL is not a valid OpenLineage spec URL.
//
// Example:
//
//	ExtractOpenLineageVersion("https://openlineage.io/spec/2-0-2/OpenLineage.json")
//	// Returns: "2.0.2"
//
//	ExtractOpenLineageVersion("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent")
//	// Returns: "2.0.2"
//
// This function is useful for:
//   - Logging/metrics: Track which OpenLineage versions are being used
//   - Debugging: Identify version-specific behavior in production
//   - Observability: Alert when new major versions appear
//
// The version is extracted by parsing the URL path and converting hyphens to dots.
// JSON Schema fragments are stripped before extraction.
func ExtractOpenLineageVersion(schemaURL string) string {
	// Validate it's a valid OpenLineage spec URL
	if !IsValidOpenLineageSchemaURL(schemaURL) {
		return ""
	}

	// Strip JSON Schema fragment before extraction
	baseURL := schemaURL
	if idx := strings.Index(schemaURL, "#"); idx != -1 {
		baseURL = schemaURL[:idx]
	}

	// Remove prefix and suffix to get version: "2-0-2/OpenLineage.json" -> "2-0-2"
	remainder := strings.TrimPrefix(baseURL, "https://openlineage.io/spec/")
	versionWithHyphens := strings.TrimSuffix(remainder, "/OpenLineage.json")

	// Convert hyphens to dots: "2-0-2" -> "2.0.2"
	version := strings.ReplaceAll(versionWithHyphens, "-", ".")

	return version
}

// IsValidOpenLineageSchemaURL validates that a URL is a valid OpenLineage schema URL.
// It checks that the URL matches the expected format:
//   - Starts with https://openlineage.io/spec/
//   - Contains a version in X-Y-Z format (e.g., 2-0-2, 1-8-0)
//   - Ends with /OpenLineage.json
//
// This function uses a pre-compiled regex pattern for performance, avoiding
// regex compilation overhead on every validation call.
//
// JSON Schema fragment references (e.g., #/$defs/RunEvent) are stripped before
// validation, as they reference definitions within the schema document and are
// valid per RFC 3986. The official OpenLineage Python library produces URLs with
// these fragments.
//
// Examples:
//
//	IsValidOpenLineageSchemaURL("https://openlineage.io/spec/2-0-2/OpenLineage.json")                    // true
//	IsValidOpenLineageSchemaURL("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent")    // true
//	IsValidOpenLineageSchemaURL("https://openlineage.io/spec/1-8-0/OpenLineage.json")                    // true
//	IsValidOpenLineageSchemaURL("https://example.com/schema.json")                                       // false
//	IsValidOpenLineageSchemaURL("https://openlineage.io/spec/")                                          // false
//	IsValidOpenLineageSchemaURL("https://openlineage.io/spec/garbage")                                   // false
func IsValidOpenLineageSchemaURL(url string) bool {
	// Strip JSON Schema fragment (everything after #) before validation.
	// Fragments like #/$defs/RunEvent reference definitions within the schema
	// and are produced by the official OpenLineage Python client library.
	baseURL := url
	if idx := strings.Index(url, "#"); idx != -1 {
		baseURL = url[:idx]
	}

	return openLineageSchemaURLPattern.MatchString(baseURL)
}
