// Package ingestion provides OpenLineage event validation.
package ingestion

import (
	"errors"
	"testing"
	"time"
)

// ==============================================================================
// Unit Tests: Valid Events (Should Pass)
// ==============================================================================

func TestValidateRunEvent_Complete(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeComplete,
		Producer:  "https://github.com/dbt-labs/dbt-core/tree/1.5.0",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID:     "550e8400-e29b-41d4-a716-446655440000",
			Facets: Facets{},
		},
		Job: Job{
			Namespace: "dbt://analytics",
			Name:      "transform_orders",
			Facets:    Facets{},
		},
		Inputs: []Dataset{
			{
				Namespace: "postgres://prod-db:5432",
				Name:      "raw.public.orders",
			},
		},
		Outputs: []Dataset{
			{
				Namespace: "postgres://prod-db:5432",
				Name:      "analytics.public.orders",
			},
		},
	}

	err := validator.ValidateRunEvent(event)
	if err != nil {
		t.Errorf("ValidateRunEvent() failed for valid dbt event: %v", err)
	}
}

func TestValidateRunEvent_Start(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "https://github.com/OpenLineage/OpenLineage/tree/0.30.0/integration/airflow",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID:     "airflow-run-id",
			Facets: Facets{},
		},
		Job: Job{
			Namespace: "airflow://production",
			Name:      "daily_etl.load_users",
			Facets:    Facets{},
		},
	}

	err := validator.ValidateRunEvent(event)
	if err != nil {
		t.Errorf("ValidateRunEvent() failed for valid Airflow event: %v", err)
	}
}

func TestValidateRunEvent_Fail(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeFail,
		Producer:  "https://github.com/OpenLineage/OpenLineage/tree/0.30.0/integration/spark",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID:     "spark-run-id",
			Facets: Facets{},
		},
		Job: Job{
			Namespace: "spark://prod-cluster",
			Name:      "recommendation.train_model",
			Facets:    Facets{},
		},
		Inputs: []Dataset{
			{
				Namespace: "hdfs://namenode:8020",
				Name:      "/data/training/features.parquet",
			},
		},
	}

	err := validator.ValidateRunEvent(event)
	if err != nil {
		t.Errorf("ValidateRunEvent() failed for valid Spark event: %v", err)
	}
}

func TestValidateRunEvent_ValidMinimalEvent(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	// This is a minimal run event. According to the openlineage schema v2,
	// this event must have "eventTime", "producer", "schemaURL", "run", "job" fields.
	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeOther,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "minimal-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err != nil {
		t.Errorf("ValidateRunEvent() failed for minimal valid event: %v", err)
	}
}

// ==============================================================================
// Unit Tests: Missing Required Fields (Should Fail)
// ==============================================================================

func TestValidateRunEvent_MissingEventTime(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		// EventTime: missing (zero value)
		EventType: EventTypeStart,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for missing eventTime")
	}

	expectedMsg := "eventTime is required"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestValidateRunEvent_InvalidEventType(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: "INVALID_STATE", // Invalid event type
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for invalid eventType")
	}

	// Error should mention valid event types
	if err != nil && err.Error() == "" {
		t.Error("Error message should not be empty")
	}
}

func TestValidateRunEvent_EmptyEventType(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: "", // Empty event type
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for empty eventType")
	}
}

func TestValidateRunEvent_MissingProducer(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "", // Missing producer
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for missing producer")
	}

	expectedMsg := "producer is required"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestValidateRunEvent_MissingSchemaURL(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "https://example.com/producer",
		SchemaURL: "", // Missing schemaURL
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for missing schemaURL")
	}

	expectedMsg := "schemaURL is required"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestValidateRunEvent_InvalidSchemaURL(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	tests := []struct {
		name      string
		schemaURL string
		wantError bool
		errorMsg  string
	}{
		{
			name:      "not an OpenLineage URL",
			schemaURL: "https://example.com/schema.json",
			wantError: true,
			errorMsg:  "schemaURL must be an OpenLineage spec URL",
		},
		{
			name:      "malformed URL",
			schemaURL: "https://openlineage.io/spec/hacked",
			wantError: true,
			errorMsg:  "schemaURL must be an OpenLineage spec URL",
		},
		{
			name:      "valid OpenLineage 2.0.2",
			schemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
			wantError: false,
		},
		{
			name:      "valid OpenLineage 1.8.0",
			schemaURL: "https://openlineage.io/spec/1-8-0/OpenLineage.json",
			wantError: false,
		},
		{
			name:      "valid OpenLineage 2.0.1",
			schemaURL: "https://openlineage.io/spec/2-0-1/OpenLineage.json",
			wantError: false,
		},
		{
			name:      "valid OpenLineage 0.9.0 (pre-release)",
			schemaURL: "https://openlineage.io/spec/0-9-0/OpenLineage.json",
			wantError: false,
		},
		// JSON Schema fragment tests (official OpenLineage Python client format)
		{
			name:      "valid OpenLineage 2.0.2 with RunEvent fragment",
			schemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
			wantError: false,
		},
		{
			name:      "valid OpenLineage 2.0.2 with DatasetEvent fragment",
			schemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent",
			wantError: false,
		},
		{
			name:      "valid OpenLineage 2.0.2 with JobEvent fragment",
			schemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &RunEvent{
				EventTime: time.Now().UTC(),
				EventType: EventTypeStart,
				Producer:  "https://example.com/producer",
				SchemaURL: tt.schemaURL,
				Run: Run{
					ID: "test-run-id",
				},
				Job: Job{
					Namespace: "test://namespace",
					Name:      "test_job",
				},
			}

			err := validator.ValidateRunEvent(event)

			switch tt.wantError {
			case true:
				if err == nil {
					t.Errorf("ValidateRunEvent() should fail for schemaURL: %s", tt.schemaURL)
				}
			case false:
				if err != nil {
					t.Errorf("ValidateRunEvent() should succeed for valid schemaURL: %s, got error: %v", tt.schemaURL, err)
				}
			}
		})
	}
}

func TestValidateRunEvent_MissingRunID(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "", // Missing runId
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for missing run.runId")
	}

	expectedMsg := "run.runId is required"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestValidateRunEvent_MissingJobNamespace(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "", // Missing namespace
			Name:      "test_job",
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for missing job.namespace")
	}

	expectedMsg := "job.namespace is required"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestValidateRunEvent_MissingJobName(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "", // Missing name
		},
	}

	err := validator.ValidateRunEvent(event)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for missing job.name")
	}

	expectedMsg := "job.name is required"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// ==============================================================================
// Unit Tests: All Event Types
// ==============================================================================

func TestValidateRunEvent_AllEventTypes(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	eventTypes := []EventType{
		EventTypeStart,
		EventTypeRunning,
		EventTypeComplete,
		EventTypeFail,
		EventTypeAbort,
		EventTypeOther,
	}

	for _, eventType := range eventTypes {
		t.Run(string(eventType), func(t *testing.T) {
			event := &RunEvent{
				EventTime: time.Now().UTC(),
				EventType: eventType,
				Producer:  "https://example.com/producer",
				SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
				Run: Run{
					ID: "test-run-id",
				},
				Job: Job{
					Namespace: "test://namespace",
					Name:      "test_job",
				},
			}

			err := validator.ValidateRunEvent(event)
			if err != nil {
				t.Errorf("ValidateRunEvent() failed for eventType %s: %v", eventType, err)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: Edge Cases
// ==============================================================================

func TestValidateRunEvent_NilEvent(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	err := validator.ValidateRunEvent(nil)
	if err == nil {
		t.Error("ValidateRunEvent() should fail for nil event")
	}
}

func TestValidateRunEvent_EmptyInputsOutputs(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	// Valid event with no inputs/outputs (allowed for START/OTHER events)
	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
		},
		Inputs:  []Dataset{},
		Outputs: []Dataset{},
	}

	err := validator.ValidateRunEvent(event)
	if err != nil {
		t.Errorf("ValidateRunEvent() should allow empty inputs/outputs: %v", err)
	}
}

func TestValidateRunEvent_NilFacets(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	// Valid event with nil facets (allowed)
	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeStart,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID:     "test-run-id",
			Facets: nil, // Nil facets should be allowed
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
			Facets:    nil,
		},
	}

	err := validator.ValidateRunEvent(event)
	if err != nil {
		t.Errorf("ValidateRunEvent() should allow nil facets: %v", err)
	}
}

func TestValidateRunEvent_WithUnknownFacets(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	// Event with custom/unknown facets (should be allowed - OpenLineage extensibility)
	event := &RunEvent{
		EventTime: time.Now().UTC(),
		EventType: EventTypeComplete,
		Producer:  "https://example.com/producer",
		SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		Run: Run{
			ID: "test-run-id",
			Facets: Facets{
				"customFacet": map[string]interface{}{
					"customField": "customValue",
				},
			},
		},
		Job: Job{
			Namespace: "test://namespace",
			Name:      "test_job",
			Facets: Facets{
				"anotherCustomFacet": "value",
			},
		},
	}

	err := validator.ValidateRunEvent(event)
	if err != nil {
		t.Errorf("ValidateRunEvent() should allow unknown facets: %v", err)
	}
}

// ==============================================================================
// Unit Tests: Dataset Validation (Advanced Format Validation)
// ==============================================================================

func TestValidateDataset_ValidPostgreSQL(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	dataset := &Dataset{
		Namespace: "postgres://prod-db:5432",
		Name:      "analytics.public.orders",
	}

	err := validator.ValidateDataset(dataset)
	if err != nil {
		t.Errorf("ValidateDataset() failed for valid PostgreSQL dataset: %v", err)
	}
}

func TestValidateDataset_ValidS3(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	dataset := &Dataset{
		Namespace: "s3://bucket",
		Name:      "/path/to/file.parquet",
	}

	err := validator.ValidateDataset(dataset)
	if err != nil {
		t.Errorf("ValidateDataset() failed for valid S3 dataset: %v", err)
	}
}

func TestValidateDataset_ValidBigQuery(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	dataset := &Dataset{
		Namespace: "bigquery",
		Name:      "project.dataset.table",
	}

	err := validator.ValidateDataset(dataset)
	if err != nil {
		t.Errorf("ValidateDataset() failed for valid BigQuery dataset: %v", err)
	}
}

func TestValidateDataset_NilDataset(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	err := validator.ValidateDataset(nil)
	if !errors.Is(err, ErrNilDataset) {
		t.Errorf("ValidateDataset(nil) should return ErrNilDataset, got %v", err)
	}
}

func TestValidateDataset_MissingNamespace(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	dataset := &Dataset{
		Namespace: "",
		Name:      "analytics.public.orders",
	}

	err := validator.ValidateDataset(dataset)
	if !errors.Is(err, ErrDatasetMissingNamespace) {
		t.Errorf("ValidateDataset() should return ErrDatasetMissingNamespace for empty namespace, got %v", err)
	}
}

func TestValidateDataset_MissingName(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	dataset := &Dataset{
		Namespace: "postgres://prod-db:5432",
		Name:      "",
	}

	err := validator.ValidateDataset(dataset)
	if !errors.Is(err, ErrDatasetMissingName) {
		t.Errorf("ValidateDataset() should return ErrDatasetMissingName for empty name, got %v", err)
	}
}

func TestValidateDataset_ValidMultipleDataSources(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	testCases := []struct {
		name      string
		dataset   *Dataset
		shouldErr bool
	}{
		{
			name: "PostgreSQL",
			dataset: &Dataset{
				Namespace: "postgres://prod-db:5432",
				Name:      "analytics.public.orders",
			},
			shouldErr: false,
		},
		{
			name: "BigQuery",
			dataset: &Dataset{
				Namespace: "bigquery",
				Name:      "project.dataset.table",
			},
			shouldErr: false,
		},
		{
			name: "S3",
			dataset: &Dataset{
				Namespace: "s3://bucket",
				Name:      "/path/to/file.parquet",
			},
			shouldErr: false,
		},
		{
			name: "Snowflake",
			dataset: &Dataset{
				Namespace: "snowflake://org-account",
				Name:      "analytics.public.customers",
			},
			shouldErr: false,
		},
		{
			name: "Kafka",
			dataset: &Dataset{
				Namespace: "kafka://broker:9092",
				Name:      "user-events",
			},
			shouldErr: false,
		},
		{
			name: "HDFS",
			dataset: &Dataset{
				Namespace: "hdfs://namenode:8020",
				Name:      "/data/warehouse/orders",
			},
			shouldErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.ValidateDataset(tc.dataset)
			if tc.shouldErr && err == nil {
				t.Errorf("ValidateDataset() should return error for %s", tc.name)
			}

			if !tc.shouldErr && err != nil {
				t.Errorf("ValidateDataset() should not return error for %s: %v", tc.name, err)
			}
		})
	}
}

func TestValidateDataset_SpecialCharactersInName(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	testCases := []string{
		"table with spaces",
		"table-with-hyphens",
		"table_with_underscores",
		"table.with.dots",
		"schema.table.column",
		"数据表_中文", //nolint: gosmopolitan
	}

	for _, name := range testCases {
		t.Run(name, func(t *testing.T) {
			dataset := &Dataset{
				Namespace: "postgres://db:5432",
				Name:      name,
			}

			err := validator.ValidateDataset(dataset)
			if err != nil {
				t.Errorf("ValidateDataset() should allow special characters in name %q: %v", name, err)
			}
		})
	}
}

func TestValidateDataset_WithFacets(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validator := NewValidator()

	dataset := &Dataset{
		Namespace: "postgres://prod-db:5432",
		Name:      "analytics.public.orders",
		Facets: Facets{
			"schema": map[string]interface{}{
				"fields": []interface{}{
					map[string]interface{}{"name": "id", "type": "INTEGER"},
					map[string]interface{}{"name": "amount", "type": "DECIMAL"},
				},
			},
		},
		InputFacets: Facets{
			"dataQualityMetrics": map[string]interface{}{
				"rowCount": 1000,
			},
		},
		OutputFacets: Facets{
			"outputStatistics": map[string]interface{}{
				"rowCount": 950,
			},
		},
	}

	err := validator.ValidateDataset(dataset)
	if err != nil {
		t.Errorf("ValidateDataset() should allow facets: %v", err)
	}
}

// ==============================================================================
// Unit Tests: IsValidOpenLineageSchemaURL Helper
// ==============================================================================

func TestIsValidOpenLineageSchemaURL(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name  string
		url   string
		valid bool
	}{
		{
			name:  "valid 2.0.2",
			url:   "https://openlineage.io/spec/2-0-2/OpenLineage.json",
			valid: true,
		},
		{
			name:  "valid 1.8.0",
			url:   "https://openlineage.io/spec/1-8-0/OpenLineage.json",
			valid: true,
		},
		{
			name:  "valid 0.9.0",
			url:   "https://openlineage.io/spec/0-9-0/OpenLineage.json",
			valid: true,
		},
		{
			name:  "valid 10.20.30 (multi-digit)",
			url:   "https://openlineage.io/spec/10-20-30/OpenLineage.json",
			valid: true,
		},
		// JSON Schema fragment tests (official OpenLineage Python client format)
		{
			name:  "valid with RunEvent fragment",
			url:   "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
			valid: true,
		},
		{
			name:  "valid with DatasetEvent fragment",
			url:   "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent",
			valid: true,
		},
		{
			name:  "valid with JobEvent fragment",
			url:   "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
			valid: true,
		},
		{
			name:  "valid with fragment and different version",
			url:   "https://openlineage.io/spec/1-8-0/OpenLineage.json#/$defs/RunEvent",
			valid: true,
		},
		{
			name:  "invalid - not OpenLineage domain",
			url:   "https://example.com/spec/2-0-2/OpenLineage.json",
			valid: false,
		},
		{
			name:  "invalid - missing version",
			url:   "https://openlineage.io/spec/OpenLineage.json",
			valid: false,
		},
		{
			name:  "invalid - malformed version (dots instead of hyphens)",
			url:   "https://openlineage.io/spec/2.0.2/OpenLineage.json",
			valid: false,
		},
		{
			name:  "invalid - incomplete version",
			url:   "https://openlineage.io/spec/2-0/OpenLineage.json",
			valid: false,
		},
		{
			name:  "invalid - garbage after prefix",
			url:   "https://openlineage.io/spec/garbage",
			valid: false,
		},
		{
			name:  "invalid - only prefix",
			url:   "https://openlineage.io/spec/",
			valid: false,
		},
		{
			name:  "invalid - empty string",
			url:   "",
			valid: false,
		},
		{
			name:  "invalid - malformed URL",
			url:   "not-a-url",
			valid: false,
		},
		{
			name:  "invalid - missing /OpenLineage.json",
			url:   "https://openlineage.io/spec/2-0-2",
			valid: false,
		},
		{
			name:  "invalid - wrong file name",
			url:   "https://openlineage.io/spec/2-0-2/Schema.json",
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidOpenLineageSchemaURL(tt.url)

			if result != tt.valid {
				t.Errorf("IsValidOpenLineageSchemaURL(%q) = %v, want %v",
					tt.url, result, tt.valid)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: ExtractOpenLineageVersion Helper
// ==============================================================================

func TestExtractOpenLineageVersion(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name            string
		schemaURL       string
		expectedVersion string
	}{
		{
			name:            "version 2.0.2",
			schemaURL:       "https://openlineage.io/spec/2-0-2/OpenLineage.json",
			expectedVersion: "2.0.2",
		},
		{
			name:            "version 1.8.0",
			schemaURL:       "https://openlineage.io/spec/1-8-0/OpenLineage.json",
			expectedVersion: "1.8.0",
		},
		{
			name:            "version 0.9.0",
			schemaURL:       "https://openlineage.io/spec/0-9-0/OpenLineage.json",
			expectedVersion: "0.9.0",
		},
		{
			name:            "version 3.0.0 (future)",
			schemaURL:       "https://openlineage.io/spec/3-0-0/OpenLineage.json",
			expectedVersion: "3.0.0",
		},
		// JSON Schema fragment tests (official OpenLineage Python client format)
		{
			name:            "version 2.0.2 with RunEvent fragment",
			schemaURL:       "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
			expectedVersion: "2.0.2",
		},
		{
			name:            "version 1.8.0 with DatasetEvent fragment",
			schemaURL:       "https://openlineage.io/spec/1-8-0/OpenLineage.json#/$defs/DatasetEvent",
			expectedVersion: "1.8.0",
		},
		{
			name:            "version 2.0.2 with JobEvent fragment",
			schemaURL:       "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent",
			expectedVersion: "2.0.2",
		},
		{
			name:            "invalid URL",
			schemaURL:       "https://example.com/schema.json",
			expectedVersion: "",
		},
		{
			name:            "malformed URL",
			schemaURL:       "not-a-url",
			expectedVersion: "",
		},
		{
			name:            "empty string",
			schemaURL:       "",
			expectedVersion: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := ExtractOpenLineageVersion(tt.schemaURL)

			if version != tt.expectedVersion {
				t.Errorf("ExtractOpenLineageVersion(%q) = %q, want %q",
					tt.schemaURL, version, tt.expectedVersion)
			}
		})
	}
}
