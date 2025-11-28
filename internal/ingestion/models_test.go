package ingestion

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunEvent_DBTExample(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Load from testdata
	data, err := os.ReadFile(filepath.Join("testdata", "dbt_complete_event.json"))
	if err != nil {
		t.Fatalf("Failed to read testdata file: %v", err)
	}

	var event RunEvent

	err = json.Unmarshal(data, &event)
	if err != nil {
		t.Fatalf("Failed to unmarshal dbt event: %v", err)
	}

	// Validate it's a dbt event
	if event.Job.Namespace != "dbt://analytics" {
		t.Errorf("Expected dbt namespace, got %s", event.Job.Namespace)
	}

	// Validate event type
	if event.EventType != EventTypeComplete {
		t.Errorf("Expected eventType COMPLETE, got %s", event.EventType)
	}

	// Validate inputs
	if len(event.Inputs) != 1 {
		t.Errorf("Expected 1 input, got %d", len(event.Inputs))
	}

	if len(event.Inputs) > 0 {
		if event.Inputs[0].Namespace != "postgres://prod-db:5432" {
			t.Errorf("Expected input namespace postgres://prod-db:5432, got %s", event.Inputs[0].Namespace)
		}

		if event.Inputs[0].Name != "raw.public.orders" {
			t.Errorf("Expected input name raw.public.orders, got %s", event.Inputs[0].Name)
		}
	}

	// Validate outputs
	if len(event.Outputs) != 1 {
		t.Errorf("Expected 1 output, got %d", len(event.Outputs))
	}

	if len(event.Outputs) > 0 {
		if event.Outputs[0].Namespace != "postgres://prod-db:5432" {
			t.Errorf("Expected output namespace postgres://prod-db:5432, got %s", event.Outputs[0].Namespace)
		}

		if event.Outputs[0].Name != "analytics.public.orders" {
			t.Errorf("Expected output name analytics.public.orders, got %s", event.Outputs[0].Name)
		}
		// Validate output facets
		if event.Outputs[0].OutputFacets == nil {
			t.Error("Expected outputFacets to be present")
		}
	}

	// Validate run facets
	if event.Run.Facets == nil {
		t.Error("Expected run facets to be present")
	}

	if sqlFacet, ok := event.Run.Facets["sql"]; ok {
		sqlMap, ok := sqlFacet.(map[string]interface{})
		if !ok {
			t.Error("Expected sql facet to be a map")
		}

		if query, ok := sqlMap["query"]; ok {
			if query != "SELECT * FROM raw.orders WHERE amount > 100" {
				t.Errorf("Expected SQL query, got %v", query)
			}
		}
	}

	// Validate eventTime parsing
	expectedTime, _ := time.Parse(time.RFC3339, "2025-10-21T10:05:00Z")
	if !event.EventTime.Equal(expectedTime) {
		t.Errorf("Expected eventTime %v, got %v", expectedTime, event.EventTime)
	}
}

func TestRunEvent_AirflowExample(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Load from testdata
	data, err := os.ReadFile(filepath.Join("testdata", "airflow_start_event.json"))
	if err != nil {
		t.Fatalf("Failed to read testdata file: %v", err)
	}

	var event RunEvent

	err = json.Unmarshal(data, &event)
	if err != nil {
		t.Fatalf("Failed to unmarshal airflow event: %v", err)
	}

	// Validate it's an Airflow event
	if event.Job.Namespace != "airflow://production" {
		t.Errorf("Expected airflow namespace, got %s", event.Job.Namespace)
	}

	if event.EventType != EventTypeStart {
		t.Errorf("Expected START event, got %s", event.EventType)
	}

	// Validate parent facet exists
	if event.Run.Facets == nil {
		t.Error("Expected run facets to contain parent facet")
	} else {
		if _, ok := event.Run.Facets["parent"]; !ok {
			t.Error("Expected parent facet in Airflow event")
		}
	}
}

func TestRunEvent_SparkExample(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Load from testdata
	data, err := os.ReadFile(filepath.Join("testdata", "spark_fail_event.json"))
	if err != nil {
		t.Fatalf("Failed to read testdata file: %v", err)
	}

	var event RunEvent

	err = json.Unmarshal(data, &event)
	if err != nil {
		t.Fatalf("Failed to unmarshal spark event: %v", err)
	}

	// Validate it's a Spark event
	if event.Job.Namespace != "spark://prod-cluster" {
		t.Errorf("Expected spark namespace, got %s", event.Job.Namespace)
	}

	if event.EventType != EventTypeFail {
		t.Errorf("Expected FAIL event, got %s", event.EventType)
	}

	// Validate error message facet exists
	if event.Run.Facets == nil {
		t.Error("Expected run facets to contain errorMessage facet")
	} else {
		if _, ok := event.Run.Facets["errorMessage"]; !ok {
			t.Error("Expected errorMessage facet in Spark FAIL event")
		}
	}
}

func TestEventType_IsValid(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name      string
		eventType EventType
		want      bool
	}{
		{"START is valid", EventTypeStart, true},
		{"RUNNING is valid", EventTypeRunning, true},
		{"COMPLETE is valid", EventTypeComplete, true},
		{"FAIL is valid", EventTypeFail, true},
		{"ABORT is valid", EventTypeAbort, true},
		{"OTHER is valid", EventTypeOther, true},
		{"INVALID is not valid", EventType("INVALID"), false},
		{"Empty is not valid", EventType(""), false},
		{"Lowercase is not valid", EventType("start"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.eventType.IsValid()
			if got != tt.want {
				t.Errorf("EventType.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventType_IsTerminal(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name      string
		eventType EventType
		want      bool
	}{
		{"START is not terminal", EventTypeStart, false},
		{"RUNNING is not terminal", EventTypeRunning, false},
		{"COMPLETE is terminal", EventTypeComplete, true},
		{"FAIL is terminal", EventTypeFail, true},
		{"ABORT is terminal", EventTypeAbort, true},
		{"OTHER is not terminal", EventTypeOther, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.eventType.IsTerminal()
			if got != tt.want {
				t.Errorf("EventType.IsTerminal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidEventTypes(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	validTypes := ValidEventTypes()

	// Should return exactly 6 event types
	if len(validTypes) != 6 {
		t.Errorf("Expected 6 valid event types, got %d", len(validTypes))
	}

	// Should contain all standard OpenLineage event types
	expected := map[EventType]bool{
		EventTypeStart:    false,
		EventTypeRunning:  false,
		EventTypeComplete: false,
		EventTypeFail:     false,
		EventTypeAbort:    false,
		EventTypeOther:    false,
	}

	for _, et := range validTypes {
		if _, ok := expected[et]; ok {
			expected[et] = true
		}
	}

	for et, found := range expected {
		if !found {
			t.Errorf("Expected event type %s not found in ValidEventTypes()", et)
		}
	}
}

// ============================================================================
// Test Result Domain Model Tests
// ============================================================================

// TestTestResult_Validate tests domain validation rules for TestResult.
func TestTestResult_Validate(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	t.Run("ValidTestResult", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_column_not_null",
			TestType:   "data_quality",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123-def",
			Status:     TestStatusFailed,
			Message:    "Column contains NULL values",
			ExecutedAt: time.Now(),
			DurationMs: 150,
			Metadata: map[string]interface{}{
				"column": "user_id",
			},
		}

		err := tr.Validate()
		assert.NoError(t, err, "Valid test result should pass validation")
	})

	t.Run("ValidTestResultWithDefaults", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "minimal_test",
			DatasetURN: "s3://bucket/path",
			JobRunID:   "airflow:run-123",
			Status:     TestStatusPassed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		assert.NoError(t, err, "Minimal valid test result should pass")
	})

	t.Run("EmptyTestName", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err, "Empty test_name should fail validation")
		assert.True(t, errors.Is(err, ErrTestNameEmpty), "Should return ErrTestNameEmpty") //nolint:testifylint
	})

	t.Run("WhitespaceOnlyTestName", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "   ",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err, "Whitespace-only test_name should fail")
		assert.True(t, errors.Is(err, ErrTestNameEmpty), "Should return ErrTestNameEmpty") //nolint:testifylint
	})

	t.Run("TestNameTooLong", func(t *testing.T) {
		longName := strings.Repeat("a", 751) // 751 chars (max is 750)

		tr := &TestResult{
			TestName:   longName,
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err, "Test name >750 chars should fail")
		assert.True(t, errors.Is(err, ErrTestNameTooLong), "Should return ErrTestNameTooLong") //nolint:testifylint
		assert.Contains(t, err.Error(), "751", "Error should mention actual length")
	})

	t.Run("TestNameExactlyMaxLength", func(t *testing.T) {
		maxName := strings.Repeat("a", 750) // Exactly 750 chars

		tr := &TestResult{
			TestName:   maxName,
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusPassed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		assert.NoError(t, err, "Test name at max length (750) should pass")
	})

	t.Run("EmptyDatasetURN", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err, "Empty dataset_urn should fail")
		assert.True(t, errors.Is(err, ErrDatasetURNEmpty), "Should return ErrDatasetURNEmpty") //nolint:testifylint
	})

	t.Run("InvalidDatasetURNFormat", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "invalid-urn-no-colon", // No ":" separator
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err, "Invalid URN format should fail")
		assert.True(t, errors.Is(err, ErrDatasetURNInvalid), "Should return ErrDatasetURNInvalid") //nolint:testifylint
		assert.Contains(t, err.Error(), "invalid-urn-no-colon", "Error should mention invalid URN")
	})

	t.Run("ValidDatasetURNFormats", func(t *testing.T) {
		validURNs := []string{
			"postgres://localhost:5432/db.schema.table",
			"s3://bucket/path/to/file.csv",
			"bigquery:project.dataset.table",
			"snowflake:database.schema.table",
			"file:///local/path/file.parquet",
		}

		for _, urn := range validURNs {
			tr := &TestResult{
				TestName:   "test_name",
				DatasetURN: urn,
				JobRunID:   "dbt:abc-123",
				Status:     TestStatusPassed,
				ExecutedAt: time.Now(),
			}

			err := tr.Validate()
			assert.NoError(t, err, "Valid URN format '%s' should pass", urn)
		}
	})

	t.Run("EmptyJobRunID", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err, "Empty job_run_id should fail")
		assert.True(t, errors.Is(err, ErrJobRunIDEmpty), "Should return ErrJobRunIDEmpty") //nolint:testifylint
	})

	t.Run("InvalidStatus", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatus("invalid_status"),
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err, "Invalid status should fail")
		assert.True(t, errors.Is(err, ErrStatusInvalid), "Should return ErrStatusInvalid") //nolint:testifylint
		assert.Contains(t, err.Error(), "invalid_status", "Error should mention invalid status")
	})

	t.Run("AllValidStatuses", func(t *testing.T) {
		validStatuses := []TestStatus{
			TestStatusPassed,
			TestStatusFailed,
			TestStatusError,
			TestStatusSkipped,
			TestStatusWarning,
		}

		for _, status := range validStatuses {
			tr := &TestResult{
				TestName:   "test_name",
				DatasetURN: "postgres://localhost:5432/db.schema.table",
				JobRunID:   "dbt:abc-123",
				Status:     status,
				ExecutedAt: time.Now(),
			}

			err := tr.Validate()
			assert.NoError(t, err, "Valid status '%s' should pass", status)
		}
	})

	t.Run("ZeroExecutedAt", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Time{}, // Zero time
		}

		err := tr.Validate()
		require.Error(t, err, "Zero executed_at should fail")
		assert.True(t, errors.Is(err, ErrExecutedAtZero), "Should return ErrExecutedAtZero") //nolint:testifylint
	})

	t.Run("NegativeDurationMs", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
			DurationMs: -100, // Negative duration
		}

		err := tr.Validate()
		require.Error(t, err, "Negative duration_ms should fail")
		assert.True(t, errors.Is(err, ErrDurationMsNegative), "Should return ErrDurationMsNegative") //nolint:testifylint
		assert.Contains(t, err.Error(), "-100", "Error should mention negative value")
	})

	t.Run("ZeroDurationMs", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusPassed,
			ExecutedAt: time.Now(),
			DurationMs: 0, // Zero is valid
		}

		err := tr.Validate()
		assert.NoError(t, err, "Zero duration_ms should be valid")
	})

	t.Run("OptionalFieldsCanBeEmpty", func(t *testing.T) {
		tr := &TestResult{
			TestName:   "test_name",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusPassed,
			ExecutedAt: time.Now(),
			// Optional fields omitted
			TestType:   "",
			Message:    "",
			Metadata:   nil,
			DurationMs: 0,
		}

		err := tr.Validate()
		assert.NoError(t, err, "Optional fields can be empty/nil")
	})
}

// TestTestStatus_IsValid tests TestStatus enum validation.
func TestTestStatus_IsValid(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name   string
		status TestStatus
		want   bool
	}{
		{"Passed", TestStatusPassed, true},
		{"Failed", TestStatusFailed, true},
		{"Error", TestStatusError, true},
		{"Skipped", TestStatusSkipped, true},
		{"Warning", TestStatusWarning, true},
		{"InvalidEmpty", TestStatus(""), false},
		{"InvalidRandom", TestStatus("random"), false},
		{"InvalidCasing", TestStatus("FAILED"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.IsValid()
			assert.Equal(t, tt.want, got, "IsValid() for status '%s'", tt.status)
		})
	}
}

// TestTestStatus_IsIncident tests incident detection logic.
func TestTestStatus_IsIncident(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name   string
		status TestStatus
		want   bool
	}{
		{"FailedIsIncident", TestStatusFailed, true},
		{"ErrorIsIncident", TestStatusError, true},
		{"PassedNotIncident", TestStatusPassed, false},
		{"SkippedNotIncident", TestStatusSkipped, false},
		{"WarningNotIncident", TestStatusWarning, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.status.IsIncident()
			assert.Equal(t, tt.want, got, "IsIncident() for status '%s'", tt.status)
		})
	}
}

// TestTestStatus_String tests string representation.
func TestTestStatus_String(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		status TestStatus
		want   string
	}{
		{TestStatusPassed, "passed"},
		{TestStatusFailed, "failed"},
		{TestStatusError, "error"},
		{TestStatusSkipped, "skipped"},
		{TestStatusWarning, "warning"},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			got := tt.status.String()
			assert.Equal(t, tt.want, got, "String() for status")
		})
	}
}

// TestTestResult_ValidationErrorMessages tests error message quality.
func TestTestResult_ValidationErrorMessages(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	t.Run("ErrorMessagesContainContext", func(t *testing.T) {
		// Test name too long - should include actual length
		tr := &TestResult{
			TestName:   strings.Repeat("a", 751),
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err := tr.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "751", "Error should include actual length")

		// Invalid URN - should include invalid value
		tr2 := &TestResult{
			TestName:   "test",
			DatasetURN: "invalid-urn",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatusFailed,
			ExecutedAt: time.Now(),
		}

		err2 := tr2.Validate()
		require.Error(t, err2)
		assert.Contains(t, err2.Error(), "invalid-urn", "Error should include invalid URN")

		// Invalid status - should include invalid value
		tr3 := &TestResult{
			TestName:   "test",
			DatasetURN: "postgres://localhost:5432/db.schema.table",
			JobRunID:   "dbt:abc-123",
			Status:     TestStatus("bad_status"),
			ExecutedAt: time.Now(),
		}

		err3 := tr3.Validate()
		require.Error(t, err3)
		assert.Contains(t, err3.Error(), "bad_status", "Error should include invalid status")
	})
}
