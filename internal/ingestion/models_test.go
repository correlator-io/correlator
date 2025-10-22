package ingestion

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
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
