package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/correlator-io/correlator/internal/ingestion"
)

//nolint:lll
func TestIsRunEvent(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		expected bool
	}{
		{
			name:     "RunEvent with START eventType",
			payload:  []byte(`{"eventType":"START","eventTime":"2025-01-01T00:00:00Z","producer":"test","schemaURL":"https://openlineage.io/spec/2-0-2/OpenLineage.json","run":{"runId":"abc"},"job":{"namespace":"ns","name":"job1"}}`),
			expected: true,
		},
		{
			name:     "RunEvent with COMPLETE eventType",
			payload:  []byte(`{"eventType":"COMPLETE","eventTime":"2025-01-01T00:00:00Z"}`),
			expected: true,
		},
		{
			name:     "RunEvent with FAIL eventType",
			payload:  []byte(`{"eventType":"FAIL","eventTime":"2025-01-01T00:00:00Z"}`),
			expected: true,
		},
		{
			name:     "DatasetEvent - no eventType field",
			payload:  []byte(`{"dataset":{"namespace":"ns","name":"table1"},"producer":"test"}`),
			expected: false,
		},
		{
			name:     "JobEvent - no eventType field",
			payload:  []byte(`{"job":{"namespace":"ns","name":"job1"},"producer":"test"}`),
			expected: false,
		},
		{
			name:     "empty JSON object",
			payload:  []byte(`{}`),
			expected: false,
		},
		{
			name:     "eventType is null",
			payload:  []byte(`{"eventType":null}`),
			expected: false,
		},
		{
			name:     "eventType is empty string",
			payload:  []byte(`{"eventType":""}`),
			expected: true, // has the field, consumer will validate later
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRunEvent(tt.payload)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRunEvent_MalformedJSON(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "invalid JSON", payload: []byte(`not json`)},
		{name: "empty bytes", payload: []byte{}},
		{name: "nil bytes", payload: nil},
		{name: "truncated JSON", payload: []byte(`{"eventType":"STA`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic, should return false
			result := isRunEvent(tt.payload)
			assert.False(t, result)
		})
	}
}

func TestParseRunEvent(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		wantEvent *ingestion.RunEvent
		wantErr   bool
	}{
		{
			name: "valid RunEvent with all fields",
			payload: []byte(`{
				"eventType": "COMPLETE",
				"eventTime": "2025-06-15T10:30:00Z",
				"producer": "https://github.com/OpenLineage/OpenLineage/tree/1.39.0/integration/dbt",
				"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
				"run": {
					"runId": "550e8400-e29b-41d4-a716-446655440000",
					"facets": {"nominalTime": {"nominalStartTime": "2025-06-15T10:00:00Z"}}
				},
				"job": {
					"namespace": "dbt_production",
					"name": "transform_orders"
				},
				"inputs": [
					{"namespace": "postgresql://db:5432", "name": "raw.orders"}
				],
				"outputs": [
					{"namespace": "postgresql://db:5432", "name": "analytics.orders_summary"}
				]
			}`),
			wantEvent: &ingestion.RunEvent{
				EventType: ingestion.EventTypeComplete,
				EventTime: time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC),
				Producer:  "https://github.com/OpenLineage/OpenLineage/tree/1.39.0/integration/dbt",
				SchemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
				Run: ingestion.Run{
					ID: "550e8400-e29b-41d4-a716-446655440000",
				},
				Job: ingestion.Job{
					Namespace: "dbt_production",
					Name:      "transform_orders",
				},
			},
			wantErr: false,
		},
		{
			name: "valid RunEvent with no inputs/outputs",
			payload: []byte(`{
				"eventType": "START",
				"eventTime": "2025-06-15T10:30:00Z",
				"producer": "test",
				"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
				"run": {"runId": "abc-123"},
				"job": {"namespace": "ns", "name": "job1"}
			}`),
			wantErr: false,
		},
		{
			name:    "malformed JSON",
			payload: []byte(`not json`),
			wantErr: true,
		},
		{
			name: "eventTime without timezone (GE-ol format)",
			payload: []byte(`{
				"eventType": "COMPLETE",
				"eventTime": "2025-06-15T10:30:00.123456",
				"producer": "test",
				"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
				"run": {"runId": "abc-123"},
				"job": {"namespace": "ns", "name": "job1"}
			}`),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := parseRunEvent(tt.payload)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, event)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, event)

			if tt.wantEvent != nil {
				assert.Equal(t, tt.wantEvent.EventType, event.EventType)
				assert.Equal(t, tt.wantEvent.EventTime, event.EventTime)
				assert.Equal(t, tt.wantEvent.Producer, event.Producer)
				assert.Equal(t, tt.wantEvent.SchemaURL, event.SchemaURL)
				assert.Equal(t, tt.wantEvent.Run.ID, event.Run.ID)
				assert.Equal(t, tt.wantEvent.Job.Namespace, event.Job.Namespace)
				assert.Equal(t, tt.wantEvent.Job.Name, event.Job.Name)
			}

			// Inputs and outputs should never be nil (normalized)
			assert.NotNil(t, event.Inputs, "Inputs should be non-nil (empty slice)")
			assert.NotNil(t, event.Outputs, "Outputs should be non-nil (empty slice)")
		})
	}
}

func TestParseRunEvent_DatasetNormalization(t *testing.T) {
	payload := []byte(`{
		"eventType": "COMPLETE",
		"eventTime": "2025-06-15T10:30:00Z",
		"producer": "test",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"run": {"runId": "abc"},
		"job": {"namespace": "ns", "name": "job"},
		"inputs": [
			{
				"namespace": "postgres://db:5432",
				"name": "public.orders",
				"inputFacets": {"dataQualityAssertions": {"assertions": []}}
			}
		],
		"outputs": [
			{
				"namespace": "postgres://db:5432",
				"name": "analytics.summary",
				"outputFacets": {"outputStatistics": {"rowCount": 100}}
			}
		]
	}`)

	event, err := parseRunEvent(payload)
	require.NoError(t, err)

	// Verify datasets were parsed
	require.Len(t, event.Inputs, 1)
	require.Len(t, event.Outputs, 1)

	// Verify namespace normalization (postgres:// → postgresql://, default port stripped)
	assert.Equal(t, "postgresql://db", event.Inputs[0].Namespace)
	assert.Equal(t, "public.orders", event.Inputs[0].Name)
	assert.Equal(t, "postgresql://db", event.Outputs[0].Namespace)
	assert.Equal(t, "analytics.summary", event.Outputs[0].Name)

	// Verify facets are preserved
	assert.NotNil(t, event.Inputs[0].InputFacets)
	assert.NotNil(t, event.Outputs[0].OutputFacets)
}

func TestParseRunEvent_NilFacetsNormalized(t *testing.T) {
	payload := []byte(`{
		"eventType": "START",
		"eventTime": "2025-06-15T10:30:00Z",
		"producer": "test",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"run": {"runId": "abc"},
		"job": {"namespace": "ns", "name": "job"}
	}`)

	event, err := parseRunEvent(payload)
	require.NoError(t, err)

	// Run and Job facets should be initialized to empty maps
	assert.NotNil(t, event.Run.Facets, "Run.Facets should be non-nil")
	assert.NotNil(t, event.Job.Facets, "Job.Facets should be non-nil")
}
