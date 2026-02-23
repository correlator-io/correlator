package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExtractProducerName verifies producer name extraction from OpenLineage URLs.
func TestExtractProducerName(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name        string
		producerURL string
		want        string
	}{
		{
			name:        "dbt-core GitHub URL",
			producerURL: "https://github.com/dbt-labs/dbt-core/tree/1.5.0",
			want:        "dbt-core",
		},
		{
			name:        "Airflow GitHub URL",
			producerURL: "https://github.com/apache/airflow/tree/2.7.0",
			want:        "airflow",
		},
		{
			name:        "Great Expectations GitHub URL",
			producerURL: "https://github.com/great-expectations/great_expectations/tree/0.17.0",
			want:        "great_expectations",
		},
		{
			name:        "Spark integration URL",
			producerURL: "https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/spark",
			want:        "spark",
		},
		{
			name:        "Flink integration URL",
			producerURL: "https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/flink",
			want:        "flink",
		},
		{
			name:        "HTTP protocol",
			producerURL: "http://github.com/dbt-labs/dbt-core/tree/1.5.0",
			want:        "dbt-core",
		},
		{
			name:        "Empty URL",
			producerURL: "",
			want:        "unknown",
		},
		{
			name:        "Non-GitHub URL",
			producerURL: "https://example.com/my-tool",
			want:        "example.com",
		},
		{
			name:        "Short URL",
			producerURL: "https://example.com",
			want:        "example.com",
		},
		{
			name:        "Correlator dbt plugin URL",
			producerURL: "https://github.com/correlator-io/dbt-correlator/0.1.1.dev0",
			want:        "dbt-correlator",
		},
		{
			name:        "Airflow OpenLineage provider URL",
			producerURL: "https://github.com/apache/airflow/tree/providers-openlineage/2.10.0",
			want:        "airflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractProducerName(tt.producerURL)
			if got != tt.want {
				t.Errorf("extractProducerName(%q) = %q, want %q", tt.producerURL, got, tt.want)
			}
		})
	}
}

// TestExtractProducerVersion verifies producer version extraction from OpenLineage URLs.
func TestExtractProducerVersion(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name        string
		producerURL string
		want        string
	}{
		{
			name:        "dbt-core GitHub URL with tree",
			producerURL: "https://github.com/dbt-labs/dbt-core/tree/1.5.0",
			want:        "1.5.0",
		},
		{
			name:        "Airflow GitHub URL with tree",
			producerURL: "https://github.com/apache/airflow/tree/2.7.0",
			want:        "2.7.0",
		},
		{
			name:        "Airflow OpenLineage provider URL",
			producerURL: "https://github.com/apache/airflow/tree/providers-openlineage/2.10.0",
			want:        "2.10.0",
		},
		{
			name:        "Great Expectations GitHub URL with tree",
			producerURL: "https://github.com/great-expectations/great_expectations/tree/0.17.0",
			want:        "0.17.0",
		},
		{
			name:        "Spark integration URL",
			producerURL: "https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/spark",
			want:        "1.0.0",
		},
		{
			name:        "Correlator dbt plugin URL (no tree)",
			producerURL: "https://github.com/correlator-io/dbt-correlator/0.1.1.dev0",
			want:        "0.1.1.dev0",
		},
		{
			name:        "Correlator airflow plugin URL",
			producerURL: "https://github.com/correlator-io/correlator-airflow/0.1.0",
			want:        "0.1.0",
		},
		{
			name:        "Version with v prefix",
			producerURL: "https://github.com/org/repo/v1.2.3",
			want:        "v1.2.3",
		},
		{
			name:        "Empty URL",
			producerURL: "",
			want:        "",
		},
		{
			name:        "Non-GitHub URL",
			producerURL: "https://example.com/my-tool",
			want:        "",
		},
		{
			name:        "GitHub URL without version",
			producerURL: "https://github.com/org/repo",
			want:        "",
		},
		{
			name:        "GitHub URL with non-version path",
			producerURL: "https://github.com/org/repo/src/main",
			want:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractProducerVersion(tt.producerURL)
			if got != tt.want {
				t.Errorf("extractProducerVersion(%q) = %q, want %q", tt.producerURL, got, tt.want)
			}
		})
	}
}

// TestExtractParentJobRunID verifies parent job run ID extraction from OpenLineage ParentRunFacet.
func TestExtractParentJobRunID(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		facets   map[string]interface{}
		expected string
	}{
		{
			name: "valid parent facet with dbt namespace",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "dbt://demo",
						"name":      "jaffle_shop.build",
					},
					"run": map[string]interface{}{
						"runId": "019c628f-d07e-7000-8000-000000000000",
					},
				},
			},
			expected: "dbt:019c628f-d07e-7000-8000-000000000000",
		},
		{
			name: "valid parent facet with airflow namespace",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "airflow://production",
						"name":      "etl_dag",
					},
					"run": map[string]interface{}{
						"runId": "manual__2026-02-15T12:00:00",
					},
				},
			},
			expected: "airflow:manual__2026-02-15T12:00:00",
		},
		{
			name:     "no parent facet",
			facets:   map[string]interface{}{},
			expected: "",
		},
		{
			name:     "nil facets",
			facets:   nil,
			expected: "",
		},
		{
			name: "malformed parent - missing job",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"run": map[string]interface{}{
						"runId": "abc",
					},
				},
			},
			expected: "",
		},
		{
			name: "malformed parent - missing run",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "dbt://demo",
					},
				},
			},
			expected: "",
		},
		{
			name: "malformed parent - empty namespace",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "",
						"name":      "jaffle_shop.build",
					},
					"run": map[string]interface{}{
						"runId": "019c628f-d07e-7000-8000-000000000000",
					},
				},
			},
			expected: "",
		},
		{
			name: "malformed parent - empty runId",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "dbt://demo",
						"name":      "jaffle_shop.build",
					},
					"run": map[string]interface{}{
						"runId": "",
					},
				},
			},
			expected: "",
		},
		{
			name: "parent facet with other facets",
			facets: map[string]interface{}{
				"errorMessage": map[string]interface{}{
					"message": "some error",
				},
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "dbt://demo",
						"name":      "jaffle_shop.build",
					},
					"run": map[string]interface{}{
						"runId": "019c628f-d07e-7000-8000-000000000000",
					},
				},
			},
			expected: "dbt:019c628f-d07e-7000-8000-000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractParentJobRunID(tt.facets)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractRootParentJobRunID verifies root parent extraction from OpenLineage ParentRunFacet.
func TestExtractRootParentJobRunID(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		facets   map[string]interface{}
		expected string
	}{
		{
			name: "valid root with airflow DAG run",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "airflow://demo",
						"name":      "demo_pipeline.dbt_run",
					},
					"run": map[string]interface{}{
						"runId": "task-run-id",
					},
					"root": map[string]interface{}{
						"job": map[string]interface{}{
							"namespace": "airflow://demo",
							"name":      "demo_pipeline",
						},
						"run": map[string]interface{}{
							"runId": "019c628f-0000-0000-0000-000000000000",
						},
					},
				},
			},
			expected: "airflow:019c628f-0000-0000-0000-000000000000",
		},
		{
			name: "parent without root field",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "dbt://demo",
						"name":      "jaffle_shop.build",
					},
					"run": map[string]interface{}{
						"runId": "019c628f-d07e-7000-8000-000000000000",
					},
				},
			},
			expected: "",
		},
		{
			name:     "no parent facet",
			facets:   map[string]interface{}{},
			expected: "",
		},
		{
			name:     "nil facets",
			facets:   nil,
			expected: "",
		},
		{
			name: "root with empty namespace",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "airflow://demo",
						"name":      "demo_pipeline.dbt_run",
					},
					"run": map[string]interface{}{
						"runId": "task-run-id",
					},
					"root": map[string]interface{}{
						"job": map[string]interface{}{
							"namespace": "",
							"name":      "demo_pipeline",
						},
						"run": map[string]interface{}{
							"runId": "abc",
						},
					},
				},
			},
			expected: "",
		},
		{
			name: "root with empty runId",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "airflow://demo",
						"name":      "demo_pipeline.dbt_run",
					},
					"run": map[string]interface{}{
						"runId": "task-run-id",
					},
					"root": map[string]interface{}{
						"job": map[string]interface{}{
							"namespace": "airflow://demo",
							"name":      "demo_pipeline",
						},
						"run": map[string]interface{}{
							"runId": "",
						},
					},
				},
			},
			expected: "",
		},
		{
			name: "malformed root - missing job",
			facets: map[string]interface{}{
				"parent": map[string]interface{}{
					"job": map[string]interface{}{
						"namespace": "airflow://demo",
						"name":      "demo_pipeline.dbt_run",
					},
					"run": map[string]interface{}{
						"runId": "task-run-id",
					},
					"root": map[string]interface{}{
						"run": map[string]interface{}{
							"runId": "abc",
						},
					},
				},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractRootParentJobRunID(tt.facets)
			assert.Equal(t, tt.expected, result)
		})
	}
}
