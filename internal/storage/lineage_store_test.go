package storage

import (
	"testing"
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
