package canonicalization

import "testing"

// ==============================================================================
// Benchmarks: Normalization Performance
// ==============================================================================

func Benchmark_NormalizeNamespace(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	namespaces := []string{
		"postgres://prod-db:5432",
		"postgresql://prod-db:5432",
		"s3a://bucket",
		"bigquery",
		"mysql://user:pass@host:3306/db?sslmode=require", // pragma: allowlist secret
		"kafka://broker:9092",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, ns := range namespaces {
			_ = NormalizeNamespace(ns)
		}
	}
}

func Benchmark_GenerateDatasetURN(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	testCases := []struct {
		namespace string
		name      string
	}{
		{"postgres://prod-db:5432", "analytics.public.orders"},
		{"s3a://bucket", "/data/file.parquet"},
		{"bigquery", "project.dataset.table"},
		{"mysql://db:3306", "schema.table"},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			_ = GenerateDatasetURN(tc.namespace, tc.name)
		}
	}
}

func Benchmark_GenerateDatasetURN_WithNormalization(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	// Benchmark specifically postgres → postgresql normalization overhead
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = GenerateDatasetURN("postgres://prod-db:5432", "analytics.orders")
	}
}

func Benchmark_GenerateDatasetURN_WithoutNormalization(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	// Benchmark passthrough (no normalization needed)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = GenerateDatasetURN("bigquery", "project.dataset.table")
	}
}
