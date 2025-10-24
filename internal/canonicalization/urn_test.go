// Package canonicalization provides dataset URN construction for correlation.
package canonicalization

import (
	"testing"
)

// ==============================================================================
// Unit Tests: Dataset URN Generation
// ==============================================================================

func TestGenerateDatasetURN_PostgreSQL(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("postgres://prod-db:5432", "analytics.public.orders")

	expected := "postgres://prod-db:5432/analytics.public.orders"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_BigQuery(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("bigquery", "project.dataset.table")

	expected := "bigquery/project.dataset.table"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_S3(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("s3://raw-data", "/orders/2025-10-18.parquet")

	expected := "s3://raw-data//orders/2025-10-18.parquet"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_S3RootPath(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// S3 root path should result in double slash (namespace + '/' + name starting with '/')
	urn := GenerateDatasetURN("s3://bucket", "/file.csv")

	expected := "s3://bucket//file.csv"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_Snowflake(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("snowflake://org-account", "analytics.public.customers")

	expected := "snowflake://org-account/analytics.public.customers"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_Kafka(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("kafka://broker:9092", "user-events")

	expected := "kafka://broker:9092/user-events"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_HDFS(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("hdfs://namenode:8020", "/data/warehouse/orders")

	expected := "hdfs://namenode:8020//data/warehouse/orders"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

// ==============================================================================
// Unit Tests: URN Generation from Parts
// ==============================================================================

func TestGenerateDatasetURN_FromParts(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name      string
		namespace string
		dataName  string
		expected  string
	}{
		{
			name:      "PostgreSQL",
			namespace: "postgres://prod-db:5432",
			dataName:  "analytics.public.orders",
			expected:  "postgres://prod-db:5432/analytics.public.orders",
		},
		{
			name:      "BigQuery",
			namespace: "bigquery",
			dataName:  "project.dataset.table",
			expected:  "bigquery/project.dataset.table",
		},
		{
			name:      "S3",
			namespace: "s3://bucket",
			dataName:  "/path/to/file.parquet",
			expected:  "s3://bucket//path/to/file.parquet",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			urn := GenerateDatasetURN(tc.namespace, tc.dataName)
			if urn != tc.expected {
				t.Errorf("GenerateDatasetURN(%q, %q) = %q, expected %q",
					tc.namespace, tc.dataName, urn, tc.expected)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: URN Parsing
// ==============================================================================

func TestParseDatasetURN_Valid(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name              string
		urn               string
		expectedNamespace string
		expectedName      string
	}{
		{
			name:              "PostgreSQL",
			urn:               "postgres://prod-db:5432/analytics.public.orders",
			expectedNamespace: "postgres://prod-db:5432",
			expectedName:      "analytics.public.orders",
		},
		{
			name:              "BigQuery",
			urn:               "bigquery/project.dataset.table",
			expectedNamespace: "bigquery",
			expectedName:      "project.dataset.table",
		},
		{
			name:              "S3",
			urn:               "s3://bucket//path/to/file.parquet",
			expectedNamespace: "s3://bucket",
			expectedName:      "/path/to/file.parquet",
		},
		{
			name:              "Kafka",
			urn:               "kafka://broker:9092/user-events",
			expectedNamespace: "kafka://broker:9092",
			expectedName:      "user-events",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			namespace, name, err := ParseDatasetURN(tc.urn)
			if err != nil {
				t.Fatalf("ParseDatasetURN(%q) failed: %v", tc.urn, err)
			}

			if namespace != tc.expectedNamespace {
				t.Errorf("namespace = %q, expected %q", namespace, tc.expectedNamespace)
			}

			if name != tc.expectedName {
				t.Errorf("name = %q, expected %q", name, tc.expectedName)
			}
		})
	}
}

func TestParseDatasetURN_InvalidFormat(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	invalidURNs := []string{
		"invalid",                   // No delimiter
		"namespace:",                // Colon instead of slash
		"namespace:name",            // Colon instead of slash
		"",                          // Empty
		"/name",                     // Missing namespace
		"namespace/",                // Missing name
		"namespace//",               // Missing name after double slash
		"://name",                   // Missing protocol
		"protocol://host:port:name", // Wrong delimiter
	}

	for _, urn := range invalidURNs {
		t.Run(urn, func(t *testing.T) {
			_, _, err := ParseDatasetURN(urn)
			if err == nil {
				t.Errorf("ParseDatasetURN(%q) should return error for invalid format", urn)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: URN Normalization
// ==============================================================================

func TestNormalizeDatasetURN_Valid(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "No changes needed",
			input:    "postgres://prod-db:5432/analytics.public.orders",
			expected: "postgres://prod-db:5432/analytics.public.orders",
		},
		{
			name:     "Trim leading whitespace",
			input:    "  postgres://prod-db:5432/analytics.public.orders",
			expected: "postgres://prod-db:5432/analytics.public.orders",
		},
		{
			name:     "Trim trailing whitespace",
			input:    "postgres://prod-db:5432/analytics.public.orders  ",
			expected: "postgres://prod-db:5432/analytics.public.orders",
		},
		{
			name:     "Trim both sides",
			input:    "  postgres://prod-db:5432/analytics.public.orders  ",
			expected: "postgres://prod-db:5432/analytics.public.orders",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized, err := NormalizeDatasetURN(tc.input)
			if err != nil {
				t.Fatalf("NormalizeDatasetURN(%q) failed: %v", tc.input, err)
			}

			if normalized != tc.expected {
				t.Errorf("NormalizeDatasetURN(%q) = %q, expected %q", tc.input, normalized, tc.expected)
			}
		})
	}
}

func TestNormalizeDatasetURN_Invalid(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	invalidURNs := []string{
		"",           // Empty
		"   ",        // Only whitespace
		"invalid",    // No delimiter
		"namespace:", // Wrong delimiter
	}

	for _, urn := range invalidURNs {
		t.Run(urn, func(t *testing.T) {
			_, err := NormalizeDatasetURN(urn)
			if err == nil {
				t.Errorf("NormalizeDatasetURN(%q) should return error for invalid URN", urn)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: Edge Cases
// ==============================================================================

func TestGenerateDatasetURN_EmptyFields(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("", "")

	// Should return "/" for empty fields (namespace + "/" + name)
	expected := "/"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q for empty fields", urn, expected)
	}
}

func TestGenerateDatasetURN_OnlyNamespace(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("postgres://prod-db:5432", "")

	expected := "postgres://prod-db:5432/"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_OnlyName(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("", "analytics.public.orders")

	expected := "/analytics.public.orders"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_SpecialCharacters(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Dataset names with special characters
	testCases := []struct {
		name      string
		namespace string
		dataName  string
		expected  string
	}{
		{
			name:      "Spaces in name",
			namespace: "test://ns",
			dataName:  "table with spaces",
			expected:  "test://ns/table with spaces",
		},
		{
			name:      "Dots in name",
			namespace: "test://ns",
			dataName:  "schema.table.column",
			expected:  "test://ns/schema.table.column",
		},
		{
			name:      "Hyphens in name",
			namespace: "test://ns",
			dataName:  "table-with-hyphens",
			expected:  "test://ns/table-with-hyphens",
		},
		{
			name:      "Underscores in name",
			namespace: "test://ns",
			dataName:  "table_with_underscores",
			expected:  "test://ns/table_with_underscores",
		},
		{
			name:      "Unicode in name",
			namespace: "test://ns",
			dataName:  "数据表_中文", //nolint: gosmopolitan
			expected:  "test://ns/数据表_中文", //nolint: gosmopolitan
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			urn := GenerateDatasetURN(tc.namespace, tc.dataName)
			if urn != tc.expected {
				t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, tc.expected)
			}
		})
	}
}

func TestGenerateDatasetURN_MultipleSlashes(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Name with multiple slashes (file paths)
	urn := GenerateDatasetURN("s3://bucket", "/path/to/nested/file.parquet")

	// Should preserve all slashes in name
	expected := "s3://bucket//path/to/nested/file.parquet"
	if urn != expected {
		t.Errorf("GenerateDatasetURN() = %q, expected %q", urn, expected)
	}
}

func TestParseDatasetURN_FirstSlashOnly(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// URN with multiple slashes - should split on FIRST slash only
	urn := "s3://bucket//path/to/file.parquet"

	namespace, name, err := ParseDatasetURN(urn)
	if err != nil {
		t.Fatalf("ParseDatasetURN(%q) failed: %v", urn, err)
	}

	expectedNamespace := "s3://bucket"
	expectedName := "/path/to/file.parquet"

	if namespace != expectedNamespace {
		t.Errorf("namespace = %q, expected %q", namespace, expectedNamespace)
	}

	if name != expectedName {
		t.Errorf("name = %q, expected %q", name, expectedName)
	}
}

func TestGenerateDatasetURN_Deterministic(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Call multiple times - should always return same URN
	urn1 := GenerateDatasetURN("postgres://prod-db:5432", "analytics.public.orders")
	urn2 := GenerateDatasetURN("postgres://prod-db:5432", "analytics.public.orders")
	urn3 := GenerateDatasetURN("postgres://prod-db:5432", "analytics.public.orders")

	if urn1 != urn2 || urn2 != urn3 {
		t.Error("GenerateDatasetURN() is not deterministic")
	}
}

func TestGenerateDatasetURN_EmptyNamespace(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("", "table_name")
	expected := "/table_name"

	if urn != expected {
		t.Errorf("GenerateDatasetURN(\"\", \"table_name\") = %q, expected %q", urn, expected)
	}
}

func TestGenerateDatasetURN_EmptyName(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	urn := GenerateDatasetURN("postgres://host:5432", "")
	expected := "postgres://host:5432/"

	if urn != expected {
		t.Errorf("GenerateDatasetURN(\"postgres://host:5432\", \"\") = %q, expected %q", urn, expected)
	}
}

func TestNormalizeDatasetURN_PreservesDoubleSlash(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// S3 URNs with root paths should preserve double slash
	input := "s3://bucket//file.csv"

	normalized, err := NormalizeDatasetURN(input)
	if err != nil {
		t.Fatalf("NormalizeDatasetURN(%q) failed: %v", input, err)
	}

	if normalized != input {
		t.Errorf("NormalizeDatasetURN(%q) = %q, should preserve double slash", input, normalized)
	}
}

func TestParseDatasetURN_RoundTrip(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Test that GenerateDatasetURN + ParseDatasetURN is idempotent
	testCases := []struct {
		namespace string
		name      string
	}{
		{"postgres://prod-db:5432", "analytics.public.orders"},
		{"bigquery", "project.dataset.table"},
		{"s3://bucket", "/path/to/file.parquet"},
		{"kafka://broker:9092", "user-events"},
		{"hdfs://namenode:8020", "/data/warehouse"},
	}

	for _, tc := range testCases {
		t.Run(tc.namespace+"/"+tc.name, func(t *testing.T) {
			// Generate URN
			urn := GenerateDatasetURN(tc.namespace, tc.name)

			// Parse it back
			namespace, name, err := ParseDatasetURN(urn)
			if err != nil {
				t.Fatalf("ParseDatasetURN(%q) failed: %v", urn, err)
			}

			// Should match original
			if namespace != tc.namespace {
				t.Errorf("Round-trip namespace: got %q, expected %q", namespace, tc.namespace)
			}

			if name != tc.name {
				t.Errorf("Round-trip name: got %q, expected %q", name, tc.name)
			}
		})
	}
}
