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

	// Namespace is normalized: postgres → postgresql, default port removed
	expected := "postgresql://prod-db/analytics.public.orders"
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
			expected:  "postgresql://prod-db/analytics.public.orders", // Normalized
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

	// Namespace is normalized: postgres → postgresql, default port removed
	expected := "postgresql://prod-db/"
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
			dataName:  "数据表_中文",           //nolint: gosmopolitan
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
	// Namespace is normalized: postgres → postgresql, default port removed
	expected := "postgresql://host/"

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

	// Test normalization idempotency: once normalized, stays normalized
	// This is the CORRECT behavior - normalization prevents duplicates
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
			// Generate URN (this normalizes the namespace)
			normalizedURN := GenerateDatasetURN(tc.namespace, tc.name)

			// Parse the normalized URN
			namespace, name, err := ParseDatasetURN(normalizedURN)
			if err != nil {
				t.Fatalf("ParseDatasetURN(%q) failed: %v", normalizedURN, err)
			}

			// Regenerate from parsed components
			regeneratedURN := GenerateDatasetURN(namespace, name)

			// Test idempotency: normalized → parsed → regenerated should be identical
			if regeneratedURN != normalizedURN {
				t.Errorf("Normalization not idempotent:\n"+
					"  First:  %q\n"+
					"  Second: %q", normalizedURN, regeneratedURN)
			}

			// Verify name is preserved (never normalized)
			if name != tc.name {
				t.Errorf("Name changed during round-trip: got %q, expected %q", name, tc.name)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: Multi-Tool Normalization (Prevents Correlation Failures)
// ==============================================================================

func TestGenerateDatasetURN_MultiToolNormalization(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Test that different tools referring to the same dataset produce identical URNs
	tests := []struct {
		name       string
		namespace1 string
		namespace2 string
		dataset    string
	}{
		{
			name:       "postgres vs postgresql scheme (dbt vs Great Expectations)",
			namespace1: "postgres://prod-db:5432",
			namespace2: "postgresql://prod-db:5432",
			dataset:    "analytics.public.orders",
		},
		{
			name:       "s3 vs s3a scheme (Airflow vs Spark)",
			namespace1: "s3://bucket",
			namespace2: "s3a://bucket",
			dataset:    "/data/orders.parquet",
		},
		{
			name:       "s3 vs s3n scheme (AWS vs legacy Hadoop)",
			namespace1: "s3://bucket",
			namespace2: "s3n://bucket",
			dataset:    "/data/orders.parquet",
		},
		{
			name:       "with vs without default PostgreSQL port",
			namespace1: "postgresql://db:5432",
			namespace2: "postgresql://db",
			dataset:    "schema.table",
		},
		{
			name:       "with vs without default MySQL port",
			namespace1: "mysql://db:3306",
			namespace2: "mysql://db",
			dataset:    "schema.table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urn1 := GenerateDatasetURN(tt.namespace1, tt.dataset)
			urn2 := GenerateDatasetURN(tt.namespace2, tt.dataset)

			if urn1 != urn2 {
				t.Errorf("Correlation WILL FAIL without normalization:\n"+
					"  Tool 1 URN: %s\n"+
					"  Tool 2 URN: %s\n"+
					"IMPACT: Dataset appears as duplicate, correlation accuracy drops below 90%%",
					urn1, urn2)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: Table Name Extraction (for orphan dataset matching)
// ==============================================================================

func TestExtractTableName(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		// Simple namespace/table format
		{
			name:     "simple namespace/table",
			input:    "demo_postgres/customers",
			expected: "customers",
		},
		{
			name:     "simple namespace/table with underscores",
			input:    "demo_postgres/stg_customers",
			expected: "stg_customers",
		},

		// PostgreSQL with schema prefix
		{
			name:     "postgresql with schema.table",
			input:    "postgresql://demo/marts.customers",
			expected: "customers",
		},
		{
			name:     "postgresql with schema.stg_table",
			input:    "postgresql://demo/staging.stg_customers",
			expected: "stg_customers",
		},
		{
			name:     "postgresql with db.schema.table",
			input:    "postgresql://host/mydb.public.orders",
			expected: "orders",
		},

		// S3 with file paths
		{
			name:     "s3 with parquet extension",
			input:    "s3://bucket/data/customers.parquet",
			expected: "customers",
		},
		{
			name:     "s3 with csv extension",
			input:    "s3://bucket/path/orders.csv",
			expected: "orders",
		},
		{
			name:     "s3 with json extension",
			input:    "s3://bucket/events.json",
			expected: "events",
		},
		{
			name:     "s3 nested path with extension",
			input:    "s3://bucket/data/warehouse/fact_sales.parquet",
			expected: "fact_sales",
		},

		// Kafka topics
		{
			name:     "kafka topic",
			input:    "kafka://cluster/user-events",
			expected: "user-events",
		},
		{
			name:     "kafka with dot notation",
			input:    "kafka://cluster/topic.events",
			expected: "events",
		},

		// BigQuery
		{
			name:     "bigquery project.dataset.table",
			input:    "bigquery/myproject.analytics.customers",
			expected: "customers",
		},

		// Edge cases
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "just table name",
			input:    "customers",
			expected: "customers",
		},
		{
			name:     "table with multiple dots",
			input:    "schema.subschema.table",
			expected: "table",
		},

		// Case normalization
		{
			name:     "uppercase table name",
			input:    "namespace/CUSTOMERS",
			expected: "customers",
		},
		{
			name:     "mixed case table name",
			input:    "postgresql://demo/marts.CustomerOrders",
			expected: "customerorders",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := ExtractTableName(tc.input)
			if result != tc.expected {
				t.Errorf("ExtractTableName(%q) = %q, expected %q", tc.input, result, tc.expected)
			}
		})
	}
}

func TestExtractTableName_MatchesSameTable(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Test that different URN formats for the same table extract to the same name
	// This is the key use case for orphan dataset matching
	testCases := []struct {
		name string
		urn1 string
		urn2 string
	}{
		{
			name: "dbt vs GE format for customers",
			urn1: "postgresql://demo/marts.customers",
			urn2: "demo_postgres/customers",
		},
		{
			name: "dbt vs GE format for orders",
			urn1: "postgresql://demo/marts.orders",
			urn2: "demo_postgres/orders",
		},
		{
			name: "different namespaces same table",
			urn1: "postgres://prod/public.users",
			urn2: "mydb/users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			table1 := ExtractTableName(tc.urn1)
			table2 := ExtractTableName(tc.urn2)

			if table1 != table2 {
				t.Errorf("Table name mismatch:\n"+
					"  URN 1: %q → %q\n"+
					"  URN 2: %q → %q\n"+
					"These should extract to the same table name for orphan matching",
					tc.urn1, table1, tc.urn2, table2)
			}
		})
	}
}
