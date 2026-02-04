// Package canonicalization provides canonical ID generation for correlation.
package canonicalization

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// ==============================================================================
// Unit Tests: Canonical Job Run ID Generation
// ==============================================================================

func TestGenerateJobRunID_DBT(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	id := GenerateJobRunID("dbt://analytics", "550e8400-e29b-41d4-a716-446655440000")

	// Should return format "dbt:runID"
	expected := "dbt:550e8400-e29b-41d4-a716-446655440000"
	if id != expected {
		t.Errorf("GenerateJobRunID() = %q, expected %q", id, expected)
	}

	// Should start with "dbt:"
	if !strings.HasPrefix(id, "dbt:") {
		t.Errorf("GenerateJobRunID() should start with 'dbt:', got %q", id)
	}
}

func TestGenerateJobRunID_Airflow(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	id := GenerateJobRunID("airflow://production", "manual__2025-01-01T12:00:00")

	// Should return format "airflow:runID"
	expected := "airflow:manual__2025-01-01T12:00:00"
	if id != expected {
		t.Errorf("GenerateJobRunID() = %q, expected %q", id, expected)
	}

	// Should start with "airflow:"
	if !strings.HasPrefix(id, "airflow:") {
		t.Errorf("GenerateJobRunID() should start with 'airflow:', got %q", id)
	}
}

func TestGenerateJobRunID_Spark(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	id := GenerateJobRunID("spark://prod-cluster", "application_123456")

	// Should return format "spark:application_id"
	expected := "spark:application_123456"
	if id != expected {
		t.Errorf("GenerateJobRunID() = %q, expected %q", id, expected)
	}

	// Should start with "spark:"
	if !strings.HasPrefix(id, "spark:") {
		t.Errorf("GenerateJobRunID() should start with 'spark:', got %q", id)
	}
}

func TestGenerateJobRunID_GreatExpectations(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		namespace string
		runID     string
		expected  string
	}{
		{"great_expectations://default", "validation-123", "ge:validation-123"},
		{"ge://prod", "checkpoint-456", "ge:checkpoint-456"},
		{"gx://demo", "run-789", "ge:run-789"},
	}

	for _, tc := range testCases {
		id := GenerateJobRunID(tc.namespace, tc.runID)
		if id != tc.expected {
			t.Errorf("GenerateJobRunID(%q, %q) = %q, expected %q", tc.namespace, tc.runID, id, tc.expected)
		}
	}
}

func TestGenerateJobRunID_SameRunDifferentEvents(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Same run parameters should produce same ID regardless of other factors
	id1 := GenerateJobRunID("dbt://analytics", "550e8400-e29b-41d4-a716-446655440000")
	id2 := GenerateJobRunID("dbt://analytics", "550e8400-e29b-41d4-a716-446655440000")

	if id1 != id2 {
		t.Errorf("GenerateJobRunID() returned different IDs for same run: %s vs %s", id1, id2)
	}
}

func TestGenerateJobRunID_DifferentRunsSameJob(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Same job, different runs should produce different IDs
	id1 := GenerateJobRunID("dbt://analytics", "run-1")
	id2 := GenerateJobRunID("dbt://analytics", "run-2")

	if id1 == id2 {
		t.Error("GenerateJobRunID() returned same ID for different runs")
	}
}

func TestGenerateJobRunID_Deterministic(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Call multiple times - should always return same ID
	id1 := GenerateJobRunID("test://namespace", "test-run-id")
	id2 := GenerateJobRunID("test://namespace", "test-run-id")
	id3 := GenerateJobRunID("test://namespace", "test-run-id")

	if id1 != id2 || id2 != id3 {
		t.Error("GenerateJobRunID() is not deterministic")
	}
}

// ==============================================================================
// Unit Tests: Idempotency Key Generation
// ==============================================================================

func TestGenerateIdempotencyKey_DBT(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	key := GenerateIdempotencyKey(
		"https://github.com/dbt-labs/dbt-core/tree/1.5.0",
		"dbt://analytics",
		"transform_orders",
		"550e8400-e29b-41d4-a716-446655440000",
		"2025-10-22T10:00:00.000000000Z",
		"COMPLETE",
	)

	// Should return a non-empty hex string (SHA256 = 64 hex chars)
	if key == "" {
		t.Error("GenerateIdempotencyKey() returned empty string")
	}

	if len(key) != 64 {
		t.Errorf("GenerateIdempotencyKey() returned %d chars, expected 64", len(key))
	}
}

func TestGenerateIdempotencyKey_DifferentEventTime(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Event 1 at 10:00
	key1 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)

	// Event 2 at 10:01 (different eventTime)
	key2 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:01:00.000000000Z",
		"START",
	)

	if key1 == key2 {
		t.Error("GenerateIdempotencyKey() returned same key for different eventTime")
	}
}

func TestGenerateIdempotencyKey_DifferentEventType(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Event 1: START
	key1 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)

	// Event 2: COMPLETE (same run, different eventType)
	key2 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:01:00.000000000Z",
		"COMPLETE",
	)

	if key1 == key2 {
		t.Error("GenerateIdempotencyKey() returned same key for different eventType")
	}
}

func TestGenerateIdempotencyKey_MultiTenantSafe(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Tenant A
	key1 := GenerateIdempotencyKey(
		"https://tenant-a.example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)

	// Tenant B (same job, different producer)
	key2 := GenerateIdempotencyKey(
		"https://tenant-b.example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)

	if key1 == key2 {
		t.Error("GenerateIdempotencyKey() returned same key for different producers (multi-tenant collision)")
	}
}

func TestGenerateIdempotencyKey_Deterministic(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Call multiple times - should always return same key
	key1 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)
	key2 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)
	key3 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)

	if key1 != key2 || key2 != key3 {
		t.Error("GenerateIdempotencyKey() is not deterministic")
	}
}

func TestGenerateIdempotencyKey_IncludesAllComponents(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Idempotency key should change when ANY component changes
	baseKey := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)

	// Change producer
	key1 := GenerateIdempotencyKey(
		"https://different-producer.example.com",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)
	if key1 == baseKey {
		t.Error("Idempotency key didn't change when producer changed")
	}

	// Change job namespace
	key2 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"different://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)
	if key2 == baseKey {
		t.Error("Idempotency key didn't change when job.namespace changed")
	}

	// Change job name
	key3 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"different_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)
	if key3 == baseKey {
		t.Error("Idempotency key didn't change when job.name changed")
	}

	// Change runId
	key4 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"different-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)
	if key4 == baseKey {
		t.Error("Idempotency key didn't change when run.runId changed")
	}

	// Change eventTime
	key5 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T11:00:00.000000000Z",
		"START",
	)
	if key5 == baseKey {
		t.Error("Idempotency key didn't change when eventTime changed")
	}

	// Change eventType
	key6 := GenerateIdempotencyKey(
		"https://example.com/producer",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"COMPLETE",
	)
	if key6 == baseKey {
		t.Error("Idempotency key didn't change when eventType changed")
	}
}

// ==============================================================================
// Unit Tests: Edge Cases
// ==============================================================================

func TestGenerateJobRunID_EmptyFields(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Event with empty required fields (validator should catch this, but canonicalizer should handle it)
	id := GenerateJobRunID("", "")

	// Should return "unknown:" for empty namespace
	expected := "unknown:"
	if id != expected {
		t.Errorf("GenerateJobRunID() = %q, expected %q for empty fields", id, expected)
	}
}

func TestGenerateIdempotencyKey_EmptyProducer(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	key := GenerateIdempotencyKey(
		"",
		"test://namespace",
		"test_job",
		"test-run-id",
		"2025-10-22T10:00:00.000000000Z",
		"START",
	)

	// Should still generate a hash
	if len(key) != 64 {
		t.Errorf("GenerateIdempotencyKey() should return 64-char hash even for empty producer, got %d chars", len(key))
	}
}

func TestGenerateJobRunID_SpecialCharacters(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// RunIDs with special characters, spaces, unicode should be preserved
	testCases := []struct {
		name      string
		namespace string
		runID     string
		expected  string
	}{
		{"spaces in runID", "dbt://test", "run with spaces", "dbt:run with spaces"},
		{"slashes in runID", "airflow://test", "dag/task/2025-01-01", "airflow:dag/task/2025-01-01"},
		{"colons in runID", "spark://test", "app:123:456", "spark:app:123:456"},
		{"unicode in runID", "dbt://test", "测试-run-123", "dbt:测试-run-123"}, //nolint:gosmopolitan
		{"emoji in runID", "custom://test", "🚀-deploy-v1", "custom:🚀-deploy-v1"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			id := GenerateJobRunID(tc.namespace, tc.runID)

			if id != tc.expected {
				t.Errorf("GenerateJobRunID() = %q, expected %q", id, tc.expected)
			}

			// Should contain colon separator
			if !strings.Contains(id, ":") {
				t.Errorf("GenerateJobRunID() should contain colon separator: %q", id)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: Parse Canonical Job Run ID
// ==============================================================================

func TestParseCanonicalJobRunID_ValidFormats(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name          string
		canonical     string
		expectedTool  string
		expectedRunID string
	}{
		{
			name:          "dbt with UUID",
			canonical:     "dbt:550e8400-e29b-41d4-a716-446655440000",
			expectedTool:  "dbt",
			expectedRunID: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:          "airflow with timestamp",
			canonical:     "airflow:manual__2025-01-01T12:00:00",
			expectedTool:  "airflow",
			expectedRunID: "manual__2025-01-01T12:00:00",
		},
		{
			name:          "spark with application ID",
			canonical:     "spark:application_123456",
			expectedTool:  "spark",
			expectedRunID: "application_123456",
		},
		{
			name:          "runID with multiple colons",
			canonical:     "spark:app:123:456",
			expectedTool:  "spark",
			expectedRunID: "app:123:456",
		},
		{
			name:          "custom tool",
			canonical:     "custom:my-run-123",
			expectedTool:  "custom",
			expectedRunID: "my-run-123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tool, runID, err := ParseCanonicalJobRunID(tc.canonical)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if tool != tc.expectedTool {
				t.Errorf("Tool = %q, expected %q", tool, tc.expectedTool)
			}

			if runID != tc.expectedRunID {
				t.Errorf("RunID = %q, expected %q", runID, tc.expectedRunID)
			}
		})
	}
}

func TestParseCanonicalJobRunID_InvalidFormats(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name      string
		canonical string
		wantError error
	}{
		{
			name:      "no colon separator",
			canonical: "invalid-format",
			wantError: ErrInvalidCanonicalFormat,
		},
		{
			name:      "empty string",
			canonical: "",
			wantError: ErrEmptyCanonicalID,
		},
		{
			name:      "whitespace only",
			canonical: "   ",
			wantError: ErrEmptyCanonicalID,
		},
		{
			name:      "empty tool prefix",
			canonical: ":run-123",
			wantError: ErrInvalidCanonicalFormat,
		},
		{
			name:      "empty runID",
			canonical: "dbt:",
			wantError: ErrInvalidCanonicalFormat,
		},
		{
			name:      "only colon",
			canonical: ":",
			wantError: ErrInvalidCanonicalFormat,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := ParseCanonicalJobRunID(tc.canonical)
			if err == nil {
				t.Errorf("Expected error but got none for input: %q", tc.canonical)
			}

			if !errors.Is(err, tc.wantError) {
				t.Errorf("Expected error %v, got %v", tc.wantError, err)
			}
		})
	}
}

func TestParseCanonicalJobRunID_RoundTrip(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		namespace string
		runID     string
	}{
		{"dbt://analytics", "550e8400-e29b-41d4-a716-446655440000"},
		{"airflow://production", "manual__2025-01-01T12:00:00"},
		{"spark://cluster", "application_123456"},
		{"custom://env", "my-custom-run"},
	}

	for _, tc := range testCases {
		t.Run(tc.namespace, func(t *testing.T) {
			// Generate canonical ID
			canonical := GenerateJobRunID(tc.namespace, tc.runID)

			// Parse it back
			_, parsedRunID, err := ParseCanonicalJobRunID(canonical)
			if err != nil {
				t.Errorf("ParseCanonicalJobRunID failed: %v", err)
			}

			if parsedRunID != tc.runID {
				t.Errorf("Round-trip failed: got runID %q, expected %q", parsedRunID, tc.runID)
			}
		})
	}
}

// ==============================================================================
// Unit Tests: Length Validation
// ==============================================================================

func TestGenerateJobRunID_LengthValidation(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name           string
		namespace      string
		runID          string
		expectTruncate bool
	}{
		{
			name:           "normal length",
			namespace:      "dbt://analytics",
			runID:          "550e8400-e29b-41d4-a716-446655440000",
			expectTruncate: false,
		},
		{
			name:           "maximum length boundary",
			namespace:      "dbt://analytics",
			runID:          strings.Repeat("a", 250), // "dbt:" + 250 = 254 chars
			expectTruncate: false,
		},
		{
			name:           "exceeds maximum length",
			namespace:      "airflow://production",
			runID:          strings.Repeat("x", 300), // Will exceed 255
			expectTruncate: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			canonical := GenerateJobRunID(tc.namespace, tc.runID)

			if len(canonical) > MaxCanonicalIDLength {
				t.Errorf("Canonical ID length %d exceeds maximum %d", len(canonical), MaxCanonicalIDLength)
			}

			if tc.expectTruncate {
				if len(canonical) != MaxCanonicalIDLength {
					t.Errorf("Expected truncation to %d chars, got %d", MaxCanonicalIDLength, len(canonical))
				}
			}
		})
	}
}

// ==============================================================================
// Unit Tests: Additional Edge Cases
// ==============================================================================

func TestGenerateJobRunID_MalformedNamespaces(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	testCases := []struct {
		name      string
		namespace string
		runID     string
		expected  string
	}{
		{
			name:      "no separator",
			namespace: "dbt",
			runID:     "run-123",
			expected:  "dbt:run-123", // Recognized as dbt even without ://
		},
		{
			name:      "trailing separator",
			namespace: "dbt://",
			runID:     "run-123",
			expected:  "dbt:run-123", // Tool extracted correctly
		},
		{
			name:      "uppercase tool",
			namespace: "DBT://PROD",
			runID:     "run-123",
			expected:  "dbt:run-123", // Normalized to lowercase
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GenerateJobRunID(tc.namespace, tc.runID)

			if result != tc.expected {
				t.Errorf("GenerateJobRunID() = %q, expected %q", result, tc.expected)
			}
		})
	}
}

func TestParseCanonicalJobRunID_PreservesColons(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Verify that colons in runID are preserved correctly
	testCases := []string{
		"spark:app:123:456",
		"custom:url:https://example.com:8080",
		"dbt:::multiple:::colons:::",
	}

	for _, canonical := range testCases {
		t.Run(canonical, func(t *testing.T) {
			tool, runID, err := ParseCanonicalJobRunID(canonical)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Reconstruct and verify
			reconstructed := fmt.Sprintf("%s:%s", tool, runID)
			if reconstructed != canonical {
				t.Errorf("Colon preservation failed: got %q, expected %q", reconstructed, canonical)
			}
		})
	}
}
