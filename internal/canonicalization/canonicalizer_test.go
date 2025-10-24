// Package canonicalization provides canonical ID generation for correlation.
package canonicalization

import (
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

	id := GenerateJobRunID("dbt://analytics", "transform_orders", "550e8400-e29b-41d4-a716-446655440000")

	// Should return a non-empty hex string (SHA256 = 64 hex chars)
	if id == "" {
		t.Error("GenerateJobRunID() returned empty string")
	}

	if len(id) != 64 {
		t.Errorf("GenerateJobRunID() returned %d chars, expected 64 (SHA256 hex)", len(id))
	}

	// Should be lowercase hex
	if !isHexString(id) {
		t.Errorf("GenerateJobRunID() returned non-hex string: %s", id)
	}
}

func TestGenerateJobRunID_Airflow(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	id := GenerateJobRunID("airflow://production", "daily_etl.load_users", "airflow-run-id")

	if id == "" {
		t.Error("GenerateJobRunID() returned empty string")
	}

	if len(id) != 64 {
		t.Errorf("GenerateJobRunID() returned %d chars, expected 64", len(id))
	}
}

func TestGenerateJobRunID_Spark(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	id := GenerateJobRunID("spark://prod-cluster", "recommendation.train_model", "spark-run-id")

	if id == "" {
		t.Error("GenerateJobRunID() returned empty string")
	}

	if len(id) != 64 {
		t.Errorf("GenerateJobRunID() returned %d chars, expected 64", len(id))
	}
}

func TestGenerateJobRunID_SameRunDifferentEvents(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Same run parameters should produce same ID regardless of other factors
	id1 := GenerateJobRunID("dbt://analytics", "transform_orders", "550e8400-e29b-41d4-a716-446655440000")
	id2 := GenerateJobRunID("dbt://analytics", "transform_orders", "550e8400-e29b-41d4-a716-446655440000")

	if id1 != id2 {
		t.Errorf("GenerateJobRunID() returned different IDs for same run: %s vs %s", id1, id2)
	}
}

func TestGenerateJobRunID_DifferentRunsSameJob(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Same job, different runs should produce different IDs
	id1 := GenerateJobRunID("dbt://analytics", "transform_orders", "run-1")
	id2 := GenerateJobRunID("dbt://analytics", "transform_orders", "run-2")

	if id1 == id2 {
		t.Error("GenerateJobRunID() returned same ID for different runs")
	}
}

func TestGenerateJobRunID_Deterministic(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Call multiple times - should always return same ID
	id1 := GenerateJobRunID("test://namespace", "test_job", "test-run-id")
	id2 := GenerateJobRunID("test://namespace", "test_job", "test-run-id")
	id3 := GenerateJobRunID("test://namespace", "test_job", "test-run-id")

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
	id := GenerateJobRunID("", "", "")

	// Should still generate a hash (even if inputs are empty)
	if len(id) != 64 {
		t.Errorf("GenerateJobRunID() should return 64-char hash even for empty fields, got %d chars", len(id))
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

	// Job names with special characters, spaces, unicode
	testCases := []string{
		"job with spaces",
		"job/with/slashes",
		"job_with_underscores",
		"job-with-dashes",
		"job.with.dots",
		"job:with:colons",
		"job#with#hashes",
		"job@with@ats",
		"job$with$dollars",
		"job%with%percents",
		"job&with&ampersands",
		"job*with*asterisks",
		"job(with)parens",
		"job[with]brackets",
		"job{with}braces",
		"job<with>angles",
		"job?with?questions",
		"job!with!bangs",
		"job~with~tildes",
		"job`with`backticks",
		"job'with'quotes",
		"job\"with\"doublequotes",
		"job\\with\\backslashes",
		"job|with|pipes",
		"job,with,commas",
		"job;with;semicolons",
		"job=with=equals",
		"job+with+plus",
		"中文作业名",     //nolint:gosmopolitan
		"作業名称日本語", //nolint:gosmopolitan
		"작업이름한국어",
		"عمل_بالعربية",      // Arabic
		"работа_на_русском", // Russian
		"τόπος_εργασίας",    // Greek
		"jöb_nåme_ñîçé",     // Accented characters
		"🚀_emoji_job",       // Emoji
	}

	for _, jobName := range testCases {
		t.Run(jobName, func(t *testing.T) {
			id := GenerateJobRunID("test://namespace", jobName, "test-run-id")

			if len(id) != 64 {
				t.Errorf("GenerateJobRunID() returned %d chars for job name %q", len(id), jobName)
			}

			// Should be valid hex
			if !isHexString(id) {
				t.Errorf("GenerateJobRunID() returned non-hex string for job name %q: %s", jobName, id)
			}
		})
	}
}

// ==============================================================================
// Helper Functions
// ==============================================================================

func isHexString(s string) bool {
	if len(s) == 0 {
		return false
	}

	for _, c := range strings.ToLower(s) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}
