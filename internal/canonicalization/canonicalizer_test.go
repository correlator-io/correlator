// Package canonicalization provides canonical ID generation for correlation.
package canonicalization

import (
	"testing"
)

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
