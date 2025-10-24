// Package canonicalization provides canonical ID generation for correlation.
//
// Canonical IDs enable correlation across different data tools by providing
// deterministic, collision-resistant identifiers for job runs and events.
//
// This package provides pure utility functions that operate on primitives (strings)
// rather than domain types, making it reusable across different event types
// (OpenLineage, OpenTelemetry, custom events).
//
// Key functions:
//   - GenerateJobRunID: Correlates events from the same job run (SHA256 of namespace + name + runID)
//   - GenerateIdempotencyKey: Prevents duplicate event processing (includes all correlation components)
//
// All IDs use SHA256 hashing for determinism and collision resistance.
package canonicalization

import (
	"crypto/sha256"
	"encoding/hex"
)

// GenerateJobRunID generates a deterministic canonical ID for a job run.
//
// Formula: SHA256(namespace + name + runID)
//
// Purpose: Correlates all events from the same job run across different event types.
// Same run with different events (START, COMPLETE) will produce the same ID.
//
// Parameters:
//   - namespace: Job namespace (e.g., "dbt://analytics", "airflow://production")
//   - name: Job name (e.g., "transform_orders", "daily_etl.load_users")
//   - runID: Run identifier (e.g., "550e8400-e29b-41d4-a716-446655440000")
//
// Examples:
//   - GenerateJobRunID("dbt://analytics", "transform_orders", "550e8400...") → "a1b2c3d4..."
//   - Same inputs always produce same ID (deterministic)
//
// Returns: 64-character lowercase hex string (SHA256 output).
func GenerateJobRunID(namespace, name, runID string) string {
	input := namespace + name + runID

	return hashSHA256(input)
}

// GenerateIdempotencyKey generates a unique key for idempotent event processing.
//
// Formula: SHA256(producer + namespace + name + runID + eventTime + eventType)
//
// Purpose: Prevents duplicate event processing while ensuring multi-tenant safety.
// Different producers with same job name will produce different keys.
//
// Parameters (IN ORDER):
//   - producer: Event producer URI (e.g., "https://github.com/dbt-labs/dbt-core/tree/1.5.0")
//   - namespace: Job namespace
//   - name: Job name
//   - runID: Run identifier
//   - eventTime: Event timestamp in RFC3339Nano format (MUST include full nanosecond precision)
//   - eventType: Event type (START, RUNNING, COMPLETE, FAIL, ABORT, OTHER)
//
// Examples:
//   - Same inputs → same key → deduplication
//   - Different eventTime → different key → allows multiple events per run
//   - Different producer → different key → multi-tenant safety
//
// Returns: 64-character lowercase hex string (SHA256 output).
func GenerateIdempotencyKey(producer, namespace, name, runID, eventTime, eventType string) string {
	input := producer + namespace + name + runID + eventTime + eventType

	return hashSHA256(input)
}

// hashSHA256 computes the SHA256 hash of the input string.
//
// This is a helper function for generating deterministic, collision-resistant
// identifiers. All canonical IDs use this function for consistency.
//
// Returns: 64-character lowercase hex string (SHA256 output).
func hashSHA256(input string) string {
	hash := sha256.Sum256([]byte(input))

	return hex.EncodeToString(hash[:])
}
