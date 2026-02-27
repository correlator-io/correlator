// Package canonicalization provides canonical ID generation for correlation.
//
// This package provides pure utility functions that operate on primitives (strings)
// rather than domain types, making it reusable across different event types
// (OpenLineage, OpenTelemetry, custom events).
//
// Key functions:
//   - GenerateIdempotencyKey: Prevents duplicate event processing (SHA256 hash)
//   - GenerateDatasetURN: Creates canonical dataset identifiers
//   - NormalizeNamespace: Normalizes namespace strings for correlation
package canonicalization

import (
	"crypto/sha256"
	"encoding/hex"
)

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
// identifiers. Used for idempotency keys.
//
// Returns: 64-character lowercase hex string (SHA256 output).
func hashSHA256(input string) string {
	hash := sha256.Sum256([]byte(input))

	return hex.EncodeToString(hash[:])
}
