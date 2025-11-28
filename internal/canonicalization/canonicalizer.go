// Package canonicalization provides canonical ID generation for correlation.
//
// Canonical IDs enable correlation across different data tools by providing
// deterministic, human-readable identifiers for job runs and events.
//
// This package provides pure utility functions that operate on primitives (strings)
// rather than domain types, making it reusable across different event types
// (OpenLineage, OpenTelemetry, custom events).
//
// Key functions:
//   - GenerateJobRunID: Correlates events from the same job run (format: "tool:runID")
//   - GenerateIdempotencyKey: Prevents duplicate event processing (SHA256 hash)
package canonicalization

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	nameSpaceParts = 2

	// MaxCanonicalIDLength is the maximum length for canonical job run IDs.
	// Must match database schema: job_runs.job_run_id VARCHAR(255).
	MaxCanonicalIDLength = 255
)

// Sentinel errors for canonicalization operations.
var (
	// ErrCanonicalIDTooLong is returned when canonical ID exceeds max length.
	ErrCanonicalIDTooLong = errors.New("canonical job run ID exceeds maximum length")

	// ErrInvalidCanonicalFormat is returned when canonical ID has invalid format.
	ErrInvalidCanonicalFormat = errors.New("invalid canonical job run ID format: expected 'tool:runID'")

	// ErrEmptyCanonicalID is returned when canonical ID is empty or whitespace.
	ErrEmptyCanonicalID = errors.New("canonical job run ID cannot be empty")
)

// GenerateJobRunID generates a canonical ID for a job run in format "tool:runID".
//
// Formula: "{tool}:{runID}" where tool is extracted from namespace
//
// Purpose: Correlates all events from the same job run across different event types.
// Same run with different events (START, COMPLETE) will produce the same ID.
// The format is human-readable and enables easy querying by tool type.
//
// Parameters:
//   - namespace: Job namespace (e.g., "dbt://analytics", "airflow://production")
//   - runID: Run identifier (e.g., "550e8400-e29b-41d4-a716-446655440000")
//
// Tool extraction from namespace:
//   - "dbt://analytics" → "dbt"
//   - "airflow://production" → "airflow"
//   - "spark://cluster" → "spark"
//   - "custom://namespace" → "custom"
//   - "" (empty) → "unknown"
//
// Examples:
//   - GenerateJobRunID("dbt://analytics", "transform_orders", "abc-123") → "dbt:abc-123"
//   - GenerateJobRunID("airflow://prod", "etl", "manual__2025-01-01T12:00:00") → "airflow:manual__2025-01-01T12:00:00"
//   - Same inputs always produce same ID (deterministic)
//
// Returns: Canonical job run ID in format "tool:runID".
func GenerateJobRunID(namespace, runID string) string {
	tool := extractToolFromNamespace(namespace)
	canonical := fmt.Sprintf("%s:%s", tool, runID)

	// Pragmatic length handling for MVP: Truncate if too long.
	// In production, consider returning error instead.
	if len(canonical) > MaxCanonicalIDLength {
		canonical = canonical[:MaxCanonicalIDLength]
	}

	return canonical
}

// ParseCanonicalJobRunID splits a canonical job run ID into tool and runID components.
//
// Expected format: "tool:runID" (e.g., "dbt:abc-123", "airflow:manual__2025-01-01T12:00:00")
//
// This function is the reverse of GenerateJobRunID and enables:
//   - Extracting tool prefix for filtering (e.g., "WHERE tool = 'dbt'")
//   - Retrieving original run ID for debugging/logging
//   - Validating canonical ID format before storage
//
// Validation rules:
//   - Must contain at least one colon separator
//   - Tool prefix (before first colon) must not be empty
//   - Run ID (after first colon) must not be empty
//   - If runID contains colons, only splits on FIRST colon (preserves rest)
//
// Examples:
//   - ParseCanonicalJobRunID("dbt:abc-123") → ("dbt", "abc-123", nil)
//   - ParseCanonicalJobRunID("spark:app:123:456") → ("spark", "app:123:456", nil)
//   - ParseCanonicalJobRunID("invalid") → ("", "", ErrInvalidCanonicalFormat)
//   - ParseCanonicalJobRunID("dbt:") → ("", "", ErrInvalidCanonicalFormat)
//   - ParseCanonicalJobRunID(":run-123") → ("", "", ErrInvalidCanonicalFormat)
//
// Returns:
//   - tool: Tool prefix (e.g., "dbt", "airflow", "spark", "custom", "unknown")
//   - runID: Original run identifier
//   - error: Validation error if format is invalid
func ParseCanonicalJobRunID(canonical string) (string, string, error) {
	// Validate non-empty
	canonical = strings.TrimSpace(canonical)
	if canonical == "" {
		return "", "", ErrEmptyCanonicalID
	}

	// Split on first colon only (preserves colons in runID)
	parts := strings.SplitN(canonical, ":", nameSpaceParts)

	// Must have exactly 2 parts
	if len(parts) != nameSpaceParts {
		return "", "", fmt.Errorf("%w: missing ':' separator", ErrInvalidCanonicalFormat)
	}

	tool := strings.TrimSpace(parts[0])
	runID := strings.TrimSpace(parts[1])

	// Both parts must be non-empty
	if tool == "" {
		return "", "", fmt.Errorf("%w: empty tool prefix", ErrInvalidCanonicalFormat)
	}

	if runID == "" {
		return "", "", fmt.Errorf("%w: empty runID", ErrInvalidCanonicalFormat)
	}

	return tool, runID, nil
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

// extractToolFromNamespace extracts the tool name from an OpenLineage namespace.
//
// OpenLineage namespaces typically follow the pattern "tool://environment"
// (e.g., "dbt://analytics", "airflow://production", "spark://cluster").
//
// This function extracts the tool prefix before "://" and normalizes it.
//
// Normalization rules:
//   - Known tools (dbt, airflow, spark) → lowercase tool name
//   - Unknown tools → "custom"
//   - Empty namespace → "unknown"
//
// Examples:
//   - "dbt://analytics" → "dbt"
//   - "airflow://production" → "airflow"
//   - "spark://cluster" → "spark"
//   - "custom-tool://env" → "custom"
//   - "" → "unknown"
//   - "test://namespace" → "custom"
//
// Returns: Normalized tool name string.
func extractToolFromNamespace(namespace string) string {
	// Handle empty namespace
	if strings.TrimSpace(namespace) == "" {
		return "unknown"
	}

	// Extract tool prefix (before "://")
	parts := strings.SplitN(namespace, "://", nameSpaceParts)
	tool := strings.ToLower(strings.TrimSpace(parts[0]))

	// Normalize known tools
	switch tool {
	case "dbt":
		return "dbt"
	case "airflow":
		return "airflow"
	case "spark":
		return "spark"
	default:
		return "custom"
	}
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
