// Package canonicalization provides dataset URN construction for correlation.
//
// Dataset URNs (Uniform Resource Names) are canonical identifiers that enable
// correlation of lineage edges across different data tools and systems.
//
// URN Format: {namespace}/{name}
// Examples:
//   - PostgreSQL: "postgres://prod-db:5432/analytics.public.orders"
//   - BigQuery: "bigquery/project.dataset.table"
//   - S3: "s3://bucket//path/to/file.parquet" (note: double slash for root paths)
//
// Spec: https://openlineage.io/docs/spec/naming#dataset-naming
package canonicalization

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors for URN operations.
var (
	ErrURNMissingDelimiter    = errors.New("invalid URN format: missing '/' delimiter")
	ErrURNEmptyNamespace      = errors.New("invalid URN format: empty namespace")
	ErrURNEmptyName           = errors.New("invalid URN format: empty name")
	ErrURNEmptyNameAfterDelim = errors.New("invalid URN format: empty name after delimiter")
)

const (
	protocolSuffixLen = 3 // Length of "://" protocol suffix
)

// GenerateDatasetURN constructs a canonical URN from namespace and name components.
//
// Format: {namespace}/{name}
//
// The URN format uses a single forward slash as delimiter, which creates
// double slashes for S3/HDFS root paths (intentional per OpenLineage spec).
//
// Parameters:
//   - namespace: Data source identifier (e.g., "postgres://prod-db:5432", "s3://bucket", "bigquery")
//   - name: Dataset path (e.g., "analytics.public.orders", "/path/to/file.parquet", "project.dataset.table")
//
// Examples:
//   - GenerateDatasetURN("postgres://prod-db:5432", "analytics.public.orders")
//     → "postgres://prod-db:5432/analytics.public.orders"
//   - GenerateDatasetURN("s3://bucket", "/file.csv") → "s3://bucket//file.csv" (double slash correct)
//   - GenerateDatasetURN("bigquery", "project.dataset.table") → "bigquery/project.dataset.table"
//   - GenerateDatasetURN("", "table") → "/table"
//   - GenerateDatasetURN("namespace", "") → "namespace/"
//
// Returns: URN string (always includes delimiter even if namespace or name is empty).
func GenerateDatasetURN(namespace, name string) string {
	// Simple concatenation with "/" delimiter
	// Intentionally preserves double slashes for S3/HDFS root paths
	return namespace + "/" + name
}

// ParseDatasetURN parses a URN string into namespace and name components.
//
// Format: {namespace}/{name}
//
// The parser handles URNs with and without "://" protocol prefixes:
//   - For URNs with "://", finds the delimiter "/" AFTER the "://"
//   - For URNs without "://", uses the FIRST "/" as delimiter
//
// Examples:
//   - "postgres://prod-db:5432/analytics.public.orders" → ("postgres://prod-db:5432", "analytics.public.orders")
//   - "s3://bucket//path/to/file" → ("s3://bucket", "/path/to/file")
//   - "bigquery/project.dataset.table" → ("bigquery", "project.dataset.table")
//
// Returns (IN ORDER):
//   - namespace: Everything before the delimiter "/"
//   - name: Everything after the delimiter "/"
//   - error: If URN format is invalid (no "/" delimiter or empty components).
func ParseDatasetURN(urn string) (string, string, error) {
	// Check for "://" protocol prefix
	protocolIdx := strings.Index(urn, "://")

	var delimiterIdx int

	if protocolIdx != -1 {
		// Has protocol (e.g., "postgres://prod-db:5432/analytics.public.orders")
		// Find the "/" AFTER the "://"
		searchStart := protocolIdx + protocolSuffixLen
		relativeIdx := strings.Index(urn[searchStart:], "/")

		if relativeIdx == -1 {
			return "", "", ErrURNMissingDelimiter
		}

		delimiterIdx = searchStart + relativeIdx
	} else {
		// No protocol (e.g., "bigquery/project.dataset.table")
		// Find the first "/"
		delimiterIdx = strings.Index(urn, "/")

		if delimiterIdx == -1 {
			return "", "", ErrURNMissingDelimiter
		}
	}

	// Split on delimiter
	namespace := urn[:delimiterIdx]
	name := urn[delimiterIdx+1:]

	// Validate components
	if namespace == "" {
		return "", "", ErrURNEmptyNamespace
	}

	if name == "" {
		return "", "", ErrURNEmptyName
	}
	// Check for lone slash (indicates malformed URN like "namespace//")
	// S3 root paths like "s3://bucket//file.csv" have name="/file.csv" which is valid
	// But "namespace//" has name="/" which is invalid (no actual name after the slash)
	if name == "/" {
		return "", "", ErrURNEmptyNameAfterDelim
	}

	return namespace, name, nil
}

// NormalizeDatasetURN normalizes a URN by trimming whitespace and validating format.
//
// Normalization steps:
//  1. Trim leading and trailing whitespace
//  2. Validate URN format (must contain "/" delimiter)
//  3. Return normalized URN
//
// Examples:
//   - "  postgres://prod-db:5432/analytics.public.orders  " → "postgres://prod-db:5432/analytics.public.orders"
//   - "s3://bucket//file.csv" → "s3://bucket//file.csv" (preserves double slash)
//
// Returns:
//   - normalized URN string
//   - error if URN format is invalid (no delimiter or empty after trimming).
func NormalizeDatasetURN(urn string) (string, error) {
	// Trim whitespace
	normalized := strings.TrimSpace(urn)

	// Validate by parsing
	_, _, err := ParseDatasetURN(normalized)
	if err != nil {
		return "", fmt.Errorf("invalid URN format: %w", err)
	}

	return normalized, nil
}
