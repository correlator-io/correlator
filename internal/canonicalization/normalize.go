// Package canonicalization provides namespace normalization for correlation.
package canonicalization

import (
	"strings"
)

const (
	twoNamespaceParts = 2
)

// NormalizeNamespace normalizes namespace URIs to prevent correlation failures
// when different tools use different driver schemes or include ports
// inconsistently.
//
// Normalization rules:
//  1. Scheme standardization:
//     - postgres:// → postgresql:// (SQLAlchemy/JDBC standard, most common)
//     - s3a://, s3n:// → s3:// (Spark/Hadoop → AWS standard)
//  2. Port removal:
//     - All ports are stripped from URL-like namespaces.
//     - GE (openlineage-integration-common) uses Python's url.hostname which
//     strips all ports unconditionally, while dbt and Airflow always include
//     ports. Stripping all ports ensures cross-tool consistency.
//  3. Non-URL namespaces (bigquery, kafka) pass through unchanged
//
// Rationale:
// OpenLineage events from different tools (dbt, Great Expectations, Airflow, Spark)
// may reference the same dataset with different URI schemes. Without normalization,
// these appear as different datasets in the lineage graph, causing correlation
// accuracy to drop below 90% (violates MVP success criteria).
//
// Design Decision:
// Standardize on most common scheme based on tool ecosystem survey:
//   - postgresql (used by: SQLAlchemy, JDBC, Knex.js, Sequelize, Prisma)
//   - postgres (used by: psycopg2, libpq)
//     Verdict: postgresql is more widely adopted across language ecosystems.
//
// Implementation Note:
// We parse the URL manually instead of using net/url.Parse() + String() to avoid
// automatic URL encoding of special characters (e.g., *** → %2A%2A%2A, @ → %40).
// OpenLineage events contain raw URIs with masked passwords and special characters
// that should be preserved as-is for string matching.
//
// Examples:
//   - NormalizeNamespace("postgres://prod-db:5432") → "postgresql://prod-db"
//   - NormalizeNamespace("postgres://prod-db:5433") → "postgresql://prod-db"
//   - NormalizeNamespace("s3a://bucket") → "s3://bucket"
//   - NormalizeNamespace("bigquery") → "bigquery" (passthrough)
//
// Returns: Normalized namespace string.
func NormalizeNamespace(namespace string) string {
	// Check if namespace contains "://" (URL-like structure)
	if !strings.Contains(namespace, "://") {
		// Not a URL - return as-is
		// Examples: "bigquery", "kafka", "snowflake"
		return namespace
	}

	// Parse URL components manually to avoid auto-encoding
	parts := strings.SplitN(namespace, "://", twoNamespaceParts)
	if len(parts) != twoNamespaceParts {
		return namespace // Malformed, return as-is
	}

	scheme := parts[0]
	remainder := parts[1]

	// 1. Normalize scheme (lowercase + standardization)
	normalizedScheme := normalizeScheme(scheme)

	// 2. Remove port if present
	remainder = removePort(remainder)

	return normalizedScheme + "://" + remainder
}

// normalizeScheme standardizes and lowercases the scheme.
func normalizeScheme(scheme string) string {
	switch strings.ToLower(scheme) {
	case "postgres":
		return "postgresql"
	case "s3a", "s3n":
		return "s3"
	default:
		return strings.ToLower(scheme)
	}
}

// removePort strips the port from the host portion of a URL remainder.
// GE uses Python's url.hostname which strips all ports unconditionally,
// while dbt and Airflow always include ports. Stripping all ports aligns
// namespaces across tools.
//
// Examples:
//   - "db:5432/mydb" → "db/mydb"
//   - "db:5433/mydb" → "db/mydb"
//   - "user@db:5432" → "user@db"
//   - "db" → "db" (no port, unchanged)
func removePort(remainder string) string {
	// Find the host portion (after the last @ if credentials exist)
	hostStart := strings.LastIndex(remainder, "@")

	var prefix, hostAndRest string
	if hostStart >= 0 {
		prefix = remainder[:hostStart+1] // includes @
		hostAndRest = remainder[hostStart+1:]
	} else {
		prefix = ""
		hostAndRest = remainder
	}

	// Split host from path/query: first occurrence of / or ?
	pathIdx := strings.IndexAny(hostAndRest, "/?")

	var host, suffix string
	if pathIdx >= 0 {
		host = hostAndRest[:pathIdx]
		suffix = hostAndRest[pathIdx:]
	} else {
		host = hostAndRest
		suffix = ""
	}

	// Strip port from host (last :digits segment)
	if colonIdx := strings.LastIndex(host, ":"); colonIdx >= 0 {
		host = host[:colonIdx]
	}

	return prefix + host + suffix
}
