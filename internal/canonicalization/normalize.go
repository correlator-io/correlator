// Package canonicalization provides namespace normalization for correlation.
package canonicalization

import (
	"strings"
)

const (
	twoNamespaceParts = 2
)

// NormalizeNamespace normalizes namespace URIs to prevent correlation failures
// when different tools use different driver schemes or include default ports
// inconsistently.
//
// Normalization rules:
//  1. Scheme standardization:
//     - postgres:// → postgresql:// (SQLAlchemy/JDBC standard, most common)
//     - s3a://, s3n:// → s3:// (Spark/Hadoop → AWS standard)
//  2. Default port removal:
//     - postgresql://:5432 → postgresql:// (prevents duplicates)
//     - mysql://:3306 → mysql://
//     - mongodb://:27017 → mongodb://
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

	// 2. Remove default ports if present
	remainder = removeDefaultPort(normalizedScheme, remainder)

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

// removeDefaultPort removes default ports from the remainder of the URL.
// Examples:
//   - "db:5432/mydb" → "db/mydb" (postgresql default)
//   - "db:5433/mydb" → "db:5433/mydb" (non-default, preserved)
//   - "user@db:5432" → "user@db" (with username)
func removeDefaultPort(scheme, remainder string) string {
	// Map of default ports by scheme
	defaults := map[string]string{
		"postgresql": ":5432",
		"mysql":      ":3306",
		"mongodb":    ":27017",
		"redis":      ":6379",
	}

	defaultPort, exists := defaults[scheme]
	if !exists {
		return remainder // No default port defined for this scheme
	}

	// Handle different URL patterns:
	// 1. "db:5432" (no path) → "db"
	// 2. "db:5432/" (trailing slash) → "db/"
	// 3. "db:5432/path" (with path) → "db/path"
	// 4. "user@db:5432" (with username) → "user@db"
	// 5. "user:pass@db:5432/path" (full URL) → "user:pass@db/path"

	// Replace ":5432/" with "/" (preserves path)
	if strings.Contains(remainder, defaultPort+"/") {
		return strings.Replace(remainder, defaultPort+"/", "/", 1)
	}

	// Replace ":5432?" with "?" (preserves query params)
	if strings.Contains(remainder, defaultPort+"?") {
		return strings.Replace(remainder, defaultPort+"?", "?", 1)
	}

	// Remove trailing ":5432" if it's at the end
	if strings.HasSuffix(remainder, defaultPort) {
		return strings.TrimSuffix(remainder, defaultPort)
	}

	return remainder
}
