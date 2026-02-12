import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Format a date as relative time (e.g., "2 hours ago", "3 days ago")
 */
export function formatRelativeTime(date: string | Date): string {
  const now = new Date();
  const then = new Date(date);
  const diffMs = now.getTime() - then.getTime();
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffSecs < 60) {
    return "just now";
  } else if (diffMins < 60) {
    return `${diffMins} minute${diffMins === 1 ? "" : "s"} ago`;
  } else if (diffHours < 24) {
    return `${diffHours} hour${diffHours === 1 ? "" : "s"} ago`;
  } else if (diffDays < 7) {
    return `${diffDays} day${diffDays === 1 ? "" : "s"} ago`;
  } else {
    return formatAbsoluteTime(date);
  }
}

/**
 * Format a date as absolute time (e.g., "Jan 23, 2026 at 10:30:00 UTC")
 */
export function formatAbsoluteTime(date: string | Date): string {
  const d = new Date(date);
  return d.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
  });
}

/**
 * Format a duration in milliseconds (e.g., "1,247 ms", "2.5 s", "1m 30s")
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms.toLocaleString()} ms`;
  } else if (ms < 60000) {
    return `${(ms / 1000).toFixed(1)} s`;
  } else {
    const mins = Math.floor(ms / 60000);
    const secs = Math.floor((ms % 60000) / 1000);
    return `${mins}m ${secs}s`;
  }
}

/**
 * Truncate a string to a maximum length with ellipsis
 */
export function truncate(str: string, maxLength: number): string {
  if (str.length <= maxLength) return str;
  return str.slice(0, maxLength - 1) + "…";
}

// ============================================================
// Incident Formatting Utilities
// ============================================================

/**
 * Extract a human-readable dataset name from a URN.
 *
 * Examples:
 * - "postgresql://demo/marts.customers" → "marts.customers"
 * - "postgresql://demo/staging.stg_orders" → "staging.stg_orders"
 * - "demo_postgres/orders" → "orders"
 * - "orders" → "orders"
 */
export function extractDatasetName(datasetUrn: string): string {
  // Try to match schema.table pattern after a slash
  const schemaTableMatch = datasetUrn.match(/\/([^/]+\.[^/]+)$/);
  if (schemaTableMatch) {
    return schemaTableMatch[1];
  }

  // Try to get last segment after slash
  const parts = datasetUrn.split("/").filter(Boolean);
  if (parts.length > 0) {
    return parts[parts.length - 1];
  }

  // Fallback to original
  return datasetUrn;
}

/**
 * Extract the table name (without schema) from a dataset name.
 *
 * Examples:
 * - "marts.customers" → "customers"
 * - "staging.stg_orders" → "stg_orders"
 * - "orders" → "orders"
 */
export function extractTableName(datasetName: string): string {
  const parts = datasetName.split(".");
  return parts[parts.length - 1];
}

/**
 * Map test type to human-readable display name.
 */
export function mapTestType(testType: string): string {
  const typeMap: Record<string, string> = {
    unique: "uniqueness",
    not_null: "not_null",
    accepted_values: "accepted_values",
    relationships: "relationships",
    freshness: "freshness",
    dataQualityAssertion: "data quality",
  };

  return typeMap[testType] || "test";
}

/**
 * Extract column name from test name if available.
 *
 * Examples:
 * - "unique(customer_id)" → "customer_id"
 * - "not_null(email)" → "email"
 * - "row_count_check" → null
 */
export function extractColumnFromTestName(testName: string): string | null {
  const match = testName.match(/\(([^)]+)\)/);
  return match ? match[1] : null;
}

/**
 * Format an incident title for display (full version with test type).
 *
 * @deprecated Use formatIncidentId instead for the simpler INC-{id} · {test_name} format.
 */
export function formatIncidentTitle(
  datasetUrn: string,
  testType: string,
  testName: string
): string {
  const datasetName = extractDatasetName(datasetUrn);
  const tableName = extractTableName(datasetName);
  const columnName = extractColumnFromTestName(testName);
  const testTypeDisplay = mapTestType(testType);

  if (columnName) {
    return `${tableName}.${columnName} ${testTypeDisplay} failure`;
  }

  return `${tableName} ${testTypeDisplay} failure`;
}

/**
 * Format incident ID with test name for display.
 *
 * Format: "INC-{id} · {testName}"
 *
 * Examples:
 * - "INC-50 · unique(customer_id)"
 * - "INC-51 · not_null(email)"
 */
export function formatIncidentId(id: string, testName: string): string {
  return `INC-${id} · ${testName}`;
}
