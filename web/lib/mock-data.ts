import type {
  Incident,
  IncidentDetail,
  CorrelationHealth,
  DownstreamDataset,
} from "./types";

/**
 * Mock incidents for the list page
 * Intentionally shows namespace inconsistency between tools:
 * - dbt uses: postgresql://prod/public.xxx
 * - GE uses: postgres_prod.public.xxx (orphan!)
 * - Airflow uses: postgresql://prod/public.xxx
 */
export const MOCK_INCIDENTS: Incident[] = [
  {
    id: "1",
    testName: "not_null_orders_customer_id",
    testType: "not_null",
    testStatus: "failed",
    datasetUrn: "postgresql://prod/public.orders",
    datasetName: "orders",
    producer: "dbt",
    jobName: "daily_finance_pipeline",
    jobRunId: "dbt:abc123-def456",
    downstreamCount: 2,
    hasCorrelationIssue: false,
    executedAt: "2026-01-23T10:30:00Z",
  },
  {
    id: "2",
    testName: "expect_column_values_to_not_be_null",
    testType: "expect_column_values_to_not_be_null",
    testStatus: "failed",
    datasetUrn: "postgres_prod.public.orders", // GE namespace - shows inconsistency!
    datasetName: "orders",
    producer: "great_expectations",
    jobName: "ge_validation_checkpoint",
    jobRunId: "ge:validation-2026-01-23",
    downstreamCount: 0, // Can't correlate due to namespace mismatch
    hasCorrelationIssue: true, // Orphan namespace
    executedAt: "2026-01-23T10:35:00Z",
  },
  {
    id: "3",
    testName: "unique_customers_customer_id",
    testType: "unique",
    testStatus: "passed",
    datasetUrn: "postgresql://prod/public.customers",
    datasetName: "customers",
    producer: "dbt",
    jobName: "customer_pipeline",
    jobRunId: "dbt:cust-789",
    downstreamCount: 3,
    hasCorrelationIssue: false,
    executedAt: "2026-01-23T09:15:00Z",
  },
  {
    id: "4",
    testName: "relationships_orders_customer_id",
    testType: "relationships",
    testStatus: "failed",
    datasetUrn: "postgresql://prod/public.orders",
    datasetName: "orders",
    producer: "dbt",
    jobName: "daily_finance_pipeline",
    jobRunId: "dbt:abc123-def456",
    downstreamCount: 2,
    hasCorrelationIssue: false,
    executedAt: "2026-01-23T10:30:15Z",
  },
  {
    id: "5",
    testName: "expect_column_values_to_be_between",
    testType: "expect_column_values_to_be_between",
    testStatus: "warning",
    datasetUrn: "postgres_prod.public.payments", // Another GE orphan
    datasetName: "payments",
    producer: "great_expectations",
    jobName: "ge_validation_checkpoint",
    jobRunId: "ge:validation-2026-01-23",
    downstreamCount: 0,
    hasCorrelationIssue: true,
    executedAt: "2026-01-23T10:36:00Z",
  },
  {
    id: "6",
    testName: "not_null_products_sku",
    testType: "not_null",
    testStatus: "passed",
    datasetUrn: "postgresql://prod/public.products",
    datasetName: "products",
    producer: "dbt",
    jobName: "inventory_pipeline",
    jobRunId: "dbt:inv-456",
    downstreamCount: 2,
    hasCorrelationIssue: false,
    executedAt: "2026-01-23T08:00:00Z",
  },
  {
    id: "7",
    testName: "accepted_values_status",
    testType: "accepted_values",
    testStatus: "passed",
    datasetUrn: "postgresql://prod/public.orders",
    datasetName: "orders",
    producer: "dbt",
    jobName: "daily_finance_pipeline",
    jobRunId: "dbt:abc123-def456",
    downstreamCount: 2,
    hasCorrelationIssue: false,
    executedAt: "2026-01-23T10:30:30Z",
  },
];

/**
 * Mock incident detail with full correlation data
 */
export const MOCK_INCIDENT_DETAILS: Record<string, IncidentDetail> = {
  "1": {
    id: "1",
    test: {
      name: "not_null_orders_customer_id",
      type: "not_null",
      status: "failed",
      message:
        "Found 847 null values in customer_id column. Expected 0 null values.\n\nFailing rows sample:\n- order_id: 12847, customer_id: NULL, created_at: 2026-01-23 09:15:00\n- order_id: 12848, customer_id: NULL, created_at: 2026-01-23 09:15:01\n- order_id: 12849, customer_id: NULL, created_at: 2026-01-23 09:15:02",
      executedAt: "2026-01-23T10:30:00Z",
      durationMs: 1247,
    },
    dataset: {
      urn: "postgresql://prod/public.orders",
      name: "orders",
      namespace: "postgresql://prod/public",
    },
    job: {
      name: "build_orders_model",
      namespace: "daily_finance_pipeline",
      runId: "dbt:abc123-def456",
      producer: "dbt",
      status: "COMPLETE",
      startedAt: "2026-01-23T10:25:00Z",
      completedAt: "2026-01-23T10:28:45Z",
    },
    downstream: [
      {
        urn: "postgresql://prod/public.fct_daily_revenue",
        name: "fct_daily_revenue",
        depth: 1,
        parentUrn: "postgresql://prod/public.orders",
      },
      {
        urn: "postgresql://prod/public.exec_dashboard",
        name: "exec_dashboard",
        depth: 2,
        parentUrn: "postgresql://prod/public.fct_daily_revenue",
      },
    ],
    correlationStatus: "correlated",
  },
  "2": {
    id: "2",
    test: {
      name: "expect_column_values_to_not_be_null",
      type: "expect_column_values_to_not_be_null",
      status: "failed",
      message:
        'Expectation failed: expect_column_values_to_not_be_null\nColumn: customer_id\nUnexpected null count: 847\nTotal rows: 15234\nFailure rate: 5.56%',
      executedAt: "2026-01-23T10:35:00Z",
      durationMs: 2341,
    },
    dataset: {
      urn: "postgres_prod.public.orders",
      name: "orders",
      namespace: "postgres_prod.public",
    },
    job: null, // No correlation - orphan namespace!
    downstream: [],
    correlationStatus: "orphan",
  },
  "3": {
    id: "3",
    test: {
      name: "unique_customers_customer_id",
      type: "unique",
      status: "passed",
      message: "All 8,432 values are unique.",
      executedAt: "2026-01-23T09:15:00Z",
      durationMs: 456,
    },
    dataset: {
      urn: "postgresql://prod/public.customers",
      name: "customers",
      namespace: "postgresql://prod/public",
    },
    job: {
      name: "build_customers_model",
      namespace: "customer_pipeline",
      runId: "dbt:cust-789",
      producer: "dbt",
      status: "COMPLETE",
      startedAt: "2026-01-23T09:10:00Z",
      completedAt: "2026-01-23T09:14:30Z",
    },
    downstream: [
      {
        urn: "postgresql://prod/public.fct_customer_ltv",
        name: "fct_customer_ltv",
        depth: 1,
        parentUrn: "postgresql://prod/public.customers",
      },
      {
        urn: "postgresql://prod/public.customer_segments",
        name: "customer_segments",
        depth: 1,
        parentUrn: "postgresql://prod/public.customers",
      },
      {
        urn: "postgresql://prod/public.marketing_targets",
        name: "marketing_targets",
        depth: 2,
        parentUrn: "postgresql://prod/public.fct_customer_ltv",
      },
    ],
    correlationStatus: "correlated",
  },
};

/**
 * Mock correlation health data showing orphan namespaces
 */
export const MOCK_CORRELATION_HEALTH: CorrelationHealth = {
  correlationRate: 0.87,
  totalDatasets: 47,
  orphanNamespaces: [
    {
      namespace: "postgres_prod",
      producer: "great_expectations",
      lastSeen: "2026-01-23T10:36:00Z",
      eventCount: 12,
      suggestedAlias: "postgresql://prod/public",
    },
    {
      namespace: "snowflake://analytics",
      producer: "airflow",
      lastSeen: "2026-01-22T14:00:00Z",
      eventCount: 8,
      suggestedAlias: null,
    },
    {
      namespace: "bigquery:project.dataset",
      producer: "dbt",
      lastSeen: "2026-01-23T06:00:00Z",
      eventCount: 5,
      suggestedAlias: null,
    },
  ],
};

/**
 * Generate YAML config example for fixing orphan namespaces
 */
export function generateYamlConfig(orphanNamespaces: typeof MOCK_CORRELATION_HEALTH.orphanNamespaces): string {
  const aliases = orphanNamespaces
    .filter((ns) => ns.suggestedAlias)
    .map(
      (ns) =>
        `  # ${ns.producer} uses "${ns.namespace}"\n  - from: "${ns.namespace}"\n    to: "${ns.suggestedAlias}"`
    )
    .join("\n\n");

  return `# correlator.yaml
namespace_aliases:
${aliases || "  # No suggested aliases"}

# Add manual aliases for remaining namespaces:
${orphanNamespaces
  .filter((ns) => !ns.suggestedAlias)
  .map((ns) => `  # - from: "${ns.namespace}"\n  #   to: "your-canonical-namespace"`)
  .join("\n")}
`;
}

/**
 * Get incident detail by ID
 */
export function getIncidentDetail(id: string): IncidentDetail | undefined {
  return MOCK_INCIDENT_DETAILS[id];
}

/**
 * Filter incidents by status
 */
export function filterIncidents(
  incidents: Incident[],
  filter: "all" | "failed" | "passed" | "correlation_issues"
): Incident[] {
  switch (filter) {
    case "failed":
      return incidents.filter((i) => i.testStatus === "failed");
    case "passed":
      return incidents.filter((i) => i.testStatus === "passed");
    case "correlation_issues":
      return incidents.filter((i) => i.hasCorrelationIssue);
    default:
      return incidents;
  }
}
