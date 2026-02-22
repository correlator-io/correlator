import type {
  Incident,
  IncidentDetail,
  CorrelationHealth,
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
      producer: "dbt",
    },
    dataset: {
      urn: "postgresql://prod/public.orders",
      name: "orders",
      namespace: "postgresql://prod/public",
    },
    job: {
      name: "model.jaffle_shop_demo.orders",
      namespace: "dbt://demo",
      runId: "dbt:abc123-def456",
      producer: "dbt",
      status: "COMPLETE",
      startedAt: "2026-01-23T10:25:00Z",
      completedAt: "2026-01-23T10:28:45Z",
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:invocation-789",
        status: "COMPLETE",
        completedAt: "2026-01-23T10:29:00Z",
      },
      rootParent: {
        name: "demo_pipeline",
        namespace: "airflow://demo",
        runId: "airflow:dag-run-001",
        producer: "airflow",
        status: "COMPLETE",
        completedAt: "2026-01-23T10:30:00Z",
      },
    },
    upstream: [
      {
        urn: "postgresql://prod/public.stg_orders",
        name: "stg_orders",
        depth: 1,
        childUrn: "postgresql://prod/public.orders",
      },
    ],
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
      producer: "great_expectations",
    },
    dataset: {
      urn: "postgres_prod.public.orders",
      name: "orders",
      namespace: "postgres_prod.public",
    },
    job: null, // No correlation - orphan namespace!
    upstream: [],
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
      producer: "dbt",
    },
    dataset: {
      urn: "postgresql://prod/public.customers",
      name: "customers",
      namespace: "postgresql://prod/public",
    },
    job: {
      name: "model.jaffle_shop_demo.customers",
      namespace: "dbt://demo",
      runId: "dbt:cust-789",
      producer: "dbt",
      status: "COMPLETE",
      startedAt: "2026-01-23T09:10:00Z",
      completedAt: "2026-01-23T09:14:30Z",
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:invocation-cust",
        status: "COMPLETE",
        completedAt: "2026-01-23T09:14:30Z",
      },
    },
    upstream: [],
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
 * Mock correlation health data showing orphan datasets
 */
export const MOCK_CORRELATION_HEALTH: CorrelationHealth = {
  correlationRate: 0.67,
  totalDatasets: 6,
  producedDatasets: 4,
  correlatedDatasets: 4,
  orphanDatasets: [
    {
      datasetUrn: "demo_postgres/customers",
      testCount: 5,
      lastSeen: "2026-01-23T10:36:00Z",
      likelyMatch: {
        datasetUrn: "postgresql://demo/marts.customers",
        confidence: 1,
        matchReason: "exact_table_name",
      },
    },
    {
      datasetUrn: "demo_postgres/orders",
      testCount: 5,
      lastSeen: "2026-01-23T10:30:00Z",
      likelyMatch: {
        datasetUrn: "postgresql://demo/marts.orders",
        confidence: 1,
        matchReason: "exact_table_name",
      },
    },
  ],
  suggestedPatterns: [
    {
      pattern: "demo_postgres/{name}",
      canonical: "postgresql://demo/marts.{name}",
      resolvesCount: 2,
      orphansResolved: ["demo_postgres/customers", "demo_postgres/orders"],
    },
  ],
};

/**
 * Get incident detail by ID (used for mock data lookups only)
 */
export function getIncidentDetail(id: string): IncidentDetail | undefined {
  return MOCK_INCIDENT_DETAILS[id];
}
