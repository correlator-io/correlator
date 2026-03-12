import type { Incident, IncidentDetail } from "./types";

// Fixed reference time to avoid SSR/client hydration mismatch from Date.now()
const REFERENCE_TIME = new Date("2026-03-08T12:00:00Z").getTime();
const hoursAgo = (h: number) => new Date(REFERENCE_TIME - h * 3600_000).toISOString();
const daysAgo = (d: number) => new Date(REFERENCE_TIME - d * 86_400_000).toISOString();
const daysFromNow = (d: number) => new Date(REFERENCE_TIME + d * 86_400_000).toISOString();

export const MOCK_INCIDENTS: Incident[] = [
  // Open — active, needs attention
  {
    id: "32",
    testName: "unique_customers_customer_id",
    testType: "uniqueness",
    testStatus: "failed",
    datasetUrn: "postgresql://demo-postgres:5432/demo.public.customers",
    datasetName: "demo.public.customers",
    producer: "dbt",
    jobName: "demo.marts.jaffle_shop.customers.test",
    jobRunId: "019cb41e-cc5e-7a88-805c-e3b8ee67ca75",
    downstreamCount: 3,
    hasCorrelationIssue: false,
    executedAt: hoursAgo(2),
    resolutionStatus: "open",
    retryContext: {
      totalAttempts: 2,
      currentAttempt: 2,
      allFailed: true,
      rootRunId: "019cb41e-a770-7fc5-a57a-8f1e91d3ab20",
    },
  },
  {
    id: "45",
    testName: "not_null_orders_order_id",
    testType: "not_null",
    testStatus: "failed",
    datasetUrn: "postgresql://demo-postgres:5432/demo.public.orders",
    datasetName: "demo.public.orders",
    producer: "dbt",
    jobName: "demo.marts.jaffle_shop.orders.test",
    jobRunId: "019cb41e-cc5e-7ace-85e3-9962b28a4e31",
    downstreamCount: 5,
    hasCorrelationIssue: false,
    executedAt: hoursAgo(3),
    resolutionStatus: "open",
    retryContext: null,
  },

  // Acknowledged — someone is investigating
  {
    id: "28",
    testName: "accepted_values_payments_payment_method",
    testType: "accepted_values",
    testStatus: "failed",
    datasetUrn: "postgresql://demo-postgres:5432/demo.public.payments",
    datasetName: "demo.public.payments",
    producer: "dbt",
    jobName: "demo.staging.stg_payments.test",
    jobRunId: "019cb41e-bb22-7abc-9def-222222222222",
    downstreamCount: 2,
    hasCorrelationIssue: false,
    executedAt: hoursAgo(6),
    resolutionStatus: "acknowledged",
    retryContext: null,
  },

  // Resolved — auto-resolved on re-pass
  {
    id: "18",
    testName: "not_null_products_product_id",
    testType: "not_null",
    testStatus: "failed",
    datasetUrn: "postgresql://demo-postgres:5432/demo.public.products",
    datasetName: "demo.public.products",
    producer: "dbt",
    jobName: "demo.marts.jaffle_shop.products.test",
    jobRunId: "019cb41e-aa11-7abc-9def-333333333333",
    downstreamCount: 1,
    hasCorrelationIssue: false,
    executedAt: daysAgo(1),
    resolutionStatus: "resolved",
    resolvedBy: "auto",
    retryContext: {
      totalAttempts: 3,
      currentAttempt: 3,
      allFailed: false,
      rootRunId: "019cb41e-9900-7abc-9def-444444444444",
    },
  },
  // Resolved — manually
  {
    id: "12",
    testName: "freshness_raw_events",
    testType: "freshness",
    testStatus: "failed",
    datasetUrn: "postgresql://demo-postgres:5432/demo.public.raw_events",
    datasetName: "demo.public.raw_events",
    producer: "airflow",
    jobName: "dag_ingest_events.task_load_raw",
    jobRunId: "019cb41e-8800-7abc-9def-555555555555",
    downstreamCount: 8,
    hasCorrelationIssue: false,
    executedAt: daysAgo(2),
    resolutionStatus: "resolved",
    resolvedBy: "user",
    retryContext: null,
  },

  // Muted — false positive, expires in 25 days
  {
    id: "9",
    testName: "schema_check_legacy_users",
    testType: "schema",
    testStatus: "failed",
    datasetUrn: "postgresql://demo-postgres:5432/demo.public.legacy_users",
    datasetName: "demo.public.legacy_users",
    producer: "great_expectations",
    jobName: "ge_validation_suite.legacy_checks",
    jobRunId: "019cb41e-7700-7abc-9def-666666666666",
    downstreamCount: 0,
    hasCorrelationIssue: false,
    executedAt: daysAgo(5),
    resolutionStatus: "muted",
    muteExpiresAt: daysFromNow(25),
    retryContext: null,
  },
];

// ============================================================
// Detail page mocks — one per resolution state
// ============================================================

export const MOCK_INCIDENT_DETAIL: IncidentDetail = {
  id: "32",
  test: {
    name: "unique_customers_customer_id",
    type: "uniqueness",
    status: "failed",
    message: "Got 47% fewer rows than expected. Expected >= 1000, got 530.",
    executedAt: hoursAgo(2),
    durationMs: 1247,
    producer: "dbt",
  },
  dataset: {
    urn: "postgresql://demo-postgres:5432/demo.public.customers",
    name: "demo.public.customers",
    namespace: "dbt",
  },
  job: {
    name: "demo.marts.jaffle_shop.customers.test",
    namespace: "dbt",
    runId: "019cb41e-cc5e-7a88-805c-e3b8ee67ca75",
    producer: "dbt",
    status: "FAILED",
    startedAt: hoursAgo(2.1),
    completedAt: hoursAgo(2),
    orchestration: [
      {
        name: "dag_daily_pipeline",
        namespace: "airflow",
        runId: "019cb41e-a770-7fc5-a57a-8f1e91d3ab20",
        producer: "airflow",
        status: "FAILED",
      },
      {
        name: "dag_daily_pipeline.task_run_dbt_test",
        namespace: "airflow",
        runId: "019cb41e-bb22-7fc5-a57a-999999999999",
        producer: "airflow",
        status: "FAILED",
      },
    ],
  },
  upstream: [
    {
      urn: "postgresql://demo-postgres:5432/demo.public.raw_customers",
      name: "demo.public.raw_customers",
      depth: 1,
      childUrn: "postgresql://demo-postgres:5432/demo.public.customers",
      producer: "airflow",
    },
  ],
  downstream: [
    {
      urn: "postgresql://demo-postgres:5432/demo.public.fct_customer_orders",
      name: "demo.public.fct_customer_orders",
      depth: 1,
      parentUrn: "postgresql://demo-postgres:5432/demo.public.customers",
      producer: "dbt",
    },
    {
      urn: "postgresql://demo-postgres:5432/demo.public.dim_customer_segments",
      name: "demo.public.dim_customer_segments",
      depth: 1,
      parentUrn: "postgresql://demo-postgres:5432/demo.public.customers",
      producer: "dbt",
    },
    {
      urn: "postgresql://demo-postgres:5432/demo.public.rpt_revenue_by_customer",
      name: "demo.public.rpt_revenue_by_customer",
      depth: 2,
      parentUrn: "postgresql://demo-postgres:5432/demo.public.fct_customer_orders",
      producer: "dbt",
    },
  ],
  correlationStatus: "correlated",
  resolutionStatus: "open",
  retryContext: {
    totalAttempts: 2,
    currentAttempt: 2,
    allFailed: true,
    rootRunId: "019cb41e-a770-7fc5-a57a-8f1e91d3ab20",
    otherAttempts: [
      {
        incidentId: "14",
        attempt: 1,
        status: "failed",
        executedAt: hoursAgo(3),
        jobRunId: "019cb41e-b123-7abc-9def-1234567890ab",
        resolutionStatus: "open",
      },
    ],
  },
};

export const MOCK_ACKNOWLEDGED_DETAIL: IncidentDetail = {
  id: "28",
  test: {
    name: "accepted_values_payments_payment_method",
    type: "accepted_values",
    status: "failed",
    message: 'Got unexpected value "crypto" in column payment_method. Accepted: ["credit_card", "coupon", "bank_transfer", "gift_card"]',
    executedAt: hoursAgo(6),
    durationMs: 523,
    producer: "dbt",
  },
  dataset: {
    urn: "postgresql://demo-postgres:5432/demo.public.payments",
    name: "demo.public.payments",
    namespace: "dbt",
  },
  job: {
    name: "demo.staging.stg_payments.test",
    namespace: "dbt",
    runId: "019cb41e-bb22-7abc-9def-222222222222",
    producer: "dbt",
    status: "FAILED",
    startedAt: hoursAgo(6.1),
    completedAt: hoursAgo(6),
  },
  upstream: [
    {
      urn: "postgresql://demo-postgres:5432/demo.public.raw_payments",
      name: "demo.public.raw_payments",
      depth: 1,
      childUrn: "postgresql://demo-postgres:5432/demo.public.payments",
      producer: "airflow",
    },
  ],
  downstream: [
    {
      urn: "postgresql://demo-postgres:5432/demo.public.fct_orders",
      name: "demo.public.fct_orders",
      depth: 1,
      parentUrn: "postgresql://demo-postgres:5432/demo.public.payments",
      producer: "dbt",
    },
    {
      urn: "postgresql://demo-postgres:5432/demo.public.rpt_revenue",
      name: "demo.public.rpt_revenue",
      depth: 2,
      parentUrn: "postgresql://demo-postgres:5432/demo.public.fct_orders",
      producer: "dbt",
    },
  ],
  correlationStatus: "correlated",
  resolutionStatus: "acknowledged",
  resolvedBy: "user",
  resolvedAt: hoursAgo(4),
  retryContext: null,
};

export const MOCK_RESOLVED_DETAIL: IncidentDetail = {
  id: "18",
  test: {
    name: "not_null_products_product_id",
    type: "not_null",
    status: "failed",
    message: "Column product_id has 3 null values",
    executedAt: daysAgo(1),
    durationMs: 842,
    producer: "dbt",
  },
  dataset: {
    urn: "postgresql://demo-postgres:5432/demo.public.products",
    name: "demo.public.products",
    namespace: "dbt",
  },
  job: {
    name: "demo.marts.jaffle_shop.products.test",
    namespace: "dbt",
    runId: "019cb41e-aa11-7abc-9def-333333333333",
    producer: "dbt",
    status: "FAILED",
    startedAt: daysAgo(1.1),
    completedAt: daysAgo(1),
  },
  upstream: [],
  downstream: [
    {
      urn: "postgresql://demo-postgres:5432/demo.public.fct_product_sales",
      name: "demo.public.fct_product_sales",
      depth: 1,
      parentUrn: "postgresql://demo-postgres:5432/demo.public.products",
      producer: "dbt",
    },
  ],
  correlationStatus: "correlated",
  resolutionStatus: "resolved",
  resolvedBy: "auto",
  resolutionReason: "auto_pass",
  resolvedAt: hoursAgo(20),
  retryContext: {
    totalAttempts: 3,
    currentAttempt: 3,
    allFailed: false,
    rootRunId: "019cb41e-9900-7abc-9def-444444444444",
    otherAttempts: [
      {
        incidentId: "10",
        attempt: 1,
        status: "failed",
        executedAt: daysAgo(1.5),
        jobRunId: "019cb41e-9900-retry1-9def-777777777777",
        resolutionStatus: "resolved",
      },
      {
        incidentId: "15",
        attempt: 2,
        status: "failed",
        executedAt: daysAgo(1.2),
        jobRunId: "019cb41e-9900-retry2-9def-888888888888",
        resolutionStatus: "resolved",
      },
    ],
  },
};

export const MOCK_MUTED_DETAIL: IncidentDetail = {
  id: "9",
  test: {
    name: "schema_check_legacy_users",
    type: "schema",
    status: "failed",
    message: "Column 'legacy_status' not found in schema. Expected columns: id, name, email, legacy_status, created_at",
    executedAt: daysAgo(5),
    durationMs: 312,
    producer: "great_expectations",
  },
  dataset: {
    urn: "postgresql://demo-postgres:5432/demo.public.legacy_users",
    name: "demo.public.legacy_users",
    namespace: "great_expectations",
  },
  job: {
    name: "ge_validation_suite.legacy_checks",
    namespace: "great_expectations",
    runId: "019cb41e-7700-7abc-9def-666666666666",
    producer: "great_expectations",
    status: "FAILED",
    startedAt: daysAgo(5.1),
    completedAt: daysAgo(5),
  },
  upstream: [],
  downstream: [],
  correlationStatus: "correlated",
  resolutionStatus: "muted",
  resolvedBy: "user",
  resolutionReason: "false_positive",
  resolvedAt: daysAgo(5),
  muteExpiresAt: daysFromNow(25),
  retryContext: null,
};

export const MOCK_MANUALLY_RESOLVED_DETAIL: IncidentDetail = {
  id: "12",
  test: {
    name: "freshness_raw_events",
    type: "freshness",
    status: "failed",
    message: "Table raw_events last updated 6 hours ago. Threshold: 2 hours.",
    executedAt: daysAgo(2),
    durationMs: 156,
    producer: "airflow",
  },
  dataset: {
    urn: "postgresql://demo-postgres:5432/demo.public.raw_events",
    name: "demo.public.raw_events",
    namespace: "airflow",
  },
  job: {
    name: "dag_ingest_events.task_load_raw",
    namespace: "airflow",
    runId: "019cb41e-8800-7abc-9def-555555555555",
    producer: "airflow",
    status: "FAILED",
    startedAt: daysAgo(2.1),
    completedAt: daysAgo(2),
  },
  upstream: [],
  downstream: [
    {
      urn: "postgresql://demo-postgres:5432/demo.public.stg_events",
      name: "demo.public.stg_events",
      depth: 1,
      parentUrn: "postgresql://demo-postgres:5432/demo.public.raw_events",
      producer: "dbt",
    },
  ],
  correlationStatus: "correlated",
  resolutionStatus: "resolved",
  resolvedBy: "user",
  resolutionReason: "manual",
  resolvedAt: daysAgo(1.5),
  retryContext: null,
};
