import { describe, it, expect } from "vitest";
import {
  __testing__,
  type ApiIncidentSummary,
  type ApiIncidentDetailResponse,
  type ApiCorrelationHealthResponse,
} from "./api";

const {
  normalizeTimestamp,
  transformIncident,
  transformIncidentDetail,
  transformCorrelationHealth,
} = __testing__;

describe("normalizeTimestamp", () => {
  it("returns null for Go zero-value timestamp", () => {
    expect(normalizeTimestamp("0001-01-01T00:00:00Z")).toBeNull();
  });

  it("returns null for Go zero-value with microseconds", () => {
    expect(normalizeTimestamp("0001-01-01T00:00:00.000000Z")).toBeNull();
  });

  it("returns null for null input", () => {
    expect(normalizeTimestamp(null)).toBeNull();
  });

  it("returns null for undefined input", () => {
    expect(normalizeTimestamp(undefined)).toBeNull();
  });

  it("passes through valid timestamps", () => {
    expect(normalizeTimestamp("2026-02-22T15:23:13Z")).toBe("2026-02-22T15:23:13Z");
  });
});

describe("transformIncident", () => {
  it("transforms snake_case API response to camelCase", () => {
    const apiIncident: ApiIncidentSummary = {
      id: "123",
      test_name: "not_null_orders_customer_id",
      test_type: "not_null",
      test_status: "failed",
      dataset_urn: "postgresql://prod/public.orders",
      dataset_name: "orders",
      producer: "dbt",
      job_name: "daily_finance_pipeline",
      job_run_id: "019c628f-d07e-7000-8000-000000000001",
      downstream_count: 5,
      has_correlation_issue: false,
      executed_at: "2026-01-23T10:30:00Z",
    };

    const result = transformIncident(apiIncident);

    expect(result).toEqual({
      id: "123",
      testName: "not_null_orders_customer_id",
      testType: "not_null",
      testStatus: "failed",
      datasetUrn: "postgresql://prod/public.orders",
      datasetName: "orders",
      producer: "dbt",
      jobName: "daily_finance_pipeline",
      jobRunId: "019c628f-d07e-7000-8000-000000000001",
      downstreamCount: 5,
      hasCorrelationIssue: false,
      executedAt: "2026-01-23T10:30:00Z",
    });
  });

  it("handles correlation issues correctly", () => {
    const apiIncident: ApiIncidentSummary = {
      id: "456",
      test_name: "expect_column_values_to_not_be_null",
      test_type: "expect_column_values_to_not_be_null",
      test_status: "failed",
      dataset_urn: "postgres_prod.public.orders",
      dataset_name: "orders",
      producer: "great_expectations",
      job_name: "ge_validation",
      job_run_id: "019c628f-d07e-7000-8000-000000000002",
      downstream_count: 0,
      has_correlation_issue: true,
      executed_at: "2026-01-23T10:35:00Z",
    };

    const result = transformIncident(apiIncident);

    expect(result.hasCorrelationIssue).toBe(true);
    expect(result.downstreamCount).toBe(0);
  });
});

describe("transformIncidentDetail", () => {
  it("transforms full incident detail with job correlation", () => {
    const apiDetail: ApiIncidentDetailResponse = {
      id: "123",
      test: {
        name: "not_null_orders_customer_id",
        type: "not_null",
        status: "failed",
        message: "Found 847 null values",
        executed_at: "2026-01-23T10:30:00Z",
        duration_ms: 1247,
        producer: "correlator-dbt",
      },
      dataset: {
        urn: "postgresql://prod/public.orders",
        name: "orders",
        namespace: "postgresql://prod/public",
      },
      job: {
        name: "build_orders_model",
        namespace: "daily_finance_pipeline",
        run_id: "019c628f-d07e-7000-8000-000000000001",
        producer: "dbt",
        status: "COMPLETE",
        started_at: "2026-01-23T10:25:00Z",
        completed_at: "2026-01-23T10:28:45Z",
      },
      upstream: [],
      downstream: [
        {
          urn: "postgresql://prod/public.fct_revenue",
          name: "fct_revenue",
          depth: 1,
          parentUrn: "postgresql://prod/public.orders",
        },
      ],
      correlation_status: "correlated",
    };

    const result = transformIncidentDetail(apiDetail);

    expect(result.id).toBe("123");
    expect(result.test.executedAt).toBe("2026-01-23T10:30:00Z");
    expect(result.test.durationMs).toBe(1247);
    expect(result.job?.runId).toBe("019c628f-d07e-7000-8000-000000000001");
    expect(result.job?.startedAt).toBe("2026-01-23T10:25:00Z");
    expect(result.job?.parent).toBeUndefined();
    expect(result.job?.orchestration).toBeUndefined();
    expect(result.correlationStatus).toBe("correlated");
    expect(result.downstream[0].parentUrn).toBe(
      "postgresql://prod/public.orders"
    );
  });

  it("handles null job for orphan incidents", () => {
    const apiDetail: ApiIncidentDetailResponse = {
      id: "456",
      test: {
        name: "expect_column_values_to_not_be_null",
        type: "expect_column_values_to_not_be_null",
        status: "failed",
        message: "Validation failed",
        executed_at: "2026-01-23T10:35:00Z",
        duration_ms: 2341,
        producer: "great_expectations",
      },
      dataset: {
        urn: "postgres_prod.public.orders",
        name: "orders",
        namespace: "postgres_prod.public",
      },
      job: null,
      upstream: [],
      downstream: [],
      correlation_status: "orphan",
    };

    const result = transformIncidentDetail(apiDetail);

    expect(result.job).toBeNull();
    expect(result.downstream).toEqual([]);
    expect(result.correlationStatus).toBe("orphan");
  });

  it("transforms parent and normalizes Go zero-time to null", () => {
    const apiDetail: ApiIncidentDetailResponse = {
      id: "789",
      test: {
        name: "unique(order_id)",
        type: "dataQualityAssertion",
        status: "failed",
        message: "Got 6 results",
        executed_at: "2026-01-23T10:30:00Z",
        duration_ms: 20,
        producer: "correlator-dbt",
      },
      dataset: {
        urn: "postgresql://demo/staging.stg_orders",
        name: "staging.stg_orders",
        namespace: "postgresql://demo",
      },
      job: {
        name: "model.jaffle_shop_demo.stg_orders",
        namespace: "dbt://demo",
        run_id: "019c628f-d07e-7000-8000-000000000001",
        producer: "correlator-dbt",
        status: "RUNNING",
        started_at: "2026-01-23T10:25:00Z",
        completed_at: "0001-01-01T00:00:00Z",
        parent: {
          name: "jaffle_shop_demo.run",
          run_id: "019c628f-d07e-7000-8000-000000000003",
          producer: "correlator-dbt",
          status: "COMPLETE",
          completed_at: "2026-01-23T10:29:00Z",
        },
      },
      upstream: [],
      downstream: [],
      correlation_status: "correlated",
    };

    const result = transformIncidentDetail(apiDetail);

    // Go zero-time completedAt normalized to null
    expect(result.job?.completedAt).toBeNull();

    // Parent with real timestamp preserved
    expect(result.job?.parent).toEqual({
      name: "jaffle_shop_demo.run",
      namespace: undefined,
      runId: "019c628f-d07e-7000-8000-000000000003",
      producer: "dbt",
      status: "COMPLETE",
      completedAt: "2026-01-23T10:29:00Z",
    });
    expect(result.job?.orchestration).toBeUndefined();
  });

  it("transforms parent + orchestration chain with producer normalization", () => {
    const apiDetail: ApiIncidentDetailResponse = {
      id: "36",
      test: {
        name: "unique(order_id)",
        type: "dataQualityAssertion",
        status: "failed",
        message: "Got 6 results",
        executed_at: "2026-02-22T15:23:12Z",
        duration_ms: 20,
        producer: "correlator-dbt",
      },
      dataset: {
        urn: "postgresql://demo/staging.stg_orders",
        name: "staging.stg_orders",
        namespace: "postgresql://demo",
      },
      job: {
        name: "model.jaffle_shop_demo.stg_orders",
        namespace: "dbt://demo",
        run_id: "019c85f1-d07e-7000-8000-000000000004",
        producer: "correlator-dbt",
        status: "RUNNING",
        started_at: "2026-02-22T15:22:07Z",
        completed_at: "0001-01-01T00:00:00Z",
        parent: {
          name: "jaffle_shop_demo.run",
          run_id: "019c85f1-d07e-7000-8000-000000000005",
          producer: "correlator-dbt",
          status: "COMPLETE",
          completed_at: "2026-02-22T15:22:07Z",
        },
        orchestration: [
          {
            name: "demo_pipeline",
            namespace: "airflow://demo",
            run_id: "019c85f1-d07e-7000-8000-000000000006",
            producer: "airflow",
            status: "FAIL",
          },
          {
            name: "jaffle_shop_demo.run",
            namespace: "dbt://demo",
            run_id: "019c85f1-d07e-7000-8000-000000000005",
            producer: "correlator-dbt",
            status: "COMPLETE",
          },
        ],
      },
      upstream: [],
      downstream: [],
      correlation_status: "correlated",
    };

    const result = transformIncidentDetail(apiDetail);

    expect(result.job?.parent).toEqual({
      name: "jaffle_shop_demo.run",
      namespace: undefined,
      runId: "019c85f1-d07e-7000-8000-000000000005",
      producer: "dbt",
      status: "COMPLETE",
      completedAt: "2026-02-22T15:22:07Z",
    });

    expect(result.job?.orchestration).toEqual([
      {
        name: "demo_pipeline",
        namespace: "airflow://demo",
        runId: "019c85f1-d07e-7000-8000-000000000006",
        producer: "airflow",
        status: "FAIL",
      },
      {
        name: "jaffle_shop_demo.run",
        namespace: "dbt://demo",
        runId: "019c85f1-d07e-7000-8000-000000000005",
        producer: "dbt",
        status: "COMPLETE",
      },
    ]);
  });
});

describe("transformCorrelationHealth", () => {
  it("transforms health response with orphan datasets", () => {
    const apiHealth: ApiCorrelationHealthResponse = {
      correlation_rate: 0.87,
      total_datasets: 47,
      produced_datasets: 30,
      correlated_datasets: 26,
      orphan_datasets: [
        {
          dataset_urn: "demo_postgres/public.orders",
          test_count: 3,
          last_seen: "2026-01-23T10:36:00Z",
          likely_match: {
            dataset_urn: "postgresql://demo/public.orders",
            confidence: 0.9,
            match_reason: "exact_table_name",
          },
        },
      ],
      suggested_patterns: [
        {
          pattern: "demo_postgres/{name}",
          canonical: "postgresql://demo/{name}",
          resolves_count: 1,
          orphans_resolved: ["demo_postgres/public.orders"],
        },
      ],
    };

    const result = transformCorrelationHealth(apiHealth);

    expect(result.correlationRate).toBe(0.87);
    expect(result.totalDatasets).toBe(47);
    expect(result.orphanDatasets).toHaveLength(1);
    expect(result.orphanDatasets[0].lastSeen).toBe("2026-01-23T10:36:00Z");
    expect(result.suggestedPatterns).toHaveLength(1);
    expect(result.suggestedPatterns[0].resolvesCount).toBe(1);
  });

  it("handles healthy state with no orphans", () => {
    const apiHealth: ApiCorrelationHealthResponse = {
      correlation_rate: 1.0,
      total_datasets: 47,
      produced_datasets: 30,
      correlated_datasets: 30,
      orphan_datasets: [],
      suggested_patterns: [],
    };

    const result = transformCorrelationHealth(apiHealth);

    expect(result.correlationRate).toBe(1.0);
    expect(result.orphanDatasets).toEqual([]);
    expect(result.suggestedPatterns).toEqual([]);
  });
});
