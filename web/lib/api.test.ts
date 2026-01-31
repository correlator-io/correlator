import { describe, it, expect } from "vitest";
import {
  __testing__,
  type ApiIncidentSummary,
  type ApiIncidentDetailResponse,
  type ApiOrphanNamespace,
  type ApiCorrelationHealthResponse,
} from "./api";

const {
  transformIncident,
  transformIncidentDetail,
  transformOrphanNamespace,
  transformCorrelationHealth,
} = __testing__;

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
      job_run_id: "dbt:abc123",
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
      jobRunId: "dbt:abc123",
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
      job_run_id: "ge:val-123",
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
      },
      dataset: {
        urn: "postgresql://prod/public.orders",
        name: "orders",
        namespace: "postgresql://prod/public",
      },
      job: {
        name: "build_orders_model",
        namespace: "daily_finance_pipeline",
        run_id: "dbt:abc123",
        producer: "dbt",
        status: "COMPLETE",
        started_at: "2026-01-23T10:25:00Z",
        completed_at: "2026-01-23T10:28:45Z",
      },
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
    expect(result.job?.runId).toBe("dbt:abc123");
    expect(result.job?.startedAt).toBe("2026-01-23T10:25:00Z");
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
      },
      dataset: {
        urn: "postgres_prod.public.orders",
        name: "orders",
        namespace: "postgres_prod.public",
      },
      job: null,
      downstream: [],
      correlation_status: "orphan",
    };

    const result = transformIncidentDetail(apiDetail);

    expect(result.job).toBeNull();
    expect(result.downstream).toEqual([]);
    expect(result.correlationStatus).toBe("orphan");
  });
});

describe("transformOrphanNamespace", () => {
  it("transforms orphan namespace with suggested alias", () => {
    const apiNamespace: ApiOrphanNamespace = {
      namespace: "postgres_prod",
      producer: "great_expectations",
      last_seen: "2026-01-23T10:36:00Z",
      event_count: 12,
      suggested_alias: "postgresql://prod/public",
    };

    const result = transformOrphanNamespace(apiNamespace);

    expect(result).toEqual({
      namespace: "postgres_prod",
      producer: "great_expectations",
      lastSeen: "2026-01-23T10:36:00Z",
      eventCount: 12,
      suggestedAlias: "postgresql://prod/public",
    });
  });

  it("handles null suggested alias", () => {
    const apiNamespace: ApiOrphanNamespace = {
      namespace: "snowflake://analytics",
      producer: "airflow",
      last_seen: "2026-01-22T14:00:00Z",
      event_count: 8,
      suggested_alias: null,
    };

    const result = transformOrphanNamespace(apiNamespace);

    expect(result.suggestedAlias).toBeNull();
  });
});

describe("transformCorrelationHealth", () => {
  it("transforms health response with orphan namespaces", () => {
    const apiHealth: ApiCorrelationHealthResponse = {
      correlation_rate: 0.87,
      total_datasets: 47,
      orphan_namespaces: [
        {
          namespace: "postgres_prod",
          producer: "great_expectations",
          last_seen: "2026-01-23T10:36:00Z",
          event_count: 12,
          suggested_alias: "postgresql://prod/public",
        },
      ],
    };

    const result = transformCorrelationHealth(apiHealth);

    expect(result.correlationRate).toBe(0.87);
    expect(result.totalDatasets).toBe(47);
    expect(result.orphanNamespaces).toHaveLength(1);
    expect(result.orphanNamespaces[0].lastSeen).toBe("2026-01-23T10:36:00Z");
  });

  it("handles empty orphan namespaces (healthy state)", () => {
    const apiHealth: ApiCorrelationHealthResponse = {
      correlation_rate: 1.0,
      total_datasets: 47,
      orphan_namespaces: [],
    };

    const result = transformCorrelationHealth(apiHealth);

    expect(result.correlationRate).toBe(1.0);
    expect(result.orphanNamespaces).toEqual([]);
  });
});
