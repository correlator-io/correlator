import { describe, it, expect } from "vitest";
import { buildOrchestrationChain } from "./orchestration";

describe("buildOrchestrationChain", () => {
  const baseJob = {
    name: "model.jaffle_shop_demo.stg_orders",
    namespace: "dbt://demo",
    runId: "dbt:leaf-001",
    producer: "dbt" as const,
    status: "RUNNING",
  };

  it("returns empty array when no parent exists", () => {
    const result = buildOrchestrationChain(baseJob);
    expect(result).toEqual([]);
  });

  it("returns 2 levels for parent only (no root)", () => {
    const result = buildOrchestrationChain({
      ...baseJob,
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:parent-001",
        status: "COMPLETE",
        completedAt: "2026-01-23T10:29:00Z",
      },
    });

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      name: "jaffle_shop_demo.run",
      namespace: undefined,
      runId: "dbt:parent-001",
      producer: undefined,
      status: "COMPLETE",
      isCurrent: false,
    });
    expect(result[1]).toEqual({
      name: "model.jaffle_shop_demo.stg_orders",
      namespace: "dbt://demo",
      runId: "dbt:leaf-001",
      producer: "dbt",
      status: "RUNNING",
      isCurrent: true,
    });
  });

  it("returns 3 levels for root + parent + leaf", () => {
    const result = buildOrchestrationChain({
      ...baseJob,
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:parent-001",
        status: "COMPLETE",
        completedAt: "2026-01-23T10:29:00Z",
      },
      rootParent: {
        name: "demo_pipeline",
        namespace: "airflow://demo",
        runId: "airflow:root-001",
        producer: "airflow",
        status: "FAIL",
        completedAt: "2026-01-23T10:30:00Z",
      },
    });

    expect(result).toHaveLength(3);
    expect(result[0].name).toBe("demo_pipeline");
    expect(result[0].producer).toBe("airflow");
    expect(result[0].isCurrent).toBe(false);
    expect(result[1].name).toBe("jaffle_shop_demo.run");
    expect(result[1].isCurrent).toBe(false);
    expect(result[2].name).toBe("model.jaffle_shop_demo.stg_orders");
    expect(result[2].isCurrent).toBe(true);
  });

  it("deduplicates when rootParent.runId === parent.runId", () => {
    const sharedRunId = "airflow:same-001";

    const result = buildOrchestrationChain({
      ...baseJob,
      parent: {
        name: "demo_pipeline",
        namespace: "airflow://demo",
        runId: sharedRunId,
        producer: "airflow",
        status: "COMPLETE",
        completedAt: "2026-01-23T10:30:00Z",
      },
      rootParent: {
        name: "demo_pipeline",
        namespace: "airflow://demo",
        runId: sharedRunId,
        producer: "airflow",
        status: "COMPLETE",
        completedAt: "2026-01-23T10:30:00Z",
      },
    });

    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("demo_pipeline");
    expect(result[1].name).toBe("model.jaffle_shop_demo.stg_orders");
  });

  it("propagates producer on each level", () => {
    const result = buildOrchestrationChain({
      ...baseJob,
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:parent-001",
        status: "COMPLETE",
        completedAt: null,
      },
      rootParent: {
        name: "demo_pipeline",
        namespace: "airflow://demo",
        runId: "airflow:root-001",
        producer: "airflow",
        status: "FAIL",
        completedAt: null,
      },
    });

    expect(result[0].producer).toBe("airflow");
    expect(result[1].producer).toBeUndefined(); // parent has no producer
    expect(result[2].producer).toBe("dbt");
  });
});
