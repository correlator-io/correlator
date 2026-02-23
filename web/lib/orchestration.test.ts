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

  it("returns 2 levels for parent only (no orchestration)", () => {
    const result = buildOrchestrationChain({
      ...baseJob,
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:parent-001",
        producer: "dbt",
        status: "COMPLETE",
      },
    });

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      name: "jaffle_shop_demo.run",
      namespace: undefined,
      runId: "dbt:parent-001",
      producer: "dbt",
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

  it("falls back to parent when orchestration is empty array", () => {
    const result = buildOrchestrationChain({
      ...baseJob,
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:parent-001",
        producer: "dbt",
        status: "COMPLETE",
      },
      orchestration: [],
    });

    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("jaffle_shop_demo.run");
    expect(result[0].isCurrent).toBe(false);
    expect(result[1].name).toBe("model.jaffle_shop_demo.stg_orders");
    expect(result[1].isCurrent).toBe(true);
  });

  it("uses orchestration chain when provided (root + parent + leaf)", () => {
    const result = buildOrchestrationChain({
      ...baseJob,
      parent: {
        name: "jaffle_shop_demo.run",
        runId: "dbt:parent-001",
        producer: "dbt",
        status: "COMPLETE",
      },
      orchestration: [
        {
          name: "demo_pipeline",
          namespace: "airflow://demo",
          runId: "airflow:root-001",
          producer: "airflow",
          status: "FAIL",
        },
        {
          name: "jaffle_shop_demo.run",
          namespace: "dbt://demo",
          runId: "dbt:parent-001",
          producer: "dbt",
          status: "COMPLETE",
        },
      ],
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

  it("propagates producer on each level", () => {
    const result = buildOrchestrationChain({
      ...baseJob,
      orchestration: [
        {
          name: "demo_pipeline",
          namespace: "airflow://demo",
          runId: "airflow:root-001",
          producer: "airflow",
          status: "FAIL",
        },
        {
          name: "jaffle_shop_demo.run",
          namespace: "dbt://demo",
          runId: "dbt:parent-001",
          producer: "dbt",
          status: "COMPLETE",
        },
      ],
    });

    expect(result[0].producer).toBe("airflow");
    expect(result[1].producer).toBe("dbt");
    expect(result[2].producer).toBe("dbt");
  });
});
