# OpenLineage: Zero to Hero

**Target Audience**: Senior engineers onboarding to Correlator
**Goal**: Understand OpenLineage specification and how it enables Correlator's incident correlation engine

---

## Table of Contents

1. [What is OpenLineage?](#what-is-openlineage)
2. [Core Concepts](#core-concepts)
3. [Event Types](#event-types)
4. [The Run Cycle](#the-run-cycle)
5. [Naming Conventions](#naming-conventions)
6. [Facets & Extensibility](#facets--extensibility)
7. [How Correlator Uses OpenLineage](#how-correlator-uses-openlineage)
8. [Practical Examples](#practical-examples)
9. [References](#references)

---

## What is OpenLineage?

OpenLineage is an **open standard for lineage and metadata collection** in the data ecosystem. It defines a common way to describe:

- **What job ran** (e.g., dbt model, Airflow task, Spark job)
- **When it ran** (timestamps, run state transitions)
- **What data it consumed** (input datasets: tables, files, topics)
- **What data it produced** (output datasets)
- **How it ran** (metadata, errors, SQL, parent jobs)

### Why It Matters for Correlator

Correlator correlates **test failures → job runs → downstream impact**. OpenLineage provides the standardized job run and dataset lineage data that Correlator needs to:

1. **Build the lineage graph**: Connect jobs to datasets across tools (dbt, Airflow, Spark)
2. **Generate canonical IDs**: Stable identifiers for correlation across systems
3. **Track run state**: Understand job lifecycle (START → RUNNING → COMPLETE/FAIL)
4. **Reduce MTTR by 75%**: Navigate from test failure to root cause in <2 clicks

---

## Core Concepts

OpenLineage has three core entities:

### 1. Job

A **Job** is a recurring data transformation process with inputs and outputs.

**Examples**:
- dbt model: `analytics.transform_orders`
- Airflow task: `daily_etl.load_users`
- Spark job: `recommendation_engine.train_model`

**Key Properties**:
- `namespace`: Identifies the scheduler/orchestrator (e.g., `airflow://prod-cluster`)
- `name`: Unique within namespace (e.g., `orders_etl.count_orders`)

```json
{
  "job": {
    "namespace": "airflow://production",
    "name": "daily_etl.load_users"
  }
}
```

### 2. Run

A **Run** is a single execution instance of a Job.

**Key Properties**:
- `runId`: Client-generated UUID (recommended: UUIDv7)
- `facets`: Metadata about this specific run (nominalTime, parent, errorMessage, sql)

```json
{
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

**Critical for Correlator**: The `runId` is maintained throughout the run lifecycle (START → COMPLETE). This enables tracking run state transitions and correlating events to the same execution.

### 3. Dataset

A **Dataset** is an abstract representation of data: a table, file, topic, or directory.

**Key Properties**:
- `namespace`: Protocol + host (e.g., `postgres://prod-db:5432`)
- `name`: Hierarchical path (e.g., `analytics.public.orders`)

```json
{
  "namespace": "postgres://prod-db:5432",
  "name": "analytics.public.orders"
}
```

**Naming follows strict conventions** (covered in [Naming Conventions](#naming-conventions)).

---

## Event Types

OpenLineage defines three event types:

### 1. RunEvent (Runtime Lineage)

Describes the **execution** of a job. Emitted at runtime when jobs start, run, or complete.

**Use Case**: Track job execution state, inputs consumed, outputs produced.

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2025-10-18T10:30:00Z",
  "run": { "runId": "550e8400-..." },
  "job": {
    "namespace": "airflow://prod",
    "name": "etl.load_orders"
  },
  "inputs": [
    {
      "namespace": "s3://raw-data",
      "name": "/orders/2025-10-18.parquet"
    }
  ],
  "outputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "analytics.public.orders"
    }
  ]
}
```

### 2. JobEvent (Static Metadata)

Describes **design-time metadata** about a job: source code location, declared inputs/outputs, documentation.

**Not associated with a Run**. Emitted by compilers, CI pipelines, or metadata extraction tools.

**Use Case**: Track job definitions, source code changes, declared dependencies.

```json
{
  "eventType": "JOB",
  "job": {
    "namespace": "dbt://analytics",
    "name": "models.transform_orders"
  },
  "facets": {
    "sourceCodeLocation": {
      "url": "https://github.com/org/repo/blob/main/models/transform_orders.sql"
    }
  }
}
```

### 3. DatasetEvent (Dataset Metadata)

Describes **design-time metadata** about a dataset: schema, ownership, documentation.

**Not associated with a Run**. Emitted when dataset metadata is created or updated.

**Use Case**: Track schema evolution, ownership changes, governance metadata.

```json
{
  "eventType": "DATASET",
  "dataset": {
    "namespace": "postgres://prod-db:5432",
    "name": "analytics.public.customers"
  },
  "facets": {
    "schema": {
      "fields": [
        { "name": "id", "type": "INTEGER" },
        { "name": "email", "type": "VARCHAR" }
      ]
    }
  }
}
```

### Correlator Focus: RunEvent

**Correlator MVP focuses on RunEvent** (runtime lineage) because we're correlating:
- Test failures → Job runs → Downstream impact

JobEvent and DatasetEvent support future features (schema change detection, static lineage analysis).

---

## The Run Cycle

The run cycle describes **state transitions** during job execution.

### Run States

| State | Description | Terminal? |
|-------|-------------|-----------|
| `START` | Job execution begins | No |
| `RUNNING` | Job is actively running | No |
| `COMPLETE` | Job finished successfully | Yes |
| `FAIL` | Job failed | Yes |
| `ABORT` | Job stopped abnormally | Yes |
| `OTHER` | Additional metadata (anytime) | No |

### State Machine

```
START → {RUNNING, COMPLETE, FAIL, ABORT}
RUNNING → {COMPLETE, FAIL, ABORT}
COMPLETE/FAIL/ABORT → Terminal (idempotent)
OTHER → Can occur anytime (even before START)
```

**Key Principles**:
1. **Events are accumulative**: Each event adds metadata (inputs, outputs, facets)
2. **Terminal states are idempotent**: Can send COMPLETE multiple times (no error)
3. **One START, one terminal event**: Required per run (additional OTHER events allowed)

### Typical Scenarios

**Batch Job (dbt model, Airflow task)**:
```
START → COMPLETE (success)
START → FAIL (error)
```

**Long-Running Job (microservice, stream processor)**:
```
START → RUNNING → RUNNING → ... → ABORT (restart)
START → RUNNING → RUNNING → ... → COMPLETE (shutdown)
```

### Correlator Implications

**Out-of-order events**: Events may arrive out of order due to network delays, retries, or distributed systems.

**Example**:
1. Job starts (START event sent)
2. Network delay causes START to arrive late
3. Job completes quickly (COMPLETE arrives first)

**Correlator must handle**: COMPLETE arrives before START. Solution: Use `eventTime` for ordering, not arrival time.

**Event lifecycle state machine** (in `internal/ingestion/lifecycle.go`) validates state transitions:
- Allow: START → COMPLETE
- Allow: COMPLETE → COMPLETE (idempotent)
- Reject: COMPLETE → START (invalid transition)

---

## Naming Conventions

Naming is **critical** for Correlator to generate **canonical IDs** that enable correlation across tools.

### Dataset Naming

**Format**: `{namespace}` + `{name}`

**Namespace**: `{protocol}://{host}:{port}` or `{protocol}://{service_identifier}`
**Name**: Hierarchical path (database.schema.table, bucket/path, topic)

#### Examples by Data Store Type

| Data Store | Namespace | Name |
|------------|-----------|------|
| PostgreSQL | `postgres://prod-db:5432` | `analytics.public.orders` |
| BigQuery | `bigquery` | `project.dataset.table` |
| S3 | `s3://raw-data` | `/orders/2025-10-18.parquet` |
| Snowflake | `snowflake://org-account` | `analytics.public.customers` |
| Kafka | `kafka://broker:9092` | `user-events` |
| HDFS | `hdfs://namenode:8020` | `/data/warehouse/orders` |

**Why it matters**: Correlator generates dataset URNs using this convention. Stable URNs enable linking test failures to affected datasets.

### Job Naming

**Format**: `{namespace}` + `{name}`

**Namespace**: Scheduler identifier (set in OpenLineage client config)
**Name**: Unique within namespace

#### Examples by Job Type

| Job Type | Name Pattern | Example |
|----------|--------------|---------|
| Airflow task | `{dag_id}.{task_id}` | `daily_etl.load_orders` |
| dbt model | `{project}.{model}` | `analytics.transform_orders` |
| Spark job | `{appName}.{command}.{table}` | `recommendation.train.model_v2` |
| Great Expectations | `{suite}.{checkpoint}` | `gx.validate_orders` |

**Why it matters**: Correlator uses job names to identify recurring jobs across executions. Stable job names enable correlation of test failures to specific jobs.

### Run Naming

**Format**: Client-generated UUID (recommended: UUIDv7)

```python
from openlineage.client.uuid import generate_new_uuid

run_id = str(generate_new_uuid())  # "550e8400-e29b-41d4-a716-446655440000"
```

**Why it matters**: The `runId` is maintained throughout the run lifecycle. Correlator uses it to track state transitions and correlate events to the same execution.

---

## Facets & Extensibility

**Facets** are key-value pairs that extend the core OpenLineage model with additional metadata.

### Facet Types

1. **Run Facets**: Metadata about a specific run instance
2. **Job Facets**: Metadata about the job definition
3. **Dataset Facets**: Metadata about datasets (common to inputs and outputs)
4. **Input Facets**: Metadata specific to input datasets
5. **Output Facets**: Metadata specific to output datasets

### Run Facets

| Facet | Description | Example |
|-------|-------------|---------|
| `nominalTime` | Scheduled time (vs actual time) | `2025-10-18T00:00:00Z` |
| `parent` | Parent job/run (for spawned jobs) | Airflow DAG → task |
| `errorMessage` | Error details + stack trace | `ValueError: Invalid input` |
| `sql` | SQL query executed | `SELECT * FROM orders WHERE ...` |

```json
{
  "run": {
    "runId": "550e8400-...",
    "facets": {
      "nominalTime": {
        "nominalStartTime": "2025-10-18T00:00:00Z"
      },
      "parent": {
        "run": {
          "runId": "parent-run-id"
        },
        "job": {
          "namespace": "airflow://prod",
          "name": "daily_etl"
        }
      }
    }
  }
}
```

### Dataset Facets

| Facet | Description | Used By |
|-------|-------------|---------|
| `schema` | Table/file schema | Common |
| `dataSource` | Database/bucket details | Common |
| `dataQualityMetrics` | Row count, null count, etc. | Input |
| `dataQualityAssertions` | Test results (pass/fail) | Input |
| `outputStatistics` | Rows written, bytes written | Output |

```json
{
  "inputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "analytics.public.orders",
      "facets": {
        "schema": {
          "fields": [
            { "name": "id", "type": "INTEGER" },
            { "name": "amount", "type": "DECIMAL" }
          ]
        }
      },
      "inputFacets": {
        "dataQualityMetrics": {
          "rowCount": 1000000,
          "columnMetrics": {
            "amount": {
              "nullCount": 0,
              "min": 1.50,
              "max": 9999.99
            }
          }
        }
      }
    }
  ]
}
```

### Custom Facets

OpenLineage supports **custom facets** for tool-specific metadata.

**Naming Convention**: `{prefix}_{facetName}` (e.g., `correlator_incidentId`)

```json
{
  "run": {
    "facets": {
      "correlator_incidentContext": {
        "_producer": "https://github.com/correlator-io/correlator",
        "_schemaURL": "https://correlator.io/spec/incident-context.json",
        "incidentId": "INC-12345",
        "severity": "critical",
        "affectedServices": ["api", "worker"]
      }
    }
  }
}
```

---

## How Correlator Uses OpenLineage

Correlator is an **incident correlation engine** that reduces MTTR by connecting:

```
Test Failure → Job Run → Downstream Impact
```

OpenLineage provides the **job run and lineage data** that powers this correlation.

### Correlator's OpenLineage Integration

#### 1. Ingestion Endpoint

Correlator exposes `/api/v1/lineage/events` to receive OpenLineage RunEvents.

**Supported Formats**:
- Single event: `POST /api/v1/lineage/events` with RunEvent
- Batch events: `POST /api/v1/lineage/events` with array of RunEvents

**HTTP Status Codes**:
- `200 OK`: Single event success, batch all success, or duplicate
- `207 Multi-Status`: Batch partial success (some events accepted, some rejected)
- `422 Unprocessable Entity`: Validation failed
- `401 Unauthorized`: Missing/invalid API key
- `429 Too Many Requests`: Rate limit exceeded

#### 2. Data Model

Correlator stores OpenLineage data in PostgreSQL tables:

**job_runs**
```sql
CREATE TABLE job_runs (
  canonical_job_run_id TEXT PRIMARY KEY,  -- Canonical ID for correlation
  job_namespace TEXT NOT NULL,
  job_name TEXT NOT NULL,
  run_id UUID NOT NULL,                   -- Client-generated UUID
  run_state TEXT NOT NULL,                -- START, RUNNING, COMPLETE, FAIL, ABORT
  event_time TIMESTAMPTZ NOT NULL,
  producer TEXT NOT NULL,
  parent_run_id TEXT,                     -- For job hierarchy
  nominal_time TIMESTAMPTZ,               -- Scheduled time
  facets JSONB                            -- Run facets (sql, errorMessage, etc.)
);
```

**datasets**
```sql
CREATE TABLE datasets (
  canonical_dataset_id TEXT PRIMARY KEY,  -- Dataset URN (namespace + name)
  dataset_namespace TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  schema_json JSONB,                      -- Schema facet
  facets JSONB                            -- Other dataset facets
);
```

**lineage_edges**
```sql
CREATE TABLE lineage_edges (
  job_run_id TEXT REFERENCES job_runs(canonical_job_run_id),
  input_dataset_id TEXT REFERENCES datasets(canonical_dataset_id),
  output_dataset_id TEXT REFERENCES datasets(canonical_dataset_id),
  edge_type TEXT NOT NULL                 -- INPUT or OUTPUT
);
```

#### 3. Canonical ID Generation

Correlator generates **stable, globally unique IDs** for correlation:

**Canonical Job Run ID**:
```
SHA256(job.namespace + job.name + run.runId)
```

**Canonical Dataset ID (URN)**:
```
{dataset.namespace}/{dataset.name}
```

**Example**:
```
Job: airflow://prod/daily_etl.load_orders
Run ID: 550e8400-e29b-41d4-a716-446655440000
Canonical Job Run ID: sha256("airflow://proddaily_etl.load_orders550e8400-...")

Dataset: postgres://prod-db:5432/analytics.public.orders
Dataset URN: postgres://prod-db:5432/analytics.public.orders
```

**Why canonical IDs?** Tools use different identifiers (Airflow task IDs, dbt model names, Spark app names). Canonical IDs enable correlation across tools.

#### 4. Idempotency

Correlator implements **idempotency** to handle duplicate events (retries, network issues).

**Idempotency Key**:
```
SHA256(producer + job.namespace + job.name + runId + eventTime + eventType)
```

**Behavior**:
- First event: Process and store, return `200 OK`
- Duplicate: Skip processing, return `200 OK` (idempotent success)
- TTL: 24 hours (events older than 24 hours are not deduped)

**Industry Standard**: Stripe, AWS, Google all return `2xx` for duplicates (not `409 Conflict`).

#### 5. Partial Success (207 Multi-Status)

Correlator uses **partial success** for batch ingestion:

**Problem**: 1 invalid event should not kill 99 valid events.

**Solution**: Process each event independently, return HTTP 207 with per-event results:

```json
{
  "results": [
    { "index": 0, "status": "success" },
    { "index": 1, "status": "error", "message": "Invalid eventTime format" },
    { "index": 2, "status": "success" }
  ],
  "summary": {
    "total": 3,
    "success": 2,
    "failed": 1
  }
}
```

**HTTP 207** signals partial success. Clients can retry failed events.

#### 6. Event Lifecycle Validation

Correlator validates state transitions using a **state machine**:

**Valid Transitions**:
- `START` → `{RUNNING, COMPLETE, FAIL, ABORT}`
- `RUNNING` → `{COMPLETE, FAIL, ABORT}`
- `COMPLETE/FAIL/ABORT` → Same state (idempotent)

**Invalid Transitions**:
- `COMPLETE` → `START` (job already completed)
- `FAIL` → `RUNNING` (job already failed)

**Implementation**: `internal/ingestion/lifecycle.go`

---

## Practical Examples

### Example 1: dbt Model Execution

**Scenario**: dbt runs `transform_orders` model, reads from `raw.orders`, writes to `analytics.orders`.

**START Event** (dbt sends when model starts):
```json
{
  "eventType": "START",
  "eventTime": "2025-10-18T10:00:00Z",
  "producer": "https://github.com/dbt-labs/dbt-core/tree/1.5.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "job": {
    "namespace": "dbt://analytics",
    "name": "transform_orders"
  },
  "inputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "raw.public.orders"
    }
  ]
}
```

**COMPLETE Event** (dbt sends when model finishes):
```json
{
  "eventType": "COMPLETE",
  "eventTime": "2025-10-18T10:05:00Z",
  "producer": "https://github.com/dbt-labs/dbt-core/tree/1.5.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000",
    "facets": {
      "sql": {
        "query": "SELECT * FROM raw.orders WHERE amount > 100"
      }
    }
  },
  "job": {
    "namespace": "dbt://analytics",
    "name": "transform_orders"
  },
  "inputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "raw.public.orders",
      "inputFacets": {
        "dataQualityMetrics": {
          "rowCount": 1000000
        }
      }
    }
  ],
  "outputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "analytics.public.orders",
      "outputFacets": {
        "outputStatistics": {
          "rowCount": 500000,
          "size": 52428800
        }
      }
    }
  ]
}
```

**Correlator Processing**:
1. Extract canonical job run ID: `sha256("dbt://analyticstransform_orders550e8400-...")`
2. Store job run: `job_runs` table
3. Store datasets: `datasets` table (upsert `raw.orders`, `analytics.orders`)
4. Store lineage: `lineage_edges` table (INPUT: raw.orders, OUTPUT: analytics.orders)
5. Update run state: START → COMPLETE

### Example 2: Airflow DAG with Parent/Child Jobs

**Scenario**: Airflow DAG `daily_etl` spawns task `load_users`.

**DAG START Event** (parent job):
```json
{
  "eventType": "START",
  "eventTime": "2025-10-18T10:00:00Z",
  "run": {
    "runId": "parent-run-id",
    "facets": {
      "nominalTime": {
        "nominalStartTime": "2025-10-18T00:00:00Z"
      }
    }
  },
  "job": {
    "namespace": "airflow://production",
    "name": "daily_etl"
  }
}
```

**Task START Event** (child job):
```json
{
  "eventType": "START",
  "eventTime": "2025-10-18T10:01:00Z",
  "run": {
    "runId": "child-run-id",
    "facets": {
      "parent": {
        "run": {
          "runId": "parent-run-id"
        },
        "job": {
          "namespace": "airflow://production",
          "name": "daily_etl"
        }
      }
    }
  },
  "job": {
    "namespace": "airflow://production",
    "name": "daily_etl.load_users"
  }
}
```

**Correlator Processing**:
1. Store parent job run: `job_runs` (runId: parent-run-id)
2. Store child job run: `job_runs` (runId: child-run-id, parent_run_id: parent-run-id)
3. Enable **job hierarchy queries**: "Show all tasks for DAG daily_etl"

### Example 3: Great Expectations Data Quality Test

**Scenario**: Great Expectations validates `orders` table, finds null values in `customer_id` column.

**COMPLETE Event** (test failed):
```json
{
  "eventType": "COMPLETE",
  "eventTime": "2025-10-18T10:30:00Z",
  "run": {
    "runId": "test-run-id",
    "facets": {
      "errorMessage": {
        "message": "Data quality test failed: customer_id contains nulls",
        "programmingLanguage": "python",
        "stackTrace": "..."
      }
    }
  },
  "job": {
    "namespace": "great_expectations://prod",
    "name": "validate_orders"
  },
  "inputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "analytics.public.orders",
      "inputFacets": {
        "dataQualityAssertions": {
          "assertions": [
            {
              "assertion": "expect_column_values_to_not_be_null",
              "column": "customer_id",
              "success": false,
              "failedRows": 500
            }
          ]
        }
      }
    }
  ]
}
```

**Correlator Correlation Flow**:
1. Receive COMPLETE event with `errorMessage` facet → Test failure detected
2. Identify affected dataset: `analytics.public.orders`
3. Query `lineage_edges`: "Which jobs produced `analytics.public.orders`?"
4. Find upstream job: `dbt://analytics/transform_orders`
5. Navigate: Test failure → `transform_orders` job run → Upstream datasets
6. **2-click navigation to root cause**

---

## References

### Official OpenLineage Documentation

- **Core Specification**: https://openlineage.io/docs/spec/object-model
- **Run Cycle**: https://openlineage.io/docs/spec/run-cycle
- **Naming Conventions**: https://openlineage.io/docs/spec/naming
- **Run Facets**: https://openlineage.io/docs/spec/facets/run-facets
- **Dataset Facets**: https://openlineage.io/docs/spec/facets/dataset-facets
- **OpenAPI Spec**: https://openlineage.io/apidocs/openapi

### Integrations

- **dbt Integration**: https://openlineage.io/docs/integrations/dbt
- **Airflow Integration**: https://openlineage.io/docs/integrations/airflow
- **Great Expectations**: https://openlineage.io/docs/integrations/great-expectations

---

## Quick Reference

### Key Terminology

| Term | Definition |
|------|------------|
| **Job** | Recurring data transformation (dbt model, Airflow task) |
| **Run** | Single execution instance of a Job |
| **Dataset** | Data artifact (table, file, topic) |
| **Facet** | Key-value metadata extension |
| **Namespace** | Logical grouping (job scheduler, data source) |
| **RunId** | Client-generated UUID for a run |
| **Canonical ID** | Correlator-generated stable identifier |
| **Dataset URN** | Globally unique dataset identifier (namespace + name) |

### Event Type Cheat Sheet

| Event Type | Purpose | Associated with Run? | Use Case |
|------------|---------|----------------------|----------|
| RunEvent | Runtime execution | Yes | Job runs, lineage, errors |
| JobEvent | Static metadata | No | Source code, declared deps |
| DatasetEvent | Dataset metadata | No | Schema, ownership, docs |

### Run State Cheat Sheet

| State | Meaning | Terminal? | Next States |
|-------|---------|-----------|-------------|
| START | Job begins | No | RUNNING, COMPLETE, FAIL, ABORT |
| RUNNING | Job running | No | COMPLETE, FAIL, ABORT |
| COMPLETE | Success | Yes | COMPLETE (idempotent) |
| FAIL | Error | Yes | FAIL (idempotent) |
| ABORT | Abnormal stop | Yes | ABORT (idempotent) |
| OTHER | Metadata update | No | Any |

---

## Appendix: Schema Migration Considerations

### Existing Schema vs OpenLineage Spec

**Current Correlator Schema** (migrations 001-004):
- `job_runs` table with `job_run_id` format: `{system}:{original_id}`
- `datasets` table with URN format: `{namespace}:{name}` (colon delimiter)
- `lineage_edges` with single row per job run (one input + one output)

**OpenLineage Spec Requirements**:
- Job run ID: Hash-based canonical ID from `namespace + name + runId`
- Dataset URN: `{namespace}/{name}` (slash delimiter)
- Lineage edges: Separate rows for each input and output (edge_type: 'input'/'output')
- Run state tracking: START → RUNNING → COMPLETE/FAIL/ABORT
- Event lifecycle: Store `eventType`, `eventTime`, state transitions

### Schema Refactoring Strategy

**Migration Path** (Task 5, Subtask 1):

1. **Add OpenLineage-specific columns to `job_runs`**:
   - `run_id` (UUID): Client-generated run ID
   - `event_type` (TEXT): START, RUNNING, COMPLETE, FAIL, ABORT, OTHER
   - `event_time` (TIMESTAMPTZ): When the event occurred
   - `state_history` (JSONB): Track all state transitions
   - Rename `status` → `current_state` for clarity

2. **Update Dataset URN delimiter**:
   - Migrate from `namespace:name` → `namespace/name`
   - Breaking change but necessary for spec compliance
   - Timing: Early development phase (migration 004), no production data

3. **Refactor `lineage_edges` structure**:
   - Add `edge_type` column: 'input' or 'output'
   - Migrate existing rows: Split into separate input/output rows
   - Enables multiple inputs/outputs per job run

4. **Add idempotency table**:
   - `lineage_event_idempotency` with 24-hour TTL
   - Prevents duplicate event processing

5. **Deferred FK constraints**:
   - Handle concurrent events (Event B references dataset from Event A)
   - Use `DEFERRABLE INITIALLY DEFERRED` on FK constraints

### Key Architectural Decisions

**Why refactor existing schema vs parallel tables?**
- Single source of truth (no duplication)
- Cleaner architecture for Phases 2-6
- Easier maintenance and correlation logic
- Timing: Migration 004, no production data to migrate

**Dataset URN delimiter (`:` vs `/`)**:
- OpenLineage spec uses `/` (e.g., `postgres://host:5432/db.schema.table`)
- Current schema uses `:` (e.g., `postgres://host:5432:db.schema.table`)
- **Decision**: Migrate to `/` for spec compliance
- Impact: Breaking change, acceptable in early development

**Canonical Job Run ID format**:
- `SHA256(job.namespace + job.name + run.runId)`
- Stable across multiple events for the same run
- Enables correlation across tool-specific identifiers
