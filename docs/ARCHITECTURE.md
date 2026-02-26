# Architecture

This document describes the internal architecture of `Correlator`, the incident correlation engine that processes
OpenLineage events and correlates test failures with job runs to reduce incident response time.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    cmd/correlator/main.go                               │
│                              (server bootstrap, dependency injection)                   │
└──────────────────────────────────────────┬──────────────────────────────────────────────┘
                                           │
           ┌───────────────────────────────┼───────────────────────────────┐
           │                               │                               │
           ▼                               ▼                               ▼
┌──────────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│   internal/api/      │     │  internal/storage/   │     │ internal/ingestion/  │
│                      │     │                      │     │                      │
│ HTTP server          │     │ PostgreSQL backend   │     │ Domain models        │
│ Routes & handlers    │────▶│ LineageStore         │◀────│ Validation           │
│ Middleware chain     │     │ APIKeyStore          │     │ Store interface      │
│ Error responses      │     │ Correlation views    │     │                      │
└──────────────────────┘     └──────────────────────┘     └──────────────────────┘
           │                               │                               │
           │                               │                               │
           ▼                               ▼                               ▼
┌──────────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│ internal/api/        │     │ internal/correlation/│     │ internal/            │
│ middleware/          │     │                      │     │ canonicalization/    │
│                      │     │ Read interface       │     │                      │
│ CorrelationID        │     │ Incident queries     │     │ URN generation       │
│ Recovery             │     │ Impact analysis      │     │ Idempotency keys     │
│ Auth (API keys)      │     │ Recent incidents     │     │                      │
│ RateLimit            │     │                      │     │                      │
│ RequestLogger        │     │                      │     │                      │
│ CORS                 │     │                      │     │                      │
└──────────────────────┘     └──────────────────────┘     └──────────────────────┘
                                           │
                                           ▼
                              ┌───────────────────────┐
                              │     PostgreSQL        │
                              │                       │
                              │ Tables:               │
                              │ ├ job_runs            │
                              │ ├ datasets            │
                              │ ├ lineage_edges       │
                              │ ├ test_results        │
                              │ └ api_keys            │
                              │                       │
                              │ Materialized Views:   │
                              │ ├ incident_correlation│
                              │ ├ lineage_impact      │
                              │ └ recent_incidents    │
                              └───────────────────────┘
```

## Request Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                       HTTP Request Flow (POST /api/v1/lineage/batch)                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│ 1. Middleware Chain      ──►    2. Handler               ──►   3. Storage           │
│ ──────────────────────           ──────────────────────        ──────────────────── │
│                                                                                     │
│ CorrelationID()               JSON decode                   Check idempotency       │
│      │                             │                               │                │
│      ▼                             ▼                               ▼                │
│ Recovery()                  Validate events                 Begin transaction       │
│      │                             │                               │                │
│      ▼                             ▼                               ▼                │
│ AuthenticatePlugin()         Map to domain                   Upsert job_run         │
│      │                             │                               │                │
│      ▼                             ▼                               ▼                │
│ RateLimit()                   Call storage                  Upsert datasets         │
│      │                             │                               │                │
│      ▼                             ▼                               ▼                │
│ RequestLogger()              Build response               Create lineage edges      │
│      │                             │                               │                │
│      ▼                             ▼                               ▼                │
│ CORS()                        Return JSON                 Extract test results      │
│                                                                    │                │
│                                                                    ▼                │
│                                                            Record idempotency       │
│                                                                    │                │
│                                                                    ▼                │
│                                                            Commit transaction       │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Correlation Query Flow                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│ Test Failure          Materialized View                Root Cause                   │
│ ────────────          ─────────────────                ──────────                   │
│                                                                                     │
│ "test_not_null        incident_correlation_view        run_id: 550e8400-...         │
│  failed on            ┌─────────────────────┐          job_name: transform_orders   │
│  orders.amount"   ──► │ test_results        │    ──►   producer: dbt-core           │
│                       │     JOIN            │          started_at: 2025-01-07 10:00 │
│                       │ lineage_edges       │          status: FAIL                 │
│                       │     JOIN            │                                       │
│                       │ job_runs            │          "This job produced the       │
│                       └─────────────────────┘           failing dataset"            │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Package Overview

### `cmd/correlator/main.go` - Application Entry Point

The bootstrap layer that initializes all dependencies and starts the HTTP server.

**Startup Sequence:**

1. Parse CLI flags (`--version`)
2. Load server configuration (host, port, timeouts)
3. Initialize rate limiter (in-memory token bucket)
4. Connect to PostgreSQL database
5. Initialize API key store (if auth enabled)
6. Initialize lineage store (event persistence)
7. Create HTTP server with middleware chain
8. Start server with graceful shutdown

**Key Dependencies:**

| Dependency               | Purpose                              |
|--------------------------|--------------------------------------|
| `api.Server`             | HTTP server with routes and handlers |
| `storage.Connection`     | PostgreSQL connection pool           |
| `storage.LineageStore`   | OpenLineage event persistence        |
| `storage.APIKeyStore`    | API key authentication (optional)    |
| `middleware.RateLimiter` | Request rate limiting                |

---

### `internal/api/` - HTTP API Layer

The HTTP server implementation with routes, handlers, and response types.

**Key Components:**

| Component               | File                       | Purpose                                   |
|-------------------------|----------------------------|-------------------------------------------|
| `Server`                | `server.go`                | HTTP server with graceful shutdown        |
| `setupRoutes()`         | `routes.go`                | Route registration with middleware bypass |
| `handleLineageEvents()` | `ingest_lineage_events.go` | OpenLineage batch ingestion               |
| `LineageResponse`       | `routes.go`                | OpenLineage-compliant batch response      |

**Routes:**

| Route                         | Auth | Purpose                                |
|-------------------------------|------|----------------------------------------|
| `GET /ping`                   | No   | Kubernetes liveness probe              |
| `GET /ready`                  | No   | Kubernetes readiness probe (checks DB) |
| `GET /health`                 | No   | Service health status with uptime      |
| `POST /api/v1/lineage`        | Yes  | OpenLineage single-event ingestion     |
| `POST /api/v1/lineage/batch`  | Yes  | OpenLineage event batch ingestion      |

**Response Types:**

```go
// LineageResponse - OpenLineage batch response with Correlator extensions
type LineageResponse struct {
Status        string          `json:"status"`  // "success" or "error"
Summary       ResponseSummary `json:"summary"` // Event counts
FailedEvents  []FailedEvent   `json:"failed_events"`  // Only failed events
CorrelationID string          `json:"correlation_id"` // Request tracing
Timestamp     string          `json:"timestamp"` // Response time
}
```

---

### `internal/api/middleware/` - Middleware Chain

HTTP middleware components applied in order to every request.

**Middleware Order:**

```
CorrelationID → Recovery → AuthPlugin → RateLimit → RequestLogger → CORS → Handler
```

| Middleware           | File             | Purpose                                           |
|----------------------|------------------|---------------------------------------------------|
| `CorrelationID`      | `correlation.go` | Adds `X-Correlation-ID` header for tracing        |
| `Recovery`           | `recovery.go`    | Panic recovery with structured logging            |
| `AuthenticatePlugin` | `plugin_auth.go` | API key validation (bcrypt + constant-time)       |
| `RateLimit`          | `ratelimit.go`   | Token bucket rate limiting (global/plugin/unauth) |
| `RequestLogger`      | `logging.go`     | Structured request/response logging               |
| `CORS`               | `cors.go`        | Cross-origin resource sharing headers             |

**Rate Limit Tiers:**

| Tier   | RPS | Burst | Use Case                 |
|--------|-----|-------|--------------------------|
| Global | 100 | 200   | Server-wide limit        |
| Plugin | 50  | 100   | Authenticated clients    |
| Unauth | 10  | 20    | Unauthenticated requests |

---

### `internal/ingestion/` - Domain Models

Pure domain models and interfaces following OpenLineage specification.

**Key Types:**

| Type         | File           | Purpose                                            |
|--------------|----------------|----------------------------------------------------|
| `RunEvent`   | `models.go`    | OpenLineage RunEvent domain model                  |
| `EventType`  | `models.go`    | Run states (START, RUNNING, COMPLETE, FAIL, ABORT) |
| `Dataset`    | `models.go`    | OpenLineage dataset with facets                    |
| `TestResult` | `models.go`    | Data quality test outcome                          |
| `Store`      | `store.go`     | Event persistence interface                        |
| `Validator`  | `validator.go` | OpenLineage semantic validation                    |

**Store Interface:**

```go
type Store interface {
StoreEvent(ctx context.Context, event *RunEvent) (stored, duplicate bool, err error)
StoreEvents(ctx context.Context, events []*RunEvent) ([]*EventStoreResult, error)
HealthCheck(ctx context.Context) error
}
```

**Key Methods:**

| Method                         | Purpose                                  |
|--------------------------------|------------------------------------------|
| `RunEvent.IdempotencyKey()`    | SHA256 deduplication key                 |
| `Dataset.URN()`                | Canonical dataset URN (`namespace/name`) |
| `Validator.ValidateRunEvent()` | OpenLineage spec compliance              |

---

### `internal/storage/` - PostgreSQL Backend

Production-ready PostgreSQL implementation with idempotency, out-of-order handling, and correlation views.

**Key Types:**

| Type                 | File                      | Purpose                       |
|----------------------|---------------------------|-------------------------------|
| `LineageStore`       | `lineage_store.go`        | OpenLineage event persistence |
| `PersistentKeyStore` | `persistent_key_store.go` | PostgreSQL API key storage    |
| `Connection`         | `config.go`               | Connection pool management    |

**LineageStore Features:**

- **Idempotency:** SHA256-based deduplication with 24-hour TTL
- **Out-of-order handling:** eventTime comparison in SQL CASE statements
- **State history:** Application-level transition tracking (not DB triggers)
- **Row locking:** `SELECT ... FOR UPDATE` for concurrent safety
- **Background cleanup:** Goroutine cleans expired idempotency keys

**Key Functions:**

```go
// Event storage with idempotency
func (s *LineageStore) StoreEvent(ctx, event) (stored, duplicate, error)

// State management
func fetchJobRunState(ctx, tx, runID) (jobRunState, error)
func validateStateTransition(oldState, newState) error
func buildUpdatedStateHistory(history, old, new, time, changed) ([]byte, error)

// Test result extraction
func (s *LineageStore) extractDataQualityAssertions(ctx, tx, event)
```

---

### `internal/correlation/` - Correlation Interface

Read-only interface for correlation queries (Interface Segregation Principle).

**Store Interface:**

```go
type Store interface {
RefreshViews(ctx context.Context) error
QueryIncidents(ctx context.Context, filter *IncidentFilter) ([]Incident, error)
QueryLineageImpact(ctx context.Context, runID string, maxDepth int) ([]ImpactResult, error)
QueryRecentIncidents(ctx context.Context, limit int) ([]RecentIncidentSummary, error)
}
```

**Interface Segregation:**

| Interface              | Purpose                 | Used By                   |
|------------------------|-------------------------|---------------------------|
| `ingestion.Store`      | Write-only (StoreEvent) | Ingestion handlers        |
| `correlation.Store`    | Read-only (queries)     | UI/Dashboard handlers     |
| `storage.LineageStore` | Implements BOTH         | Single PostgreSQL backend |

---

### `internal/canonicalization/` - URN Generation

Canonical identifier generation for deduplication and dataset correlation.

**Key Functions:**

| Function                              | Purpose                  | Example Output                    |
|---------------------------------------|--------------------------|-----------------------------------|
| `GenerateDatasetURN(namespace, name)` | Canonical dataset URN    | `postgres://db:5432/schema.table` |
| `GenerateIdempotencyKey(...)`         | SHA256 deduplication key | `a1b2c3d4...` (64 chars)          |

**Note:** `GenerateJobRunID()` was removed in B.2 -- the OpenLineage `run.runId` UUID is used
directly as the primary key, eliminating the `tool:runId` canonical prefix format.

---

## Database Schema

### Tables

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   job_runs      │    │    datasets     │    │  lineage_edges  │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ run_id (PK,UUID)│◀───│ dataset_urn (PK)│◀───│ id (PK)         │
│ job_name        │    │ name            │    │ run_id (FK,UUID)│
│                 │    │ namespace       │    │ dataset_urn (FK)│
│ job_namespace   │    │ facets (JSONB)  │    │ edge_type       │
│ current_state   │    │ created_at      │    │ input_facets    │
│ state_history   │    │ updated_at      │    │ output_facets   │
│ event_time      │    └─────────────────┘    │ created_at      │
│ metadata (JSONB)│                           └─────────────────┘
│ producer_name   │
│ created_at      │    ┌─────────────────┐    ┌─────────────────┐
│ updated_at      │    │  test_results   │    │    api_keys     │
└─────────────────┘    ├─────────────────┤    ├─────────────────┤
                       │ id (PK)         │    │ id (PK)         │
                       │ test_name       │    │ plugin_id       │
                       │ dataset_urn (FK)│    │ key_hash        │
                       │ run_id (FK,UUID)│    │ key_prefix      │
                       │ status          │    │ permissions     │
                       │ metadata (JSONB)│    │ active          │
                       │ executed_at     │    │ expires_at      │
                       │ created_at      │    │ created_at      │
                       │ updated_at      │    │ updated_at      │
                       └─────────────────┘    └─────────────────┘
```

### Materialized Views

| View                        | Purpose                                 | Refresh      |
|-----------------------------|-----------------------------------------|--------------|
| `incident_correlation_view` | Correlates test failures → job runs     | CONCURRENTLY |
| `lineage_impact_analysis`   | Recursive downstream impact (10 levels) | CONCURRENTLY |
| `recent_incidents_summary`  | 7-day rolling incident aggregation      | CONCURRENTLY |

**Correlation Query:**

```sql
-- Find which job produced the failing dataset
SELECT run_id, job_name, producer_name, job_started_at
FROM incident_correlation_view
WHERE test_status IN ('failed', 'error')
  AND dataset_urn = 'postgres://db:5432/marts.orders';
```

---

## OpenLineage Event Structure

### RunEvent (Ingestion)

```json
{
  "eventTime": "2025-01-07T10:30:00Z",
  "eventType": "COMPLETE",
  "producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "job": {
    "namespace": "dbt://analytics",
    "name": "jaffle_shop.test"
  },
  "inputs": [
    {
      "namespace": "postgres://analytics_db",
      "name": "marts.orders",
      "inputFacets": {
        "dataQualityAssertions": {
          "_producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
          "assertions": [
            {
              "assertion": "not_null_orders_order_id",
              "success": true,
              "column": "order_id"
            },
            {
              "assertion": "unique_orders_order_id",
              "success": false,
              "column": "order_id"
            }
          ]
        }
      }
    }
  ],
  "outputs": []
}
```

### Correlation Response

```json
{
  "test_name": "unique_orders_order_id",
  "test_status": "failed",
  "dataset_urn": "postgres://analytics_db/marts.orders",
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "job_name": "jaffle_shop.test",
  "producer_name": "dbt-correlator",
  "job_started_at": "2025-01-07T10:29:55Z",
  "root_cause": "Job 'jaffle_shop.test' produced dataset 'marts.orders' which failed test 'unique_orders_order_id'"
}
```

---

## Design Decisions

### 1. Interface Segregation (Read/Write Separation)

**Decision:** Separate `ingestion.Store` (write) and `correlation.Store` (read) interfaces.

**Rationale:**

- Clients depend only on methods they need
- Enables future CQRS pattern (separate read/write stores)
- Single `LineageStore` implements both interfaces today
- Clean separation for testing and mocking

### 2. Application-Level State History

**Decision:** Track state transitions in Go application code, not PostgreSQL triggers.

**Rationale:**

- DB triggers couldn't detect "no actual state change" scenarios
- Application code has full context (eventTime comparison)
- Better testability (unit tests vs integration tests)
- Clearer logic flow with explicit functions
- Row locking (`FOR UPDATE`) ensures concurrent safety

### 3. Idempotency with SHA256 + 24-Hour TTL

**Decision:** SHA256 hash of event fields with 24-hour expiration.

**Rationale:**

- Prevents duplicate processing on client retries
- 24 hours balances retry window with storage growth
- Background cleanup every hour (batch deletes)
- Follows industry standard (Stripe, AWS, Google)

### 4. Deferred Foreign Key Constraints

**Decision:** All FK constraints are `DEFERRABLE INITIALLY DEFERRED`.

**Rationale:**

- Handles out-of-order OpenLineage events
- Event B (references dataset X) can arrive before Event A (creates dataset X)
- Both events succeed within same transaction
- Critical for distributed systems reliability

### 5. Materialized Views with CONCURRENTLY

**Decision:** Use `REFRESH MATERIALIZED VIEW CONCURRENTLY` for correlation views.

**Rationale:**

- Zero-downtime updates (no table locks)
- ~650ms-2s refresh time acceptable for MVP
- Queries always return results (never blocked)
- Can upgrade to incremental refresh later

### 6. Fire-and-Forget Test Result Extraction

**Decision:** `extractDataQualityAssertions` logs errors but doesn't fail event storage.

**Rationale:**

- Primary concern is OpenLineage event storage
- Test result extraction is secondary (correlation enhancement)
- Failed extraction shouldn't break data pipeline
- Errors logged for debugging/alerting

---

## File Locations

| Component               | Location                     |
|-------------------------|------------------------------|
| Application entry point | `cmd/correlator/main.go`     |
| HTTP server & routes    | `internal/api/`              |
| Middleware components   | `internal/api/middleware/`   |
| Domain models           | `internal/ingestion/`        |
| PostgreSQL backend      | `internal/storage/`          |
| Correlation interface   | `internal/correlation/`      |
| URN generation          | `internal/canonicalization/` |
| Configuration helpers   | `internal/config/`           |
| Database migrations     | `migrations/`                |
| Documentation           | `docs/`                      |

---

## Related Documentation

- [Development Guide](DEVELOPMENT.md) - Setup and contributing
- [OpenLineage Spec](OPENLINEAGE.md) - Event structure reference
- [Plugin Authentication](PLUGIN-AUTHENTICATION.md) - API key management
- [Rate Limiting](RATE-LIMITING.md) - Rate limit configuration
- [Idempotency Cleanup](IDEMPOTENCY-CLEANUP.md) - TTL cleanup runbook
- [Migrations](MIGRATIONS.md) - Database schema evolution
- [dbt-correlator](https://github.com/correlator-io/correlator-dbt) - dbt plugin that emits OpenLineage events