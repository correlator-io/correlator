# OpenLineage Event Validation Architecture

**Version:** 1.0  
**Last Updated:** November 2025  
**Status:** Production-Ready (Week 1 MVP)

---

## Overview

Correlator implements a three-layered validation architecture for OpenLineage events, providing defense-in-depth through application-level validation, sequence validation, and database-level enforcement. This design ensures data integrity while delivering excellent developer experience.

### Key Features

- **Three-layer validation**: Application, sequence, and database validation
- **Semantic validation**: 400x faster than JSON schema validation (~5µs vs ~20ms)
- **State machine enforcement**: OpenLineage run cycle compliance
- **Out-of-order handling**: Events sorted by eventTime for distributed systems
- **Defense in depth**: Multiple validation layers prevent data corruption
- **Client-friendly errors**: HTTP 422 with clear validation messages
- **Database integrity**: Triggers enforce terminal state immutability

---

## Architecture

### Three-Layer Validation Model

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: Field Validation (validator.go)                        │
│                                                                 │
│ • Validates individual event fields                             │
│ • ValidateRunEvent() - eventTime, job.name, schemaURL, etc.     │
│ • Returns: HTTP 422 with field-level errors                     │
│ • Performance: ~5µs per event (232K events/sec)                 │
│ • Triggers: Every event in request                              │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2: Sequence Validation (lifecycle.go)                     │
│                                                                 │
│ • Validates state transition sequences                          │
│ • ValidateEventSequence() - sorts + validates transitions       │
│   ├─ SortEventsByTime() - chronological ordering                │
│   └─ ValidateStateTransition() - per-transition rules           │
│ • Returns: HTTP 422 with sequence errors                        │
│ • Performance: ~2µs per transition                              │
│ • Triggers: Single-run batches only (len > 1 && same runId)     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3: Database Trigger (migration 005)                       │
│                                                                 │
│ • Enforces terminal state immutability                          │
│ • validate_job_run_state_transition() trigger                   │
│ • Returns: SQL exception (raises error)                         │
│ • Performance: ~50µs overhead                                   │
│ • Triggers: BEFORE UPDATE on job_runs table                     │
│ • Purpose: Defense against application bugs                     │
└─────────────────────────────────────────────────────────────────┘
```

### Validation Layers

| Layer | Scope | Trigger | Returns | Purpose |
|-------|-------|---------|---------|---------|
| **Field Validation** | Individual events | Every event | HTTP 422 | Validate required fields, formats |
| **Sequence Validation** | Event batches | Single-run batches | HTTP 422 | Validate state transitions |
| **Database Trigger** | State transitions | UPDATE job_runs | SQL Exception | Prevent data corruption |

---

## Layer 1: Field Validation

### Implementation

```go
// internal/ingestion/validator.go
validator := ingestion.NewValidator()
err := validator.ValidateRunEvent(event)
// Returns error if: eventTime zero, job.name empty, invalid schemaURL, etc.
```

### What It Validates

- **Required fields**: eventTime, eventType, run.runId, job.name, job.namespace, producer, schemaURL
- **Field formats**: eventTime (not zero), schemaURL (OpenLineage URL pattern)
- **Data types**: EventType must be START/RUNNING/COMPLETE/FAIL/ABORT/OTHER
- **Nil checks**: Inputs/Outputs must be non-nil arrays (empty OK)

### Performance

- **Latency**: ~5µs per event (semantic validation)
- **Throughput**: 232,000 events/second (single-threaded)
- **Memory**: Zero allocations (pre-compiled regex patterns)

### Error Example

```json
{
  "correlation_id": "abc123",
  "timestamp": "2024-01-01T12:00:00Z",
  "stored": 2,
  "duplicates": 0,
  "failed": 1,
  "results": [
    {"index": 0, "status": 200, "message": "stored"},
    {"index": 1, "status": 422, "error": "job.name is required"},
    {"index": 2, "status": 200, "message": "stored"}
  ]
}
```

---

## Layer 2: Sequence Validation

### Implementation

```go
// internal/ingestion/lifecycle.go
if len(events) > 1 && isSingleRunBatch(events) {
    sorted, finalState, err := ValidateEventSequence(events)
    // Returns error if: duplicate START, backward transition, terminal mutation
}
```

### When It Runs

**Single-run batches ONLY:**
- All events have same run.runId
- Example: `[START, RUNNING, COMPLETE]` for runId "abc123"

**Skipped for multi-run batches:**
- Events for different runs (independent validation)
- Example: `[START(run1), START(run2), START(run3)]`

### What It Validates

**Valid Transitions:**
- START → RUNNING, COMPLETE, FAIL, ABORT
- RUNNING → RUNNING, COMPLETE, FAIL, ABORT
- COMPLETE/FAIL/ABORT → same state (idempotent)
- OTHER → any state (metadata events)

**Invalid Transitions:**
- START → START (duplicate START)
- RUNNING → START (backward transition)
- COMPLETE → RUNNING (terminal state mutation)

### Error Example

```http
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/problem+json

{
  "type": "https://getcorrelator.io/problems/422",
  "title": "Unprocessable Entity",
  "status": 422,
  "detail": "Invalid event sequence: duplicate START event: runId already has START state",
  "instance": "/api/v1/lineage/events",
  "correlation_id": "abc123"
}
```

---

## Layer 3: Database Trigger

### Implementation

```sql
-- migration 005
CREATE TRIGGER job_run_state_validation
  BEFORE UPDATE ON job_runs
  FOR EACH ROW EXECUTE FUNCTION validate_job_run_state_transition();
```

### When It Runs

**On job_runs UPDATE only:**
- Single event storage (bypasses Layer 2)
- Out-of-order events that pass application checks
- Defense against application bugs

### What It Validates

**Terminal state immutability:**
- COMPLETE → START ❌ (rejected)
- FAIL → RUNNING ❌ (rejected)  
- COMPLETE → COMPLETE ✅ (idempotent, allowed)

### Error Example

```
ERROR: Invalid state transition: COMPLETE -> START (terminal states are immutable)
HINT: Terminal states (COMPLETE/FAIL/ABORT) can only transition to themselves (idempotent)
```

---

## Why Three Layers?

### Design Rationale

| Aspect | Without Layers | With Three Layers |
|--------|----------------|-------------------|
| **Batch UX** | Database error (500) | Clear sequence error (422) |
| **Performance** | DB validates everything | Application filters invalid |
| **Debugging** | Technical SQL errors | Human-readable messages |
| **Data Safety** | Application is authority | Database prevents corruption |

### Defense in Depth

```
Scenario: Application bug bypasses Layer 2

Layer 1: ✅ Validates fields (catches most errors)
Layer 2: ❌ Bug - doesn't validate sequence
Layer 3: ✅ Database trigger catches terminal state violation

Result: Data corruption prevented by Layer 3
```

---

## Implementation Details

### Validation Call Chain

The validation layers are implemented through a series of function calls that execute in sequence:

```
┌─────────────────────────────────────────────┐
│ HTTP Handler (routes.go)                    │
│ handleLineageEvents()                       │
│ • Parses JSON request body                  │
│ • Normalizes nil slices (Inputs/Outputs)    │
│ • Orchestrates validation layers            │
│ • Returns: 200/207/422/4xx/5xx              │
└──────────────────┬──────────────────────────┘
                   │
                   ├─► Field Validation (Layer 1)
                   │   for event in events:
                   │       s.validator.ValidateRunEvent(event)
                   │       ↓
                   │       Checks: eventTime, job.name, schemaURL, etc.
                   │       Returns: error with field details
                   │   
                   ├─► Sequence Validation (Layer 2)
                   │   if len(events) > 1 && isSingleRunBatch(events):
                   │       ValidateEventSequence(events)
                   │           ├─ SortEventsByTime(events)
                   │           │  ↓ Sort by eventTime (handle out-of-order)
                   │           │
                   │           └─ for each transition:
                   │                 ValidateStateTransition(from, to)
                   │                 ↓
                   │                 Check: START→START? COMPLETE→START?
                   │                 Returns: ErrDuplicateStart, ErrTerminalStateImmutable
                   │
                   └─► Storage (triggers Layer 3)
                       s.lineageStore.StoreEvents(ctx, events)
                           ↓
                           For each valid event:
                               StoreEvent(event)
                                   ↓
                                   Upsert job_runs (ON CONFLICT DO UPDATE)
                                   ↓
                                   Database Trigger (Layer 3):
                                       validate_job_run_state_transition()
                                       ↓
                                       IF OLD.state terminal AND NEW.state != OLD.state:
                                           RAISE EXCEPTION 'Invalid state transition'
```

### Key Functions

| Function | File | Purpose | Performance |
|----------|------|---------|-------------|
| **ValidateRunEvent()** | validator.go | Validates individual event fields | ~5µs |
| **ValidateEventSequence()** | lifecycle.go | Validates event sequence for single run | ~2µs/event |
| **ValidateStateTransition()** | lifecycle.go | Validates single state transition (helper) | ~100ns |
| **SortEventsByTime()** | lifecycle.go | Sorts events by eventTime (helper) | ~1µs |
| **validate_job_run_state_transition()** | migration 005 | Database trigger (terminal state check) | ~50µs |

### Code Locations

```
internal/
├── api/
│   └── routes.go               # HTTP handler (orchestration)
│       └── handleLineageEvents()
│
├── ingestion/
│   ├── validator.go            # Layer 1: Field validation
│   │   └── ValidateRunEvent()
│   │
│   └── lifecycle.go            # Layer 2: Sequence validation
│       ├── ValidateEventSequence()
│       ├── ValidateStateTransition() (helper)
│       └── SortEventsByTime() (helper)
│
└── storage/
    └── lineage_store.go        # Layer 3 integration
        └── StoreEvent() - Triggers database validation

migrations/
└── 005_openlineage_job_runs.up.sql  # Layer 3: Database trigger
    └── validate_job_run_state_transition()
```

---

## Request Flow

### Single Event (Bypasses Layer 2)

```text
POST /api/v1/lineage/events
Body: [{"eventType": "START", ...}]

→ Layer 1: ValidateRunEvent()      // Validates fields
→ Layer 2: SKIPPED                  // Single event, no sequence
→ Storage: StoreEvent()
→ Layer 3: Database trigger         // If UPDATE, validates transition
```

### Single-Run Batch (All 3 Layers)

```text
POST /api/v1/lineage/events
Body: [
  {"eventType": "START", "run": {"runId": "abc"}},
  {"eventType": "COMPLETE", "run": {"runId": "abc"}}
]

→ Layer 1: ValidateRunEvent() × 2     // Both events validated
→ Layer 2: ValidateEventSequence()    // START → COMPLETE validated
→ Storage: StoreEvents()
→ Layer 3: Database trigger × 2       // Each UPDATE validated
```

### Multi-Run Batch (Layers 1 & 3)

```text
POST /api/v1/lineage/events
Body: [
  {"eventType": "START", "run": {"runId": "abc"}},
  {"eventType": "START", "run": {"runId": "def"}}
]

→ Layer 1: ValidateRunEvent() × 2     // Both events validated
→ Layer 2: SKIPPED                    // Different runIds, independent
→ Storage: StoreEvents()
→ Layer 3: Database trigger × 2       // Each event validated
```

---

## Performance Characteristics

| Operation | Latency | Throughput          | Notes |
|-----------|---------|---------------------|-------|
| **Field validation** | ~5µs | 232K events/sec     | Semantic validation (no JSON schema) |
| **Sequence validation** | ~2µs/event | 500K events/sec     | State transition checks |
| **Database trigger** | ~50µs | 20K events/sec      | SQL function execution |
| **Total (single event, validation only)** | **~57µs** | **~18k events/sec** | Sum of validation steps (excludes I/O) |

**Target**: <100ms per event ✅ **Achieved: 57µs (validation only)**

---

## Troubleshooting

### Problem: Batch rejected with "Invalid event sequence"

**Symptoms:**
- 422 Unprocessable Entity
- Error mentions "duplicate START" or "backward transition"

**Cause:** Events in batch violate OpenLineage state machine

**Solution:**
```bash
# Check if all events are for same run
curl -X POST /api/v1/lineage/events -d '[
  {"eventType": "START", "run": {"runId": "run-1"}, ...},
  {"eventType": "START", "run": {"runId": "run-1"}, ...}  # Duplicate START!
]'

# Fix: Send events in separate requests or use unique runIds
curl -X POST /api/v1/lineage/events -d '[
  {"eventType": "START", "run": {"runId": "run-1"}, ...},
  {"eventType": "START", "run": {"runId": "run-2"}, ...}  # Different runs OK
]'
```

### Problem: Database error "Invalid state transition"

**Symptoms:**
- 500 Internal Server Error  
- Logs show: "pq: Invalid state transition: COMPLETE -> START"

**Cause:** Application layer allowed invalid transition, database caught it

**Solution:**
```bash
# This is a bug! File an issue with:
# 1. Events that caused the error
# 2. Correlation ID from error response
# 3. Timestamp of the request

# Workaround: Send events in chronological order
```

### Problem: Out-of-order events rejected

**Symptoms:**
- Events sent as: COMPLETE, START, RUNNING
- Rejected with "Invalid event sequence"

**Expected Behavior:**
- Application SHOULD sort by eventTime
- START → RUNNING → COMPLETE after sorting

**If rejected:** Check that all events have same runId (sequence validation only for single-run batches)

---

## Configuration

### Validation Strategy (ADR 001)

**Choice:** Semantic validation (not JSON schema)

**Rationale:**
- 400x faster (~5µs vs ~20ms)
- Better error messages
- OpenLineage schema uses oneOf at root (slow validation)

**Alternative Considered:**
- JSON schema validation with santhosh-tekuri/jsonschema/v6
- Rejected due to performance and error quality

### Customization

**Not configurable in MVP:**
- Validation is always enabled
- All three layers always active
- Future: Validation bypass for trusted plugins (Phase 2)

---

## References

### Internal Documentation
- `/internal/ingestion/validator.go` - Field validation implementation
- `/internal/ingestion/lifecycle.go` - Sequence validation implementation
- `/migrations/005_openlineage_job_runs.up.sql` - Database trigger
- `/docs/adr/001-openlineage-validation-strategy.md` - Validation decision

### External Resources
- [OpenLineage Run Cycle Specification](https://openlineage.io/docs/spec/run-cycle)
- [RFC 7807: Problem Details for HTTP APIs](https://datatracker.ietf.org/doc/html/rfc7807)

---

## Changelog

### Version 1.0 (November 2025) - Initial Release
- Three-layer validation architecture (application, sequence, database)
- Semantic validation strategy (5µs per event)
- State machine enforcement (ValidateStateTransition, ValidateEventSequence)
- Database triggers for terminal state protection
- Out-of-order event handling via eventTime sorting
- Single-run batch detection (isSingleRunBatch)

---

**Questions or issues?** File a GitHub issue at `github.com/correlator-io/correlator/issues`
