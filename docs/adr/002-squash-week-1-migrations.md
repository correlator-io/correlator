# ADR 002: Squash Week 1 Migrations

**Status:** Accepted  
**Date:** November 18, 2025  
**Author:** Correlator Engineering Team  
**Deciders:** Engineering, Architecture  
**Supersedes:** None  
**Related ADRs:** None

---

## Context

During Week 1 development, we created 11 separate migrations (001-011) as we built the OpenLineage-compliant schema and correlation views. These migrations included:

- 001: Initial schema with basic job_runs, datasets, lineage_edges, test_results
- 002: Performance optimization indexes and materialized views (deprecated)
- 003-004: API keys authentication system
- 005: OpenLineage RunEvent compliance (added run_id, event_type, state_history)
- 006: Idempotency table
- 007-008: Dataset URN delimiter fixes (`:` → `/`)
- 009: Deferred foreign key constraints
- 010: Lineage edges cleanup (removed deprecated columns)
- 011: Correlation views for incident analysis

Many of these migrations (003-010) were **corrections** to OpenLineage specification compliance issues discovered during development.

### Problems Identified

1. **Four broken down migrations (010, 008, 007, 005):**
   - Missing data migration logic
   - Constraint violations during rollback
   - State value mismatches (lowercase ↔ UPPERCASE)
   - Would require 8+ hours to fix properly

2. **Migration history represents archaeology, not value:**
   - Documents mistakes and corrections, not intentional design
   - Creates confusion for future developers ("why did we do this?")
   - Intermediate states should never have existed

3. **Pre-alpha status:**
   - Week 1 of 36-week roadmap
   - Zero production deployments
   - Zero customer data
   - No external team members affected

---

## Decision

**Squash migrations 001-011 into single `001_initial_openlineage_schema` migration.**

### What Was Done

1. **Created squashed migration:**
   - File: `migrations/001_initial_openlineage_schema.up.sql`
   - Single ~800-line migration
   - OpenLineage v1.0 compliant from day 1
   - Includes all correlation views and functions
   
2. **Created simple down migration:**
   - File: `migrations/001_initial_openlineage_schema.down.sql`
   - Clean rollback with correct dependency order
   - No data migration complexity (clean drop)

3. **Archived old migrations:**
   - Moved to: `.idea/archive-migrations/`
   - Preserved in git history
   - Out of embedded filesystem scope

---

## Rationale

### Industry Standard Practice

- Rails: `db:migrate:squash` exists for exactly this reason
- Django: Recommends squashing before releases
- Flyway/Liquibase: "Baseline" migrations are common practice

### Time Investment Comparison

| Approach | Time | Customer Value |
|----------|------|----------------|
| Fix broken down migrations | 8+ hours | Zero (development tooling) |
| Squash migrations | 2 hours | Zero (development tooling) |
| **Build correlation logic** | **8 hours** | **HIGH (core product value)** |

**Decision:** Invest time in core value (correlation logic), not maintenance overhead.

### Technical Benefits

**Immediate:**
1. ✅ Zero broken down migrations (vs 4 broken)
2. ✅ Single migration file (~800 lines vs 11 files with 2000+ lines)
3. ✅ Faster integration tests (1 migration cycle vs 11)
4. ✅ Simpler onboarding (new devs understand 1 migration, not 11)
5. ✅ OpenLineage compliant from day 1 (no intermediate broken states)

**Long-term:**
1. ✅ Easier maintenance (1 file vs 11)
2. ✅ Less technical debt
3. ✅ Better foundation for Week 2+
4. ✅ No "why did we do this?" confusion

---

## Consequences

### Positive

1. **Clean OpenLineage-compliant foundation:**
   - All tables, views, indexes, and functions in correct state
   - No intermediate broken states
   - No migration archaeology

2. **Zero broken down migrations:**
   - Simple down migration (clean drop)
   - No complex data migration logic required
   - Idempotent up/down cycles work flawlessly

3. **Faster development:**
   - Integration tests run faster (1 migration vs 11)
   - Less cognitive overhead for new developers
   - Focus on core value (correlation logic)

4. **Better maintenance:**
   - Single source of truth for schema
   - Easier to understand and modify
   - No legacy migration baggage

### Negative

1. **Lose intermediate migration history:**
   - Mitigated: Preserved in git history
   - Mitigated: Archived in `.idea/archive-migrations/`
   - Reality: History was "mistakes", not valuable design decisions

2. **Can't rollback to intermediate states:**
   - Mitigated: Pre-alpha phase (no production data)
   - Reality: Intermediate states were broken (010, 008, 007, 005)
   - Reality: No use case for partial rollback in Week 1

---

## Alternatives Considered

### Alternative 1: Fix All Down Migrations

**Approach:** Fix broken down migrations in 010, 008, 007, 005

**Time Investment:** 8+ hours

**Pros:**
- Preserve intermediate migration history
- Allows rollback to any point

**Cons:**
- 8+ hours with zero customer value
- Week 1 is 48% complete (should focus on core value)
- Intermediate states represent mistakes, not design
- Still have 11 migrations to maintain

**Decision:** Rejected - Time better spent on core correlation logic

### Alternative 2: Keep Broken Migrations

**Approach:** Document down migrations as broken, only support forward migrations

**Pros:**
- No time investment
- Keep existing migrations

**Cons:**
- Sets bad precedent (broken migrations acceptable)
- Integration tests fail (drop command tests)
- Technical debt accumulates
- Confusing for future developers

**Decision:** Rejected - Unacceptable to ship broken migrations

---

## Implementation

### Files Created

- `migrations/001_initial_openlineage_schema.up.sql`
- `migrations/001_initial_openlineage_schema.down.sql`

### Files Archived

Moved to `.idea/archive-migrations/`:
- `001_initial_schema.{up,down}.sql`
- `002_performance_optimization.{up,down}.sql`
- `003_api_keys.{up,down}.sql`
- `004_key_lookup.{up,down}.sql`
- `005_openlineage_job_runs.{up,down}.sql`
- `006_lineage_idempotency.{up,down}.sql`
- `007_dataset_urn_delimiter.{up,down}.sql`
- `008_lineage_edges_refactor.{up,down}.sql`
- `009_deferred_constraints.{up,down}.sql`
- `010_lineage_edges_cleanup.{up,down}.sql`
- `011_correlation_views.{up,down}.sql`

### Testing

All migration tests pass after squashing:
```bash
$ go test -v ./migrations
PASS
ok      github.com/correlator-io/correlator/migrations  3.660s
```

Migration cycles work correctly:
- Up migration: All tables, views, and functions created ✅
- Down migration: Clean rollback ✅
- Up/down/up cycles: Idempotent ✅

---

## Monitoring

### Success Criteria

- [x] All migration integration tests pass
- [x] Up/down/up cycles work correctly
- [x] Squashed migration is OpenLineage v1.0 compliant
- [x] No application layer changes required
- [x] Time saved can be invested in core correlation logic

### Risks

**Risk:** Lose ability to understand migration history  
**Mitigation:** Git history preserved, archived migrations available

**Risk:** Can't rollback to intermediate state  
**Mitigation:** Pre-alpha phase, no production data exists

**Risk:** Future developers confused  
**Mitigation:** This ADR documents the decision, rationale clear

---

## References

- Week 1 Progress Review: `notes/reviews/review-001.md`
- Week 1 Critical Path Plan: `.idea/implementation-plans/week-1-critical-path-plan.md`
- Archived Migrations: `.idea/archive-migrations/`
- Git History: Commits from 2025-09-10 to 2025-11-18

---

## Notes

### Timeline

- **2025-09-10:** Initial schema created (migration 001)
- **2025-09-10 - 2025-11-15:** Incremental migrations 002-011 added
- **2025-11-18:** Decision to squash made (Week 1, Day 5)
- **2025-11-18:** Squashing completed and tested

### Key Insight

This decision exemplifies **pragmatic engineering** over perfectionism:
- Acknowledged mistakes (broken down migrations)
- Evaluated context (pre-alpha, Week 1)
- Made reversible decision (git history preserved)
- Prioritized customer value (correlation logic) over infrastructure maintenance

### Quote

> "You built a perfect foundation but forgot to build the house."  
> — Week 1 Progress Review

This squashing decision ensures we focus on building the house (correlation logic) rather than perfecting the foundation (migrations).

---

**Decision made:** 2025-11-18  
**Status:** Implemented and tested ✅  
**Next steps:** Continue with Week 1 critical path (Task 1.3 - Correlation view integration tests)

