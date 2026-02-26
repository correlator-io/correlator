# ADR 003: Unified OpenLineage Ingestion for Test Results

**Status:** Accepted (Updated January 2026)
**Date:** December 1, 2025
**Updated:** January 7, 2026 - Week 2 E2E Testing Findings
**Author:** Correlator Engineering Team
**Deciders:** Product Owner, Engineering, Architecture
**Supersedes:** None
**Related ADRs:** ADR-001 (Database Schema), ADR-002 (Migration Squashing)

> **Note (February 2026):** This ADR references `job_run_id` (the old `"tool:uuid"` canonical format).
> As (Week 4), `job_run_id` has been removed. The `run_id UUID` from OpenLineage is now stored
> as-is and used as the primary key. The architectural decision in this ADR (unified ingestion via
> OpenLineage facets) remains valid — only the ID format has changed.

---

## Context

During Week 1 implementation, we built a dedicated test results API endpoint (`/api/v1/test-results`) to enable
integration with data quality testing frameworks (dbt-tests, Great Expectations). The endpoint was designed to accept
test results and correlate them with job runs via canonical `job_run_id`.

### Problems Discovered

1. **Separate API Endpoint Creates Integration Friction**
    - Great Expectations emits OpenLineage events with `dataQualityAssertions` facet natively
    - Users cannot simply point `OPENLINEAGE_URL` to Correlator (need custom adapter)
    - Custom API format diverges from OpenLineage specification

2. **Duplicate Correlation Logic**
    - Custom test results API requires its own correlation logic
    - `dataQualityAssertions` facet extraction requires separate correlation logic
    - Two code paths doing the same thing (violates DRY principle)

3. **Not True OpenLineage Compliance**
    - Test results API uses custom schema (`dataset_urn` string)
    - OpenLineage spec embeds test results in events as dataset facets
    - Forces Correlator-specific integration instead of standard

4. **Research Findings**
    - **Great Expectations:** Already emits `dataQualityAssertions` facet in OpenLineage events
    - **dbt:** Does NOT emit test results facets (gap in dbt-ol wrapper)
    - **Solution:** Build dbt plugin that emits OpenLineage events WITH facets

---

## Decision

**Eliminate the dedicated `/api/v1/test-results` endpoint** and adopt a **unified OpenLineage ingestion approach** where
test results are extracted from the `dataQualityAssertions` dataset facet within standard OpenLineage events.

This means:

1. **Single ingestion endpoint** (`POST /api/v1/lineage/events`) handles all data
2. **Test results extracted from facets** during OpenLineage event processing
3. **dbt-correlator plugin** emits standard OpenLineage events with `dataQualityAssertions` facet
4. ~~**Great Expectations works out-of-the-box** (already emits compliant events)~~ **See Update Below**

---

## Update: Week 2 E2E Testing Findings (January 2026)

### Original Assumption (INCORRECT)

> "Great Expectations works out-of-the-box — Point `OPENLINEAGE_URL` to Correlator, done"

### Actual Finding

During Week 2 end-to-end testing with `dbt-correlator`, we discovered that **native OpenLineage emitters are NOT
directly compatible with Correlator's backend**. The OpenLineage Python client and tools that use it (including Great
Expectations' native integration) have subtle incompatibilities:

1. **Schema URL format:** The official OpenLineage Python client emits JSON Schema fragment references (e.g.,
   `...OpenLineage.json#/$defs/RunEvent`) that required validation fixes in Correlator
2. **Event structure variations:** Native emitters may structure events differently than expected
3. **Missing correlation context:** Native emitters don't include Correlator-specific context needed for optimal
   correlation

### Revised Decision

**All data quality tools require dedicated Correlator plugins** to ensure:

- Consistent event structure
- Proper correlation context (namespace, job naming conventions)
- Reliable `dataQualityAssertions` facet extraction
- Tested compatibility with Correlator backend

### Impact on Roadmap

| Tool               | Original Plan                     | Revised Plan                            |
|--------------------|-----------------------------------|-----------------------------------------|
| dbt                | Build `dbt-correlator` plugin     | ✅ Complete (Week 2)                     |
| Great Expectations | "Just works" via native OL        | ❌ Build `correlator-ge` plugin (Week 3) |
| Airflow            | Build `correlator-airflow` plugin | ✅ Unchanged (Week 3)                    |

---

## Consequences

### Positive

- **Single correlation implementation** — One code path, easier maintenance
- **True OpenLineage compliance** — Events follow OpenLineage spec structure
- **Consistent integration pattern** — All tools use dedicated plugins with tested compatibility
- **Future-proof** — New tools follow established plugin pattern
- **Architectural simplicity** — No custom API schemas to maintain

### Negative

- **All tools need plugins** — Cannot rely on native OL emitters (more development work)
- **dbt plugin required** — Must build `dbt-correlator` to emit `dataQualityAssertions` facet ✅ Done
- **GE plugin required** — Must build `correlator-ge` despite native OL support (Week 3)
- **Dependency on OpenLineage spec** — Tied to facet schema evolution
- **Week 1 code removed** — `/api/v1/test-results` endpoint and related code deleted

---

## References

- [OpenLineage Data Quality Assertions Facet](https://openlineage.io/docs/spec/facets/dataset-facets/data-quality-assertions)
- [OpenLineage Object Model](https://openlineage.io/docs/spec/object-model)
- [Great Expectations OpenLineage Integration](https://openlineage.io/docs/integrations/great-expectations)
- [dbt OpenLineage Integration](https://openlineage.io/docs/integrations/dbt)
- [correlator-dbt](https://github.com/correlator-io/correlator-dbt) - Reference plugin implementation
