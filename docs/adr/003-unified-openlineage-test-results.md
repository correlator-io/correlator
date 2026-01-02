# ADR 003: Unified OpenLineage Ingestion for Test Results

**Status:** Accepted  
**Date:** December 1, 2025  
**Author:** Correlator Engineering Team  
**Deciders:** Product Owner, Engineering, Architecture  
**Supersedes:** None  
**Related ADRs:** ADR-001 (Database Schema), ADR-002 (Migration Squashing)

---

## Context

During Week 1 implementation, we built a dedicated test results API endpoint (`/api/v1/test-results`) to enable integration with data quality testing frameworks (dbt-tests, Great Expectations). The endpoint was designed to accept test results and correlate them with job runs via canonical `job_run_id`.

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

**Eliminate the dedicated `/api/v1/test-results` endpoint** and adopt a **unified OpenLineage ingestion approach** where test results are extracted from the `dataQualityAssertions` dataset facet within standard OpenLineage events.

This means:

1. **Single ingestion endpoint** (`POST /api/v1/lineage/events`) handles all data
2. **Test results extracted from facets** during OpenLineage event processing
3. **dbt-correlator plugin** emits standard OpenLineage events with `dataQualityAssertions` facet
4. **Great Expectations works out-of-the-box** (already emits compliant events)

---

## Consequences

### Positive

- **Single correlation implementation** — One code path, easier maintenance
- **True OpenLineage compliance** — Works with ANY standard-compliant tool
- **Zero-friction Great Expectations integration** — Point `OPENLINEAGE_URL` to Correlator, done
- **Future-proof** — New tools (Soda, Monte Carlo, etc.) work automatically if they emit standard facets
- **Architectural simplicity** — No custom API schemas to maintain

### Negative

- **dbt plugin required** — Must build `dbt-correlator` to emit `dataQualityAssertions` facet
- **Dependency on OpenLineage spec** — Tied to facet schema evolution
- **Week 1 code removed** — `/api/v1/test-results` endpoint and related code deleted

---

## References

- [OpenLineage Data Quality Assertions Facet](https://openlineage.io/docs/spec/facets/dataset-facets/data-quality-assertions)
- [OpenLineage Object Model](https://openlineage.io/docs/spec/object-model)
- [Great Expectations OpenLineage Integration](https://openlineage.io/docs/integrations/great-expectations)
- [dbt OpenLineage Integration](https://openlineage.io/docs/integrations/dbt)
