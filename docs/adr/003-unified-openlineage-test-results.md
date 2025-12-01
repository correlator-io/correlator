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

### Initial Architecture (Week 1):

```
┌─────────────────┐         ┌──────────────────┐
│ dbt-tests       │────────▶│ /api/v1/         │
└─────────────────┘         │ test-results     │
                            └──────────────────┘
                                     │
                                     ▼
┌─────────────────┐         ┌──────────────────┐
│ Great           │────────▶│ /api/v1/         │
│ Expectations    │         │ lineage/events   │
└─────────────────┐         └──────────────────┘
                  │                  │
                  └──────────────────┴──────────▶ Correlator

Result: TWO different ingestion paths for test results
```

### Problems Discovered:

1. **Separate API Endpoint Creates Integration Friction**
   - Great Expectations emits OpenLineage events with `dataQualityAssertions` facet
   - Users cannot simply point `OPENLINEAGE_URL` to Correlator (need adapter)
   - Custom API format diverges from OpenLineage specification
   - Spec: https://openlineage.io/docs/spec/facets/dataset-facets/data-quality-assertions

2. **Duplicate Correlation Logic**
   - Week 2: Build correlation logic for custom test results API
   - Week 3: Build correlation logic for `dataQualityAssertions` facet extraction
   - Two code paths doing the same thing (violates DRY principle)

3. **Not True OpenLineage Compliance**
   - Test results API uses custom schema (`dataset_urn` string)
   - OpenLineage spec embeds test results in events as dataset facets
   - Forces Correlator-specific integration instead of standard
   - Spec: https://openlineage.io/docs/spec/object-model

4. **Research Findings:**
   - **Great Expectations:** Already emits `dataQualityAssertions` facet in OpenLineage events (https://openlineage.io/docs/integrations/great-expectations)
   - **dbt:** Does NOT emit test results facets (gap in dbt-ol wrapper) (https://openlineage.io/docs/integrations/dbt)
   - **Solution:** Build dbt plugin that emits OpenLineage events WITH facets (like GE)

---

## Decision

**We will eliminate the dedicated `/api/v1/test-results` endpoint** and adopt a **unified OpenLineage ingestion approach** where test results are extracted from the `dataQualityAssertions` dataset facet within OpenLineage events.

### New Architecture:

```
┌─────────────────────────────────────────────┐
│ OpenLineage Producers                       │
├─────────────────────────────────────────────┤
│ •dbt-correlator (emit dataQualityAssertions)│
│ •Great Expectations (native support)        │
│ •Future tools (Soda, Monte Carlo, etc.)     │
└─────────────────────────────────────────────┘
                    ↓
        ALL emit standard OpenLineage events
                    ↓
┌─────────────────────────────────────────────┐
│ POST /api/v1/lineage/events                 │
│ (Single Ingestion Endpoint)                 │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ Correlator Ingestion Pipeline               │
├─────────────────────────────────────────────┤
│ 1. Parse OpenLineage event                  │
│ 2. Store job_runs, datasets, lineage_edges  │
│ 3. Extract dataQualityAssertions facet      │
│ 4. Store in test_results table              │
│ 5. Correlation via materialized views       │
└─────────────────────────────────────────────┘
```

### Implementation Strategy:

**1. Enhance OpenLineage Ingestion (internal/storage/lineage_store.go):**

```go
func (s *LineageStore) upsertDataset(ctx context.Context, tx *sql.Tx, 
    dataset *ingestion.Dataset, jobRunID string, eventTime time.Time) error {
    
    // Store dataset (existing logic)
    datasetURN := dataset.URN()
    // ... existing code ...
    
    // NEW: Extract test results from dataQualityAssertions facet
    if assertions, ok := dataset.Facets["dataQualityAssertions"].(map[string]interface{}); ok {
        if err := s.extractAndStoreTestResults(ctx, tx, datasetURN, jobRunID, eventTime, assertions); err != nil {
            // Log warning but don't fail (test results are optional enhancement)
            s.logger.Warn("failed to extract test results from dataQualityAssertions", 
                "error", err, "dataset_urn", datasetURN)
        }
    }
    
    return nil
}
```

**2. Build dbt-correlator Plugin (Week 2):**

```python
# dbt-correlator plugin emits OpenLineage WITH dataQualityAssertions
from openlineage.client import OpenLineageClient
from openlineage.client.facet import DataQualityAssertionsDatasetFacet

# Read dbt run_results.json → Construct OpenLineage event → Emit to Correlator
```

**3. Remove `/api/v1/test-results` endpoint** (Week 2, Late)

---

## Consequences

### Positive:

1. ✅ **Single Correlation Implementation** - One code path, easier maintenance
2. ✅ **True OpenLineage Compliance** - Works with ANY standard tool
3. ✅ **Zero-Friction GE Integration** - Point URL, it works
4. ✅ **Time Savings** - 22 hours net (Week 3 eliminated)
5. ✅ **Architectural Purity** - Clean separation of concerns
6. ✅ **Future-Proof** - Extensible to new tools automatically

### Negative:

1. ⚠️ dbt Plugin Complexity (+2-4 hours in Week 2)
2. ⚠️ Dependency on OpenLineage spec evolution
3. ⚠️ Week 1 test results code will be removed (learning experience)

### Net Impact:

**+22 hours saved, cleaner architecture, better UX** ✅

---

## References

- **OpenLineage Data Quality Assertions:** https://openlineage.io/docs/spec/facets/dataset-facets/data-quality-assertions
- **OpenLineage Naming Conventions:** https://openlineage.io/docs/spec/naming/
- **Great Expectations Integration:** https://openlineage.io/docs/integrations/great-expectations
- **dbt Integration:** https://openlineage.io/docs/integrations/dbt

---

**Signed:**
- Product Owner: [Approved December 1, 2025]
- Senior Backend Engineer: [Approved December 1, 2025]

**Status:** ✅ **ACCEPTED** - Proceed with implementation in Week 2
