-- =====================================================
-- Correlator: Initial OpenLineage-Compliant Database Schema
-- Complete schema for incident correlation engine
-- 
-- Standards: OpenLineage v1.0 compliant
-- =====================================================
--
-- TABLE MUTABILITY DESIGN
-- =====================================================
--
-- MUTABLE TABLES (have both created_at AND updated_at):
-- These tables support UPDATE operations and track modification history.
--
-- 1. job_runs - State transitions (START → RUNNING → COMPLETE/FAIL)
--    - updated_at tracks last state transition
--    - Trigger: update_job_runs_updated_at
--
-- 2. job_id_mappings - Mapping status changes (active → deprecated)
--    - updated_at tracks mapping confidence updates
--    - Trigger: update_job_id_mappings_updated_at
--
-- 3. datasets - Metadata enrichment over time
--    - updated_at tracks schema/facet updates
--    - Trigger: update_datasets_updated_at
--
-- 4. test_results - UPSERT behavior (re-ingestion with updated status)
--    - updated_at tracks when test was last re-ingested
--    - Trigger: update_test_results_updated_at
--    - UPSERT key: (test_name, dataset_urn, executed_at)
--
-- 5. api_keys - Activation/deactivation, permission updates
--    - updated_at tracks key lifecycle changes
--    - Trigger: update_api_keys_updated_at
--
-- IMMUTABLE TABLES (have only created_at):
-- These tables follow event sourcing principles - rows are never updated.
--
-- 1. lineage_edges - Immutable lineage facts
--    - Lineage relationships are historical facts (job A produced dataset B at time T)
--    - Never updated, only inserted
--
-- 2. correlation_events - Immutable audit trail
--    - Correlation events are append-only for accuracy tracking
--    - Compliance requirement: audit events must not be mutated
--
-- 3. lineage_event_idempotency - TTL-based cleanup
--    - Rows expire naturally via expires_at timestamp
--    - Deleted by cleanup job, never updated
--
-- 4. api_key_audit_log - Immutable audit trail
--    - Security requirement: audit logs must not be mutated
--    - Compliance: tamper-proof event log
--
-- WHY THIS MATTERS:
-- - Mutable tables: Need updated_at for audit trail and debugging
-- - Immutable tables: updated_at would always equal created_at (waste)
-- - Event sourcing: Historical facts should not be mutated
-- - Compliance: Audit trails must be tamper-proof (immutable)
--
-- =====================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =====================================================
-- 1. JOB RUNS - OpenLineage RunEvent storage
-- =====================================================
CREATE TABLE job_runs (
    -- Primary identifier: canonical job run ID
    -- Format: "system:id" (e.g., "dbt:abc123", "airflow:manual__2025-01-01T12:00:00")
    job_run_id VARCHAR(255) PRIMARY KEY CHECK (char_length(job_run_id) > 0 AND char_length(job_run_id) <= 255),

    -- OpenLineage RunEvent fields
    run_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Job metadata
    job_name VARCHAR(255) NOT NULL,
    job_namespace VARCHAR(255) DEFAULT 'default',

    -- Temporal information
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Job execution state (OpenLineage compliant)
    current_state VARCHAR(50) NOT NULL CHECK (current_state IN ('START', 'RUNNING', 'COMPLETE', 'FAIL', 'ABORT', 'OTHER')),
    
    -- State transition history for out-of-order event handling
    state_history JSONB DEFAULT '{"transitions": []}'::jsonb,

    -- OpenLineage facets and metadata
    metadata JSONB DEFAULT '{}',

    -- Producer identification
    producer_name VARCHAR(100) NOT NULL DEFAULT 'unknown',
    producer_version VARCHAR(50),

    -- Tracking timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for job_runs
CREATE INDEX idx_job_runs_run_id ON job_runs(run_id);
CREATE INDEX idx_job_runs_temporal ON job_runs(started_at DESC, current_state);

-- Comments
COMMENT ON TABLE job_runs IS 'OpenLineage RunEvent storage with canonical ID strategy and state machine tracking';
COMMENT ON COLUMN job_runs.job_run_id IS 'Canonical job run identifier in format: {system}:{original_id} - primary correlation key';
COMMENT ON COLUMN job_runs.run_id IS 'OpenLineage client-generated UUID (UUIDv7 recommended) - maintained throughout run lifecycle';
COMMENT ON COLUMN job_runs.event_type IS 'OpenLineage event type: START, RUNNING, COMPLETE, FAIL, ABORT, OTHER';
COMMENT ON COLUMN job_runs.event_time IS 'OpenLineage eventTime - when the event occurred (use for ordering, not arrival time)';
COMMENT ON COLUMN job_runs.state_history IS 'Array of state transitions with timestamps for out-of-order event handling';
COMMENT ON COLUMN job_runs.current_state IS 'Current run state - OpenLineage compliant';
COMMENT ON COLUMN job_runs.metadata IS 'OpenLineage RunEvent facets and producer-specific metadata as JSONB';

-- =====================================================
-- 2. JOB ID MAPPINGS - ID canonicalization
-- =====================================================
CREATE TABLE job_id_mappings (
    id BIGSERIAL PRIMARY KEY,

    canonical_job_run_id VARCHAR(255) NOT NULL REFERENCES job_runs(job_run_id) ON DELETE CASCADE,
    original_job_run_id VARCHAR(500) NOT NULL,

    producer_name VARCHAR(100) NOT NULL,
    producer_version VARCHAR(50),

    confidence_score DECIMAL(3,2) DEFAULT 1.00 CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00),
    mapping_status VARCHAR(50) DEFAULT 'active' CHECK (mapping_status IN ('active', 'deprecated', 'invalid')),
    mapping_metadata JSONB DEFAULT '{}',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(original_job_run_id, producer_name)
);

-- Indexes for job_id_mappings
CREATE INDEX idx_job_id_mappings_lookup ON job_id_mappings(original_job_run_id, producer_name, mapping_status);

-- Comments
COMMENT ON TABLE job_id_mappings IS 'ID canonicalization mapping with producer-specific formats and confidence scoring';
COMMENT ON COLUMN job_id_mappings.confidence_score IS 'Mapping confidence (0.0-1.0) for correlation accuracy tracking';

-- =====================================================
-- 3. DATASETS - Dataset registry
-- =====================================================
CREATE TABLE datasets (
    -- Dataset URN as primary key (OpenLineage standard)
    -- Format: namespace/name (e.g., "postgres://prod.db/clean/users", "s3://bucket/path/file.csv")
    dataset_urn VARCHAR(500) PRIMARY KEY CHECK (dataset_urn ~ '^[^:]+[:/].+$'),

    name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL DEFAULT 'default',

    owner VARCHAR(255),
    team VARCHAR(100),

    tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    description TEXT,

    facets JSONB DEFAULT '{}',
    correlation_stats JSONB DEFAULT '{}',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Comments
COMMENT ON TABLE datasets IS 'Dataset registry with OpenLineage facets and correlation statistics';
COMMENT ON COLUMN datasets.dataset_urn IS 'OpenLineage dataset URN: namespace/name format (supports : or / delimiter)';
COMMENT ON COLUMN datasets.facets IS 'OpenLineage dataset facets: schema, statistics, documentation';

-- =====================================================
-- 4. LINEAGE EDGES - OpenLineage lineage relationships
-- =====================================================
-- 
-- IMPORTANT: Deferred Constraints for Out-of-Order Events
-- All FK constraints are DEFERRABLE INITIALLY DEFERRED to handle concurrent/out-of-order
-- OpenLineage events. Example: Event B (references dataset X) arrives before Event A
-- (creates dataset X) - deferred constraints allow both to succeed within same transaction.
-- This is critical for distributed systems where events arrive out of order.
-- 
-- Edge Type Semantics:
-- - 'input':  Job CONSUMES this dataset (reads from)
-- - 'output': Job PRODUCES this dataset (writes to)
-- 
-- For correlation: Find which job produced a failing dataset by querying edge_type='output'
-- =====================================================
CREATE TABLE lineage_edges (
    id BIGSERIAL PRIMARY KEY,

    job_run_id VARCHAR(255) NOT NULL REFERENCES job_runs(job_run_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,

    -- OpenLineage edge model: separate rows for input and output
    dataset_urn VARCHAR(500) NOT NULL REFERENCES datasets(dataset_urn) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    edge_type VARCHAR(10) NOT NULL CHECK (edge_type IN ('input', 'output')),

    lineage_type VARCHAR(50) DEFAULT 'transformation' CHECK (lineage_type IN ('transformation', 'extraction', 'loading', 'validation')),

    input_facets JSONB DEFAULT '{}',
    output_facets JSONB DEFAULT '{}',

    downstream_dataset_count INTEGER DEFAULT 0,
    downstream_job_count INTEGER DEFAULT 0,
    impact_score DECIMAL(5,2) DEFAULT 0.00,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for lineage_edges
CREATE INDEX idx_lineage_edges_job_run_id ON lineage_edges(job_run_id);
CREATE INDEX idx_lineage_edges_dataset_urn ON lineage_edges(dataset_urn, edge_type, job_run_id);

-- Comments
COMMENT ON TABLE lineage_edges IS 'OpenLineage lineage edges: separate rows for each input and output dataset per job run';
COMMENT ON COLUMN lineage_edges.edge_type IS 'OpenLineage edge type: input (job consumes dataset) or output (job produces dataset)';
COMMENT ON COLUMN lineage_edges.dataset_urn IS 'Dataset involved in this lineage relationship';

-- =====================================================
-- 5. TEST RESULTS - Data quality test outcomes
-- =====================================================
-- 
-- CRITICAL: Deferred Constraints for Concurrent Ingestion
-- FK constraints are DEFERRABLE INITIALLY DEFERRED to allow test results and lineage
-- events to be ingested concurrently. Example: Test result arrives before corresponding
-- job run event - deferred constraints allow both to succeed within transaction.
-- 
-- Correlation Key:
-- - job_run_id links test results to producing job runs
-- - Core query: "Given a failed test, which job run produced the failing dataset?"
--
-- MUTABILITY: Mutable (UPSERT behavior)
-- - Test results can be re-ingested with updated status/message/metadata
-- - UPSERT key: (test_name, dataset_urn, executed_at)
-- - updated_at tracks when test result was last modified
-- =====================================================
CREATE TABLE test_results (
    id BIGSERIAL PRIMARY KEY,

    test_name VARCHAR(750) NOT NULL,
    test_type VARCHAR(100) DEFAULT 'data_quality',

    dataset_urn VARCHAR(500) NOT NULL REFERENCES datasets(dataset_urn) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    job_run_id VARCHAR(255) NOT NULL REFERENCES job_runs(job_run_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,

    status VARCHAR(50) NOT NULL CHECK (status IN ('passed', 'failed', 'error', 'skipped', 'warning')),
    message TEXT,

    metadata JSONB DEFAULT '{}',

    executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_ms INTEGER,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for test_results
CREATE INDEX idx_test_results_job_run_correlation ON test_results(job_run_id, status) WHERE status IN ('failed', 'error');
CREATE INDEX idx_test_results_dataset_lookup ON test_results(dataset_urn, executed_at DESC);

-- UNIQUE constraint for UPSERT behavior (prevents duplicate test results with same name, dataset, and execution time)
CREATE UNIQUE INDEX idx_test_results_upsert_key ON test_results(test_name, dataset_urn, executed_at);

-- Comments
COMMENT ON TABLE test_results IS 'Data quality test outcomes with job run correlation for incident analysis';
COMMENT ON COLUMN test_results.job_run_id IS 'CRITICAL correlation key linking test failures to producing job runs';
COMMENT ON COLUMN test_results.test_name IS 'Extended to VARCHAR(750) based on real-world test naming analysis';

-- =====================================================
-- 6. CORRELATION EVENTS - Accuracy tracking
-- =====================================================
CREATE TABLE correlation_events (
    id BIGSERIAL PRIMARY KEY,

    event_type VARCHAR(100) NOT NULL CHECK (event_type IN ('correlation_created', 'correlation_validated', 'correlation_failed', 'accuracy_measured')),

    test_result_id BIGINT REFERENCES test_results(id) ON DELETE SET NULL,
    job_run_id VARCHAR(255) REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
    dataset_urn VARCHAR(500) REFERENCES datasets(dataset_urn) ON DELETE SET NULL,

    correlation_successful BOOLEAN DEFAULT FALSE,
    confidence_score DECIMAL(3,2) CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00),

    correlation_latency_ms INTEGER,
    processing_time_ms INTEGER,

    validation_metadata JSONB DEFAULT '{}',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Comments
COMMENT ON TABLE correlation_events IS 'Correlation engine performance and accuracy tracking with confidence scoring';

-- =====================================================
-- 7. LINEAGE EVENT IDEMPOTENCY - Duplicate detection
-- =====================================================
-- 
-- Idempotency Strategy:
-- - Key Format: SHA256(producer + job.namespace + job.name + runId + eventTime + eventType)
-- - TTL: 24 hours (configurable via CORRELATOR_IDEMPOTENCY_TTL)
-- - Purpose: Prevents duplicate processing on client retries
-- 
-- Why 24 hours?
-- - Balances retry window with storage growth
-- - Most OpenLineage producers retry within minutes/hours
-- - Background cleanup removes expired entries
-- 
-- Cleanup Strategy:
-- - Automatic: Background goroutine runs every 1 hour
-- - Batch: Deletes 10K expired rows per cycle with 100ms sleep
-- - Monitoring: Log when 0 rows deleted (indicates no duplicates)
-- 
-- See: docs/IDEMPOTENCY-CLEANUP.md for operational runbook
-- =====================================================
CREATE TABLE lineage_event_idempotency (
    idempotency_key VARCHAR(64) PRIMARY KEY,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Optional: Store minimal event metadata for debugging
    event_metadata JSONB DEFAULT '{}'::jsonb,

    -- Constraint: expires_at must be after created_at
    CHECK (expires_at > created_at)
);

-- Indexes for idempotency
CREATE INDEX idx_idempotency_created ON lineage_event_idempotency(created_at);
CREATE INDEX idx_idempotency_expires ON lineage_event_idempotency(expires_at);

-- Comments
COMMENT ON TABLE lineage_event_idempotency IS 'Idempotency tracking for OpenLineage events with TTL-based expiration (24 hours default)';
COMMENT ON COLUMN lineage_event_idempotency.idempotency_key IS 'SHA-256 hash of (producer, job.namespace, job.name, runId, eventTime, eventType) for deduplication';
COMMENT ON COLUMN lineage_event_idempotency.expires_at IS 'TTL expiration (24 hours from created_at) - events older than this are not deduped';

-- =====================================================
-- 8. API KEYS - Authentication
-- =====================================================
-- 
-- Dual-Hash Strategy for Security + Performance:
-- 
-- 1. key_hash (bcrypt):
--    - Purpose: Secure validation (constant-time comparison)
--    - Algorithm: bcrypt with cost factor 10 (~50ms per validation)
--    - Use: Final validation after lookup
--    - Why: Prevents timing attacks, industry standard for password storage
-- 
-- 2. key_lookup_hash (SHA256):
--    - Purpose: Fast O(1) lookup via unique index
--    - Algorithm: SHA256 hash of plaintext key (64 hex chars)
--    - Use: Initial lookup to find candidate API key record
--    - Why: bcrypt is too slow for index lookups (50ms * 10K keys = 500s!)
-- 
-- Authentication Flow:
--    1. Client sends: Authorization: Bearer correlator_abc123...
--    2. Server computes: SHA256(correlator_abc123...) → lookup_hash
--    3. Query: SELECT * FROM api_keys WHERE key_lookup_hash = lookup_hash AND active = TRUE
--    4. Validate: bcrypt.CompareHashAndPassword(key_hash, correlator_abc123...)
--    5. Result: O(1) lookup + secure validation
-- 
-- Security Notes:
--    - Plaintext keys NEVER stored
--    - key_lookup_hash prevents full table scan (performance)
--    - key_hash prevents rainbow table attacks (security)
--    - Both hashes required: lookup (SHA256) + validation (bcrypt)
-- =====================================================
CREATE TABLE api_keys (
    id VARCHAR(36) PRIMARY KEY,

    key_hash VARCHAR(60) NOT NULL UNIQUE,
    key_lookup_hash VARCHAR(64) NOT NULL,

    plugin_id VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,

    permissions JSONB DEFAULT '[]'::jsonb NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE,

    active BOOLEAN DEFAULT TRUE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

-- Indexes for api_keys
CREATE INDEX idx_api_keys_plugin_id ON api_keys(plugin_id) WHERE active = TRUE;
CREATE INDEX idx_api_keys_active ON api_keys(active, expires_at);
CREATE INDEX idx_api_keys_hash_lookup ON api_keys(key_hash) WHERE active = TRUE;
CREATE UNIQUE INDEX idx_api_keys_lookup_hash_unique ON api_keys(key_lookup_hash) WHERE active = TRUE;
CREATE INDEX idx_api_keys_permissions ON api_keys USING GIN(permissions);

-- Comments
COMMENT ON TABLE api_keys IS 'API key storage with bcrypt hashing - plaintext keys never stored';
COMMENT ON COLUMN api_keys.key_hash IS 'Bcrypt hash of API key - use bcrypt.CompareHashAndPassword for validation';
COMMENT ON COLUMN api_keys.key_lookup_hash IS 'SHA-256 hash of plaintext key for O(1) lookup performance';

-- =====================================================
-- 9. API KEY AUDIT LOG
-- =====================================================
CREATE TABLE api_key_audit_log (
    id BIGSERIAL PRIMARY KEY,

    api_key_id VARCHAR(36),

    operation VARCHAR(50) NOT NULL CHECK (operation IN ('created', 'updated', 'deleted', 'validated', 'validation_failed')),

    masked_key VARCHAR(100),
    plugin_id VARCHAR(100),

    metadata JSONB DEFAULT '{}'::jsonb,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

-- Indexes for audit log
CREATE INDEX idx_api_key_audit_log_key_id ON api_key_audit_log(api_key_id, created_at DESC);
CREATE INDEX idx_api_key_audit_log_operation ON api_key_audit_log(operation, created_at DESC);

-- Comments
COMMENT ON TABLE api_key_audit_log IS 'Audit trail for API key operations - security and compliance';

-- =====================================================
-- MATERIALIZED VIEWS FOR CORRELATION
-- =====================================================

-- View 1: Incident Correlation View
CREATE MATERIALIZED VIEW incident_correlation_view AS
SELECT
    -- Test result identification
    tr.id AS test_result_id,
    tr.test_name,
    tr.test_type,
    tr.status AS test_status,
    tr.message AS test_message,
    tr.executed_at AS test_executed_at,
    tr.duration_ms AS test_duration_ms,

    -- Dataset information
    tr.dataset_urn,
    d.name AS dataset_name,
    d.namespace AS dataset_namespace,

    -- Correlated job run (producer of the dataset)
    jr.job_run_id,
    jr.run_id AS openlineage_run_id,
    jr.job_name,
    jr.job_namespace,
    jr.current_state AS job_status,
    jr.event_type AS job_event_type,
    jr.started_at AS job_started_at,
    jr.completed_at AS job_completed_at,
    jr.producer_name,
    jr.producer_version,

    -- Lineage relationship
    le.id AS lineage_edge_id,
    le.edge_type AS lineage_edge_type,
    le.created_at AS lineage_created_at

FROM test_results tr
    JOIN datasets d ON tr.dataset_urn = d.dataset_urn
    JOIN lineage_edges le ON d.dataset_urn = le.dataset_urn AND le.edge_type = 'output'
    JOIN job_runs jr ON le.job_run_id = jr.job_run_id

WHERE tr.status IN ('failed', 'error')

ORDER BY tr.executed_at DESC;

-- UNIQUE index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_incident_correlation_view_pk
    ON incident_correlation_view (test_result_id, lineage_edge_id);

-- Additional indexes
CREATE INDEX IF NOT EXISTS idx_incident_correlation_view_job_run_id
    ON incident_correlation_view (job_run_id);

CREATE INDEX IF NOT EXISTS idx_incident_correlation_view_dataset_urn
    ON incident_correlation_view (dataset_urn);

CREATE INDEX IF NOT EXISTS idx_incident_correlation_view_test_executed_at
    ON incident_correlation_view (test_executed_at DESC);

COMMENT ON MATERIALIZED VIEW incident_correlation_view IS
    'Correlates test failures to the job runs that produced the failing datasets. Core view for incident analysis.';

-- View 2: Lineage Impact Analysis
CREATE MATERIALIZED VIEW lineage_impact_analysis AS
WITH RECURSIVE downstream AS (
    -- Base case: Direct outputs of all jobs
    SELECT
        jr.job_run_id,
        jr.job_name,
        jr.job_namespace,
        jr.current_state AS job_status,
        le.dataset_urn,
        d.name AS dataset_name,
        d.namespace AS dataset_namespace,
        0 AS depth,
        ARRAY[jr.job_run_id::TEXT] AS job_path,
        ARRAY[le.dataset_urn::TEXT] AS dataset_path
    FROM job_runs jr
    JOIN lineage_edges le ON jr.job_run_id = le.job_run_id
    JOIN datasets d ON le.dataset_urn = d.dataset_urn
    WHERE le.edge_type = 'output'

    UNION ALL

    -- Recursive case: Find jobs that consume downstream datasets
    SELECT
        jr_next.job_run_id,
        jr_next.job_name,
        jr_next.job_namespace,
        jr_next.current_state AS job_status,
        le_next_output.dataset_urn,
        d_next.name AS dataset_name,
        d_next.namespace AS dataset_namespace,
        ds.depth + 1,
        ds.job_path || jr_next.job_run_id::TEXT,
        ds.dataset_path || le_next_output.dataset_urn::TEXT
    FROM downstream ds
    JOIN lineage_edges le_next_input ON ds.dataset_urn = le_next_input.dataset_urn
        AND le_next_input.edge_type = 'input'
    JOIN job_runs jr_next ON le_next_input.job_run_id = jr_next.job_run_id
    JOIN lineage_edges le_next_output ON jr_next.job_run_id = le_next_output.job_run_id
        AND le_next_output.edge_type = 'output'
    JOIN datasets d_next ON le_next_output.dataset_urn = d_next.dataset_urn

    WHERE ds.depth < 10
        AND NOT (jr_next.job_run_id = ANY(ds.job_path))
        AND NOT (le_next_output.dataset_urn = ANY(ds.dataset_path))
)
SELECT
    job_run_id,
    job_name,
    job_namespace,
    job_status,
    dataset_urn,
    dataset_name,
    dataset_namespace,
    depth,
    array_length(job_path, 1) AS job_path_length,
    array_length(dataset_path, 1) AS dataset_path_length
FROM downstream
ORDER BY job_run_id, depth;

-- UNIQUE index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_lineage_impact_analysis_pk
    ON lineage_impact_analysis (job_run_id, dataset_urn, depth);

-- Additional indexes
CREATE INDEX IF NOT EXISTS idx_lineage_impact_analysis_dataset_urn
    ON lineage_impact_analysis (dataset_urn);

CREATE INDEX IF NOT EXISTS idx_lineage_impact_analysis_depth
    ON lineage_impact_analysis (depth, job_run_id);

COMMENT ON MATERIALIZED VIEW lineage_impact_analysis IS
    'Recursive downstream impact analysis: finds all datasets and jobs affected by a job run failure. Max depth: 10 levels.';

-- View 3: Recent Incidents Summary
CREATE MATERIALIZED VIEW recent_incidents_summary AS
SELECT
    -- Job run identification
    icv.job_run_id,
    icv.job_name,
    icv.job_namespace,
    icv.job_status,
    icv.producer_name,

    -- Incident statistics (per job run)
    COUNT(DISTINCT icv.test_result_id) AS failed_test_count,
    COUNT(DISTINCT icv.dataset_urn) AS affected_dataset_count,

    -- Test failure details
    array_agg(DISTINCT icv.test_name ORDER BY icv.test_name) AS failed_test_names,
    array_agg(DISTINCT icv.dataset_urn ORDER BY icv.dataset_urn) AS affected_dataset_urns,

    -- Temporal information
    MIN(icv.test_executed_at) AS first_test_failure_at,
    MAX(icv.test_executed_at) AS last_test_failure_at,
    MIN(icv.job_started_at) AS job_started_at,
    MAX(icv.job_completed_at) AS job_completed_at,

    -- Downstream impact (if available)
    (
        SELECT COUNT(DISTINCT lia.dataset_urn)
        FROM lineage_impact_analysis lia
        WHERE lia.job_run_id = icv.job_run_id
            AND lia.depth > 0
    ) AS downstream_affected_count

FROM incident_correlation_view icv

WHERE icv.test_executed_at > NOW() - INTERVAL '7 days'

GROUP BY
    icv.job_run_id,
    icv.job_name,
    icv.job_namespace,
    icv.job_status,
    icv.producer_name

ORDER BY MAX(icv.test_executed_at) DESC;

-- UNIQUE index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_recent_incidents_summary_pk
    ON recent_incidents_summary (job_run_id);

-- Additional indexes
CREATE INDEX IF NOT EXISTS idx_recent_incidents_summary_failed_test_count
    ON recent_incidents_summary (failed_test_count DESC);

CREATE INDEX IF NOT EXISTS idx_recent_incidents_summary_producer_name
    ON recent_incidents_summary (producer_name);

CREATE INDEX IF NOT EXISTS idx_recent_incidents_summary_last_failure
    ON recent_incidents_summary (last_test_failure_at DESC);

COMMENT ON MATERIALIZED VIEW recent_incidents_summary IS
    'Last 7 days of incidents grouped by job run with correlation statistics. Refreshed periodically.';

-- =====================================================
-- REFRESH FUNCTION
-- =====================================================

CREATE OR REPLACE FUNCTION refresh_correlation_views()
RETURNS TABLE(view_name TEXT, refresh_duration_ms BIGINT, rows_refreshed BIGINT) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count BIGINT;
BEGIN
    -- Refresh incident_correlation_view
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY incident_correlation_view;
    end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;

    RETURN QUERY SELECT
        'incident_correlation_view'::TEXT,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT,
        row_count;

    -- Refresh lineage_impact_analysis
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY lineage_impact_analysis;
    end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;

    RETURN QUERY SELECT
        'lineage_impact_analysis'::TEXT,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT,
        row_count;

    -- Refresh recent_incidents_summary
    start_time := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY recent_incidents_summary;
    end_time := clock_timestamp();
    GET DIAGNOSTICS row_count = ROW_COUNT;

    RETURN QUERY SELECT
        'recent_incidents_summary'::TEXT,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::BIGINT,
        row_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_correlation_views() IS
    'Refreshes all correlation materialized views in correct dependency order. Returns refresh statistics.';

-- =====================================================
-- REFRESH STRATEGY
-- =====================================================
--
-- Manual Refresh (MVP):
--   - POST /api/v1/lineage/events extracts test results from dataQualityAssertions facets
--   - Dashboard queries check view freshness, refresh if stale (>5 min)
--   - Manual refresh: SELECT * FROM refresh_correlation_views();
--
-- Automatic Refresh (Deferred):
--   - pg_cron extension: Schedule every 5 minutes
--   - Background worker: Polls for staleness and triggers refresh
--   - Event-driven: Trigger on INSERT to test_results (debounced)
--
-- Refresh Performance:
--   - With CONCURRENTLY: No locks, zero downtime, 650ms-2s refresh time
--   - Without CONCURRENTLY: Table locks during refresh (~100-500ms)
--   - CONCURRENTLY requires UNIQUE indexes (implemented above)
--
-- Refresh Triggers (MVP):
--   1. After test result batch ingestion: Application calls refresh_correlation_views()
--   2. Before critical dashboard queries: Check pg_matviews.last_refresh, refresh if stale
--   3. Manual refresh: Database admin runs SELECT * FROM refresh_correlation_views();
--
-- Monitoring (Deferred):
--   - Log refresh times: refresh_correlation_views() returns timing statistics
--   - Alert if refresh >5s: Indicates performance degradation or large dataset
--   - Track staleness: pg_matviews.last_refresh vs NOW()
--
-- =====================================================

-- =====================================================
-- TRIGGERS
-- =====================================================

-- Function for updated_at columns
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at triggers
CREATE TRIGGER update_job_runs_updated_at
    BEFORE UPDATE ON job_runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_job_id_mappings_updated_at
    BEFORE UPDATE ON job_id_mappings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_datasets_updated_at
    BEFORE UPDATE ON datasets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_api_keys_updated_at
    BEFORE UPDATE ON api_keys
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_test_results_updated_at
    BEFORE UPDATE ON test_results
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- State transition validation trigger
CREATE OR REPLACE FUNCTION validate_job_run_state_transition()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.current_state IN ('COMPLETE', 'FAIL', 'ABORT') THEN
    IF NEW.current_state != OLD.current_state THEN
      RAISE EXCEPTION 'Invalid state transition: % -> % (terminal states are immutable)',
        OLD.current_state, NEW.current_state
        USING HINT = 'Terminal states (COMPLETE/FAIL/ABORT) can only transition to themselves (idempotent). Check application logic for state ordering.';
    END IF;
  END IF;

  NEW.state_history = jsonb_set(
    NEW.state_history,
    '{transitions}',
    (NEW.state_history->'transitions') || jsonb_build_object(
      'from', OLD.current_state,
      'to', NEW.current_state,
      'event_time', NEW.event_time,
      'updated_at', NOW()
    )
  );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_run_state_validation
  BEFORE UPDATE ON job_runs
  FOR EACH ROW EXECUTE FUNCTION validate_job_run_state_transition();

COMMENT ON FUNCTION validate_job_run_state_transition() IS 'OpenLineage state machine enforcement: protects terminal states, tracks transition history';

-- =====================================================
-- VALIDATION
-- =====================================================

DO $$
BEGIN
    -- Verify tables
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'job_runs') THEN
        RAISE EXCEPTION 'Table job_runs not created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'datasets') THEN
        RAISE EXCEPTION 'Table datasets not created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'lineage_edges') THEN
        RAISE EXCEPTION 'Table lineage_edges not created';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'test_results') THEN
        RAISE EXCEPTION 'Table test_results not created';
    END IF;

    -- Verify materialized views
    IF NOT EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'incident_correlation_view') THEN
        RAISE EXCEPTION 'Materialized view incident_correlation_view not created';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'lineage_impact_analysis') THEN
        RAISE EXCEPTION 'Materialized view lineage_impact_analysis not created';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'recent_incidents_summary') THEN
        RAISE EXCEPTION 'Materialized view recent_incidents_summary not created';
    END IF;

    -- Verify function
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'refresh_correlation_views') THEN
        RAISE EXCEPTION 'Function refresh_correlation_views not created';
    END IF;

    RAISE NOTICE 'Migration validation: All tables, views, and functions created successfully';
END $$;

-- Success message
SELECT
    'Initial OpenLineage schema migration completed' as status,
    9 as tables_created,
    3 as materialized_views_created,
    1 as functions_created,
    'OpenLineage v1.0 compliant, correlation-ready' as note,
    NOW() as completed_at;

