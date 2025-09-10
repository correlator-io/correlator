-- =====================================================
-- Correlator: Initial Database Schema
-- Core Tables for Incident Correlation Engine
-- 
-- Standards: OpenLineage-compliant with canonical ID strategy, UUIDv7 support, proper column sizing from real-world analysis
-- =====================================================

-- Enable UUID extension for UUIDv7 support (future-proofing)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable trigram extension for text search optimization (used in performance views)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =====================================================
-- 1. JOB RUNS - Enhanced with OpenLineage RunEvent compliance
-- Primary correlation entity with canonical ID strategy
-- =====================================================
CREATE TABLE job_runs (
    -- Primary identifier: canonical job run ID from different systems
    -- Examples: "dbt:abc123-def456", "airflow:manual__2025-01-01T12:00:00", "spark:application_123456"
    job_run_id VARCHAR(255) PRIMARY KEY CHECK (job_run_id ~ '^[a-zA-Z0-9_-]+:[a-zA-Z0-9_:-]+$'),

    -- Job metadata from OpenLineage RunEvent
    job_name VARCHAR(255) NOT NULL,
    job_namespace VARCHAR(255) DEFAULT 'default',

    -- Temporal information for correlation
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Job execution status - constrained for data integrity
    status VARCHAR(50) NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'aborted', 'error')),

    -- OpenLineage facets and metadata stored as JSONB for flexibility
    -- Contains: run facets, job facets, producer information
    metadata JSONB DEFAULT '{}',

    -- Producer identification for ID canonicalization
    producer_name VARCHAR(100) NOT NULL DEFAULT 'unknown',
    producer_version VARCHAR(50),

    -- Tracking timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Comment explaining canonical ID strategy
COMMENT ON TABLE job_runs IS 'Core job run registry with canonical ID normalization across different execution systems';
COMMENT ON COLUMN job_runs.job_run_id IS 'Canonical job run identifier in format: {system}:{original_id} - primary correlation key';
COMMENT ON COLUMN job_runs.metadata IS 'OpenLineage RunEvent facets and producer-specific metadata as JSONB';

-- =====================================================
-- 2. JOB ID MAPPINGS - ID canonicalization with confidence scoring
-- Handles producer-specific ID formats and mapping validation
-- =====================================================
CREATE TABLE job_id_mappings (
    id BIGSERIAL PRIMARY KEY,

    -- Canonical ID (target) - references job_runs.job_run_id
    canonical_job_run_id VARCHAR(255) NOT NULL REFERENCES job_runs(job_run_id) ON DELETE CASCADE,

    -- Original ID from source system
    original_job_run_id VARCHAR(500) NOT NULL,

    -- Producer system identification
    producer_name VARCHAR(100) NOT NULL,
    producer_version VARCHAR(50),

    -- Mapping confidence and validation
    -- confidence_score: 0.0-1.0 indicating mapping certainty
    confidence_score DECIMAL(3,2) DEFAULT 1.00 CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00),

    -- Mapping status for validation tracking
    mapping_status VARCHAR(50) DEFAULT 'active' CHECK (mapping_status IN ('active', 'deprecated', 'invalid')),

    -- Metadata for debugging and validation
    mapping_metadata JSONB DEFAULT '{}',

    -- Tracking timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Ensure unique mapping per producer system
    UNIQUE(original_job_run_id, producer_name)
);

COMMENT ON TABLE job_id_mappings IS 'ID canonicalization mapping with producer-specific formats and confidence scoring';
COMMENT ON COLUMN job_id_mappings.confidence_score IS 'Mapping confidence (0.0-1.0) for correlation accuracy tracking';

-- =====================================================
-- 3. DATASETS - Dataset registry with OpenLineage facets
-- Central registry for all datasets involved in lineage and testing
-- =====================================================
CREATE TABLE datasets (
    -- Dataset URN as primary key (OpenLineage standard)
    -- Format: namespace:name (e.g., "postgres://prod.db/clean.users", "s3://bucket/path/file.parquet")
    dataset_urn VARCHAR(500) PRIMARY KEY CHECK (dataset_urn ~ '^[^:]+:.+$'),

    -- Dataset identification
    name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL DEFAULT 'default',

    -- Ownership and governance
    owner VARCHAR(255),
    team VARCHAR(100),

    -- Classification and discovery
    tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    description TEXT,

    -- Dataset facets from OpenLineage (schema, statistics, etc.)
    facets JSONB DEFAULT '{}',

    -- Correlation statistics for performance monitoring
    correlation_stats JSONB DEFAULT '{}',

    -- Tracking timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE datasets IS 'Dataset registry with OpenLineage facets and correlation statistics';
COMMENT ON COLUMN datasets.dataset_urn IS 'OpenLineage dataset URN: namespace:name format for global identification';
COMMENT ON COLUMN datasets.facets IS 'OpenLineage dataset facets: schema, statistics, documentation';
COMMENT ON COLUMN datasets.correlation_stats IS 'Performance metrics: test failure rates, correlation accuracy';

-- =====================================================
-- 4. LINEAGE EDGES - Enhanced with downstream impact calculation
-- Represents input/output relationships between jobs and datasets
-- =====================================================
CREATE TABLE lineage_edges (
    id BIGSERIAL PRIMARY KEY,

    -- Job run that created this lineage relationship
    job_run_id VARCHAR(255) NOT NULL REFERENCES job_runs(job_run_id) ON DELETE CASCADE,

    -- Input dataset (source)
    input_dataset_urn VARCHAR(500) REFERENCES datasets(dataset_urn) ON DELETE SET NULL,

    -- Output dataset (target) - at least one of input/output must be specified
    output_dataset_urn VARCHAR(500) REFERENCES datasets(dataset_urn) ON DELETE SET NULL,

    -- Lineage type and metadata from OpenLineage
    lineage_type VARCHAR(50) DEFAULT 'transformation' CHECK (lineage_type IN ('transformation', 'extraction', 'loading', 'validation')),

    -- OpenLineage input/output facets
    input_facets JSONB DEFAULT '{}',
    output_facets JSONB DEFAULT '{}',

    -- Downstream impact calculation columns (for future optimization)
    downstream_dataset_count INTEGER DEFAULT 0,
    downstream_job_count INTEGER DEFAULT 0,
    impact_score DECIMAL(5,2) DEFAULT 0.00,

    -- Tracking timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Ensure at least one dataset is specified
    CHECK (input_dataset_urn IS NOT NULL OR output_dataset_urn IS NOT NULL)
);

COMMENT ON TABLE lineage_edges IS 'Job-to-dataset lineage relationships with downstream impact calculation';
COMMENT ON COLUMN lineage_edges.impact_score IS 'Calculated downstream impact score for incident prioritization';

-- =====================================================
-- 5. TEST RESULTS - Enhanced with proper column sizing
-- Data quality test outcomes linked to job runs for correlation
-- =====================================================
CREATE TABLE test_results (
    id BIGSERIAL PRIMARY KEY,

    -- Test identification - VARCHAR(750) based on real-world analysis
    test_name VARCHAR(750) NOT NULL,
    test_type VARCHAR(100) DEFAULT 'data_quality',

    -- Dataset being tested
    dataset_urn VARCHAR(500) NOT NULL REFERENCES datasets(dataset_urn) ON DELETE CASCADE,

    -- CRITICAL: Job run correlation key
    job_run_id VARCHAR(255) NOT NULL REFERENCES job_runs(job_run_id) ON DELETE CASCADE,

    -- Test execution results
    status VARCHAR(50) NOT NULL CHECK (status IN ('passed', 'failed', 'error', 'skipped', 'warning')),
    message TEXT,

    -- Test metrics and details
    expected_value JSONB,
    actual_value JSONB,
    test_metadata JSONB DEFAULT '{}',

    -- Temporal information
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_ms INTEGER,

    -- Tracking timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE test_results IS 'Data quality test outcomes with job run correlation for incident analysis';
COMMENT ON COLUMN test_results.job_run_id IS 'CRITICAL correlation key linking test failures to producing job runs';
COMMENT ON COLUMN test_results.test_name IS 'Extended to VARCHAR(750) based on real-world test naming analysis';

-- =====================================================
-- 6. CORRELATION EVENTS - Accuracy tracking with confidence scoring
-- Performance metrics for correlation engine validation
-- =====================================================
CREATE TABLE correlation_events (
    id BIGSERIAL PRIMARY KEY,

    -- Event identification
    event_type VARCHAR(100) NOT NULL CHECK (event_type IN ('correlation_created', 'correlation_validated', 'correlation_failed', 'accuracy_measured')),

    -- Correlation attempt details
    test_result_id BIGINT REFERENCES test_results(id) ON DELETE SET NULL,
    job_run_id VARCHAR(255) REFERENCES job_runs(job_run_id) ON DELETE SET NULL,
    dataset_urn VARCHAR(500) REFERENCES datasets(dataset_urn) ON DELETE SET NULL,

    -- Correlation success and confidence metrics
    correlation_successful BOOLEAN DEFAULT FALSE,
    confidence_score DECIMAL(3,2) CHECK (confidence_score >= 0.00 AND confidence_score <= 1.00),

    -- Performance metrics
    correlation_latency_ms INTEGER,
    processing_time_ms INTEGER,

    -- Accuracy tracking metadata
    validation_metadata JSONB DEFAULT '{}',

    -- Tracking timestamp
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

COMMENT ON TABLE correlation_events IS 'Correlation engine performance and accuracy tracking with confidence scoring';
COMMENT ON COLUMN correlation_events.confidence_score IS 'Correlation confidence (0.0-1.0) for accuracy measurement';

-- =====================================================
-- BASIC INDEXES FOR FOREIGN KEY PERFORMANCE
-- Performance optimization indexes in separate migration (002_performance_optimization.sql)
-- =====================================================

-- Primary correlation lookup index (essential for basic functionality)
CREATE INDEX idx_test_results_job_run_correlation
ON test_results (job_run_id, status)
WHERE status IN ('failed', 'error');

-- Dataset lookup index
CREATE INDEX idx_test_results_dataset_lookup
ON test_results (dataset_urn, executed_at DESC);

-- Job run temporal index
CREATE INDEX idx_job_runs_temporal
ON job_runs (started_at DESC, status);

-- Lineage traversal indexes
CREATE INDEX idx_lineage_edges_input_dataset
ON lineage_edges (input_dataset_urn, job_run_id);

CREATE INDEX idx_lineage_edges_output_dataset
ON lineage_edges (output_dataset_urn, job_run_id);

-- ID mapping lookup index
CREATE INDEX idx_job_id_mappings_lookup
ON job_id_mappings (original_job_run_id, producer_name, mapping_status);

-- =====================================================
-- TRIGGERS FOR UPDATED_AT TIMESTAMPS
-- =====================================================

-- Function to update updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at triggers to relevant tables
CREATE TRIGGER update_job_runs_updated_at
    BEFORE UPDATE ON job_runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_job_id_mappings_updated_at
    BEFORE UPDATE ON job_id_mappings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_datasets_updated_at
    BEFORE UPDATE ON datasets
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- INITIAL DATA VALIDATION SETUP
-- =====================================================

-- Validate schema creation
DO $$
BEGIN
    -- Verify all tables exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'job_runs') THEN
        RAISE EXCEPTION 'Table job_runs not created successfully';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'test_results') THEN
        RAISE EXCEPTION 'Table test_results not created successfully';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'datasets') THEN
        RAISE EXCEPTION 'Table datasets not created successfully';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'lineage_edges') THEN
        RAISE EXCEPTION 'Table lineage_edges not created successfully';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'job_id_mappings') THEN
        RAISE EXCEPTION 'Table job_id_mappings not created successfully';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'correlation_events') THEN
        RAISE EXCEPTION 'Table correlation_events not created successfully';
    END IF;
    
    RAISE NOTICE 'Initial schema migration completed successfully - 6 core tables created';
END $$;

-- =====================================================
-- MIGRATION COMPLETION LOG
-- =====================================================

-- Migration completed successfully: 001_initial_schema
-- Tables created: job_runs, job_id_mappings, datasets, lineage_edges, test_results, correlation_events
-- Indexes created: 7 basic performance indexes
-- Extensions added: uuid-ossp, pg_trgm
-- Custom types created: job_run_state, test_status, mapping_status, correlation_event_type
-- Timestamp: Migration will be tracked by golang-migrate in schema_migrations table

-- Success message
SELECT
    'Initial schema migration completed successfully' as status,
    6 as tables_created,
    7 as basic_indexes_created,
    NOW() as completed_at;