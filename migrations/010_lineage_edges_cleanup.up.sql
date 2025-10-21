-- =====================================================
-- Migration 010: Drop Deprecated lineage_edges Columns (Contract Phase)
-- Removes old input_dataset_urn and output_dataset_urn columns
-- Pattern: Expand-Migrate-Contract (this is CONTRACT phase)
-- =====================================================

-- =====================================================
-- STEP 1: Drop Materialized Views (depend on old columns)
-- These will be recreated in migration 011 with OpenLineage schema
-- =====================================================

-- Drop materialized views in reverse dependency order
DROP MATERIALIZED VIEW IF EXISTS recent_incidents_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS correlation_accuracy_metrics CASCADE;
DROP MATERIALIZED VIEW IF EXISTS lineage_impact_analysis CASCADE;
DROP MATERIALIZED VIEW IF EXISTS incident_correlation_view CASCADE;

-- Drop performance monitoring views
DROP VIEW IF EXISTS correlation_sla_monitor CASCADE;
DROP VIEW IF EXISTS correlation_latency_monitor CASCADE;
DROP VIEW IF EXISTS index_performance_monitor CASCADE;
DROP VIEW IF EXISTS materialized_view_stats CASCADE;

-- Drop refresh function (depends on views)
DROP FUNCTION IF EXISTS refresh_correlation_views();

-- =====================================================
-- STEP 2: Drop Old Indexes (reference deprecated columns)
-- =====================================================

-- Drop old indexes for deprecated columns
DROP INDEX IF EXISTS idx_lineage_edges_input_dataset;
DROP INDEX IF EXISTS idx_lineage_edges_output_dataset;
DROP INDEX IF EXISTS idx_lineage_edges_downstream_traversal;
DROP INDEX IF EXISTS idx_lineage_edges_upstream_traversal;

-- Drop indexes that reference renamed 'status' column
DROP INDEX IF EXISTS idx_job_runs_status_temporal;

-- Drop old FK constraints
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_input_dataset_urn_fkey;
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_output_dataset_urn_fkey;

-- Drop the old CHECK constraint (at least one dataset required)
-- This is replaced by NOT NULL on dataset_urn
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_check;

-- Drop deprecated columns
ALTER TABLE lineage_edges DROP COLUMN IF EXISTS input_dataset_urn;
ALTER TABLE lineage_edges DROP COLUMN IF EXISTS output_dataset_urn;

-- Make new columns NOT NULL now that old columns are gone
-- This enforces data integrity for OpenLineage schema
ALTER TABLE lineage_edges ALTER COLUMN edge_type SET NOT NULL;
ALTER TABLE lineage_edges ALTER COLUMN dataset_urn SET NOT NULL;

-- Update table comment to reflect completed migration
COMMENT ON TABLE lineage_edges IS 'OpenLineage lineage edges: separate rows for each input and output dataset per job run (migration complete)';

-- Success message
SELECT
    'Migration 010 completed: deprecated lineage_edges columns dropped (CONTRACT phase)' as status,
    4 as materialized_views_dropped,
    4 as monitoring_views_dropped,
    2 as columns_dropped,
    7 as indexes_dropped,
    3 as constraints_dropped,
    2 as columns_set_not_null,
    'OpenLineage schema migration complete - views will be recreated in migration 011' as note,
    NOW() as completed_at;

