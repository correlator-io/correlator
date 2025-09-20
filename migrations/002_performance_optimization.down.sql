-- Migration: 002_performance_optimization.down.sql
-- Description: Rollback performance optimization indexes and materialized views
-- Created: 2025-01-08
-- Architect: correlator-architect-engineer

-- ==========================================
-- ROLLBACK PERFORMANCE OPTIMIZATION
-- ==========================================

BEGIN;

-- Drop materialized views first (they depend on tables and indexes)

-- 1. Drop correlation accuracy metrics view
DROP MATERIALIZED VIEW IF EXISTS correlation_accuracy_metrics CASCADE;

-- 2. Drop recent incidents summary view  
DROP MATERIALIZED VIEW IF EXISTS recent_incidents_summary CASCADE;

-- 3. Drop lineage impact analysis view
DROP MATERIALIZED VIEW IF EXISTS lineage_impact_analysis CASCADE;

-- 4. Drop primary incident correlation view
DROP MATERIALIZED VIEW IF EXISTS incident_correlation_view CASCADE;

-- Drop monitoring views
DROP VIEW IF EXISTS correlation_sla_monitor CASCADE;
DROP VIEW IF EXISTS correlation_latency_monitor CASCADE;
DROP VIEW IF EXISTS index_performance_monitor CASCADE;
DROP VIEW IF EXISTS materialized_view_stats CASCADE;

-- Drop performance functions
DROP FUNCTION IF EXISTS refresh_correlation_views() CASCADE;

-- Drop system_logs table (created in up migration)
DROP TABLE IF EXISTS system_logs CASCADE;

-- Drop performance optimization indexes (in reverse order of creation)

-- 10. Composite query optimization indexes
DROP INDEX IF EXISTS idx_correlation_events_composite_analysis;
DROP INDEX IF EXISTS idx_job_runs_producer_status_time;

-- 9. Accuracy monitoring indexes  
DROP INDEX IF EXISTS idx_correlation_events_accuracy_tracking;

-- 8. Job status tracking indexes
DROP INDEX IF EXISTS idx_job_runs_status_timeline;

-- 7. Composite correlation indexes
DROP INDEX IF EXISTS idx_test_dataset_job_composite;

-- 6. Dataset correlation indexes
DROP INDEX IF EXISTS idx_test_results_dataset_failures;

-- 5. Upstream impact indexes
DROP INDEX IF EXISTS idx_lineage_edges_upstream_traversal;

-- 4. Downstream impact indexes
DROP INDEX IF EXISTS idx_lineage_edges_downstream_traversal;

-- 3. Temporal correlation indexes
DROP INDEX IF EXISTS idx_test_results_recent_failures;

-- 2. Canonical ID mapping indexes
DROP INDEX IF EXISTS idx_job_runs_canonical_id;

-- 1. Primary correlation lookup indexes
DROP INDEX IF EXISTS idx_test_results_correlation_lookup;

-- Rollback completed successfully: 002_performance_optimization
-- Indexes dropped: 10 performance optimization indexes  
-- Views dropped: 4 materialized views + 4 monitoring views
-- Functions dropped: refresh_correlation_views()
-- Performance state: Basic (only foreign key indexes remain)
-- Timestamp: Rollback will be tracked by golang-migrate in schema_migrations table

COMMIT;

-- ==========================================
-- PERFORMANCE ROLLBACK COMPLETE
-- ==========================================
-- Indexes dropped: 10 performance optimization indexes
-- Views dropped: 4 materialized views + 4 monitoring views  
-- Functions dropped: 1 refresh function
-- Performance state: Basic (only foreign key indexes remain)
-- ==========================================