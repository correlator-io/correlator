-- =====================================================
-- Rollback: Initial OpenLineage Schema
-- Drops all tables, views, functions, and extensions
-- =====================================================

BEGIN;

-- Drop materialized views (they depend on tables)
DROP MATERIALIZED VIEW IF EXISTS recent_incidents_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS lineage_impact_analysis CASCADE;
DROP MATERIALIZED VIEW IF EXISTS incident_correlation_view CASCADE;

-- Drop functions
DROP FUNCTION IF EXISTS refresh_correlation_views() CASCADE;
DROP FUNCTION IF EXISTS validate_job_run_state_transition() CASCADE;
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS test_results CASCADE;
DROP TABLE IF EXISTS lineage_event_idempotency CASCADE;
DROP TABLE IF EXISTS lineage_edges CASCADE;
DROP TABLE IF EXISTS job_runs CASCADE;
DROP TABLE IF EXISTS datasets CASCADE;
DROP TABLE IF EXISTS api_key_audit_log CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;

-- Drop extensions (only if not used elsewhere)
DROP EXTENSION IF EXISTS "uuid-ossp";
DROP EXTENSION IF EXISTS pg_trgm;

COMMIT;

-- Success message
SELECT
    'Initial schema rollback completed' as status,
    7 as tables_dropped,
    3 as materialized_views_dropped,
    3 as functions_dropped,
    'Database clean - ready for fresh installation' as note,
    NOW() as completed_at;

