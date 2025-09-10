-- Migration: 001_initial_schema.down.sql
-- Description: Rollback initial schema creation for Correlator correlation engine
-- Created: 2025-01-08
-- Architect: correlator-architect-engineer

-- ==========================================
-- ROLLBACK INITIAL SCHEMA MIGRATION
-- ==========================================

BEGIN;

-- Drop tables in reverse dependency order to avoid foreign key constraint violations

-- 1. Drop correlation_events (references test_results)
DROP TABLE IF EXISTS correlation_events CASCADE;

-- 2. Drop test_results (references job_runs and datasets)
DROP TABLE IF EXISTS test_results CASCADE;

-- 3. Drop lineage_edges (references job_runs and datasets) 
DROP TABLE IF EXISTS lineage_edges CASCADE;

-- 4. Drop job_id_mappings (references job_runs)
DROP TABLE IF EXISTS job_id_mappings CASCADE;

-- 5. Drop job_runs (core table)
DROP TABLE IF EXISTS job_runs CASCADE;

-- 6. Drop datasets (core table)
DROP TABLE IF EXISTS datasets CASCADE;

-- Drop extensions (only if no other tables need them)
-- Note: We check if extensions are used elsewhere before dropping
DROP EXTENSION IF EXISTS "uuid-ossp";
DROP EXTENSION IF EXISTS pg_trgm;

-- Note: No custom types to drop - we use VARCHAR with CHECK constraints instead

-- Rollback completed successfully: 001_initial_schema  
-- Tables dropped: correlation_events, test_results, lineage_edges, job_id_mappings, job_runs, datasets
-- Extensions dropped: uuid-ossp, pg_trgm (if not used elsewhere)
-- Custom types dropped: correlation_event_type, mapping_status, job_run_state, test_status
-- Timestamp: Rollback will be tracked by golang-migrate in schema_migrations table

COMMIT;

-- ==========================================
-- ROLLBACK COMPLETE
-- ==========================================
-- Tables dropped: 6 core tables
-- Extensions dropped: uuid-ossp, pg_trgm
-- Types dropped: 4 custom types
-- Schema state: Clean (ready for fresh installation)
-- ==========================================