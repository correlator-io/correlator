-- PostgreSQL initialization script for Correlator Demo
-- Creates multiple schemas for logical separation within a single database

-- Create extensions required for correlation workloads
CREATE EXTENSION IF NOT EXISTS "pg_trgm";           -- Trigram matching for canonical ID fuzzy search
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements"; -- Query performance monitoring

-- =============================================================================
-- Schema: airflow (Airflow metadata)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS airflow;
COMMENT ON SCHEMA airflow IS 'Airflow metadata tables';

-- =============================================================================
-- Schema: correlator (Correlator lineage data)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS correlator;
COMMENT ON SCHEMA correlator IS 'Correlator lineage and correlation data';

-- =============================================================================
-- Permissions
-- =============================================================================
-- Grant permissions on all schemas to the correlator user
GRANT ALL PRIVILEGES ON SCHEMA public TO correlator;
GRANT ALL PRIVILEGES ON SCHEMA airflow TO correlator;
GRANT ALL PRIVILEGES ON SCHEMA correlator TO correlator;

-- Grant default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO correlator;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow GRANT ALL ON TABLES TO correlator;
ALTER DEFAULT PRIVILEGES IN SCHEMA correlator GRANT ALL ON TABLES TO correlator;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO correlator;
ALTER DEFAULT PRIVILEGES IN SCHEMA airflow GRANT ALL ON SEQUENCES TO correlator;
ALTER DEFAULT PRIVILEGES IN SCHEMA correlator GRANT ALL ON SEQUENCES TO correlator;

-- =============================================================================
-- Logging
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Correlator Demo PostgreSQL initialization completed';
    RAISE NOTICE 'Schemas created: public (demo data), airflow (metadata), correlator (lineage)';
    RAISE NOTICE 'Extensions enabled: pg_trgm, pg_stat_statements';
END
$$;
