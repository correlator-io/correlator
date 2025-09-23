-- PostgreSQL initialization script for Correlator
-- This script runs automatically when the container starts for the first time

-- Create extensions required for correlation workloads
CREATE EXTENSION IF NOT EXISTS "pg_trgm";      -- Trigram matching for canonical ID fuzzy search
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";  -- Query performance monitoring

-- Set correlation-optimized configuration
-- Note: Most configuration is set via command line in docker-compose.yml
-- These are runtime settings that can be adjusted per session

-- Optimize for correlation query patterns
SET default_statistics_target = 100;  -- Better query planning for JOIN operations
SET constraint_exclusion = partition; -- Optimize constraint checking
-- Note: checkpoint_segments was replaced with min_wal_size/max_wal_size in PostgreSQL 9.5+
-- We configure WAL settings via command line parameters instead

-- Create correlator database and user (if not exists via environment variables)
-- This is defensive - Docker handles this via POSTGRES_DB/POSTGRES_USER env vars
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'correlator') THEN
        CREATE DATABASE correlator;
    END IF;
END
$$;

-- Grant necessary permissions for correlation operations
-- The correlator user needs to create tables, indexes, and materialized views
GRANT ALL PRIVILEGES ON DATABASE correlator TO correlator;

-- Connect to correlator database for remaining operations
\c correlator

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO correlator;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO correlator;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO correlator;

-- Set up performance monitoring
-- Create a view to monitor correlation query performance
-- Note: PostgreSQL 13+ uses total_exec_time and mean_exec_time instead of total_time and mean_time
CREATE OR REPLACE VIEW correlation_query_stats AS
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements
WHERE query LIKE '%job_runs%'
   OR query LIKE '%correlation_events%'
   OR query LIKE '%lineage_edges%'
ORDER BY total_exec_time DESC;

-- Log initialization completion
DO $$
BEGIN
    RAISE NOTICE 'Correlator PostgreSQL initialization completed successfully';
    RAISE NOTICE 'Extensions enabled: pg_trgm, pg_stat_statements';
    RAISE NOTICE 'Database ready for correlation workloads';
END
$$;
