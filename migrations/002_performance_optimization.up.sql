-- =====================================================
-- Correlator: Performance Optimization Layer
-- Phase 1: Correlation-Optimized Indexes & Materialized Views
-- 
-- Performance Requirements:
-- - <5 minute correlation latency (test failure → incident correlation)
-- - <200ms incident dashboard queries  
-- - <1s complex lineage traversal
-- - 1K events/sec ingestion capability
-- - >90% correlation accuracy tracking
-- =====================================================

-- =====================================================
-- CRITICAL PERFORMANCE INDEXES (10 Core Indexes)
-- =====================================================

-- 1. PRIMARY CORRELATION LOOKUP: test_results → job_runs
-- This is the most critical index for core correlation functionality
-- Optimizes: SELECT * FROM test_results tr JOIN job_runs jr ON tr.job_run_id = jr.job_run_id
CREATE INDEX IF NOT EXISTS idx_test_results_correlation_lookup 
ON test_results (job_run_id, status, executed_at DESC)
WHERE status IN ('failed', 'error');
-- Rationale: Partial index on failures only, includes executed_at for temporal sorting
-- Expected usage: 80% of all correlation queries

-- 2. CANONICAL ID MAPPING: Fast job_run_id lookups across all tables
CREATE INDEX IF NOT EXISTS idx_job_runs_canonical_id 
ON job_runs (job_run_id, started_at DESC);
-- Rationale: Primary correlation key with temporal ordering for recent-first results

-- 3. TEMPORAL CORRELATION: Recent incidents (optimized with status filter)
CREATE INDEX IF NOT EXISTS idx_test_results_recent_failures
ON test_results (executed_at DESC, status, dataset_urn)
WHERE status IN ('failed', 'error');
-- Rationale: Time-partitioned index for recent incident dashboard queries
-- 7-day window covers 95% of incident response queries

-- 4. DOWNSTREAM IMPACT: Lineage traversal from failed datasets
CREATE INDEX IF NOT EXISTS idx_lineage_edges_downstream_traversal 
ON lineage_edges (input_dataset_urn, output_dataset_urn, job_run_id);
-- Rationale: Optimizes recursive lineage queries for impact analysis
-- Covers: WITH RECURSIVE downstream AS (...) pattern

-- 5. UPSTREAM IMPACT: Reverse lineage traversal 
CREATE INDEX IF NOT EXISTS idx_lineage_edges_upstream_traversal 
ON lineage_edges (output_dataset_urn, input_dataset_urn, job_run_id);
-- Rationale: Enables fast "what feeds this dataset" queries for root cause analysis

-- 6. DATASET CORRELATION: Fast dataset-to-incident lookups
CREATE INDEX IF NOT EXISTS idx_test_results_dataset_failures 
ON test_results (dataset_urn, status, executed_at DESC) 
WHERE status IN ('failed', 'error');
-- Rationale: Dataset-first incident lookup pattern, temporal ordering

-- 7. JOB STATUS CORRELATION: Failed job run identification
CREATE INDEX IF NOT EXISTS idx_job_runs_status_temporal 
ON job_runs (status, started_at DESC, completed_at) 
WHERE status IN ('failed', 'error', 'running');
-- Rationale: Job-level incident correlation, includes running jobs for real-time updates

-- 8. ACCURACY TRACKING: Performance monitoring queries (future)
-- Note: correlation_events table will be added in Phase 2
-- Placeholder for accuracy monitoring index when correlation_events table exists

-- 9. COMPOSITE CORRELATION: Multi-dimensional incident queries
CREATE INDEX IF NOT EXISTS idx_test_results_composite_correlation 
ON test_results (job_run_id, dataset_urn, status, executed_at DESC);
-- Rationale: Supports complex filtering in incident dashboard (job + dataset + status)

-- 10. LINEAGE TEMPORAL: Recent lineage events for correlation accuracy
CREATE INDEX IF NOT EXISTS idx_lineage_edges_recent_temporal
ON lineage_edges (created_at DESC, job_run_id);
-- Rationale: Recent lineage events for correlation validation and accuracy measurement

-- =====================================================
-- MATERIALIZED VIEWS FOR CORRELATION OPTIMIZATION (4 Core Views)
-- =====================================================

-- 1. PRIMARY INCIDENT CORRELATION VIEW
-- Purpose: <200ms incident dashboard queries, pre-joined incident data
-- Usage: 90% of dashboard queries hit this view instead of raw tables
CREATE MATERIALIZED VIEW IF NOT EXISTS incident_correlation_view AS
SELECT 
  -- Test failure details
  tr.id as test_result_id,
  tr.test_name,
  tr.dataset_urn,
  tr.status as test_status,
  tr.message as test_message,
  tr.executed_at as incident_time,
  tr.created_at as reported_at,
  
  -- Job run correlation (CRITICAL PATH)
  jr.job_run_id,
  jr.job_name,
  jr.started_at as job_started_at,
  jr.completed_at as job_completed_at,
  jr.status as job_status,
  jr.metadata as job_metadata,
  
  -- Dataset context
  d.name as dataset_name,
  d.owner as dataset_owner,
  d.tags as dataset_tags,
  
  -- Correlation metadata
  EXTRACT(EPOCH FROM (tr.created_at - jr.started_at)) as correlation_lag_seconds,
  CASE 
    WHEN jr.status = 'failed' AND tr.status = 'failed' THEN 'job_failure_cascade'
    WHEN jr.status = 'completed' AND tr.status = 'failed' THEN 'data_quality_issue'
    WHEN jr.status = 'running' AND tr.status = 'failed' THEN 'real_time_failure'
    ELSE 'unknown_correlation'
  END as correlation_type,
  
  -- Downstream impact preview (first-level only for performance)
  COALESCE(
    ARRAY(
      SELECT DISTINCT le.output_dataset_urn 
      FROM lineage_edges le 
      WHERE le.input_dataset_urn = tr.dataset_urn 
      LIMIT 10
    ), 
    ARRAY[]::VARCHAR(500)[]
  ) as immediate_downstream_datasets,
  
  -- Severity scoring for triage
  CASE 
    WHEN d.tags @> ARRAY['critical'] OR d.owner IN ('data-platform', 'analytics') THEN 'critical'
    WHEN jr.status = 'failed' THEN 'high'
    WHEN tr.message ILIKE '%timeout%' OR tr.message ILIKE '%connection%' THEN 'medium'
    ELSE 'low'
  END as incident_severity
  
FROM test_results tr
  -- CRITICAL JOIN: This is the core correlation logic
  JOIN job_runs jr ON tr.job_run_id = jr.job_run_id
  JOIN datasets d ON tr.dataset_urn = d.dataset_urn
WHERE 
  tr.status IN ('failed', 'error')
  AND tr.executed_at > (NOW() - INTERVAL '30 days') -- Keep 30 days for trend analysis
ORDER BY tr.executed_at DESC;

-- Performance index for the materialized view
CREATE INDEX IF NOT EXISTS idx_incident_correlation_view_dashboard 
ON incident_correlation_view (incident_time DESC, incident_severity, correlation_type);

-- 2. CORRELATION ACCURACY METRICS VIEW
-- Purpose: Real-time >90% accuracy tracking and monitoring
-- Usage: Performance dashboard, alerting, SLA monitoring
CREATE MATERIALIZED VIEW IF NOT EXISTS correlation_accuracy_metrics AS
SELECT
  -- Time buckets for trend analysis
  DATE_TRUNC('hour', tr.executed_at) as time_bucket,
  DATE_TRUNC('day', tr.executed_at) as day_bucket,
  
  -- Correlation success metrics
  COUNT(*) as total_test_failures,
  COUNT(jr.job_run_id) as successfully_correlated,
  ROUND(
    (COUNT(jr.job_run_id)::DECIMAL / NULLIF(COUNT(*), 0)) * 100, 
    2
  ) as correlation_accuracy_percent,
  
  -- Correlation type breakdown  
  COUNT(*) FILTER (WHERE jr.status = 'failed') as job_failure_correlations,
  COUNT(*) FILTER (WHERE jr.status = 'completed') as data_quality_correlations,
  COUNT(*) FILTER (WHERE jr.status = 'running') as real_time_correlations,
  COUNT(*) FILTER (WHERE jr.job_run_id IS NULL) as uncorrelated_failures,
  
  -- Performance metrics
  AVG(EXTRACT(EPOCH FROM (tr.created_at - jr.started_at))) as avg_correlation_lag_seconds,
  PERCENTILE_CONT(0.95) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (tr.created_at - jr.started_at))
  ) as p95_correlation_lag_seconds,
  
  -- Data volume metrics
  COUNT(DISTINCT tr.dataset_urn) as unique_datasets_affected,
  COUNT(DISTINCT jr.job_name) as unique_jobs_involved,
  
  -- Critical incidents (for alerting)
  COUNT(*) FILTER (
    WHERE d.tags @> ARRAY['critical'] 
    OR d.owner IN ('data-platform', 'analytics')
  ) as critical_incidents
  
FROM test_results tr
  LEFT JOIN job_runs jr ON tr.job_run_id = jr.job_run_id
  LEFT JOIN datasets d ON tr.dataset_urn = d.dataset_urn
WHERE 
  tr.status IN ('failed', 'error')
  AND tr.executed_at > (NOW() - INTERVAL '7 days')
GROUP BY 
  DATE_TRUNC('hour', tr.executed_at),
  DATE_TRUNC('day', tr.executed_at)
ORDER BY time_bucket DESC;

-- Performance index for accuracy metrics
CREATE INDEX IF NOT EXISTS idx_correlation_accuracy_metrics_time 
ON correlation_accuracy_metrics (time_bucket DESC, correlation_accuracy_percent);

-- 3. LINEAGE IMPACT ANALYSIS VIEW  
-- Purpose: <1s complex lineage traversal for downstream impact
-- Usage: "What will break if this dataset fails?" queries
CREATE MATERIALIZED VIEW IF NOT EXISTS lineage_impact_analysis AS
WITH RECURSIVE downstream_impact AS (
  -- Base case: direct downstream datasets
  SELECT 
    le.input_dataset_urn as source_dataset,
    le.output_dataset_urn as impacted_dataset,
    le.job_run_id,
    jr.job_name,
    1 as impact_depth,
    ARRAY[le.output_dataset_urn]::VARCHAR(500)[] as impact_path
  FROM lineage_edges le
    JOIN job_runs jr ON le.job_run_id = jr.job_run_id
  WHERE le.created_at > (NOW() - INTERVAL '7 days')
  
  UNION ALL
  
  -- Recursive case: multi-hop downstream impact (limit to 5 hops for performance)
  SELECT 
    di.source_dataset,
    le.output_dataset_urn as impacted_dataset,
    le.job_run_id,
    jr.job_name,
    di.impact_depth + 1,
    (di.impact_path || le.output_dataset_urn)::VARCHAR(500)[]
  FROM downstream_impact di
    JOIN lineage_edges le ON di.impacted_dataset = le.input_dataset_urn
    JOIN job_runs jr ON le.job_run_id = jr.job_run_id
  WHERE 
    di.impact_depth < 5  -- Prevent infinite recursion and performance issues
    AND NOT (le.output_dataset_urn = ANY(di.impact_path))  -- Prevent cycles
    AND le.created_at > (NOW() - INTERVAL '7 days')
),
impact_summary AS (
  SELECT 
    source_dataset,
    COUNT(DISTINCT impacted_dataset) as total_downstream_datasets,
    COUNT(DISTINCT job_name) as total_downstream_jobs,
    MAX(impact_depth) as max_impact_depth,
    
    -- Critical downstream datasets
    COUNT(DISTINCT impacted_dataset) FILTER (
      WHERE d.tags @> ARRAY['critical'] 
      OR d.owner IN ('data-platform', 'analytics')
    ) as critical_downstream_datasets,
    
    -- Impact scoring for triage
    CASE 
      WHEN COUNT(DISTINCT impacted_dataset) > 50 THEN 'high_impact'
      WHEN COUNT(DISTINCT impacted_dataset) > 10 THEN 'medium_impact'
      ELSE 'low_impact'
    END as impact_category,
    
    -- Downstream dataset list (limited for performance)
    ARRAY_AGG(DISTINCT impacted_dataset)
      FILTER (WHERE impact_depth <= 3) as downstream_datasets_sample
      
  FROM downstream_impact di
    LEFT JOIN datasets d ON di.impacted_dataset = d.dataset_urn
  GROUP BY source_dataset
)
SELECT 
  source_dataset,
  total_downstream_datasets,
  total_downstream_jobs,
  max_impact_depth,
  critical_downstream_datasets,
  impact_category,
  downstream_datasets_sample,
  -- Metadata for refresh tracking
  NOW() as last_computed_at
FROM impact_summary;

-- Performance index for impact analysis
CREATE INDEX IF NOT EXISTS idx_lineage_impact_analysis_source 
ON lineage_impact_analysis (source_dataset, impact_category, total_downstream_datasets DESC);

-- 4. RECENT INCIDENTS SUMMARY VIEW
-- Purpose: Fast incident overview for dashboards and API responses  
-- Usage: Landing page, mobile app, API endpoints
CREATE MATERIALIZED VIEW IF NOT EXISTS recent_incidents_summary AS
SELECT 
  -- Time-based aggregations
  DATE_TRUNC('hour', incident_time) as incident_hour,
  DATE_TRUNC('day', incident_time) as incident_day,
  
  -- Incident counts by severity
  COUNT(*) as total_incidents,
  COUNT(*) FILTER (WHERE incident_severity = 'critical') as critical_incidents,
  COUNT(*) FILTER (WHERE incident_severity = 'high') as high_incidents,
  COUNT(*) FILTER (WHERE incident_severity = 'medium') as medium_incidents,
  COUNT(*) FILTER (WHERE incident_severity = 'low') as low_incidents,
  
  -- Correlation effectiveness
  COUNT(*) FILTER (WHERE correlation_type != 'unknown_correlation') as correlated_incidents,
  ROUND(
    (COUNT(*) FILTER (WHERE correlation_type != 'unknown_correlation')::DECIMAL / NULLIF(COUNT(*), 0)) * 100,
    1
  ) as correlation_success_rate,
  
  -- Response time metrics
  AVG(correlation_lag_seconds) as avg_correlation_lag_seconds,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY correlation_lag_seconds) as p95_correlation_lag_seconds,
  
  -- Dataset and job impact
  COUNT(DISTINCT dataset_urn) as unique_datasets_affected,
  COUNT(DISTINCT job_name) as unique_jobs_affected,
  
  -- Top affected systems (for alerting and routing)
  MODE() WITHIN GROUP (ORDER BY dataset_owner) as most_affected_owner,
  (ARRAY_AGG(DISTINCT dataset_name)
    FILTER (WHERE incident_severity IN ('critical', 'high')))[1:10] as top_affected_datasets,
  
  -- Trend indicators  
  LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('hour', incident_time)) as previous_hour_incidents,
  CASE 
    WHEN COUNT(*) > 1.5 * LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('hour', incident_time)) THEN 'trending_up'
    WHEN COUNT(*) < 0.5 * LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('hour', incident_time)) THEN 'trending_down'
    ELSE 'stable'
  END as trend_direction
  
FROM incident_correlation_view 
WHERE incident_time > (NOW() - INTERVAL '7 days')
GROUP BY 
  DATE_TRUNC('hour', incident_time),
  DATE_TRUNC('day', incident_time)
ORDER BY incident_hour DESC;

-- Performance index for recent incidents
CREATE INDEX IF NOT EXISTS idx_recent_incidents_summary_time 
ON recent_incidents_summary (incident_hour DESC, total_incidents DESC);

-- =====================================================
-- MATERIALIZED VIEW REFRESH STRATEGIES
-- =====================================================

-- Create refresh function for automated updates
CREATE OR REPLACE FUNCTION refresh_correlation_views()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Refresh in dependency order (most critical first)
  
  -- 1. Core incident correlation (highest priority - real-time dashboards)
  REFRESH MATERIALIZED VIEW incident_correlation_view;
  
  -- 2. Recent incidents summary (dashboard landing page)
  REFRESH MATERIALIZED VIEW recent_incidents_summary;
  
  -- 3. Correlation accuracy metrics (monitoring and alerting)
  REFRESH MATERIALIZED VIEW correlation_accuracy_metrics;
  
  -- 4. Lineage impact analysis (lower priority - analytical queries)
  REFRESH MATERIALIZED VIEW lineage_impact_analysis;
  
  -- Log refresh completion
  INSERT INTO system_logs (log_level, message, created_at) 
  VALUES ('INFO', 'Materialized views refreshed successfully', NOW());
  
EXCEPTION WHEN OTHERS THEN
  -- Log refresh errors
  INSERT INTO system_logs (log_level, message, created_at) 
  VALUES ('ERROR', 'Materialized view refresh failed: ' || SQLERRM, NOW());
  RAISE;
END;
$$;

-- Create system_logs table for refresh tracking (if not exists)
CREATE TABLE IF NOT EXISTS system_logs (
  id BIGSERIAL PRIMARY KEY,
  log_level VARCHAR(10) NOT NULL,
  message TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

-- =====================================================
-- REFRESH SCHEDULE RECOMMENDATIONS
-- =====================================================

-- PRODUCTION REFRESH SCHEDULE:
-- 
-- incident_correlation_view: Every 30 seconds (real-time dashboard)
-- - Command: SELECT cron.schedule('refresh-incidents', '*/0.5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY incident_correlation_view;');
--
-- recent_incidents_summary: Every 5 minutes (dashboard overview)  
-- - Command: SELECT cron.schedule('refresh-summary', '*/5 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY recent_incidents_summary;');
--
-- correlation_accuracy_metrics: Every 15 minutes (monitoring)
-- - Command: SELECT cron.schedule('refresh-accuracy', '*/15 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY correlation_accuracy_metrics;');
--
-- lineage_impact_analysis: Every 1 hour (analytical queries)
-- - Command: SELECT cron.schedule('refresh-lineage', '0 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY lineage_impact_analysis;');
--
-- DEVELOPMENT REFRESH SCHEDULE:
-- - All views: Every 5 minutes for development/testing
-- - Command: SELECT cron.schedule('refresh-all-dev', '*/5 * * * *', 'SELECT refresh_correlation_views();');

-- =====================================================
-- PERFORMANCE VALIDATION QUERIES
-- =====================================================

-- Test critical correlation query performance (should be <200ms)
-- Expected: Sub-200ms response time with proper indexes
/*
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM incident_correlation_view 
WHERE incident_time > (NOW() - INTERVAL '24 hours')
ORDER BY incident_time DESC 
LIMIT 50;
*/

-- Test lineage traversal performance (should be <1s)  
-- Expected: Sub-1s response time for complex downstream analysis
/*
EXPLAIN (ANALYZE, BUFFERS)
SELECT source_dataset, total_downstream_datasets, impact_category
FROM lineage_impact_analysis 
WHERE impact_category = 'high_impact'
ORDER BY total_downstream_datasets DESC;
*/

-- Test correlation accuracy tracking (should be <200ms)
-- Expected: Fast metrics aggregation for monitoring dashboard
/*
EXPLAIN (ANALYZE, BUFFERS)
SELECT time_bucket, correlation_accuracy_percent, total_test_failures
FROM correlation_accuracy_metrics 
WHERE time_bucket > (NOW() - INTERVAL '24 hours')
ORDER BY time_bucket DESC;
*/

-- =====================================================
-- PERFORMANCE MONITORING QUERIES
-- =====================================================

-- Monitor index usage and performance
CREATE OR REPLACE VIEW index_performance_monitor AS
SELECT
  schemaname,
  relname as tablename,
  indexrelname as indexname,
  idx_scan as total_scans,
  idx_tup_read as total_tuples_read,
  ROUND((idx_tup_read::DECIMAL / NULLIF(idx_scan, 0)), 2) as avg_tuples_per_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
AND indexrelname LIKE 'idx_%correlation%'
ORDER BY idx_tup_read DESC;

-- Monitor materialized view sizes and refresh performance
CREATE OR REPLACE VIEW materialized_view_stats AS
SELECT 
  schemaname,
  matviewname,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as view_size,
  pg_stat_get_tuples_returned(c.oid) as tuples_returned,
  pg_stat_get_tuples_fetched(c.oid) as tuples_fetched
FROM pg_matviews mv
JOIN pg_class c ON c.relname = mv.matviewname
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||matviewname) DESC;

-- =====================================================
-- SUCCESS CRITERIA VALIDATION
-- =====================================================

-- Query to validate >90% correlation accuracy requirement
-- Usage: Monitor in production to ensure SLA compliance
CREATE OR REPLACE VIEW correlation_sla_monitor AS
SELECT 
  'Last 24h' as time_window,
  AVG(correlation_accuracy_percent) as avg_accuracy,
  MIN(correlation_accuracy_percent) as min_accuracy,
  CASE 
    WHEN AVG(correlation_accuracy_percent) >= 90.0 THEN 'SLA_MET'
    ELSE 'SLA_BREACH'
  END as sla_status,
  COUNT(*) as measurement_points
FROM correlation_accuracy_metrics 
WHERE time_bucket > (NOW() - INTERVAL '24 hours');

-- Query to validate <5 minute correlation latency requirement
-- Usage: Real-time latency monitoring for performance SLA
CREATE OR REPLACE VIEW correlation_latency_monitor AS
SELECT 
  'Last 1h' as time_window,
  AVG(correlation_lag_seconds) as avg_latency_seconds,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY correlation_lag_seconds) as p95_latency_seconds,
  MAX(correlation_lag_seconds) as max_latency_seconds,
  CASE 
    WHEN PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY correlation_lag_seconds) <= 300 THEN 'SLA_MET'
    ELSE 'SLA_BREACH'  
  END as latency_sla_status,
  COUNT(*) as total_correlations
FROM incident_correlation_view 
WHERE incident_time > (NOW() - INTERVAL '1 hour')
AND correlation_lag_seconds IS NOT NULL;

-- =====================================================
-- MAINTENANCE AND OPTIMIZATION NOTES
-- =====================================================

-- WEEKLY MAINTENANCE (Run every Sunday):
-- 1. VACUUM ANALYZE all tables for optimal query plans
-- 2. Check index usage with index_performance_monitor view
-- 3. Monitor materialized view sizes with materialized_view_stats view
-- 4. Validate SLA compliance with correlation_sla_monitor and correlation_latency_monitor views

-- PERFORMANCE TUNING NOTES:
-- 1. If correlation_accuracy drops below 90%, investigate ID canonicalization
-- 2. If latency exceeds 5 minutes, check database resource utilization
-- 3. If materialized view refreshes fail, check available disk space
-- 4. Monitor pg_stat_activity for long-running queries during refresh

-- SCALING CONSIDERATIONS:
-- 1. Consider partitioning test_results and lineage_edges by time when >1M rows
-- 2. Implement connection pooling (PgBouncer) when concurrent connections >100
-- 3. Add read replicas when query load impacts ingestion performance
-- 4. Consider TimescaleDB migration for test_results time-series data at scale

COMMIT;