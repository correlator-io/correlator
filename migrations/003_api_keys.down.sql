-- =====================================================
-- API Keys Management Schema Rollback
-- Clean removal of API key tables and indexes
-- =====================================================

-- Drop audit log first (no foreign key constraints to worry about)
DROP TABLE IF EXISTS api_key_audit_log;

-- Drop main API keys table (will cascade drop all associated indexes and triggers)
DROP TABLE IF EXISTS api_keys;

-- Success message
SELECT
    'API keys schema rollback completed successfully' as status,
    2 as tables_dropped,
    NOW() as completed_at;