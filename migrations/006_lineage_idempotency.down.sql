-- =====================================================
-- Migration 006 Rollback: Drop idempotency table
-- =====================================================

-- Drop indexes first
DROP INDEX IF EXISTS idx_idempotency_created;
DROP INDEX IF EXISTS idx_idempotency_expires;

-- Drop table
DROP TABLE IF EXISTS lineage_event_idempotency;

-- Success message
SELECT
    'Migration 006 rolled back successfully' as status,
    NOW() as completed_at;