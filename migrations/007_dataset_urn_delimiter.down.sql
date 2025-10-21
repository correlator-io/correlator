-- =====================================================
-- Migration 007 Rollback: Revert Dataset URN Delimiter
-- =====================================================

-- Drop OpenLineage-compliant constraint
ALTER TABLE datasets DROP CONSTRAINT IF EXISTS datasets_dataset_urn_check;

-- Restore original constraint with ':' delimiter
ALTER TABLE datasets ADD CONSTRAINT datasets_dataset_urn_check
  CHECK (dataset_urn ~ '^[^:]+:.+$');

-- Restore original comment
COMMENT ON COLUMN datasets.dataset_urn IS 'OpenLineage dataset URN: namespace:name format for global identification';

-- Success message
SELECT
    'Migration 007 rolled back successfully' as status,
    NOW() as completed_at;