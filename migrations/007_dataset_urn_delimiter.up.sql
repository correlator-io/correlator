-- =====================================================
-- Migration 007: Update Dataset URN Delimiter for OpenLineage Compliance
-- Changes delimiter from ':' to '/' to match OpenLineage spec
-- BREAKING CHANGE: Acceptable in early development (migration 004, no production data)
-- =====================================================

-- Drop existing constraint on dataset_urn
ALTER TABLE datasets DROP CONSTRAINT IF EXISTS datasets_dataset_urn_check;

-- Add new constraint that accepts '/' delimiter (OpenLineage spec)
-- Format: {namespace}/{name} where namespace includes protocol://host:port
-- Example: postgres://prod-db:5432/analytics.public.orders
ALTER TABLE datasets ADD CONSTRAINT datasets_dataset_urn_check
  CHECK (dataset_urn ~ '^[^/]+/.+$' AND char_length(dataset_urn) <= 500);

-- Update table comment to reflect OpenLineage compliance
COMMENT ON COLUMN datasets.dataset_urn IS 'OpenLineage dataset URN: {namespace}/{name} format (e.g., postgres://host:5432/db.schema.table)';

-- Note: No data migration needed as we're in early development (migration 004)
-- If there were existing data, we would need:
-- UPDATE datasets SET dataset_urn = replace(dataset_urn, ':', '/') WHERE dataset_urn LIKE '%:%';

-- Success message
SELECT
    'Migration 007 completed: dataset URN delimiter updated to OpenLineage spec' as status,
    'Breaking change: colon to slash delimiter' as note,
    NOW() as completed_at;