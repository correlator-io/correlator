-- =====================================================
-- Migration 008 Rollback: Remove OpenLineage columns
-- =====================================================

-- Drop new indexes
DROP INDEX IF EXISTS idx_lineage_edges_dataset_urn;
DROP INDEX IF EXISTS idx_lineage_edges_edge_type;

-- Drop FK constraint and new columns
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_dataset_urn_fkey;
ALTER TABLE lineage_edges DROP COLUMN IF EXISTS dataset_urn;
ALTER TABLE lineage_edges DROP COLUMN IF EXISTS edge_type;

-- Restore original column comments (remove deprecation warnings)
COMMENT ON COLUMN lineage_edges.input_dataset_urn IS NULL;
COMMENT ON COLUMN lineage_edges.output_dataset_urn IS NULL;

-- Restore original table comment
COMMENT ON TABLE lineage_edges IS 'Job-to-dataset lineage relationships with downstream impact calculation';

-- Success message
SELECT
    'Migration 008 rolled back successfully' as status,
    'Old columns (input/output_dataset_urn) restored to active status' as note,
    NOW() as completed_at;