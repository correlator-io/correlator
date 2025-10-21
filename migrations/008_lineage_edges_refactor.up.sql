-- =====================================================
-- Migration 008: Add OpenLineage Columns to lineage_edges (Expand Phase)
-- Adds edge_type and dataset_urn columns for OpenLineage compliance
-- OpenLineage spec: Separate rows for each input and output dataset
-- Pattern: Expand-Migrate-Contract (this is EXPAND phase)
-- =====================================================

-- Add edge_type column to distinguish input vs output edges
ALTER TABLE lineage_edges ADD COLUMN edge_type VARCHAR(10) CHECK (edge_type IN ('input', 'output'));

-- Add combined dataset_urn column (will replace input_dataset_urn and output_dataset_urn)
ALTER TABLE lineage_edges ADD COLUMN dataset_urn VARCHAR(500);

-- Add FK constraint for dataset_urn (will be set to NOT NULL in migration 010 after cleanup)
ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_dataset_urn_fkey
  FOREIGN KEY (dataset_urn) REFERENCES datasets(dataset_urn) ON DELETE CASCADE;

-- Create indexes for new columns
CREATE INDEX idx_lineage_edges_edge_type ON lineage_edges(edge_type, job_run_id);
CREATE INDEX idx_lineage_edges_dataset_urn ON lineage_edges(dataset_urn, edge_type, job_run_id);

-- Add comments
COMMENT ON COLUMN lineage_edges.edge_type IS 'OpenLineage edge type: input (job consumes dataset) or output (job produces dataset). REPLACES input/output_dataset_urn columns.';
COMMENT ON COLUMN lineage_edges.dataset_urn IS 'Dataset involved in this lineage relationship. REPLACES input/output_dataset_urn columns (will be dropped in migration 010).';
COMMENT ON TABLE lineage_edges IS 'OpenLineage lineage edges: separate rows for each input and output dataset per job run. Transitioning from input/output columns to edge_type + dataset_urn.';

-- Note: Old columns (input_dataset_urn, output_dataset_urn) are DEPRECATED but kept for migration 010
-- This follows the expand-migrate-contract pattern:
-- - Migration 008 (EXPAND): Add new columns alongside old ones
-- - Migration 010 (CONTRACT): Drop old columns after validation

COMMENT ON COLUMN lineage_edges.input_dataset_urn IS 'DEPRECATED: Use dataset_urn with edge_type=input instead. Will be dropped in migration 010.';
COMMENT ON COLUMN lineage_edges.output_dataset_urn IS 'DEPRECATED: Use dataset_urn with edge_type=output instead. Will be dropped in migration 010.';

-- Success message
SELECT
    'Migration 008 completed: OpenLineage columns added to lineage_edges (EXPAND phase)' as status,
    2 as columns_added,
    2 as indexes_created,
    'Old columns (input/output_dataset_urn) DEPRECATED but kept for migration 010' as note,
    'Pattern: Expand-Migrate-Contract (this is EXPAND)' as pattern,
    NOW() as completed_at;