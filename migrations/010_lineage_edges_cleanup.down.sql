-- =====================================================
-- Migration 010 Rollback: Restore deprecated lineage_edges columns
-- NOTE: Materialized views are NOT recreated in this rollback
-- They will exist in the state from migration 002 (using old columns)
-- =====================================================

-- Make new columns nullable again (to allow old columns to coexist)
ALTER TABLE lineage_edges ALTER COLUMN dataset_urn DROP NOT NULL;
ALTER TABLE lineage_edges ALTER COLUMN edge_type DROP NOT NULL;

-- Recreate deprecated columns
ALTER TABLE lineage_edges ADD COLUMN input_dataset_urn VARCHAR(500);
ALTER TABLE lineage_edges ADD COLUMN output_dataset_urn VARCHAR(500);

-- Recreate FK constraints for old columns
ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_input_dataset_urn_fkey
  FOREIGN KEY (input_dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE SET NULL;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_output_dataset_urn_fkey
  FOREIGN KEY (output_dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE SET NULL;

-- Recreate CHECK constraint (at least one dataset required)
ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_check
  CHECK (input_dataset_urn IS NOT NULL OR output_dataset_urn IS NOT NULL);

-- Recreate old indexes
CREATE INDEX idx_lineage_edges_input_dataset
ON lineage_edges (input_dataset_urn, job_run_id);

CREATE INDEX idx_lineage_edges_output_dataset
ON lineage_edges (output_dataset_urn, job_run_id);

-- Recreate indexes dropped in migration 010
CREATE INDEX IF NOT EXISTS idx_lineage_edges_downstream_traversal 
ON lineage_edges (input_dataset_urn, output_dataset_urn, job_run_id);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_upstream_traversal 
ON lineage_edges (output_dataset_urn, input_dataset_urn, job_run_id);

CREATE INDEX IF NOT EXISTS idx_job_runs_status_temporal 
ON job_runs (current_state, started_at DESC, completed_at) 
WHERE current_state IN ('FAIL', 'error', 'RUNNING');

-- Restore deprecation warnings
COMMENT ON COLUMN lineage_edges.input_dataset_urn IS 'DEPRECATED: Use dataset_urn with edge_type=input instead. Will be dropped in migration 010.';
COMMENT ON COLUMN lineage_edges.output_dataset_urn IS 'DEPRECATED: Use dataset_urn with edge_type=output instead. Will be dropped in migration 010.';

-- Restore transitional table comment
COMMENT ON TABLE lineage_edges IS 'OpenLineage lineage edges: separate rows for each input and output dataset per job run. Transitioning from input/output columns to edge_type + dataset_urn.';

-- Success message
SELECT
    'Migration 010 rolled back successfully' as status,
    'Deprecated columns restored - materialized views remain from migration 002' as note,
    'Run migration 002 if views were dropped' as warning,
    NOW() as completed_at;

