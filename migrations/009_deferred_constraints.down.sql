-- =====================================================
-- Migration 009 Rollback: Revert to immediate FK constraints
-- =====================================================

-- Revert lineage_edges constraints to immediate (non-deferrable)
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_job_run_id_fkey;
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_input_dataset_urn_fkey;
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_output_dataset_urn_fkey;
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_dataset_urn_fkey;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_job_run_id_fkey
  FOREIGN KEY (job_run_id) REFERENCES job_runs(job_run_id)
  ON DELETE CASCADE;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_input_dataset_urn_fkey
  FOREIGN KEY (input_dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE SET NULL;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_output_dataset_urn_fkey
  FOREIGN KEY (output_dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE SET NULL;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_dataset_urn_fkey
  FOREIGN KEY (dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE CASCADE;

-- Revert test_results constraints to immediate
ALTER TABLE test_results DROP CONSTRAINT IF EXISTS test_results_dataset_urn_fkey;
ALTER TABLE test_results DROP CONSTRAINT IF EXISTS test_results_job_run_id_fkey;

ALTER TABLE test_results ADD CONSTRAINT test_results_dataset_urn_fkey
  FOREIGN KEY (dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE CASCADE;

ALTER TABLE test_results ADD CONSTRAINT test_results_job_run_id_fkey
  FOREIGN KEY (job_run_id) REFERENCES job_runs(job_run_id)
  ON DELETE CASCADE;

-- Success message
SELECT
    'Migration 009 rolled back successfully' as status,
    NOW() as completed_at;