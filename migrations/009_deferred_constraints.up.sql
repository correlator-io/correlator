-- =====================================================
-- Migration 009: Deferred FK Constraints for Concurrent Events
-- Handles out-of-order OpenLineage events where Event B references dataset from Event A
-- =====================================================

-- Make lineage_edges FK constraints deferrable
-- This allows Event B (references dataset X) to arrive before Event A (creates dataset X)
-- within the same transaction

-- Drop existing FK constraints
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_job_run_id_fkey;
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_input_dataset_urn_fkey;
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_output_dataset_urn_fkey;
ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_dataset_urn_fkey;

-- Recreate with DEFERRABLE INITIALLY DEFERRED
ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_job_run_id_fkey
  FOREIGN KEY (job_run_id) REFERENCES job_runs(job_run_id)
  ON DELETE CASCADE
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_input_dataset_urn_fkey
  FOREIGN KEY (input_dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE SET NULL
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_output_dataset_urn_fkey
  FOREIGN KEY (output_dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE SET NULL
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_dataset_urn_fkey
  FOREIGN KEY (dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE CASCADE
  DEFERRABLE INITIALLY DEFERRED;

-- Make test_results FK constraints deferrable as well
ALTER TABLE test_results DROP CONSTRAINT IF EXISTS test_results_dataset_urn_fkey;
ALTER TABLE test_results DROP CONSTRAINT IF EXISTS test_results_job_run_id_fkey;

ALTER TABLE test_results ADD CONSTRAINT test_results_dataset_urn_fkey
  FOREIGN KEY (dataset_urn) REFERENCES datasets(dataset_urn)
  ON DELETE CASCADE
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE test_results ADD CONSTRAINT test_results_job_run_id_fkey
  FOREIGN KEY (job_run_id) REFERENCES job_runs(job_run_id)
  ON DELETE CASCADE
  DEFERRABLE INITIALLY DEFERRED;

-- Add comments explaining deferrable constraints
COMMENT ON CONSTRAINT lineage_edges_job_run_id_fkey ON lineage_edges IS 'Deferrable FK: allows out-of-order event processing within transaction';
COMMENT ON CONSTRAINT lineage_edges_dataset_urn_fkey ON lineage_edges IS 'Deferrable FK: handles concurrent events referencing same dataset';

-- Success message
SELECT
    'Migration 009 completed: deferred FK constraints for concurrent event handling' as status,
    6 as constraints_updated,
    'Enables out-of-order OpenLineage event processing' as benefit,
    NOW() as completed_at;