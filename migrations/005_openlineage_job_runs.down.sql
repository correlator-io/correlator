-- =====================================================
-- Migration 005 Rollback: Revert job_runs OpenLineage changes
-- =====================================================

-- Drop trigger and function
DROP TRIGGER IF EXISTS job_run_state_validation ON job_runs;
DROP FUNCTION IF EXISTS validate_job_run_state_transition();

-- Drop new index
DROP INDEX IF EXISTS idx_job_runs_run_id;

-- Rename current_state back to status
ALTER TABLE job_runs RENAME COLUMN current_state TO status;

-- Revert status constraint
ALTER TABLE job_runs DROP CONSTRAINT IF EXISTS job_runs_current_state_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_status_check
  CHECK (status IN ('running', 'completed', 'failed', 'aborted', 'error'));

-- Revert job_run_id constraint
ALTER TABLE job_runs DROP CONSTRAINT IF EXISTS job_runs_job_run_id_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_job_run_id_check
  CHECK (job_run_id ~ '^[a-zA-Z0-9_-]+:[a-zA-Z0-9_:-]+$');

-- Drop OpenLineage-specific columns
ALTER TABLE job_runs DROP COLUMN IF EXISTS state_history;
ALTER TABLE job_runs DROP COLUMN IF EXISTS event_time;
ALTER TABLE job_runs DROP COLUMN IF EXISTS event_type;
ALTER TABLE job_runs DROP COLUMN IF EXISTS run_id;

-- Success message
SELECT
    'Migration 005 rolled back successfully' as status,
    NOW() as completed_at;