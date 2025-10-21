-- =====================================================
-- Migration 005: Refactor job_runs for OpenLineage Compliance
-- Adds OpenLineage RunEvent fields and updates constraints
-- =====================================================

-- Add OpenLineage-specific columns to job_runs
ALTER TABLE job_runs ADD COLUMN run_id UUID NOT NULL;
ALTER TABLE job_runs ADD COLUMN event_type VARCHAR(50) NOT NULL;
ALTER TABLE job_runs ADD COLUMN event_time TIMESTAMP WITH TIME ZONE NOT NULL;
ALTER TABLE job_runs ADD COLUMN state_history JSONB DEFAULT '{"transitions": []}'::jsonb;

-- Rename status to current_state for clarity
ALTER TABLE job_runs RENAME COLUMN status TO current_state;

-- Update current_state constraint to match OpenLineage run states
ALTER TABLE job_runs DROP CONSTRAINT job_runs_status_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_current_state_check
  CHECK (current_state IN ('START', 'RUNNING', 'COMPLETE', 'FAIL', 'ABORT', 'OTHER'));

-- Update job_run_id constraint to be more flexible (will store hash-based canonical IDs)
ALTER TABLE job_runs DROP CONSTRAINT job_runs_job_run_id_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_job_run_id_check
  CHECK (char_length(job_run_id) > 0 AND char_length(job_run_id) <= 255);

-- Add critical index for state transition queries (run_id lookups)
CREATE INDEX idx_job_runs_run_id ON job_runs(run_id);

-- Note: Other indexes deferred until we have actual query patterns (premature optimization)
-- Future consideration: idx_job_runs_event_type, idx_job_runs_current_state

-- Add comments for new columns
COMMENT ON COLUMN job_runs.run_id IS 'OpenLineage client-generated UUID (UUIDv7 recommended) - maintained throughout run lifecycle';
COMMENT ON COLUMN job_runs.event_type IS 'OpenLineage event type: START, RUNNING, COMPLETE, FAIL, ABORT, OTHER';
COMMENT ON COLUMN job_runs.event_time IS 'OpenLineage eventTime - when the event occurred (use for ordering, not arrival time)';
COMMENT ON COLUMN job_runs.state_history IS 'Array of state transitions with timestamps for out-of-order event handling';
COMMENT ON COLUMN job_runs.current_state IS 'Current run state (renamed from status) - OpenLineage compliant';

-- Update table comment
COMMENT ON TABLE job_runs IS 'OpenLineage RunEvent storage with canonical ID strategy and state machine tracking';

-- =====================================================
-- State Transition Validation Trigger
-- Protects terminal states from mutation (defense in depth)
-- =====================================================

CREATE OR REPLACE FUNCTION validate_job_run_state_transition()
RETURNS TRIGGER AS $$
BEGIN
  -- Protect terminal states: COMPLETE, FAIL, ABORT can only transition to themselves (idempotent)
  -- This prevents bugs in application code from corrupting terminal state
  -- Note: Application layer handles out-of-order events by sorting on event_time
  IF OLD.current_state IN ('COMPLETE', 'FAIL', 'ABORT') THEN
    IF NEW.current_state != OLD.current_state THEN
      RAISE EXCEPTION 'Invalid state transition: % -> % (terminal states are immutable)',
        OLD.current_state, NEW.current_state
        USING HINT = 'Terminal states (COMPLETE/FAIL/ABORT) can only transition to themselves (idempotent). Check application logic for state ordering.';
    END IF;
  END IF;

  -- Append to state_history for audit trail
  NEW.state_history = jsonb_set(
    NEW.state_history,
    '{transitions}',
    (NEW.state_history->'transitions') || jsonb_build_object(
      'from', OLD.current_state,
      'to', NEW.current_state,
      'event_time', NEW.event_time,
      'updated_at', NOW()
    )
  );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_run_state_validation
  BEFORE UPDATE ON job_runs
  FOR EACH ROW EXECUTE FUNCTION validate_job_run_state_transition();

COMMENT ON FUNCTION validate_job_run_state_transition() IS 'OpenLineage state machine enforcement: protects terminal states, tracks transition history';

-- Success message
SELECT
    'Migration 005 completed: job_runs refactored for OpenLineage' as status,
    4 as columns_added,
    1 as columns_renamed,
    1 as indexes_created,
    1 as triggers_created,
    'State transition validation and idempotent terminal states enforced' as note,
    NOW() as completed_at;