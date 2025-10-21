-- =====================================================
-- Migration 006: OpenLineage Event Idempotency Table
-- Prevents duplicate event processing with 24-hour TTL
-- =====================================================

-- Idempotency table for OpenLineage RunEvents
CREATE TABLE lineage_event_idempotency (
    -- Idempotency key: SHA256(producer + job.namespace + job.name + runId + eventTime + eventType)
    idempotency_key VARCHAR(64) PRIMARY KEY,

    -- Timestamp tracking
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Optional: Store minimal event metadata for debugging
    event_metadata JSONB DEFAULT '{}'::jsonb,

    -- Constraint: expires_at must be after created_at
    CHECK (expires_at > created_at)
);

-- Index for TTL cleanup queries
CREATE INDEX idx_idempotency_expires ON lineage_event_idempotency(expires_at);

-- Index for lookup performance
CREATE INDEX idx_idempotency_created ON lineage_event_idempotency(created_at DESC);

-- Comments
COMMENT ON TABLE lineage_event_idempotency IS 'OpenLineage event deduplication with 24-hour TTL - prevents duplicate processing on retries';
COMMENT ON COLUMN lineage_event_idempotency.idempotency_key IS 'SHA256 hash of producer + job namespace + job name + runId + eventTime + eventType';
COMMENT ON COLUMN lineage_event_idempotency.expires_at IS 'TTL expiration (24 hours from created_at) - events older than this are not deduped';

-- Success message
SELECT
    'Migration 006 completed: idempotency table created' as status,
    1 as tables_created,
    2 as indexes_created,
    NOW() as completed_at;