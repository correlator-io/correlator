-- =====================================================
-- API Keys Management Schema
-- Production-ready API key storage with security features
-- =====================================================

-- API Keys table with secure hashing
CREATE TABLE api_keys (
    -- Primary identifier (UUID v4)
    id VARCHAR(36) PRIMARY KEY,

    -- Bcrypt hash of the API key (NOT the plaintext key)
    -- bcrypt produces 60-character strings
    key_hash VARCHAR(60) NOT NULL,

    -- Plugin identification
    plugin_id VARCHAR(100) NOT NULL,

    -- Human-readable name
    name VARCHAR(255) NOT NULL,

    -- Permissions array (JSONB for flexibility with indexing)
    permissions JSONB DEFAULT '[]'::jsonb NOT NULL,

    -- Temporal columns
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE,

    -- Status flag
    active BOOLEAN DEFAULT TRUE NOT NULL,

    -- Updated timestamp
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,

    -- Hash uniqueness constraint
    UNIQUE(key_hash)
);

-- Comment on security approach
COMMENT ON TABLE api_keys IS 'API key storage with bcrypt hashing - plaintext keys never stored';
COMMENT ON COLUMN api_keys.key_hash IS 'Bcrypt hash of API key - use bcrypt.CompareHashAndPassword for validation';
COMMENT ON COLUMN api_keys.permissions IS 'JSONB array of permission strings for flexible querying';

-- Performance indexes
CREATE INDEX idx_api_keys_plugin_id ON api_keys(plugin_id) WHERE active = TRUE;
CREATE INDEX idx_api_keys_active ON api_keys(active, expires_at);
CREATE INDEX idx_api_keys_hash_lookup ON api_keys(key_hash) WHERE active = TRUE;

-- GIN index for permissions querying
CREATE INDEX idx_api_keys_permissions ON api_keys USING GIN(permissions);

-- Trigger for updated_at
CREATE TRIGGER update_api_keys_updated_at
    BEFORE UPDATE ON api_keys
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Audit table for API key operations
CREATE TABLE api_key_audit_log (
    id BIGSERIAL PRIMARY KEY,

    -- API key reference (nullable for key deletion events)
    api_key_id VARCHAR(36),

    -- Operation type
    operation VARCHAR(50) NOT NULL CHECK (operation IN ('created', 'updated', 'deleted', 'validated', 'validation_failed')),

    -- Masked key for reference (18 prefix + stars + 4 suffix)
    masked_key VARCHAR(100),

    -- Plugin context
    plugin_id VARCHAR(100),

    -- Operation metadata (IP, user agent, etc.)
    metadata JSONB DEFAULT '{}'::jsonb,

    -- Timestamp
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_api_key_audit_log_key_id ON api_key_audit_log(api_key_id, created_at DESC);
CREATE INDEX idx_api_key_audit_log_operation ON api_key_audit_log(operation, created_at DESC);

COMMENT ON TABLE api_key_audit_log IS 'Audit trail for API key operations - security and compliance';

-- Success message
SELECT
    'API keys schema migration completed successfully' as status,
    2 as tables_created,
    5 as indexes_created,
    NOW() as completed_at;