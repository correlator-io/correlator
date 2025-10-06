-- =====================================================
-- API Key Lookup Hash Column
-- Adds SHA256 hash column for O(1) API key lookup performance
-- =====================================================

-- Add key_lookup_hash column (NOT NULL)
-- This column stores SHA256(plaintext_key) for fast O(1) lookups
-- Note: This is separate from key_hash (bcrypt) which is used for security
ALTER TABLE api_keys
ADD COLUMN key_lookup_hash VARCHAR(64) NOT NULL;

-- Add unique index for O(1) lookup performance
-- Partial index only on active keys for efficiency
CREATE UNIQUE INDEX idx_api_keys_lookup_hash_unique
ON api_keys(key_lookup_hash)
WHERE active = TRUE;

-- Comment on the new column
COMMENT ON COLUMN api_keys.key_lookup_hash IS 'SHA256 hash of plaintext API key for O(1) lookup. Separate from bcrypt key_hash used for security validation.';

-- Success message
SELECT
    'API key lookup hash migration completed successfully' as status,
    1 as columns_added,
    1 as indexes_created,
    NOW() as completed_at;