-- =====================================================
-- Rollback API Key Lookup Hash Column
-- Removes SHA256 hash column and related index
-- =====================================================

-- Drop the unique index first
DROP INDEX IF EXISTS idx_api_keys_lookup_hash_unique;

-- Drop the key_lookup_hash column
ALTER TABLE api_keys DROP COLUMN IF EXISTS key_lookup_hash;

-- Success message
SELECT
    'API key lookup hash rollback completed successfully' as status,
    1 as indexes_dropped,
    1 as columns_dropped,
    NOW() as completed_at;