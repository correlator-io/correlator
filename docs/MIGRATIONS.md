# Writing Database Migrations

**Audience:** Backend Engineers, Database Developers  
**Purpose:** Guide for writing SQL migration files for Correlator

---

## Table of Contents

1. [Migration File Format](#migration-file-format)
2. [Adding New Migrations](#adding-new-migrations)
3. [Migration Best Practices](#migration-best-practices)
4. [Common Patterns](#common-patterns)
5. [Testing Migrations](#testing-migrations)
6. [Squashing Migrations](#squashing-migrations)

---

## Migration File Format

### Naming Convention

**Pattern:** `NNN_description.{up,down}.sql`

- `NNN`: Zero-padded migration number (001, 002, 003, ...)
- `description`: Snake_case description (lowercase, underscores)
- `.up.sql`: Forward migration (apply schema changes)
- `.down.sql`: Reverse migration (rollback schema changes)

### Examples

**Good:**
```
✅ 001_initial_openlineage_schema.up.sql
✅ 002_add_correlation_metrics.up.sql
✅ 003_add_plugin_registry.up.sql
```

**Bad:**
```
❌ 1_init.up.sql                    # Not zero-padded
❌ 002-AddMetrics.up.sql            # Wrong case, hyphens
❌ 002_add_metrics.sql              # Missing .up suffix
❌ 002_add_metrics.up.sql (no down) # Missing down migration
```

### File Pair Requirement

**Every up migration MUST have a corresponding down migration:**
```
✅ 002_add_metrics.up.sql
✅ 002_add_metrics.down.sql
```

**Why:** Enables rollback if deployment fails.

---

## Adding New Migrations

### Step-by-Step Guide

**Step 1: Determine next migration number**

```bash
# Check current latest migration
ls migrations/*.up.sql | sort | tail -1

# Output: migrations/001_initial_openlineage_schema.up.sql
# Next number: 002
```

**Step 2: Create both files**

```bash
cd migrations

# Create up migration
touch 002_add_plugin_registry.up.sql

# Create down migration
touch 002_add_plugin_registry.down.sql
```

**Step 3: Write SQL**

**002_add_plugin_registry.up.sql:**
```sql
-- =====================================================
-- Migration 002: Plugin Registry
-- Tracks registered plugins and their health status
-- =====================================================

CREATE TABLE plugins (
    id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    version VARCHAR(50),
    
    status VARCHAR(50) DEFAULT 'active' 
        CHECK (status IN ('active', 'inactive', 'disabled')),
    
    health_check_url VARCHAR(500),
    last_health_check TIMESTAMP WITH TIME ZONE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_plugins_status 
    ON plugins(status) 
    WHERE status = 'active';

COMMENT ON TABLE plugins IS 
    'Plugin registry for tracking connected data tools';

-- Success message (helpful for logs)
SELECT 
    'Migration 002 completed: plugin registry created' as status,
    1 as tables_created,
    NOW() as completed_at;
```

**002_add_plugin_registry.down.sql:**
```sql
-- =====================================================
-- Rollback Migration 002: Plugin Registry
-- =====================================================

DROP TABLE IF EXISTS plugins CASCADE;

SELECT 'Migration 002 rolled back: plugin registry removed' as status;
```

**Step 4: Test the migration**

```bash
# Rebuild migrator with new migration
make build migrator

# Test apply
make run migrate up

# Verify table created
psql $DATABASE_URL -c "\d plugins"

# Test rollback
make run migrate down

# Verify table dropped
psql $DATABASE_URL -c "\d plugins"

# Reapply (test idempotence)
make run migrate up
```

**Step 5: Write tests** (optional but recommended)

```go
// migrations/integration_test.go
func TestMigration002_PluginRegistry(t *testing.T) {
    // Verify plugins table exists
    // Verify indexes created
    // Verify constraints work
}
```

**Step 6: Update tests, rebuild, commit**

```bash
# Run all tests
go test ./migrations -v

# Rebuild migrator for next person
make build migrator

# Commit changes
git add migrations/002_*
git commit -m "feat: add plugin registry migration"
```

---

## Migration Best Practices

### 1. Idempotent Operations

**Always use `IF NOT EXISTS` / `IF EXISTS`:**

**Good:**
```sql
CREATE TABLE IF NOT EXISTS plugins (...);
CREATE INDEX IF NOT EXISTS idx_plugins_status ON plugins(status);
DROP TABLE IF EXISTS plugins CASCADE;
```

**Bad:**
```sql
CREATE TABLE plugins (...);  -- ❌ Fails if table exists
DROP TABLE plugins;          -- ❌ Fails if table doesn't exist
```

**Why:** Allows migrations to be re-run safely (development, recovery scenarios).

---

### 2. Explicit Down Migrations

**Every up migration MUST have explicit rollback:**

**Good:**
```sql
-- 002_add_column.up.sql
ALTER TABLE job_runs ADD COLUMN retry_count INTEGER DEFAULT 0;

-- 002_add_column.down.sql
ALTER TABLE job_runs DROP COLUMN IF EXISTS retry_count;
```

**Bad:**
```sql
-- 002_add_column.down.sql
-- (empty file)  ❌ No rollback defined
```

**Why:** Enables safe rollback if deployment fails.

---

### 3. Data Migrations with Schema Changes

**When changing schema with existing data:**

**Good:**
```sql
-- Add column with default (safe)
ALTER TABLE job_runs 
    ADD COLUMN retry_count INTEGER DEFAULT 0 NOT NULL;

-- Backfill data (if default isn't correct)
UPDATE job_runs 
    SET retry_count = 
        CASE 
            WHEN current_state = 'FAIL' THEN 1
            ELSE 0
        END;
```

**Bad:**
```sql
-- Add NOT NULL column without default
ALTER TABLE job_runs 
    ADD COLUMN retry_count INTEGER NOT NULL;  -- ❌ Fails if table has data!
```

**Down migration for data changes:**
```sql
-- 003_add_retry_count.down.sql
ALTER TABLE job_runs DROP COLUMN IF EXISTS retry_count;
-- Note: Data loss is acceptable on rollback (going backwards)
```

---

### 4. Deferred Constraints for Event Systems

**For out-of-order event handling:**

```sql
-- Foreign keys should be DEFERRABLE for OpenLineage events
CREATE TABLE lineage_edges (
    run_id UUID NOT NULL
        REFERENCES job_runs(run_id)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED,  -- ← Important!
    
    dataset_urn VARCHAR(500) NOT NULL 
        REFERENCES datasets(dataset_urn) 
        ON DELETE CASCADE 
        DEFERRABLE INITIALLY DEFERRED   -- ← Important!
);
```

**Why:** OpenLineage events may arrive out of order. Deferred constraints allow both events to succeed within same transaction.

See [Migration 001](../migrations/001_initial_openlineage_schema.up.sql) lines 124-146 for complete example.

---

### 5. Comments and Documentation

**Every migration should have:**

```sql
-- =====================================================
-- Migration NNN: Purpose Statement
-- Detailed description of what this migration does
-- =====================================================

-- Section comments
-- Subsection explanations

COMMENT ON TABLE table_name IS 'Purpose and usage';
COMMENT ON COLUMN table_name.column_name IS 'What this column stores';
```

**Why:** Future developers (including you!) need context.

---

### 6. Validation Queries

**Add validation at end of up migrations:**

```sql
-- Validation block
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'plugins'
    ) THEN
        RAISE EXCEPTION 'Table plugins not created';
    END IF;
    
    RAISE NOTICE 'Migration validation: All tables created successfully';
END $$;

-- Success message
SELECT 
    'Migration 002 completed' as status,
    1 as tables_created,
    NOW() as completed_at;
```

**Why:** Catches migration bugs immediately (table creation failed but migration continued).

---

## Common Patterns

### Adding a Table

```sql
-- 00X_add_table_name.up.sql
CREATE TABLE table_name (
    id BIGSERIAL PRIMARY KEY,
    
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'active' 
        CHECK (status IN ('active', 'inactive')),
    
    metadata JSONB DEFAULT '{}'::jsonb,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_table_name_status ON table_name(status);

-- Comments
COMMENT ON TABLE table_name IS 'Description of purpose';

-- 00X_add_table_name.down.sql
DROP TABLE IF EXISTS table_name CASCADE;
```

---

### Adding an Index

```sql
-- 00X_add_index_name.up.sql
CREATE INDEX IF NOT EXISTS idx_table_column 
    ON table_name(column_name);

-- For partial indexes (filtered)
CREATE INDEX IF NOT EXISTS idx_table_active 
    ON table_name(status) 
    WHERE status = 'active';

-- 00X_add_index_name.down.sql
DROP INDEX IF EXISTS idx_table_column;
DROP INDEX IF EXISTS idx_table_active;
```

---

### Adding a Materialized View

```sql
-- 00X_add_view_name.up.sql
CREATE MATERIALIZED VIEW view_name AS
SELECT 
    t1.id,
    t1.name,
    COUNT(t2.id) as count
FROM table1 t1
LEFT JOIN table2 t2 ON t1.id = t2.table1_id
GROUP BY t1.id, t1.name;

-- UNIQUE index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_view_name_pk 
    ON view_name (id);

COMMENT ON MATERIALIZED VIEW view_name IS 
    'Aggregated view for fast queries';

-- 00X_add_view_name.down.sql
DROP MATERIALIZED VIEW IF EXISTS view_name CASCADE;
```

---

### Altering Existing Tables

```sql
-- 00X_alter_table.up.sql

-- Add column (safe with default)
ALTER TABLE job_runs 
    ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;

-- Modify column (requires migration logic)
ALTER TABLE job_runs 
    ALTER COLUMN job_name TYPE VARCHAR(500);

-- Add constraint
ALTER TABLE job_runs 
    ADD CONSTRAINT check_retry_count 
    CHECK (retry_count >= 0 AND retry_count <= 10);

-- 00X_alter_table.down.sql

-- Remove constraint
ALTER TABLE job_runs 
    DROP CONSTRAINT IF EXISTS check_retry_count;

-- Revert column type (careful with data loss)
ALTER TABLE job_runs 
    ALTER COLUMN job_name TYPE VARCHAR(255);

-- Remove column
ALTER TABLE job_runs 
    DROP COLUMN IF EXISTS retry_count;
```

---

## Testing Migrations

### Manual Testing Checklist

Before committing new migration:

**1. Apply (fresh database):**
```bash
make run migrate drop
make run migrate up
# ✅ Should succeed, no errors
```

**2. Rollback:**
```bash
make run migrate down
# ✅ Should remove all changes
# ✅ No orphaned tables/indexes/constraints
```

**3. Reapply (idempotence):**
```bash
make run migrate up
make run migrate up  # Run twice!
# ✅ Second run should be no-op (idempotent)
```

**4. Data persistence:**
```bash
# Insert test data
psql $DATABASE_URL -c "INSERT INTO table_name VALUES (...)"

# Apply next migration
make run migrate up

# Verify data still exists
psql $DATABASE_URL -c "SELECT * FROM table_name"
# ✅ Data should be preserved
```

**5. Integration tests:**
```bash
go test ./migrations -v
go test ./internal/storage -v
# ✅ All tests should pass
```

---

### Automated Testing

**Add integration test for new migration:**

```go
// migrations/integration_test.go
func TestMigration00X_Description(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    ctx := context.Background()
    testDB := config.SetupTestDatabase(ctx, t)
    defer testDB.Connection.Close()
    
    // Verify table exists
    var exists bool
    err := testDB.Connection.QueryRowContext(ctx, `
        SELECT EXISTS(
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'plugins'
        )
    `).Scan(&exists)
    require.NoError(t, err)
    assert.True(t, exists, "Table plugins should exist after migration")
    
    // Verify indexes exist
    err = testDB.Connection.QueryRowContext(ctx, `
        SELECT EXISTS(
            SELECT 1 FROM pg_indexes 
            WHERE indexname = 'idx_plugins_status'
        )
    `).Scan(&exists)
    require.NoError(t, err)
    assert.True(t, exists, "Index idx_plugins_status should exist")
    
    // Test constraints work
    _, err = testDB.Connection.ExecContext(ctx, `
        INSERT INTO plugins (id, name, status) 
        VALUES ('test-plugin', 'Test Plugin', 'invalid_status')
    `)
    assert.Error(t, err, "Should reject invalid status values")
}
```

---

## Squashing Migrations

### When to Squash

**Appropriate times:**
- ✅ **Pre-alpha** (no production deployments)
- ✅ **Development** (no customers, no data)
- ✅ **Broken down migrations** (simpler to squash than fix)

**Inappropriate times:**
- ❌ **Post-release** (customers running old versions)
- ❌ **Production data exists** (data loss risk)
- ❌ **Multiple deployment environments** (version tracking complexity)

### How to Squash

**Example: Squash migrations 001-011 into single 001**

**Step 1: Create combined up migration**
```bash
# Create new 001_initial_schema.up.sql combining all up migrations
cat 001_*.up.sql 002_*.up.sql ... 011_*.up.sql > 001_squashed.up.sql

# Edit to remove duplicates, conflicts
# Preserve comments and documentation
```

**Step 2: Create combined down migration**
```bash
# Reverse order: Start with 011 down, end with 001 down
cat 011_*.down.sql ... 002_*.down.sql 001_*.down.sql > 001_squashed.down.sql

# Simplify: Just drop everything
echo "DROP SCHEMA public CASCADE;" > 001_squashed.down.sql
echo "CREATE SCHEMA public;" >> 001_squashed.down.sql
```

**Step 3: Archive old migrations**
```bash
mkdir -p .idea/archives/pre-squash-migrations
mv 001_*.sql 002_*.sql ... 011_*.sql .idea/archives/pre-squash-migrations/
```

**Step 4: Rename squashed files**
```bash
mv 001_squashed.up.sql 001_initial_schema.up.sql
mv 001_squashed.down.sql 001_initial_schema.down.sql
```

**Step 5: Test thoroughly**
```bash
# Rebuild migrator
make build migrator

# Test on clean database
make run migrate drop
make run migrate up
make run migrate down
make run migrate up

# Run all tests
go test ./migrations -v
go test ./internal/... -v
```

**Step 6: Document decision**

Create ADR (Architecture Decision Record):
```bash
# Example: docs/adr/002-squash-week-1-migrations.md
# Documents WHY squashing was appropriate
```

See [adr/002-squash-week-1-migrations.md](adr/002-squash-week-1-migrations.md) for real example.

---

## Migration Best Practices

### 1. Keep Migrations Small and Focused

**Good:**
```
✅ 002_add_plugins_table.sql        # One concern
✅ 003_add_plugin_indexes.sql       # One concern
✅ 004_add_plugin_health_checks.sql # One concern
```

**Bad:**
```
❌ 002_add_everything.sql  # Multiple concerns
    - Adds 5 tables
    - Adds 20 indexes
    - Adds views
    - Changes existing tables
```

**Why:** Smaller migrations are easier to review, test, and rollback.

---

### 2. Never Modify Applied Migrations

**Rule:** Once a migration is applied in ANY environment, **never modify it**.

**Bad:**
```bash
# ❌ Migration 002 already applied in dev
# ❌ You edit 002_add_plugins.up.sql to add more columns
# ❌ Database thinks it's at version 002 (won't reapply)
# ❌ New columns never get created
```

**Good:**
```bash
# ✅ Migration 002 already applied
# ✅ Create NEW migration 003_add_plugin_columns.sql
# ✅ Add columns in migration 003
# ✅ Migrator sees pending migration, applies it
```

**Exception:** Pre-alpha squashing (no production data, documented in ADR).

---

### 3. Test Constraints and Validation

**Add data validation to migrations:**

```sql
-- Add constraint
ALTER TABLE job_runs 
    ADD CONSTRAINT check_state_values 
    CHECK (current_state IN ('START', 'RUNNING', 'COMPLETE', 'FAIL', 'ABORT'));

-- Test constraint works (in migration!)
DO $$
BEGIN
    -- Try invalid value (should fail)
    PERFORM * FROM (
        SELECT 'INVALID'::VARCHAR AS state
        WHERE 'INVALID' NOT IN ('START', 'RUNNING', 'COMPLETE', 'FAIL', 'ABORT')
    ) valid_check;
    
    IF NOT FOUND THEN
        RAISE NOTICE 'State constraint validation passed';
    END IF;
END $$;
```

---

### 4. Preserve Data on Rollback (When Possible)

**Good (data-preserving rollback):**
```sql
-- 003_add_metadata.up.sql
ALTER TABLE job_runs 
    ADD COLUMN metadata JSONB DEFAULT '{}'::jsonb;

-- 003_add_metadata.down.sql
-- Preserve data in backup table before dropping
CREATE TABLE IF NOT EXISTS job_runs_metadata_backup AS
SELECT run_id, metadata FROM job_runs WHERE metadata IS NOT NULL;

ALTER TABLE job_runs DROP COLUMN IF EXISTS metadata;

COMMENT ON TABLE job_runs_metadata_backup IS 
    'Backup of metadata column before migration 003 rollback';
```

**When data loss is acceptable:**
```sql
-- down.sql
ALTER TABLE job_runs DROP COLUMN IF EXISTS metadata;
-- Data loss is acceptable on rollback (going backwards)
```

**Document decision:** Add comment explaining why data loss is OK.

---

### 5. Use Transactions Wisely

**DDL in PostgreSQL supports transactions:**

```sql
BEGIN;

CREATE TABLE plugins (...);
CREATE INDEX idx_plugins_status ON plugins(status);
ALTER TABLE job_runs ADD COLUMN plugin_id VARCHAR(100);

-- If anything fails, entire migration rolls back
COMMIT;
```

**But be careful:**
- Some DDL locks tables (ALTER TABLE)
- Long transactions can block other operations
- CONCURRENTLY operations cannot run in transactions

**For CONCURRENTLY operations:**
```sql
-- Cannot be in transaction
CREATE INDEX CONCURRENTLY idx_large_table ON large_table(column);
```

---

### 6. Index Naming Convention

**Follow consistent naming:**

```
idx_{table}_{columns}        # Regular index
idx_{table}_{column}_unique  # Unique index  
idx_{table}_{columns}_partial # Partial index
```

**Examples:**
```sql
idx_job_runs_temporal              # (started_at, current_state)
idx_api_keys_lookup_hash_unique    # Unique on key_lookup_hash
idx_api_keys_active                # Partial: WHERE active = TRUE
```

---

### 7. JSONB Columns Best Practices

**Use JSONB (not JSON) for better performance:**

```sql
-- Good
metadata JSONB DEFAULT '{}'::jsonb

-- Bad
metadata JSON DEFAULT '{}'
```

**Add GIN indexes for JSONB queries:**

```sql
CREATE INDEX idx_job_runs_metadata 
    ON job_runs USING GIN(metadata);
```

**Document JSONB schema in comments:**

```sql
COMMENT ON COLUMN job_runs.metadata IS 
    'OpenLineage RunEvent facets: {job_facets: {...}, run_facets: {...}, producer: "...", schema_url: "..."}';
```

---

## Migration Patterns

### Pattern 1: Adding a Feature Table

**Use case:** New feature needs dedicated table

**Example: Plugin health tracking**

```sql
-- 00X_add_plugin_health.up.sql
CREATE TABLE plugin_health_checks (
    id BIGSERIAL PRIMARY KEY,
    plugin_id VARCHAR(100) NOT NULL 
        REFERENCES plugins(id) ON DELETE CASCADE,
    
    health_status VARCHAR(50) NOT NULL 
        CHECK (health_status IN ('healthy', 'degraded', 'down')),
    
    response_time_ms INTEGER,
    error_message TEXT,
    
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_plugin_health_plugin 
    ON plugin_health_checks(plugin_id, checked_at DESC);

-- down.sql
DROP TABLE IF EXISTS plugin_health_checks CASCADE;
```

---

### Pattern 2: Adding Indexes for Performance

**Use case:** Queries are slow, need indexes

**Example: Speed up correlation queries**

```sql
-- 00X_optimize_correlation_queries.up.sql

-- Index for filtering by producer
CREATE INDEX IF NOT EXISTS idx_job_runs_producer 
    ON job_runs(producer_name) 
    WHERE producer_name IN ('dbt', 'airflow');

-- Composite index for common query pattern
CREATE INDEX IF NOT EXISTS idx_test_results_correlation 
    ON test_results(run_id, status, executed_at DESC);

-- down.sql
DROP INDEX IF EXISTS idx_job_runs_producer;
DROP INDEX IF EXISTS idx_test_results_correlation;
```

---

### Pattern 3: Materialized Views for Complex Queries

**Use case:** Complex JOIN queries are slow

**Example: Incident correlation view**

```sql
-- 00X_add_correlation_view.up.sql
CREATE MATERIALIZED VIEW incident_correlation AS
SELECT 
    tr.id AS test_result_id,
    tr.test_name,
    jr.run_id,
    jr.job_name,
    d.dataset_urn
FROM test_results tr
JOIN datasets d ON tr.dataset_urn = d.dataset_urn
JOIN lineage_edges le ON d.dataset_urn = le.dataset_urn
JOIN job_runs jr ON le.run_id = jr.run_id
WHERE tr.status IN ('failed', 'error')
    AND le.edge_type = 'output';

-- UNIQUE index (required for CONCURRENTLY refresh)
CREATE UNIQUE INDEX idx_incident_correlation_pk 
    ON incident_correlation (test_result_id);

-- Refresh function
CREATE OR REPLACE FUNCTION refresh_incident_correlation() 
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY incident_correlation;
END;
$$ LANGUAGE plpgsql;

-- down.sql
DROP FUNCTION IF EXISTS refresh_incident_correlation();
DROP MATERIALIZED VIEW IF EXISTS incident_correlation CASCADE;
```

---

## Performance Considerations

### Index Creation Strategies

**Small tables (<10K rows):**
```sql
CREATE INDEX idx_name ON table(column);  # Fast, OK to lock
```

**Large tables (>100K rows):**
```sql
CREATE INDEX CONCURRENTLY idx_name ON table(column);  # Slow, no locks
```

**Trade-off:**
- `CONCURRENTLY`: No locks (production-safe) but slower (2-10x)
- Regular: Fast but locks table (avoid in production)

---

### Materialized View Refresh Performance

**For views with <10K rows:**
```sql
REFRESH MATERIALIZED VIEW view_name;  # ~50-200ms
```

**For views with >100K rows:**
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY view_name;  # ~500ms-5s, no locks
-- Requires UNIQUE index!
```

**See:** [internal/storage/correlation_views_performance_test.go](../internal/storage/correlation_views_performance_test.go) for benchmark results.

---

## Migration Safety Checklist

Before committing migration:

- [ ] Both `.up.sql` and `.down.sql` files created
- [ ] Used `IF NOT EXISTS` / `IF EXISTS` for idempotence
- [ ] Tested apply on fresh database
- [ ] Tested rollback (down migration)
- [ ] Tested reapply (idempotence)
- [ ] Added validation queries
- [ ] Added comments and documentation
- [ ] Ran integration tests
- [ ] Rebuilt migrator (`make build migrator`)
- [ ] Verified migrator version (`./bin/migrator --version`)

---

## Related Documentation

- **Operating migrator**: [MIGRATOR.md](../docs/MIGRATOR.md) - CLI commands, Make targets, troubleshooting
- **Development setup**: [DEVELOPMENT.md](DEVELOPMENT.md) - Full environment setup
- **Squashing ADR**: [adr/002-squash-week-1-migrations.md](adr/002-squash-week-1-migrations.md)
- **Schema documentation**: [migrations/001_initial_openlineage_schema.up.sql](../migrations/001_initial_openlineage_schema.up.sql) - Inline documentation

---

**Last Updated:** November 21, 2025 - Session 24  
**Current Migration:** 001 (squashed from 001-011)  
**Next Migration:** 002 (when needed)

