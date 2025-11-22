# Migrator Technical Documentation

**Audience:** DevOps Engineers, Site Reliability Engineers, Backend Developers  
**Purpose:** Operating and troubleshooting the Correlator database migrator  
**Last Verified:** November 22, 2025 - All commands and outputs tested

---

## Table of Contents

1. [CLI Commands](#cli-commands)
2. [Make Targets](#make-targets)
3. [Environment Variables](#environment-variables)
4. [Developer Workflows](#developer-workflows)
5. [Critical Gotchas](#critical-gotchas)
6. [Testing](#testing)
7. [Troubleshooting](#troubleshooting)

---

## CLI Commands

### Available Commands

The migrator binary supports the following commands:

| Command        | Description                       | Safety             |
|----------------|-----------------------------------|--------------------|
| `up`           | Apply all pending migrations      | Safe               |
| `down`         | Rollback the last migration       | Safe               |
| `status`       | Show current migration status     | Read-only          |
| `version`      | Show current migration version    | Read-only          |
| `drop --force` | Drop all tables (**DESTRUCTIVE**) | Requires `--force` |

### Command Line Flags

| Flag        | Description                | Example                   |
|-------------|----------------------------|---------------------------|
| `--help`    | Show this help message     | `./migrator --help`       |
| `--version` | Show version information   | `./migrator --version`    |
| `--force`   | Force dangerous operations | `./migrator drop --force` |

### CLI Usage Examples

```shell
# Show help and available commands
./bin/migrator --help

# Show version information
./bin/migrator --version

# Apply all pending migrations
./bin/migrator up

# Show current migration status
./bin/migrator status

# Show current migration version
./bin/migrator version

# Rollback the last migration
./bin/migrator down

# Drop all tables (DESTRUCTIVE - development only)
./bin/migrator drop --force
```

### Safety: Drop Command

The `drop` command **destroys all data** and cannot be undone. It requires the `--force` flag:

```shell
# ❌ This will fail with safety error
./bin/migrator drop

# Error output:
# 2025/11/22 13:40:15 Migration failed: drop command requires --force flag for safety (this will destroy all data)

# ✅ This will actually drop all tables
./bin/migrator drop --force
```

**Production usage:** Never use `drop --force` in production. This is for development only.

---

## Make Targets

### Quick Reference

| Command                    | Description                       | When to Use                  |
|----------------------------|-----------------------------------|------------------------------|
| `make build migrator`      | Rebuild migrator binary           | After changing `.sql` files  |
| `make run migrate up`      | Apply all pending migrations      | After rebuild or first setup |
| `make run migrate status`  | Check migration status            | Debugging, verification      |
| `make run migrate version` | Show current version              | Version tracking             |
| `make run migrate down`    | Rollback last migration           | Undo mistakes                |
| `make run migrate drop`    | Drop all tables (**DESTRUCTIVE**) | Development only             |

### Environment Detection

The `make run migrate <command>` target **automatically detects** your environment:

**Inside dev container:**

```shell
make run migrate up
# → Assumes DATABASE_URL is set in environment
```

**From host machine:**

```shell
make run migrate up
# → Connects to PostgreSQL container in docker network
```

**Why this matters:**

- ✅ Same command works everywhere (dev container, host, CI/CD)
- ✅ No need to remember different commands per environment
- ✅ Automatic network configuration

### Detailed Make Target Behavior

#### `make build migrator`

**Purpose:** Rebuild migrator binary with latest embedded migrations

**When to use:**

- ✅ After modifying any `.sql` file in `/migrations`
- ✅ After changing migrator code (embed.go, runner.go, config.go)
- ✅ After git pull with migration changes

**Output:** `bin/migrator` (executable)

**Example:**

```shell
# Edit migration file
vim migrations/001_initial_openlineage_schema.up.sql

# MUST rebuild before running
make build migrator

# Now apply changes
make run migrate up
```

**Output from `make build migrator`:**

```shell
🔨 Building local migrator binary...
go build -ldflags "..." -o bin/migrator ./migrations
✅ Local migrator binary ready
```

#### `make run migrate up`

**Purpose:** Apply all pending migrations

**Behavior:**

1. Detects environment (dev container vs host)
2. Rebuilds migrator if needed
3. Validates embedded migrations
4. Connects to PostgreSQL
5. Applies pending migrations in order
6. Updates schema_migrations table

**Example output (when migrations applied):**

```shell
🔄 Applying database migrations...
2025/11/22 13:40:15 [MIGRATE] Applied migration 001
✅ Migration up completed via newly built binary
```

**Example output (when already up-to-date):**

```
2025/11/22 12:47:58 Starting migration up...
2025/11/22 12:47:58 No new migrations to apply
2025/11/22 12:47:58 [MIGRATE] Closing source and database
```

#### `make run migrate status`

**Purpose:** Check current migration state

**Output:**

```shell
2025/11/22 13:40:15 Migration Status: Version 1 (clean)
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v001
2025/11/22 13:40:15   Migrator Supports: v001
2025/11/22 13:40:15   Status: ✅ Up to date
```

**Interpretation:**

- `Version 1` - Currently at migration 001
- `(clean)` - Migration completed successfully (not stuck mid-migration)
- `(dirty)` - Migration failed partway (manual intervention needed)
- `Database Schema: v001` - Database is at version 1
- `Migrator Supports: v001` - Migrator has migrations up to version 1
- `Status: ✅ Up to date` - Database and migrator are in sync

#### `make run migrate version`

**Purpose:** Show current migration version

**Output:**

```shell
2025/11/22 13:40:15 Current Version: 1
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v001
2025/11/22 13:40:15   Migrator Supports: v001
2025/11/22 13:40:15   Status: ✅ Up to date
```

**Note:** Similar to `status` but focuses on version number. The `status` command shows additional information about
dirty state.

#### `make run migrate drop`

**Purpose:** Drop all database tables (**DESTRUCTIVE**)

**Safety:** Automatically includes `--force` flag

**When to use:**

- ✅ Development: Reset database to clean state
- ✅ Testing: Clean up after test runs
- ❌ **NEVER in production** (will destroy all data)

**Output:**

```shell
⚠️  Dropping all database tables...
2025/11/22 13:40:15 Pre-operation validation: checking embedded migrations...
2025/11/22 13:40:15 WARNING: Dropping all tables...
2025/11/22 13:40:15 All tables dropped successfully
✅ Migration drop completed via newly built binary
```

---

## Environment Variables

### Required Variables

| Variable       | Description                  |                                                          
|----------------|------------------------------|
| `DATABASE_URL` | PostgreSQL connection string |

### Optional Variables

| Variable          | Description                   | Default             |
|-------------------|-------------------------------|---------------------|
| `MIGRATION_TABLE` | Migration tracking table name | `schema_migrations` |
| `LOG_LEVEL`       | Logging verbosity             | `INFO`              |

### Setting Environment Variables

**Dev container (deployments/docker/.env):**

```shell
DATABASE_URL=postgres://correlator:correlator@postgres:5432/correlator?sslmode=disable # pragma: allowlist secret
MIGRATION_TABLE=schema_migrations
```

**Local terminal:**

```shell
export DATABASE_URL="postgres://correlator:correlator@localhost:5432/correlator?sslmode=disable" # pragma: allowlist secret
./bin/migrator up
```

**Docker Compose:**

```yaml
services:
  migrator:
    environment:
      - DATABASE_URL=postgres://correlator:correlator@postgres:5432/correlator?sslmode=disable # pragma: allowlist secret
```

---

## Developer Workflows

### Workflow 1: Daily Development (Inside Dev Container)

**Most common workflow:**

```shell
# 1. Edit migration file
vim migrations/001_initial_openlineage_schema.up.sql

# 2. Rebuild migrator (CRITICAL STEP!)
make build migrator

# 3. Apply migrations
make run migrate up

# 4. Verify success
make run migrate status

# 5. Test your changes
go test ./internal/storage -v
```

**Common mistake:** Forgetting step 2 (rebuild). See [Critical Gotchas](#critical-gotchas).

---

### Workflow 2: Testing Migration Changes

**Before committing migration changes:**

```bash
# 1. Rebuild migrator with changes
make build migrator

# 2. Drop database (clean slate)
make run migrate drop

# 3. Apply all migrations (fresh)
make run migrate up

# 4. Verify schema correct
make run migrate status

# 5. Test rollback (verify down migration works)
make run migrate down

# 6. Reapply (verify idempotence)
make run migrate up

# 7. Run integration tests
go test ./migrations -v
```

**Why this workflow:**

- ✅ Verifies up migration works
- ✅ Verifies down migration works
- ✅ Verifies idempotence (can apply twice safely)
- ✅ Catches migration bugs before commit

---

### Workflow 3: Containerized Development (From Host)

**When working from host machine (not in dev container):**

```bash
# 1. Start PostgreSQL
cd deployments/docker
docker-compose up -d postgres

# 2. Apply migrations (uses Docker network)
make run migrate up

# 3. Check status
make run migrate status
```

**Note:** Automatic environment detection handles Docker networking.

---

### Workflow 4: CI/CD Pipeline

**Status:** TODO - CI/CD workflows not yet implemented

**Implementation planned:** Week 2+ per implementation roadmap

**Will include:**

- GitHub Actions or GitLab CI workflow
- Test pipeline: Start PostgreSQL → Run migrations → Run tests → Cleanup
- Production deployment pipeline: Build migrator → Apply migrations → Verify → Deploy services
- Rollback procedures

**Example pattern (will be implemented later):**

```yaml
# Will be created in future session
# .github/workflows/test.yml or .gitlab-ci.yml
# - Test workflow
# - Deploy workflow
```

**Documentation will be updated when CI/CD is implemented.**

---

## Critical Gotchas

### 🔴 Gotcha #1: Forgot to Rebuild Migrator

**Symptom:** You changed a `.sql` file but migration didn't apply the new changes.

**Cause:** Migrations are embedded at compile time using `go:embed`. The binary contains the OLD version of your SQL
files.

**Solution:**

```bash
# Always rebuild after changing .sql files!
make build migrator
make run migrate up
```

**How to verify you have the latest version:**

```bash
./bin/migrator --version
```

**Output:**

```shell
2025/11/22 13:36:40 migrator vb3a44e4-dirty
2025/11/22 13:36:40 Git Commit: b3a44e4
2025/11/22 13:36:40 Build Time: 2025-11-22 12:36:25 UTC
2025/11/22 13:36:40 Max Schema Version: v001
2025/11/22 13:36:40 Database Migration Tool for Correlator
```

**Check:** `Build Time` should be AFTER your `.sql` file changes, and `Max Schema Version` should match your latest
migration number.

**Prevention:**

- ✅ Always run `make build migrator` after editing `.sql` files
- ✅ Add to your mental checklist: Edit → Rebuild → Migrate

**Estimated time lost if you forget:** 10-15 minutes of debugging why changes didn't apply.

---

### 🟡 Gotcha #2: Migration Version vs Database State

**Symptom:** `make run migrate up` says "no change" but you expected migrations to run.

**Cause:** Database schema_migrations table tracks version. Migrator sees database is already at target version.

**Diagnosis:**

```shell
# Check what database thinks current version is
make run migrate status
```

**Output:**

```shell
2025/11/22 13:40:15 Migration Status: Version 1 (clean)
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v001
2025/11/22 13:40:15   Migrator Supports: v001
2025/11/22 13:40:15   Status: ✅ Up to date
```

**Solution:**

```shell
# Option 1: Drop and reapply (development only)
make run migrate drop
make run migrate up

# Option 2: Manually reset version (advanced)
psql $DATABASE_URL -c "TRUNCATE schema_migrations"
make run migrate up
```

---

### 🟡 Gotcha #3: Dirty Migration State

**Symptom:** Migration says "dirty" and won't apply new migrations.

**Cause:** Previous migration failed mid-execution. Database is in inconsistent state.

**Diagnosis:**

```shell
make run migrate status
```

**Output when dirty:**

```shell
2025/11/22 13:40:15 Migration Status: Version 1 (dirty (needs manual intervention))
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v001
2025/11/22 13:40:15   Migrator Supports: v001
2025/11/22 13:40:15   Status: ✅ Up to date
```

**Solution:**

```shell
# Option 1: Drop and rebuild (safest, development only)
make run migrate drop
make run migrate up

# Option 2: Manual recovery (advanced)
# 1. Inspect what failed:
psql $DATABASE_URL -c "\d"  # List tables
psql $DATABASE_URL -c "SELECT * FROM schema_migrations"

# 2. Manually fix database to consistent state

# 3. Force version to clean:
psql $DATABASE_URL -c "UPDATE schema_migrations SET dirty = false WHERE version = 1"

# 4. Retry migration
make run migrate up
```

**Prevention:**

- ✅ Test migrations locally before committing
- ✅ Use transactions in migration files where possible
- ✅ Test rollback (down migrations) before deploying

---

### 🟢 Gotcha #4: Connection String Format

**Symptom:** `connection refused` or `could not connect to database`

**Common mistakes:**

```shell
# ❌ Wrong: Using localhost from inside Docker container
DATABASE_URL=postgres://user:pass@localhost:5432/db # pragma: allowlist secret

# ✅ Correct: Using service name from Docker Compose
DATABASE_URL=postgres://user:pass@postgres:5432/db # pragma: allowlist secret

# ❌ Wrong: Missing sslmode parameter
DATABASE_URL=postgres://user:pass@host:5432/db # pragma: allowlist secret

# ✅ Correct: Explicit sslmode (disable for dev)
DATABASE_URL=postgres://user:pass@host:5432/db?sslmode=disable # pragma: allowlist secret
```

**Solution:**
Check your environment and use correct connection string pattern.

---

## Testing

### Running Migrator Tests

**Unit tests only (fast, no Docker):**

```shell
go test -short ./migrations
```

**All tests including integration (requires Docker):**

```shell
go test ./migrations -v -timeout=5m
```

### Test Categories

**1. Unit Tests**

- Configuration validation
- Embedded migration validation
- Utility functions
- Run with `-short` flag (< 100ms)

**2. Integration Tests**

- Full migration workflow with real PostgreSQL (testcontainers)
- Up/down cycle testing
- Error scenario testing
- Requires Docker (2-5 seconds per test)

**3. CLI Tests**

- Command execution
- Flag parsing
- Error handling

**4. Benchmark Tests**

- Migration performance
- Large dataset migrations

### Test Files

```
migrations/
├── config_test.go        # Configuration validation tests
├── embed_test.go         # Embedded migration tests
├── runner_test.go        # Runner unit tests
├── integration_test.go   # Full workflow tests
└── benchmark_test.go     # Performance tests
```

### Running Specific Tests

**Verified test names from actual code:**

```shell
# Configuration tests
go test ./migrations -run TestConfig -v

# Integration workflow tests
go test ./migrations -run TestMigrationRunnerWorkFlow -v

# Drop command tests
go test ./migrations -run TestDropCommandIntegration -v

# Embedded migration tests
go test ./migrations -run TestValidateEmbeddedMigrations -v

# Benchmarks
go test ./migrations -bench=. -benchmem
```

---

## Troubleshooting

### Error: "no change"

**Full error output:**

```shell
2025/11/22 12:47:58 Starting migration up...
2025/11/22 12:47:58 No new migrations to apply
2025/11/22 12:47:58 [MIGRATE] Closing source and database
```

**Cause:** Database is already at latest migration version.

**Diagnosis:**

```shell
make run migrate status
```

**Expected output:**

```shell
2025/11/22 13:40:15 Migration Status: Version 1 (clean)
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v001
2025/11/22 13:40:15   Migrator Supports: v001
2025/11/22 13:40:15   Status: ✅ Up to date
```

**Solution:**

- If `Status: ✅ Up to date`: You're already current ✅
- If you expected changes: Did you rebuild? See [Gotcha #1](#gotcha-1-forgot-to-rebuild-migrator)
- If Database Schema < Migrator Supports: There ARE pending migrations (check error logs)

---

### Error: "migration file not found"

**Cause:** Migrator binary doesn't have embedded migrations (not rebuilt after changes).

**Solution:**

```shell
make build migrator  # Rebuild with latest migrations
make run migrate up
```

**Verify rebuild succeeded:**

```shell
./bin/migrator --version
```

**Check output:**

```shell
2025/11/22 13:36:40 Build Time: 2025-11-22 12:36:25 UTC  # ← Should be recent
2025/11/22 13:36:40 Max Schema Version: v001  # ← Should match latest .sql file
```

---

### Error: "Dirty database version X. Fix and force version"

**Cause:** Previous migration failed mid-execution. Database in inconsistent state.

**Diagnosis:**

```shell
make run migrate status
```

**Actual output when dirty:**

```shell
2025/11/22 13:40:15 Migration Status: Version 1 (dirty (needs manual intervention))
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v001
2025/11/22 13:40:15   Migrator Supports: v001
2025/11/22 13:40:15   Status: ✅ Up to date
```

**Recovery (Development):**

```shell
# Option 1: Drop and rebuild (safest, development only)
make run migrate drop
make run migrate up

# Option 2: Manual recovery (advanced)
# 1. Inspect what failed:
psql $DATABASE_URL -c "\d"  # List tables
psql $DATABASE_URL -c "SELECT * FROM schema_migrations"

# 2. Manually fix database to consistent state

# 3. Force version to clean:
psql $DATABASE_URL -c "UPDATE schema_migrations SET dirty = false WHERE version = 1"

# 4. Retry migration
make run migrate up
```

**Recovery (Production):**

- ⚠️ DO NOT use `drop` command
- ✅ Manually inspect database state
- ✅ Fix inconsistencies manually
- ✅ Update schema_migrations table
- ✅ Document incident in runbook

---

### Error: "connection refused"

**Full error output:**

```
2025/11/22 13:40:15 Failed to load configuration: pq: connection refused
```

**Cause:** Cannot connect to PostgreSQL database.

**Common causes:**

**1. PostgreSQL not running:**

```shell
# Check PostgreSQL is running
docker ps | grep postgres

# If not running, start it:
cd deployments/docker
docker-compose up -d postgres
```

**2. Wrong connection string:**

```shell
# Check environment variable
echo $DATABASE_URL

# Should be:
# Inside container: postgres://...@postgres:5432/...
# From host:        postgres://...@localhost:5432/...
```

**3. Network issues (Docker):**

```shell
# Verify PostgreSQL is accessible
psql $DATABASE_URL -c "SELECT 1"

# If fails, check Docker network:
docker network ls
docker network inspect correlator_default
```

---

### Error: "migration X is ahead of database"

**Cause:** Database has newer migration version than migrator binary supports.

**Scenario:**

```shell
Database: Version 005
Migrator: Only has migrations 001-003 embedded
```

**Output:**

```shell
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v005
2025/11/22 13:40:15   Migrator Supports: v003
2025/11/22 13:40:15   Status: ⚠️  Database schema newer than migrator supports
2025/11/22 13:40:15   Warning: Please update migrator tool to handle schema v005
```

**Solution:**

```shell
# Pull latest code with new migrations
git pull origin main

# Rebuild migrator
make build migrator

# Verify version now matches
./bin/migrator --version
# Check: Max Schema Version should now be v005

# Now apply (should be no-op if already at v005)
make run migrate up
```

**Prevention:** Always pull latest code before running migrations.

---

## Current Migration State

**Active migrations:**

```shell
migrations/
├── 001_initial_openlineage_schema.up.sql      # Complete schema (squashed from 001-011)
└── 001_initial_openlineage_schema.down.sql    # Rollback for entire schema
```

**What's in migration 001:**

- 9 tables (job_runs, datasets, lineage_edges, test_results, api_keys, etc.)
- 3 materialized views (incident_correlation_view, lineage_impact_analysis, recent_incidents_summary)
- 1 refresh function (refresh_correlation_views)
- 20+ indexes for performance
- OpenLineage v1.0 compliance

**History:** Squashed from migrations 001-011 in Session 23 (ADR-002).
See [docs/adr/002-squash-week-1-migrations.md](adr/002-squash-week-1-migrations.md) for rationale.

---

## Migration Versioning

### How Versioning Works

**Migration file format:**

```
001_initial_openlineage_schema.up.sql
^^^                                        
Migration number (determines order)
```

**Current state:**

- Latest migration: **001**
- Next migration: **002** (when needed)

**Checking version:**

```shell
# Show current database version
make run migrate version
```

**Output:**

```shell
2025/11/22 13:40:15 Current Version: 1
2025/11/22 13:40:15 Schema Compatibility:
2025/11/22 13:40:15   Database Schema: v001
2025/11/22 13:40:15   Migrator Supports: v001
2025/11/22 13:40:15   Status: ✅ Up to date
```

**Version tracking:**

- Stored in `schema_migrations` table
- Format: Integer (1, 2, 3...)
- File format: Zero-padded (001, 002, 003...)
- Mapping: Database version 1 = File 001

---

## Performance Characteristics

**Current migrations (001):**

- Apply time: ~200-500ms (cold start, creates 9 tables + 3 views + 20+ indexes)
- Rollback time: ~100-200ms (drops entire schema)
- Tables created: 9
- Indexes created: 20+
- Views created: 3 materialized

**Performance targets (after migrations applied):**

- Correlation queries: <10ms (P95)
- View refresh: <100ms (P95)
- Lineage impact: <20ms (P95)

See [internal/storage/correlation_views_performance_test.go](../internal/storage/correlation_views_performance_test.go)
for verified benchmark results.

---

## Advanced Usage

### Running Migrator Directly (No Make)

**Inside dev container:**

```shell
export DATABASE_URL="postgres://correlator:correlator@postgres:5432/correlator?sslmode=disable" # pragma: allowlist secret
./bin/migrator up
```

**From host (Docker):**

```shell
docker run --rm \
  --network correlator_default \
  -e DATABASE_URL="postgres://correlator:correlator@postgres:5432/correlator?sslmode=disable" \ # pragma: allowlist secret
  correlator/migrator:latest \
  up
```

### Custom Migration Table Name

**Use case:** Multiple schemas in same database

```shell
export MIGRATION_TABLE="correlator_schema_migrations"
./bin/migrator up
```

### Checking Migration Integrity

**Verify embedded migrations:**

```shell
# Migrator validates embedded migrations on startup
./bin/migrator --help
```

**Output includes validation:**

```
2025/11/22 13:36:41 migrator vb3a44e4-dirty - Database Migration Tool for Correlator
... (shows commands if validation passes)
```

**If validation fails, migrator exits with error before showing help.**

---

## Deployment

### Local Development Deployment

**Full stack (PostgreSQL + services):**

```shell
cd deployments/docker
docker-compose up -d
# Includes: PostgreSQL + automatic migrations via postgres-init/01-init.sql
```

**Note:** `docker-compose.yml` automatically applies migrations on PostgreSQL startup using init scripts.

### Production Deployment

**Status:** TODO - Production deployment patterns not yet finalized

**Implementation planned:** Week 20 (Enterprise Deployment phase) per implementation roadmap

**Will include:**

- Kubernetes Helm charts with init containers
- Docker standalone deployment
- Cloud provider templates (AWS ECS, GCP Cloud Run, Azure Container Apps)
- High availability patterns
- Blue-green deployment strategies
- Rollback procedures

**Documentation will be updated in Week 20 when deployment patterns are implemented.**

---

## Related Documentation

- **Writing migrations**: [MIGRATIONS.md](MIGRATIONS.md) - SQL patterns and best practices
- **Development setup**: [DEVELOPMENT.md](DEVELOPMENT.md) - Full environment setup
- **Architecture decisions**: [adr/002-squash-week-1-migrations.md](adr/002-squash-week-1-migrations.md)
- **Idempotency cleanup**: [IDEMPOTENCY-CLEANUP.md](IDEMPOTENCY-CLEANUP.md)
- **Main project README**: [../README.md](../README.md) - Correlator overview

---

**Last Updated:** November 22, 2025 - Session 24 (Verified)  
**Verification:** All commands and outputs tested against actual code  
**Current Migration:** 001 (squashed)  
**Next Update:** When CI/CD implemented (Week 2+) or Production deployment (Week 20)

