# Correlator Database Migrator

The Correlator database migrator is a **zero-config deployment** migration tool that uses embedded SQL migrations for seamless database schema management. It ensures reliable, production-ready database migrations with no external file dependencies.

## Zero-Config Deployment Architecture

The migrator is designed around Correlator's core principle of **zero-config deployment**:

- ✅ **No file system dependencies**: Migrations are embedded at compile time using Go's `go:embed`
- ✅ **Single binary deployment**: All migrations bundled into the executable
- ✅ **Production-ready out-of-the-box**: No configuration files, directories, or external dependencies
- ✅ **Docker-friendly**: Supports `docker-compose up` for <30 minute production deployment

This eliminates common deployment issues like missing migration files, incorrect permissions, or path dependencies.

## Why Migrator is Co-located with Migration Files

**Design Decision**: The migrator code is co-located in `/migrations` instead of the conventional `/cmd` structure.

### Benefits of Co-location

1. **Cohesion**: Migration logic and migration files are maintained together as a single unit
2. **Dependency Injection**: Easy access to embedded migrations for testing and validation
3. **Test Architecture**: Integration tests can use the same embedded migrations as production
4. **Development Workflow**: Changes to migrations and migrator logic are versioned together
5. **Zero-Config Principle**: Self-contained migration module with no external dependencies

### Trade-offs Accepted

- **Convention**: Deviates from typical Go project structure (`cmd/` for executables)
- **Package Organization**: Migrator becomes a specialized module rather than a general-purpose command

The benefits outweigh the trade-offs for Correlator's **correlation-first, zero-config** architecture.

## How It Works

### Architecture Flow

```
1. Startup → Embedded Migration Validation → Database Connection → Migration Execution
```

### Technical Implementation

1. **Embedded Migrations Loading**:
   ```go
   //go:embed *.sql
   var embeddedMigrations embed.FS
   ```
   - Only `*.sql` files are embedded at compile time
   - Files are validated for naming convention (`001_name.up.sql`, `001_name.down.sql`)

2. **Startup Validation** (`ValidateEmbeddedMigrations`):
   - **File naming validation**: Ensures correct migration file naming pattern
   - **Sequence validation**: Checks for gaps or duplicates in migration sequence
   - **Content validation**: Verifies files are readable and non-empty
   - **Checksum validation**: Ensures migration file integrity

3. **Pre-Operation Validation**:
   - Every `Up()`, `Down()`, and `Drop()` command runs validation before execution
   - Prevents execution with corrupted or invalid migrations
   - Ensures production reliability

4. **Migration Execution**:
   - Uses `golang-migrate/migrate` with embedded filesystem driver (`iofs`)
   - PostgreSQL-optimized with proper transaction handling
   - Comprehensive error handling and logging

### Migration File Structure

```
migrations/
├── 001_initial_schema.up.sql          # Core correlator database schema
├── 001_initial_schema.down.sql        # Rollback for initial schema
├── 002_performance_optimization.up.sql # Performance indexes and materialized views
├── 002_performance_optimization.down.sql # Rollback for performance optimization
├── embed.go                           # Embedded migration management
├── runner.go                          # Migration runner implementation
├── config.go                          # Configuration and validation
└── main.go                           # CLI interface
```

### Available Commands

- **`up`**: Apply all pending migrations
- **`down`**: Rollback the last migration
- **`status`**: Show current migration status
- **`version`**: Show current migration version
- **`drop --force`**: Drop all tables (**DESTRUCTIVE** - requires `--force` flag for safety)

### Command Line Flags

- **`--help`**: Show help information and usage examples
- **`--version`**: Show migrator version information
- **`--force`**: Force dangerous operations without confirmation (**required for `drop` command**)

### CLI Usage Examples

```bash
# Show help and available commands
./migrator --help

# Show version information
./migrator --version

# Apply all pending migrations
./migrator up

# Show current migration status
./migrator status

# Show current migration version
./migrator version

# Rollback the last migration
./migrator down

# Drop all tables (DESTRUCTIVE - use with extreme caution)
./migrator drop --force
```

**⚠️ SAFETY WARNING**: The `drop` command **destroys all data** and cannot be undone. It requires the `--force` flag as a safety mechanism:

```bash
# ❌ This will fail with safety error
./migrator drop

# ✅ This will actually drop all tables
./migrator drop --force
```

## Testing Architecture

**Critical**: Tests must be run and added every time new migration features are added.

### Test Categories

1. **Unit Tests**: Configuration, embedded migration validation, utility functions
2. **Integration Tests**: Full migration workflow with real PostgreSQL using testcontainers
   - **Drop Command Integration**: Comprehensive testing of `--force` flag safety mechanism
   - **Database Lifecycle**: Complete drop → recovery workflow validation
   - **Error Scenarios**: Connection failures, database unavailability
3. **CLI Tests**: Command execution, flag parsing, error handling
4. **SQL Error Tests**: Migration error handling using embedded test filesystems
5. **Benchmark Tests**: Performance testing of migration operations

### Running Tests in Dev Container

```bash
# Unit tests only
go test -short ./migrations

# All tests including integration (requires testcontainers)
go test ./migrations -v -timeout=5m

# Specific test categories
go test ./migrations -run "TestMigrationRunnerWorkFlow" -v
go test ./migrations -run "TestMigrationRunnerSQLErrors" -v
go test ./migrations -run "TestMigrationRunnerBadConfiguration" -v
go test ./migrations -run "TestDropCommandIntegration" -v
go test ./migrations -run "TestExecuteCommand" -v

# Benchmarks
go test ./migrations -bench="Benchmark" -benchmem -v

# Using Make targets
make test-unit
make test-integration
```

### Testing Best Practices

- ✅ **Use actual embedded migrations**: Integration tests use production migration files
- ✅ **Never use dummy migrations**: Test against real SQL to catch production issues early
- ✅ **Test error conditions**: Use `fstest.MapFS` for test cases that don't need to use the real migration files.
- ✅ **Validate in CI**: All tests run in GitHub Actions with Docker services

## Production Usage

### Environment Variables

```bash
DATABASE_URL="postgres://user:***@host:5432/db?sslmode=disable"
MIGRATION_TABLE="schema_migrations"  # Optional, defaults to "schema_migrations"
```

### Production Usage with Make Commands

**Apply all pending migrations:**
```bash
make run migrate up
```

**Check migration status:**
```bash
make run migrate status
```

**Show current migration version:**
```bash
make run migrate version
```

**Rollback last migration:**
```bash
make run migrate down
```

**Drop all tables (DESTRUCTIVE - development only):**
```bash
make run migrate drop
```

**Note**: These commands work consistently across all environments:
- ✅ **Local development**: Uses local binary or builds one automatically
- ✅ **Dev container**: Uses containerized environment with proper networking
- ✅ **Production**: Uses Docker Compose with database connectivity
- ✅ **Zero-config**: Automatically detects environment and handles setup


### Migration Safety

The migrator implements multiple layers of safety mechanisms:

#### **Operational Safety**
- **Pre-operation validation**: Every command validates embedded migrations first
- **Transaction safety**: DDL operations use appropriate transaction boundaries
- **Error recovery**: Clear error messages with guidance for resolution
- **Rollback support**: Every `up` migration has a corresponding `down` migration

#### **Command Safety**
- **`--force` Flag Requirement**: Destructive operations require explicit `--force` flag
  - **`drop` command**: Cannot be executed without `--force` flag
  - **Safety Error**: `drop command requires --force flag for safety (this will destroy all data)`
  - **Non-interactive**: Works in Docker containers and CI pipelines (no interactive prompts)

#### **Error Handling**
- **Modern Error Handling**: Uses Go 1.20+ `errors.Join()` for cleaner multi-error reporting
- **Comprehensive Logging**: Detailed operation logging with `[MIGRATE]` prefixes
- **Connection Management**: Proper database connection lifecycle with cleanup

## Development Workflow

1. **Add new migration files**: Follow naming convention `00X_description.{up,down}.sql`
2. **Update tests**: Add tests for new migration functionality
3. **Run full test suite**: Ensure integration tests pass with new migrations
4. **Validate in dev container**: Test complete migration workflow locally
5. **CI validation**: GitHub Actions runs all tests with Docker services

## Performance Characteristics

The current embedded migrations include:

- **001_initial_schema**: Complete correlator database schema (datasets, job_runs, test_results, lineage_edges)
- **002_performance_optimization**: 10 specialized indexes, 4 materialized views, monitoring functions

**Target Performance**:
- <5 minute correlation latency (test failure → incident correlation)
- <200ms incident dashboard queries
- <1s complex lineage traversal
- >90% correlation accuracy tracking

The migrator's embedded architecture ensures these performance optimizations are consistently deployed across all environments.