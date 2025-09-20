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
- **`drop`**: Drop all tables (destructive, use with caution)

## Testing Architecture

**Critical**: Tests must be run and added every time new migration features are added.

### Test Categories

1. **Unit Tests**: Configuration, embedded migration validation, utility functions
2. **Integration Tests**: Full migration workflow with real PostgreSQL using testcontainers
3. **SQL Error Tests**: Migration error handling using embedded test filesystems
4. **Benchmark Tests**: Performance testing of migration operations

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

### Docker Deployment

```yaml
# docker-compose.yml
services:
  migrator:
    build: .
    command: ./migrator up
    environment:
      DATABASE_URL: postgres://correlator:${DB_PASSWORD}@postgres:5432/correlator?sslmode=disable
    depends_on:
      - postgres
```

### Migration Safety

- **Pre-operation validation**: Every command validates embedded migrations first
- **Transaction safety**: DDL operations use appropriate transaction boundaries
- **Error recovery**: Clear error messages with guidance for resolution
- **Rollback support**: Every `up` migration has a corresponding `down` migration

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