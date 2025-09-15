# Migration Runner

The migration runner is a production-ready database migration system built on golang-migrate v4.19.0 with PostgreSQL support.

## Architecture

- **Configuration Management**: Environment-variable based configuration with validation
- **Embedded Migration Support**: File-based migrations with comprehensive validation (Phase 4 will add true embedded migrations)
- **Migration Runner**: Interface-based design with golang-migrate integration
- **Error Handling**: Production-ready error handling with proper error wrapping

## Running Tests

### Unit Tests

Unit tests run fast and don't require external dependencies:

```bash
# Run all unit tests
go test ./cmd/migrator -short -v

# Run specific unit test
go test ./cmd/migrator -short -run TestMigrationRunnerUp -v
```

### Integration Tests

Integration tests use testcontainers to spin up real PostgreSQL databases and test end-to-end functionality:

```bash
# Run all tests including integration tests (requires Docker)
go test ./cmd/migrator -v -timeout=5m

# Run only integration tests (requires Docker)
go test ./cmd/migrator -run TestMigrationRunnerIntegration -v -timeout=5m

# Run integration tests for error conditions
go test ./cmd/migrator -run TestMigrationRunnerErrorConditions -v -timeout=5m
```

**Requirements for Integration Tests:**
- Docker must be installed and running
- Docker daemon must be accessible (not rootless Docker)
- Sufficient timeout (5+ minutes) for container startup

### Integration Test Coverage

The integration tests cover scenarios that require real database connectivity:

1. **TestMigrationRunnerIntegration**: Complete migration workflow with real PostgreSQL
2. **TestMigrationRunnerErrorConditions**: Error conditions that can't be tested in unit tests
3. **TestMigrationRunnerWithRealPostgreSQL**: PostgreSQL-specific error scenarios
4. **TestMigrationRunnerSQLErrors**: Invalid SQL and constraint violation handling
5. **TestMigrationRunnerIntegrationConcurrency**: Concurrent access testing
6. **BenchmarkMigrationRunnerIntegrationOperations**: Performance benchmarking

## Error Conditions Tested

### Unit Tests (with mocks)
- Configuration validation
- Migration runner interface compliance
- Error handling and recovery
- Resource management
- Business logic validation

### Integration Tests (with real database)
- Database connectivity failures
- Invalid database URLs
- Missing migrations directories  
- SQL syntax errors
- Foreign key constraint violations
- PostgreSQL driver errors
- Migration file validation

## Configuration

The migration runner uses environment variables for configuration:

```bash
export DATABASE_URL="postgres://user:password@localhost:5432/dbname?sslmode=disable"
export MIGRATIONS_PATH="./migrations"  # Optional, defaults to "./migrations"
export MIGRATION_TABLE="schema_migrations"  # Optional, defaults to "schema_migrations"
```

## Usage

```go
config, err := LoadConfig()
if err != nil {
    log.Fatal(err)
}

runner, err := NewMigrationRunner(config)
if err != nil {
    log.Fatal(err)
}
defer runner.Close()

// Apply migrations
if err := runner.Up(); err != nil {
    log.Fatal(err)
}

// Check status
if err := runner.Status(); err != nil {
    log.Fatal(err)
}
```

## Migration File Format

Migration files must follow the strict naming convention:

```
001_initial_schema.up.sql    # Up migration
001_initial_schema.down.sql  # Down migration
002_add_users.up.sql         # Next up migration  
002_add_users.down.sql       # Next down migration
```

- **Sequential numbering**: 001, 002, 003, etc.
- **Paired migrations**: Every up migration must have a corresponding down migration
- **No gaps**: Migration sequence must be continuous
- **Strict naming**: `NNN_description.(up|down).sql` format only

## Phase 3 vs Phase 4

**Phase 3 (Current)**: File-based migrations using `os.DirFS`
- Migrations read from filesystem at runtime
- Comprehensive validation and error handling
- Production-ready for file-based deployments

**Phase 4 (Future)**: True embedded migrations using `//go:embed`
- Migrations compiled into binary
- Zero external dependencies
- Checksum validation for integrity
- Faster startup and execution

The current implementation is designed for seamless migration to Phase 4 embedded migrations.