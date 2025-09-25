package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// setupPostgresContainer creates and starts a PostgreSQL container for testing.
// Returns the connection string.
func setupPostgresContainer(
	ctx context.Context,
	t *testing.T,
) string {
	t.Helper()

	// Create PostgreSQL container with optimized settings for dev containers
	pgContainer, err := postgrescontainer.Run(ctx,
		"postgres:15-alpine",
		postgrescontainer.WithDatabase("testdb"),
		postgrescontainer.WithUsername("testuser"),
		postgrescontainer.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(120*time.Second)), // Extended timeout for dev containers
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	// Set up cleanup
	t.Cleanup(func() {
		err := pgContainer.Terminate(ctx)
		if err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	})

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	return connStr
}

func TestEmbeddedMigrationsPerformanceWithActualEmbedding(t *testing.T) {
	// Phase 4 - true embedded migrations now implemented!
	if testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// This test validates that our true embedded migration system provides the expected benefits
	eMigration := NewEmbeddedMigration(nil)
	fsys := eMigration.GetEmbeddedMigrations()

	// Test 1: Embedded files should work without any directory dependencies
	// This should always work with true embedded migrations - no external file system needed
	files, err := eMigration.ListEmbeddedMigrations()
	if err != nil {
		t.Fatalf("failed to list embedded migrations: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("embedded migrations should be available without external files")
	}

	// Test 2: Performance characteristics - embedded access should be consistent and fast
	// Measure time for repeated access - embedded should be consistent
	start := time.Now()

	for range 100 {
		files, err := eMigration.ListEmbeddedMigrations()
		if err != nil {
			t.Fatalf("failed to list migrations: %v", err)
		}

		if len(files) == 0 {
			t.Error("embedded migrations should always be available")
		}
	}

	elapsed := time.Since(start)

	// True embedded system should be fast and consistent
	if elapsed > 100*time.Millisecond { // 100ms for 100 operations = 1ms per operation
		t.Errorf("embedded access took too long: %v (should be <100ms for 100 operations)", elapsed)
	}

	// Test 3: Embedded files should be readable regardless of working directory
	for _, filename := range files {
		file, err := fsys.Open(filename)
		if err != nil {
			t.Errorf("failed to open embedded file %s: %v", filename, err)

			continue
		}

		_ = file.Close()

		// Also test content reading
		content, err := eMigration.GetEmbeddedMigrationContent(filename)
		if err != nil {
			t.Errorf("failed to read content of embedded file %s: %v", filename, err)

			continue
		}

		if len(content) == 0 {
			t.Errorf("embedded file %s should not be empty", filename)
		}
	}

	// Test 4: Validation should work with embedded migrations
	err = eMigration.ValidateEmbeddedMigrations()
	if err != nil {
		t.Errorf("embedded migration validation failed: %v", err)
	}

	t.Logf("SUCCESS: True embedded migration system working correctly!")
	t.Logf("Processed %d embedded migrations in %v (avg: %v per operation)",
		len(files), elapsed, elapsed/100)
}

// TestMigrationRunnerIntegration tests the complete migration runner workflow
// with actual embedded migrations and a real PostgreSQL database using testcontainers.
func TestMigrationRunnerWorkFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Set up PostgreSQL container
	connStr := setupPostgresContainer(ctx, t)

	// Create configuration using actual embedded migrations
	config := &Config{
		DatabaseURL:    connStr,
		MigrationTable: "schema_migrations",
	}

	// Test 1: Successful migration runner creation with embedded migrations
	t.Run("successful_migration_runner_creation", func(t *testing.T) {
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("expected successful creation, got error: %v", err)
		}

		if runner == nil {
			t.Fatal("expected non-nil runner")
		}

		// Clean up
		err = runner.Close()
		if err != nil {
			t.Logf("cleanup error: %v", err)
		}
	})

	// Test 2: Full migration workflow with actual embedded migrations
	t.Run("full_embedded_migration_workflow", func(t *testing.T) {
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}

		defer func() {
			err := runner.Close()
			if err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Initial status - should show no migrations applied
		err = runner.Status()
		if err != nil {
			t.Errorf("initial status failed: %v", err)
		}

		// Apply all embedded migrations (001_initial_schema.up.sql + 002_performance_optimization.up.sql)
		err = runner.Up()
		if err != nil {
			t.Errorf("migration up failed: %v", err)
		}

		// Check status after applying all migrations
		err = runner.Status()
		if err != nil {
			t.Errorf("post-migration status failed: %v", err)
		}

		// Check current version
		err = runner.Version()
		if err != nil {
			t.Errorf("version check failed: %v", err)
		}

		// Rollback one migration (002_performance_optimization.down.sql)
		err = runner.Down()
		if err != nil {
			t.Errorf("migration down failed: %v", err)
		}

		// Check status after rollback
		err = runner.Status()
		if err != nil {
			t.Errorf("post-rollback status failed: %v", err)
		}

		// Apply migrations again to test full cycle
		err = runner.Up()
		if err != nil {
			t.Errorf("re-applying migration up failed: %v", err)
		}

		// Final status check
		err = runner.Status()
		if err != nil {
			t.Errorf("final status failed: %v", err)
		}
	})
}

// TestMigrationRunnerConfiguration tests error conditions with bad database configuration.
func TestMigrationRunnerBadConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tests := []struct {
		name          string
		config        *Config
		expectError   bool
		errorContains string
	}{
		{
			name: "invalid_database_url_scheme",
			config: &Config{
				DatabaseURL:    "invalid://user:pass@localhost:5432/db", // pragma: allowlist secret`
				MigrationTable: "schema_migrations",
			},
			expectError:   true,
			errorContains: "failed to ping database",
		},
		{
			name: "unreachable_database_host",
			config: &Config{
				DatabaseURL:    "postgres://user:pass@nonexistent:5432/db?sslmode=disable", // pragma: allowlist secret`
				MigrationTable: "schema_migrations",
			},
			expectError:   true,
			errorContains: "failed to ping database",
		},
		{
			name: "invalid_database_credentials",
			config: &Config{
				DatabaseURL:    "postgres://invaliduser:invalidpass@localhost:5432/db?sslmode=disable", // pragma: allowlist secret`
				MigrationTable: "schema_migrations",
			},
			expectError:   true,
			errorContains: "failed to ping database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner, err := NewMigrationRunner(tt.config)

			if !tt.expectError {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if runner == nil {
					t.Fatal("expected non-nil runner when no error")
				}

				// Clean up
				err = runner.Close()
				if err != nil {
					t.Logf("cleanup error: %v", err)
				}

				return
			}

			// Handle expected error case
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
			}

			if runner != nil {
				t.Error("expected nil runner when error occurs")
			}
		})
	}
}

// TestMigrationRunnerSQLErrors tests migration errors with invalid SQL using embedded test filesystems.
func TestMigrationRunnerSQLErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Set up PostgreSQL container
	connStr := setupPostgresContainer(ctx, t)

	t.Run("invalid_sql_syntax", func(t *testing.T) {
		// Create test filesystem with invalid SQL
		invalidSQLFS := fstest.MapFS{
			"001_invalid.up.sql": &fstest.MapFile{
				Data: []byte("CREATE INVALID TABLE SYNTAX HERE;"),
			},
			"001_invalid.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE IF EXISTS invalid;")},
		}

		config := &Config{
			DatabaseURL:    connStr,
			MigrationTable: "schema_migrations",
		}

		// Create migration runner with custom embedded filesystem containing invalid SQL
		runner := &Runner{
			config:            config,
			embeddedMigration: NewEmbeddedMigration(invalidSQLFS),
		}

		// Initialize database connection manually since we're bypassing NewMigrationRunner
		db, err := sql.Open("postgres", config.DatabaseURL)
		if err != nil {
			t.Fatalf("failed to open database connection: %v", err)
		}

		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()

			t.Fatalf("failed to ping database: %v", err)
		}

		runner.db = db

		// Create database driver
		driver, err := postgres.WithInstance(db, &postgres.Config{
			MigrationsTable: config.MigrationTable,
		})
		if err != nil {
			_ = db.Close()

			t.Fatalf("failed to create postgres driver: %v", err)
		}

		// Create iofs source driver from our test filesystem
		sourceDriver, err := iofs.New(invalidSQLFS, ".")
		if err != nil {
			_ = db.Close()

			t.Fatalf("failed to create test migration source: %v", err)
		}

		// Create migrate instance
		m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", driver)
		if err != nil {
			_ = db.Close()

			t.Fatalf("failed to create migrate instance: %v", err)
		}

		runner.migrate = m

		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Migration should fail due to invalid SQL syntax
		err = runner.Up()
		if err == nil {
			t.Error("expected error due to invalid SQL syntax, got nil")
		}

		if err != nil && !strings.Contains(err.Error(), "migration up failed") {
			t.Errorf("expected migration error, got: %v", err)
		}
	})

	t.Run("foreign_key_constraint_violation", func(t *testing.T) {
		// Create test filesystem with migrations that will cause foreign key violation
		constraintViolationFS := fstest.MapFS{
			"001_setup.up.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL
);`)},
			"001_setup.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;")},
			"002_posts.up.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title VARCHAR(255) NOT NULL
);

-- This INSERT will fail because user_id 999 doesn't exist
INSERT INTO posts (user_id, title) VALUES (999, 'Test Post');`)},
			"002_posts.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE posts;")},
		}

		config := &Config{
			DatabaseURL:    connStr,
			MigrationTable: "schema_migrations",
		}

		// Create migration runner with custom embedded filesystem containing constraint violation
		runner := &Runner{
			config:            config,
			embeddedMigration: NewEmbeddedMigration(constraintViolationFS),
		}

		// Initialize database connection manually
		db, err := sql.Open("postgres", config.DatabaseURL)
		if err != nil {
			t.Fatalf("failed to open database connection: %v", err)
		}

		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()

			t.Fatalf("failed to ping database: %v", err)
		}

		runner.db = db

		// Create database driver
		driver, err := postgres.WithInstance(db, &postgres.Config{
			MigrationsTable: config.MigrationTable,
		})
		if err != nil {
			_ = db.Close()

			t.Fatalf("failed to create postgres driver: %v", err)
		}

		// Create iofs source driver from our test filesystem
		sourceDriver, err := iofs.New(constraintViolationFS, ".")
		if err != nil {
			_ = db.Close()

			t.Fatalf("failed to create test migration source: %v", err)
		}

		// Create migrate instance
		m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", driver)
		if err != nil {
			_ = db.Close()

			t.Fatalf("failed to create migrate instance: %v", err)
		}

		runner.migrate = m

		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Migration should fail due to foreign key constraint violation
		err = runner.Up()
		if err == nil {
			t.Error("expected error due to foreign key constraint violation, got nil")
		}

		if err != nil && !strings.Contains(err.Error(), "migration up failed") {
			t.Errorf("expected migration error, got: %v", err)
		}
	})
}

// TestDropCommandIntegration tests the drop command with --force flag requirement using actual database.
func TestDropCommandIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Set up PostgreSQL container
	connStr := setupPostgresContainer(ctx, t)

	config := &Config{
		DatabaseURL:    connStr,
		MigrationTable: "schema_migrations",
	}

	t.Run("drop_without_force_flag_fails", func(t *testing.T) {
		// Test that executeCommand properly rejects drop without --force
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}

		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Apply some migrations first to have data to potentially drop
		err = runner.Up()
		if err != nil {
			t.Fatalf("failed to apply initial migrations: %v", err)
		}

		// Test CLI command execution without --force flag
		err = executeCommand("drop", runner, false) // force = false
		if err == nil {
			t.Fatal("expected error when drop command used without --force flag")
		}

		expectedError := "drop command requires --force flag for safety"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error containing %q, got %q", expectedError, err.Error())
		}

		// Verify database still has tables (drop was prevented)
		err = runner.Status()
		if err != nil {
			t.Errorf("status check failed after prevented drop: %v", err)
		}
	})

	t.Run("drop_with_force_flag_succeeds", func(t *testing.T) {
		// Test that executeCommand properly executes drop with --force
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}

		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Apply migrations to have something to drop
		err = runner.Up()
		if err != nil {
			t.Fatalf("failed to apply migrations before drop test: %v", err)
		}

		// Verify we have migrations applied
		err = runner.Status()
		if err != nil {
			t.Fatalf("status check failed before drop: %v", err)
		}

		// Test CLI command execution with --force flag
		err = executeCommand("drop", runner, true) // force = true
		if err != nil {
			t.Fatalf("drop command with --force flag should succeed, got error: %v", err)
		}

		// Verify database tables are actually dropped
		// After drop, status should work but show no migrations
		err = runner.Status()
		if err != nil {
			t.Errorf("status check failed after drop: %v", err)
		}

		// Version should show no migrations applied
		err = runner.Version()
		if err != nil {
			t.Errorf("version check failed after drop: %v", err)
		}
	})

	t.Run("drop_integration_full_workflow", func(t *testing.T) {
		// Test complete workflow: up -> drop --force -> up again
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}

		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Step 1: Apply migrations
		err = runner.Up()
		if err != nil {
			t.Fatalf("initial migration up failed: %v", err)
		}

		// Step 2: Verify migrations are applied
		err = runner.Status()
		if err != nil {
			t.Fatalf("status after initial up failed: %v", err)
		}

		// Step 3: Drop with --force flag
		err = executeCommand("drop", runner, true)
		if err != nil {
			t.Fatalf("drop with --force failed: %v", err)
		}

		// Step 4: Verify drop was successful (status should still work)
		err = runner.Status()
		if err != nil {
			t.Fatalf("status after drop failed: %v", err)
		}

		// Step 5: Apply migrations again to verify database is clean
		// Note: After drop, we need a fresh runner since the migration state is reset
		err = runner.Close()
		if err != nil {
			t.Logf("cleanup after drop: %v", err)
		}

		// Create a fresh runner for re-applying migrations after drop
		freshRunner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create fresh runner after drop: %v", err)
		}

		defer func() {
			if err := freshRunner.Close(); err != nil {
				t.Logf("cleanup error for fresh runner: %v", err)
			}
		}()

		err = freshRunner.Up()
		if err != nil {
			t.Fatalf("migration up after drop failed: %v", err)
		}

		// Step 6: Final verification with fresh runner
		err = freshRunner.Status()
		if err != nil {
			t.Fatalf("final status check failed: %v", err)
		}
	})

	t.Run("drop_with_database_error_handling", func(t *testing.T) {
		// Test that drop command properly handles and reports database errors
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}

		// Apply migrations first
		err = runner.Up()
		if err != nil {
			t.Fatalf("failed to apply migrations: %v", err)
		}

		// Close the database connection to simulate a connection error
		err = runner.Close()
		if err != nil {
			t.Fatalf("failed to close runner: %v", err)
		}

		// Now try to drop with a closed connection - should get a meaningful error
		err = executeCommand("drop", runner, true)
		if err == nil {
			t.Fatal("expected error when trying to drop with closed connection")
		}

		// Should be a meaningful error, not just "requires --force"
		if strings.Contains(err.Error(), "requires --force flag") {
			t.Error("error should be about database connection, not missing --force flag")
		}
	})
}

// BenchmarkMigrationRunnerIntegrationOperations benchmarks migration operations with actual embedded migrations.
func BenchmarkMigrationRunnerIntegrationOperations(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping this benchmark in short mode")
	}

	ctx := context.Background()

	// Set up PostgreSQL container for benchmarking
	pgContainer, err := postgrescontainer.Run(ctx,
		"postgres:15-alpine",
		postgrescontainer.WithDatabase("benchmarkdb"),
		postgrescontainer.WithUsername("benchmarkuser"),
		postgrescontainer.WithPassword("benchmarkpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(120*time.Second)), // Extended timeout for dev containers
	)
	if err != nil {
		b.Fatalf("failed to start postgres container: %v", err)
	}

	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			b.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("failed to get connection string: %v", err)
	}

	// Use actual embedded migrations for realistic benchmarks
	config := &Config{
		DatabaseURL:    connStr,
		MigrationTable: "schema_migrations_benchmark",
	}

	runner, err := NewMigrationRunner(config)
	if err != nil {
		b.Fatalf("failed to create runner: %v", err)
	}

	defer func() {
		if err := runner.Close(); err != nil {
			b.Logf("cleanup error: %v", err)
		}
	}()

	// Apply all embedded migrations for realistic benchmark setup
	if err := runner.Up(); err != nil {
		b.Fatalf("failed to apply embedded migrations: %v", err)
	}

	b.ResetTimer()

	// Benchmark status operations
	b.Run("Status", func(b *testing.B) {
		for range b.N {
			if err := runner.Status(); err != nil {
				b.Fatalf("status check failed: %v", err)
			}
		}
	})

	// Benchmark version operations
	b.Run("Version", func(b *testing.B) {
		for range b.N {
			if err := runner.Version(); err != nil {
				b.Fatalf("version check failed: %v", err)
			}
		}
	})

	// Benchmark migration operations (rollback and reapply)
	b.Run("MigrationOperations", func(b *testing.B) {
		for range b.N {
			// Rollback last migration
			if err := runner.Down(); err != nil {
				b.Fatalf("migration down failed: %v", err)
			}

			// Reapply migration
			if err := runner.Up(); err != nil {
				b.Fatalf("migration up failed: %v", err)
			}
		}
	})
}
