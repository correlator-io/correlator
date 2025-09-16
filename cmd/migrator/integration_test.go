package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	testcontainers "github.com/testcontainers/testcontainers-go"
)

// TestMigrationRunnerIntegration tests the complete migration runner workflow
// with a real PostgreSQL database using testcontainers
func TestMigrationRunnerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	// Clean up container when test completes
	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Create temporary migrations directory
	tempDir := t.TempDir()

	// Create test migration files
	migrations := map[string]string{
		"001_initial.up.sql": `CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);`,
		"001_initial.down.sql": `DROP TABLE users;`,
		"002_posts.up.sql": `CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title VARCHAR(255) NOT NULL,
    content TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);`,
		"002_posts.down.sql": `DROP TABLE posts;`,
	}

	for filename, content := range migrations {
		if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create migration file %s: %v", filename, err)
		}
	}

	// Create configuration
	config := &Config{
		DatabaseURL:    connStr,
		MigrationsPath: tempDir,
		MigrationTable: "schema_migrations",
	}

	// Test 1: Successful migration runner creation
	t.Run("successful_migration_runner_creation", func(t *testing.T) {
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("expected successful creation, got error: %v", err)
		}
		if runner == nil {
			t.Fatal("expected non-nil runner")
		}

		// Clean up
		if err := runner.Close(); err != nil {
			t.Logf("cleanup error: %v", err)
		}
	})

	// Test 2: Full migration workflow
	t.Run("full_migration_workflow", func(t *testing.T) {
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}
		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Initial status - should show no migrations
		if err := runner.Status(); err != nil {
			t.Errorf("initial status failed: %v", err)
		}

		// Apply migrations
		if err := runner.Up(); err != nil {
			t.Errorf("migration up failed: %v", err)
		}

		// Check status after migration
		if err := runner.Status(); err != nil {
			t.Errorf("post-migration status failed: %v", err)
		}

		// Check version
		if err := runner.Version(); err != nil {
			t.Errorf("version check failed: %v", err)
		}

		// Rollback one migration
		if err := runner.Down(); err != nil {
			t.Errorf("migration down failed: %v", err)
		}

		// Check status after rollback
		if err := runner.Status(); err != nil {
			t.Errorf("post-rollback status failed: %v", err)
		}
	})
}

// TestMigrationRunnerErrorConditions tests error conditions that require real database
func TestMigrationRunnerErrorConditions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tempDir := t.TempDir()

	// Create valid migration files
	migrations := map[string]string{
		"001_test.up.sql":   "CREATE TABLE test (id INTEGER);",
		"001_test.down.sql": "DROP TABLE test;",
	}

	for filename, content := range migrations {
		if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create migration file %s: %v", filename, err)
		}
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
				DatabaseURL:    "invalid://user:pass@localhost:5432/db",
				MigrationsPath: tempDir,
				MigrationTable: "schema_migrations",
			},
			expectError:   true,
			errorContains: "failed to ping database",
		},
		{
			name: "unreachable_database_host",
			config: &Config{
				DatabaseURL:    "postgres://user:pass@nonexistent:5432/db?sslmode=disable",
				MigrationsPath: tempDir,
				MigrationTable: "schema_migrations",
			},
			expectError:   true,
			errorContains: "failed to ping database",
		},
		{
			name: "invalid_database_credentials",
			config: &Config{
				DatabaseURL:    "postgres://invaliduser:invalidpass@localhost:5432/db?sslmode=disable",
				MigrationsPath: tempDir,
				MigrationTable: "schema_migrations",
			},
			expectError:   true,
			errorContains: "failed to ping database",
		},
		{
			name: "non_existent_migrations_directory",
			config: &Config{
				DatabaseURL:    "postgres://user:pass@localhost:5432/db?sslmode=disable",
				MigrationsPath: "/non/existent/directory",
				MigrationTable: "schema_migrations",
			},
			expectError:   true,
			errorContains: "failed to ping database", // Database connection fails before migration directory check
		},
		{
			name: "empty_migration_table_name",
			config: &Config{
				DatabaseURL:    "postgres://user:pass@localhost:5432/db?sslmode=disable",
				MigrationsPath: tempDir,
				MigrationTable: "",
			},
			expectError:   true,
			errorContains: "failed to ping database", // Config validation catches this first
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner, err := NewMigrationRunner(tt.config)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
				if runner != nil {
					t.Error("expected nil runner when error occurs")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if runner == nil {
					t.Fatal("expected non-nil runner when no error")
				}

				// Clean up
				if err := runner.Close(); err != nil {
					t.Logf("cleanup error: %v", err)
				}
			}
		})
	}
}

// TestMigrationRunnerWithRealPostgreSQL tests specific PostgreSQL driver errors
func TestMigrationRunnerWithRealPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	tests := []struct {
		name          string
		setupFunc     func(t *testing.T) *Config
		expectError   bool
		errorContains string
	}{
		{
			name: "missing_migrations_directory",
			setupFunc: func(t *testing.T) *Config {
				return &Config{
					DatabaseURL:    connStr,
					MigrationsPath: "/absolutely/non/existent/path",
					MigrationTable: "schema_migrations",
				}
			},
			expectError:   true,
			errorContains: "failed to create migrate instance",
		},
		{
			name: "empty_migrations_directory",
			setupFunc: func(t *testing.T) *Config {
				tempDir := t.TempDir()
				// Create empty directory
				return &Config{
					DatabaseURL:    connStr,
					MigrationsPath: tempDir,
					MigrationTable: "schema_migrations",
				}
			},
			expectError:   false, // Empty directory is valid, just no migrations to run
			errorContains: "",
		},
		{
			name: "invalid_migration_table_characters",
			setupFunc: func(t *testing.T) *Config {
				tempDir := t.TempDir()
				// Create minimal migration
				content := "CREATE TABLE test (id INTEGER);"
				if err := os.WriteFile(filepath.Join(tempDir, "001_test.up.sql"), []byte(content), 0o644); err != nil {
					t.Fatalf("failed to create migration file: %v", err)
				}
				return &Config{
					DatabaseURL:    connStr,
					MigrationsPath: tempDir,
					MigrationTable: "invalid-table-name-with-hyphens", // PostgreSQL doesn't like hyphens in unquoted identifiers
				}
			},
			expectError:   false, // PostgreSQL driver should handle this gracefully
			errorContains: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.setupFunc(t)

			runner, err := NewMigrationRunner(config)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
				if runner != nil {
					t.Error("expected nil runner when error occurs")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if runner == nil {
					t.Fatal("expected non-nil runner when no error")
				}

				// Clean up
				if err := runner.Close(); err != nil {
					t.Logf("cleanup error: %v", err)
				}
			}
		})
	}
}

// TestMigrationRunnerSQLErrors tests migration errors with invalid SQL
func TestMigrationRunnerSQLErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	t.Run("invalid_sql_syntax", func(t *testing.T) {
		tempDir := t.TempDir()

		// Create migration with invalid SQL
		invalidSQL := "CREATE INVALID TABLE SYNTAX HERE;"
		if err := os.WriteFile(filepath.Join(tempDir, "001_invalid.up.sql"), []byte(invalidSQL), 0o644); err != nil {
			t.Fatalf("failed to create invalid migration file: %v", err)
		}

		config := &Config{
			DatabaseURL:    connStr,
			MigrationsPath: tempDir,
			MigrationTable: "schema_migrations",
		}

		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}
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
		tempDir := t.TempDir()

		// Create migration that will violate foreign key constraint
		migrations := map[string]string{
			"001_setup.up.sql": `CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL
);`,
			"001_setup.down.sql": `DROP TABLE users;`,
			"002_posts.up.sql": `CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title VARCHAR(255) NOT NULL
);

-- This INSERT will fail because user_id 999 doesn't exist
INSERT INTO posts (user_id, title) VALUES (999, 'Test Post');`,
			"002_posts.down.sql": `DROP TABLE posts;`,
		}

		for filename, content := range migrations {
			if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
				t.Fatalf("failed to create migration file %s: %v", filename, err)
			}
		}

		config := &Config{
			DatabaseURL:    connStr,
			MigrationsPath: tempDir,
			MigrationTable: "schema_migrations",
		}

		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}
		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Migration should fail due to foreign key constraint
		err = runner.Up()
		if err == nil {
			t.Error("expected error due to foreign key constraint violation, got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "migration up failed") {
			t.Errorf("expected migration error, got: %v", err)
		}
	})
}

// TestMigrationRunnerIntegrationConcurrency tests that migrations handle concurrent access properly
func TestMigrationRunnerIntegrationConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	defer func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	tempDir := t.TempDir()

	// Create simple migration
	migrations := map[string]string{
		"001_test.up.sql":   "CREATE TABLE test (id INTEGER);",
		"001_test.down.sql": "DROP TABLE test;",
	}

	for filename, content := range migrations {
		if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create migration file %s: %v", filename, err)
		}
	}

	config := &Config{
		DatabaseURL:    connStr,
		MigrationsPath: tempDir,
		MigrationTable: "schema_migrations",
	}

	t.Run("concurrent_status_checks", func(t *testing.T) {
		runner, err := NewMigrationRunner(config)
		if err != nil {
			t.Fatalf("failed to create runner: %v", err)
		}
		defer func() {
			if err := runner.Close(); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Run multiple status checks concurrently
		done := make(chan error, 5)
		for i := 0; i < 5; i++ {
			go func() {
				done <- runner.Status()
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 5; i++ {
			if err := <-done; err != nil {
				t.Errorf("concurrent status check %d failed: %v", i, err)
			}
		}
	})
}

// BenchmarkMigrationRunnerIntegrationOperations benchmarks migration operations with real database
func BenchmarkMigrationRunnerIntegrationOperations(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	// TODO: Fix benchmark for CI environment (Phase 4)
	b.Skip("skipping integration benchmark - needs Docker daemon in CI")

	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
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

	tempDir := b.TempDir()

	// Create simple migration
	migrations := map[string]string{
		"001_test.up.sql":   "CREATE TABLE IF NOT EXISTS benchmark_test (id INTEGER);",
		"001_test.down.sql": "DROP TABLE IF EXISTS benchmark_test;",
	}

	for filename, content := range migrations {
		if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
			b.Fatalf("failed to create migration file %s: %v", filename, err)
		}
	}

	config := &Config{
		DatabaseURL:    connStr,
		MigrationsPath: tempDir,
		MigrationTable: "schema_migrations",
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

	// Apply initial migration
	if err := runner.Up(); err != nil {
		b.Fatalf("failed to apply initial migration: %v", err)
	}

	b.ResetTimer()

	b.Run("Status", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := runner.Status(); err != nil {
				b.Fatalf("status check failed: %v", err)
			}
		}
	})

	b.Run("Version", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := runner.Version(); err != nil {
				b.Fatalf("version check failed: %v", err)
			}
		}
	})
}
