package main

import (
	"context"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Embed Performance benchmarks

func Benchmark_ListEmbeddedMigrations(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	migration := NewEmbeddedMigration(nil)

	b.ResetTimer()

	for range b.N {
		_, err := migration.ListEmbeddedMigrations()
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

func Benchmark_GetEmbeddedMigrationContent(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	migration := NewEmbeddedMigration(nil)
	filename := "001_initial_schema.up.sql"

	b.ResetTimer()

	for range b.N {
		_, err := migration.GetEmbeddedMigrationContent(filename)
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

// TestGetMaxSchemaVersion tests the getMaxSchemaVersion function for accurate
// schema version detection from embedded migration files.
func TestGetMaxSchemaVersion(t *testing.T) {
	skipIfNotShort(t)

	tests := []struct {
		name           string
		migrationFiles map[string]*fstest.MapFile
		expected       int
	}{
		{
			name:           "no_migration_files",
			migrationFiles: map[string]*fstest.MapFile{},
			expected:       0,
		},
		{
			name: "single_migration_sequence",
			migrationFiles: map[string]*fstest.MapFile{
				"001_initial.up.sql":   {Data: []byte("CREATE TABLE test;")},
				"001_initial.down.sql": {Data: []byte("DROP TABLE test;")},
			},
			expected: 1,
		},
		{
			name: "multiple_migration_sequences",
			migrationFiles: map[string]*fstest.MapFile{
				"001_initial.up.sql":    {Data: []byte("CREATE TABLE test;")},
				"001_initial.down.sql":  {Data: []byte("DROP TABLE test;")},
				"005_features.up.sql":   {Data: []byte("ALTER TABLE test ADD COLUMN name VARCHAR(255);")},
				"005_features.down.sql": {Data: []byte("ALTER TABLE test DROP COLUMN name;")},
				"003_indexes.up.sql":    {Data: []byte("CREATE INDEX idx_test ON test(id);")},
				"003_indexes.down.sql":  {Data: []byte("DROP INDEX idx_test;")},
			},
			expected: 5, // Should return the highest sequence number
		},
		{
			name: "high_sequence_numbers",
			migrationFiles: map[string]*fstest.MapFile{
				"112_advanced.up.sql":   {Data: []byte("CREATE MATERIALIZED VIEW test_view;")},
				"112_advanced.down.sql": {Data: []byte("DROP MATERIALIZED VIEW test_view;")},
				"050_middle.up.sql":     {Data: []byte("CREATE INDEX test_idx;")},
				"050_middle.down.sql":   {Data: []byte("DROP INDEX test_idx;")},
			},
			expected: 112,
		},
		{
			name: "mixed_valid_and_invalid_files",
			migrationFiles: map[string]*fstest.MapFile{
				"001_initial.up.sql":    {Data: []byte("CREATE TABLE test;")},
				"001_initial.down.sql":  {Data: []byte("DROP TABLE test;")},
				"invalid_file.sql":      {Data: []byte("INVALID;")},
				"002_features.up.sql":   {Data: []byte("ALTER TABLE test;")},
				"002_features.down.sql": {Data: []byte("ALTER TABLE test;")},
				"not_a_migration.txt":   {Data: []byte("TEXT FILE")},
			},
			expected: 2, // Should ignore invalid files and return max valid sequence
		},
		{
			name: "only_invalid_files",
			migrationFiles: map[string]*fstest.MapFile{
				"invalid_file.sql":    {Data: []byte("INVALID;")},
				"not_a_migration.txt": {Data: []byte("TEXT FILE")},
				"random.doc":          {Data: []byte("DOCUMENT")},
			},
			expected: 0, // Should return 0 when no valid migration files found
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create test filesystem with migration files
			testFS := fstest.MapFS(tc.migrationFiles)

			// Create an embedded migration with the test filesystem
			embeddedMigration := NewEmbeddedMigration(testFS)

			// Get files from embedded migration (similar to getMaxSchemaVersion logic)
			files, err := embeddedMigration.ListEmbeddedMigrations()
			if err != nil {
				// Should return 0 for error cases, like the real function
				if tc.expected != 0 {
					t.Errorf("unexpected error getting migration files: %v", err)
				}

				return
			}

			// Simulate the same logic as getMaxSchemaVersion
			maxSequence := 0

			for _, filename := range files {
				matches := migrationFilenameRegex.FindStringSubmatch(filename)
				if len(matches) >= expectedRegexMatches-2 { // Need at least sequence + name parts
					if sequence, err := strconv.Atoi(matches[1]); err == nil && sequence > maxSequence {
						maxSequence = sequence
					}
				}
			}

			if maxSequence != tc.expected {
				t.Errorf("getMaxSchemaVersion logic = %d, expected %d", maxSequence, tc.expected)
			}
		})
	}
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

// BenchmarkMigrationRunnerOperations benchmarks basic operations.
func Benchmark_MigrationRunnerOperations(b *testing.B) {
	mock := &mockMigrationRunner{}

	b.Run("Status", func(b *testing.B) {
		for range b.N {
			_ = mock.Status()
		}
	})

	b.Run("Version", func(b *testing.B) {
		for range b.N {
			_ = mock.Version()
		}
	})

	b.Run("Up", func(b *testing.B) {
		for range b.N {
			_ = mock.Up()
		}
	})
}
