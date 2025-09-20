package main

import (
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
)

func TestNewEmbeddedMigration(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Test that constructor creates valid instance with embedded migrations
	eMigration := NewEmbeddedMigration(nil)

	if eMigration == nil {
		t.Fatal("expected non-nil EmbeddedMigration instance")
	}

	// Test that embedded FS is accessible
	embeddedFS := eMigration.GetEmbeddedMigrations()
	if embeddedFS == nil {
		t.Fatal("expected non-nil embedded file system")
	}

	// Test that we can list embedded migrations (should find actual migration files)
	files, err := eMigration.ListEmbeddedMigrations()
	if err != nil {
		t.Fatalf("failed to list embedded migrations: %v", err)
	}

	// Should find the actual migration files that are embedded
	if len(files) == 0 {
		t.Error("expected to find embedded migration files")
	}
}

func TestGetEmbeddedMigrations(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	eMigration := NewEmbeddedMigration(nil)
	fsys := eMigration.GetEmbeddedMigrations()

	if fsys == nil {
		t.Fatal("expected non-nil fs.FS")
	}

	// Verify the returned fs.FS implements the interface properly
	if _, ok := fsys.(fs.FS); !ok {
		t.Fatal("returned object does not implement fs.FS interface")
	}

	// Test if we can read actual embedded migration files
	// Try to read a known embedded file
	_, err := fsys.Open("001_initial_schema.up.sql")
	if err != nil {
		t.Errorf(
			"expected to be able to read embedded migration file from fs.FS, got error: %v",
			err,
		)
	}

	// Test that non-existent files fail appropriately
	_, err = fsys.Open("non_existent.sql")
	if err == nil {
		t.Error("expected error when opening non-existent file from embedded fs.FS, got nil")
	}
}

func TestListEmbeddedMigrations(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	eMigration := NewEmbeddedMigration(nil)
	result, err := eMigration.ListEmbeddedMigrations()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The embedded system should return the actual migration files from the migrations directory
	// These files follow our strict naming convention: 001_name.(up|down).sql
	expectedFiles := []string{
		"001_initial_schema.down.sql",
		"001_initial_schema.up.sql",
		"002_performance_optimization.down.sql",
		"002_performance_optimization.up.sql",
	}

	// Sort both slices for comparison
	sort.Strings(result)
	sort.Strings(expectedFiles)

	if !reflect.DeepEqual(result, expectedFiles) {
		t.Errorf("expected files %v, got %v", expectedFiles, result)
	}

	// Verify that all returned files match our strict naming convention
	for _, file := range result {
		if !migrationFilenameRegex.MatchString(file) {
			t.Errorf("file %s does not match strict naming convention", file)
		}
	}
}

func TestValidateEmbeddedMigrations(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	eMigration := NewEmbeddedMigration(nil)
	err := eMigration.ValidateEmbeddedMigrations()
	// The embedded migration system should validate successfully with our actual migration files
	// since they follow the strict naming convention and are properly paired
	if err != nil {
		t.Errorf("embedded migration validation failed: %v", err)
	}

	// Verify that the validation checked the expected number of files
	files, listErr := eMigration.ListEmbeddedMigrations()
	if listErr != nil {
		t.Fatalf("failed to list migrations for verification: %v", listErr)
	}

	if len(files) == 0 {
		t.Error("validation should have found embedded migration files")
	}

	// All files should be readable
	for _, file := range files {
		_, contentErr := eMigration.GetEmbeddedMigrationContent(file)
		if contentErr != nil {
			t.Errorf(
				"validation should ensure file %s is readable, but got error: %v",
				file,
				contentErr,
			)
		}
	}
}

func TestGetEmbeddedMigrationContent(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	eMigration := NewEmbeddedMigration(nil)

	// Test 1: Read actual embedded migration files
	t.Run("read actual embedded migration files", func(t *testing.T) {
		// Test reading each actual embedded migration file
		expectedFiles := []string{
			"001_initial_schema.up.sql",
			"001_initial_schema.down.sql",
			"002_performance_optimization.up.sql",
			"002_performance_optimization.down.sql",
		}

		for _, filename := range expectedFiles {
			content, err := eMigration.GetEmbeddedMigrationContent(filename)
			if err != nil {
				t.Errorf("failed to read embedded migration file %s: %v", filename, err)
				continue
			}

			// Content should not be empty and should contain SQL statements
			if len(content) == 0 {
				t.Errorf("embedded migration file %s should not be empty", filename)
			}

			// Verify it contains SQL-like content (basic sanity check)
			contentStr := string(content)
			if !strings.Contains(contentStr, "CREATE") &&
				!strings.Contains(contentStr, "DROP") &&
				!strings.Contains(contentStr, "ALTER") &&
				!strings.Contains(contentStr, "INDEX") {
				previewLen := 100
				if len(contentStr) < previewLen {
					previewLen = len(contentStr)
				}
				t.Logf(
					"WARNING: file %s might not contain SQL statements: %s",
					filename,
					contentStr[:previewLen],
				)
			}
		}
	})

	// Test 2: Non-existent file should fail
	t.Run("read non-existent file", func(t *testing.T) {
		_, err := eMigration.GetEmbeddedMigrationContent("non_existent.sql")
		if err == nil {
			t.Error("expected error when reading non-existent file, got nil")
		}
		if !strings.Contains(err.Error(), "file does not exist") {
			t.Errorf("expected 'file does not exist' error, got: %v", err)
		}
	})
}

func TestEmbeddedMigrationsSortingBehavior(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create a test filesystem with migrations out of order to verify sorting works
	testFS := fstest.MapFS{
		"010_migration.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE test10 (id INTEGER);"),
		},
		"010_migration.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE test10;")},
		"002_migration.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE test2 (id INTEGER);")},
		"002_migration.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE test2;")},
		"001_migration.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE test1 (id INTEGER);")},
		"001_migration.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE test1;")},
		"100_migration.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE test100 (id INTEGER);"),
		},
		"100_migration.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE test100;")},
		"020_migration.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE test20 (id INTEGER);"),
		},
		"020_migration.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE test20;")},
	}

	// Test with injected filesystem
	eMigration := NewEmbeddedMigration(testFS)
	result, err := eMigration.ListEmbeddedMigrations()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Expected order after sorting (lexicographic with 3-digit prefixes ensures proper numeric order)
	expected := []string{
		"001_migration.down.sql",
		"001_migration.up.sql",
		"002_migration.down.sql",
		"002_migration.up.sql",
		"010_migration.down.sql",
		"010_migration.up.sql",
		"020_migration.down.sql",
		"020_migration.up.sql",
		"100_migration.down.sql",
		"100_migration.up.sql",
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("migrations not properly sorted. Expected %v, got %v", expected, result)
	}

	t.Logf("SUCCESS: Found %d test migrations in correct sorted order: %v", len(result), result)
}

func TestEmbeddedMigrationsFilenameValidation(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create a test filesystem with invalid migration filenames
	invalidTestFS := fstest.MapFS{
		"migration.sql":            &fstest.MapFile{Data: []byte("-- Missing version number")},
		"001.sql":                  &fstest.MapFile{Data: []byte("-- Missing direction")},
		"001_test.invalid.sql":     &fstest.MapFile{Data: []byte("-- Invalid direction")},
		"invalid_migration.up.sql": &fstest.MapFile{Data: []byte("-- Non-numeric prefix")},
		"001_migration.UP.sql":     &fstest.MapFile{Data: []byte("-- Wrong case")},
	}

	eMigration := NewEmbeddedMigration(invalidTestFS)

	// Current implementation should filter out invalid filenames during listing
	// So we should get "no embedded migration files found" error
	err := eMigration.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should fail when no embedded migration files found")
	}

	// With strict naming enforcement, invalid files are filtered out during listing
	// So we should get "no embedded migration files found" error
	if err != nil && !strings.Contains(err.Error(), "no embedded migration files found") {
		t.Logf(
			"EXPECTED FAILURE: with strict naming, should get 'no embedded migration files found', got: %v",
			err,
		)
	}
}

func TestEmbeddedMigrationsPairedValidation(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create a test filesystem with unpaired migrations
	unpairedTestFS := fstest.MapFS{
		"001_initial.up.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER);")},
		// Missing 001_initial.down.sql
		"002_posts.up.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE posts (id INTEGER);")},
		"002_posts.down.sql":  &fstest.MapFile{Data: []byte("DROP TABLE posts;")},
		"003_orphan.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE orphan;")},
		// Missing 003_orphan.up.sql
	}

	eMigration := NewEmbeddedMigration(unpairedTestFS)

	// SHOULD FAIL: Current implementation should validate migration pairs
	err := eMigration.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should fail for unpaired migrations")
	}

	if err != nil && !strings.Contains(err.Error(), "pair") &&
		!strings.Contains(err.Error(), "orphan") {
		t.Errorf("EXPECTED FAILURE: error should mention pairing validation, got: %v", err)
	}
}

func TestEmbeddedMigrationsSequenceValidation(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create a test filesystem with migrations that have gaps in sequence
	gappedTestFS := fstest.MapFS{
		"001_first.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE first (id INTEGER);")},
		"001_first.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE first;")},
		// Missing 002_*
		"003_third.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE third (id INTEGER);")},
		"003_third.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE third;")},
		"005_fifth.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE fifth (id INTEGER);")},
		"005_fifth.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE fifth;")},
	}

	eMigration := NewEmbeddedMigration(gappedTestFS)

	// SHOULD FAIL: Current implementation should validate migration sequence
	err := eMigration.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should fail for gaps in migration sequence")
	}

	if err != nil && !strings.Contains(err.Error(), "sequence") &&
		!strings.Contains(err.Error(), "gap") {
		t.Errorf("EXPECTED FAILURE: error should mention sequence validation, got: %v", err)
	}
}

func TestEmbeddedMigrationsChecksumValidation(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Create a test filesystem with initial migrations
	initialTestFS := fstest.MapFS{
		"001_initial.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER);")},
		"001_initial.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;")},
	}

	eMigration := NewEmbeddedMigration(initialTestFS)

	// First validation should pass and store checksums
	err := eMigration.ValidateEmbeddedMigrations()
	if err != nil {
		t.Fatalf("initial validation failed: %v", err)
	}

	// Create a modified test filesystem (simulating file tampering)
	modifiedTestFS := fstest.MapFS{
		"001_initial.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id INTEGER, email VARCHAR(255));"),
		},
		"001_initial.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;")},
	}

	modifiedMigration := NewEmbeddedMigration(modifiedTestFS)
	// Copy the stored checksums from the original migration to simulate checksum comparison
	modifiedMigration.checksums = eMigration.checksums

	// SHOULD FAIL: Validation should detect that embedded content doesn't match stored checksums
	err = modifiedMigration.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should detect modified migration files")
	}

	if err != nil && !strings.Contains(err.Error(), "checksum") &&
		!strings.Contains(err.Error(), "modified") {
		t.Errorf("EXPECTED FAILURE: error should mention checksum validation, got: %v", err)
	}
}

// Benchmark tests for performance validation
func BenchmarkListEmbeddedMigrations(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	eMigration := NewEmbeddedMigration(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := eMigration.ListEmbeddedMigrations()
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkGetEmbeddedMigrationContent(b *testing.B) {
	if !testing.Short() {
		b.Skip("skipping benchmark in non-short mode")
	}

	eMigration := NewEmbeddedMigration(nil)

	// Use an actual embedded migration file for the benchmark
	filename := "001_initial_schema.up.sql"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := eMigration.GetEmbeddedMigrationContent(filename)
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}
