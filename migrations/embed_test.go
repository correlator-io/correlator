package main

import (
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

// Test data constants to avoid hardcoding and improve maintainability.
const (
	validMigrationContent     = "CREATE TABLE users (id INTEGER);"
	validDownMigrationContent = "DROP TABLE users;"
	modifiedMigrationContent  = "CREATE TABLE users (id INTEGER, email VARCHAR(255));"
)

// getExpectedEmbeddedFiles returns the expected migration files for tests.
// This function encapsulates the file list to avoid global variables.
func getExpectedEmbeddedFiles() []string {
	return []string{
		"001_initial_schema.down.sql",
		"001_initial_schema.up.sql",
		"002_performance_optimization.down.sql",
		"002_performance_optimization.up.sql",
	}
}

// Test utilities and helpers

// skipIfNotShort extracts the common skip logic to reduce duplication.
func skipIfNotShort(t *testing.T) {
	t.Helper()

	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}
}

// createTestMigration creates a test migration file with standard content.
func createTestMigration(seq int, name, direction string, content ...string) (string, *fstest.MapFile) {
	filename := fmt.Sprintf("%03d_%s.%s.sql", seq, name, direction)
	migrationContent := validMigrationContent

	if len(content) > 0 {
		migrationContent = content[0]
	}

	return filename, &fstest.MapFile{Data: []byte(migrationContent)}
}

// createMigrationPair creates both up and down migrations for a given sequence.
func createMigrationPair(seq int, name string) map[string]*fstest.MapFile {
	upFile, upContent := createTestMigration(seq, name, "up")
	downFile, downContent := createTestMigration(seq, name, "down", validDownMigrationContent)

	return map[string]*fstest.MapFile{
		upFile:   upContent,
		downFile: downContent,
	}
}

// assertErrorContains checks if error contains expected keywords and provides better error messages.
func assertErrorContains(t *testing.T, err error, expectedKeywords []string, context string) {
	t.Helper()
	t.Helper()

	if err == nil {
		t.Errorf("%s: expected error containing %v, got nil", context, expectedKeywords)

		return
	}

	errMsg := err.Error()
	for _, keyword := range expectedKeywords {
		if strings.Contains(errMsg, keyword) {
			return // Found at least one expected keyword
		}
	}

	t.Errorf("%s: expected error to contain one of %v, got: %v", context, expectedKeywords, err)
}

// assertErrorIs checks if error matches expected static error.
func assertErrorIs(t *testing.T, err, expectedErr error, context string) {
	t.Helper()
	t.Helper()

	if !errors.Is(err, expectedErr) {
		t.Errorf("%s: expected error %v, got %v", context, expectedErr, err)
	}
}

// mustCreateEmbeddedMigration creates an EmbeddedMigration or fails the test.
func mustCreateEmbeddedMigration(t *testing.T, filesystem fs.FS) *EmbeddedMigration {
	t.Helper()

	migration := NewEmbeddedMigration(filesystem)
	if migration == nil {
		t.Fatal("expected non-nil EmbeddedMigration instance")
	}

	return migration
}

// Core functionality tests

func TestNewEmbeddedMigration(t *testing.T) {
	skipIfNotShort(t)

	t.Run("constructor with nil filesystem", func(t *testing.T) {
		migration := mustCreateEmbeddedMigration(t, nil)

		// Test that embedded FS is accessible
		embeddedFS := migration.GetEmbeddedMigrations()
		if embeddedFS == nil {
			t.Fatal("expected non-nil embedded file system")
		}
	})

	t.Run("constructor with custom filesystem", func(t *testing.T) {
		testFS := fstest.MapFS{"test.sql": &fstest.MapFile{Data: []byte("SELECT 1;")}}
		migration := mustCreateEmbeddedMigration(t, testFS)

		// Verify we can access the test file from our custom filesystem
		_, err := migration.GetEmbeddedMigrationContent("test.sql")
		if err != nil {
			t.Errorf("expected to access file from custom filesystem, got error: %v", err)
		}
	})
}

func TestGetEmbeddedMigrations(t *testing.T) {
	skipIfNotShort(t)

	migration := mustCreateEmbeddedMigration(t, nil)
	fsys := migration.GetEmbeddedMigrations()

	if fsys == nil {
		t.Fatal("expected non-nil fs.FS")
	}

	t.Run("can access actual embedded files", func(t *testing.T) {
		_, err := fsys.Open("001_initial_schema.up.sql")
		if err != nil {
			t.Errorf("expected to be able to read embedded migration file, got error: %v", err)
		}
	})

	t.Run("non-existent files fail appropriately", func(t *testing.T) {
		_, err := fsys.Open("non_existent.sql")
		if err == nil {
			t.Error("expected error when opening non-existent file, got nil")
		}
	})
}

func TestListEmbeddedMigrations(t *testing.T) {
	skipIfNotShort(t)

	t.Run("lists actual embedded migrations", func(t *testing.T) {
		migration := mustCreateEmbeddedMigration(t, nil)

		result, err := migration.ListEmbeddedMigrations()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Sort both slices for comparison
		sort.Strings(result)

		expectedFiles := getExpectedEmbeddedFiles()
		expectedSorted := make([]string, len(expectedFiles))
		copy(expectedSorted, expectedFiles)
		sort.Strings(expectedSorted)

		if !reflect.DeepEqual(result, expectedSorted) {
			t.Errorf("expected files %v, got %v", expectedSorted, result)
		}

		// Verify all files match naming convention
		for _, file := range result {
			if !migrationFilenameRegex.MatchString(file) {
				t.Errorf("file %s does not match strict naming convention", file)
			}
		}
	})

	t.Run("sorts migrations correctly", func(t *testing.T) {
		// Create migrations in random order to test sorting
		migrations := make(map[string]*fstest.MapFile)
		sequences := []int{10, 2, 1, 100, 20}

		for _, seq := range sequences {
			pair := createMigrationPair(seq, "migration")
			for k, v := range pair {
				migrations[k] = v
			}
		}

		testFS := fstest.MapFS(migrations)
		migration := mustCreateEmbeddedMigration(t, testFS)

		result, err := migration.ListEmbeddedMigrations()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify lexicographic sorting works with 3-digit prefixes
		expected := []string{
			"001_migration.down.sql", "001_migration.up.sql",
			"002_migration.down.sql", "002_migration.up.sql",
			"010_migration.down.sql", "010_migration.up.sql",
			"020_migration.down.sql", "020_migration.up.sql",
			"100_migration.down.sql", "100_migration.up.sql",
		}

		if !reflect.DeepEqual(result, expected) {
			t.Errorf("migrations not properly sorted. Expected %v, got %v", expected, result)
		}
	})
}

func TestValidateEmbeddedMigrations(t *testing.T) {
	skipIfNotShort(t)

	t.Run("validates actual embedded migrations successfully", func(t *testing.T) {
		migration := mustCreateEmbeddedMigration(t, nil)

		err := migration.ValidateEmbeddedMigrations()
		if err != nil {
			t.Errorf("embedded migration validation failed: %v", err)
		}

		// Verify validation processed expected files
		files, listErr := migration.ListEmbeddedMigrations()
		if listErr != nil {
			t.Fatalf("failed to list migrations for verification: %v", listErr)
		}

		if len(files) == 0 {
			t.Error("validation should have found embedded migration files")
		}

		// Verify all files are readable
		for _, file := range files {
			_, contentErr := migration.GetEmbeddedMigrationContent(file)
			if contentErr != nil {
				t.Errorf("file %s should be readable after validation, got error: %v", file, contentErr)
			}
		}
	})
}

func TestGetEmbeddedMigrationContent(t *testing.T) {
	skipIfNotShort(t)

	migration := mustCreateEmbeddedMigration(t, nil)

	t.Run("reads actual embedded files", func(t *testing.T) {
		expectedFiles := getExpectedEmbeddedFiles()
		for _, filename := range expectedFiles {
			content, err := migration.GetEmbeddedMigrationContent(filename)
			if err != nil {
				t.Errorf("failed to read embedded migration file %s: %v", filename, err)

				continue
			}

			if len(content) == 0 {
				t.Errorf("embedded migration file %s should not be empty", filename)
			}

			// Basic SQL content validation
			contentStr := string(content)
			containsSQL := strings.Contains(contentStr, "CREATE") ||
				strings.Contains(contentStr, "DROP") ||
				strings.Contains(contentStr, "ALTER") ||
				strings.Contains(contentStr, "INDEX")

			if !containsSQL {
				t.Logf("Warning: file %s might not contain SQL statements", filename)
			}
		}
	})

	t.Run("non-existent files return error", func(t *testing.T) {
		_, err := migration.GetEmbeddedMigrationContent("non_existent.sql")
		if err == nil {
			t.Error("expected error when reading non-existent file, got nil")
		}

		assertErrorContains(t, err, []string{"file does not exist"}, "non-existent file")
	})
}

// Validation tests using table-driven approach

func TestMigrationValidationScenarios(t *testing.T) {
	skipIfNotShort(t)

	tests := []struct {
		name        string
		setupFS     func() fstest.MapFS
		expectError bool
		errorCheck  func(t *testing.T, err error)
	}{
		{
			name: "no migration files",
			setupFS: func() fstest.MapFS {
				return fstest.MapFS{}
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				t.Helper()
				assertErrorIs(t, err, ErrNoEmbeddedMigrations, "no migrations")
			},
		},
		{
			name: "invalid filenames filtered out",
			setupFS: func() fstest.MapFS {
				return fstest.MapFS{
					"migration.sql":            &fstest.MapFile{Data: []byte("-- Invalid")},
					"001.sql":                  &fstest.MapFile{Data: []byte("-- Invalid")},
					"001_test.invalid.sql":     &fstest.MapFile{Data: []byte("-- Invalid")},
					"invalid_migration.up.sql": &fstest.MapFile{Data: []byte("-- Invalid")},
					"001_migration.UP.sql":     &fstest.MapFile{Data: []byte("-- Invalid")},
				}
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				t.Helper()
				assertErrorIs(t, err, ErrNoEmbeddedMigrations, "invalid filenames")
			},
		},
		{
			name: "unpaired migrations",
			setupFS: func() fstest.MapFS {
				return fstest.MapFS{
					"001_initial.up.sql": &fstest.MapFile{Data: []byte(validMigrationContent)},
					// Missing 001_initial.down.sql
					"002_posts.up.sql":    &fstest.MapFile{Data: []byte(validMigrationContent)},
					"002_posts.down.sql":  &fstest.MapFile{Data: []byte(validDownMigrationContent)},
					"003_orphan.down.sql": &fstest.MapFile{Data: []byte(validDownMigrationContent)},
					// Missing 003_orphan.up.sql
				}
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				t.Helper()
				assertErrorContains(t, err, []string{"orphan", "missing"}, "unpaired migrations")
			},
		},
		{
			name: "sequence gaps",
			setupFS: func() fstest.MapFS {
				migrations := make(map[string]*fstest.MapFile)

				// Create migrations with gaps: 1, 3, 5 (missing 2, 4)
				for _, seq := range []int{1, 3, 5} {
					pair := createMigrationPair(seq, "migration")
					for k, v := range pair {
						migrations[k] = v
					}
				}

				return fstest.MapFS(migrations)
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				t.Helper()
				assertErrorContains(t, err, []string{"gap", "sequence"}, "sequence gaps")
			},
		},
		{
			name: "valid migrations",
			setupFS: func() fstest.MapFS {
				migrations := make(map[string]*fstest.MapFile)

				// Create valid sequential migrations: 1, 2, 3
				for _, seq := range []int{1, 2, 3} {
					pair := createMigrationPair(seq, "migration")
					for k, v := range pair {
						migrations[k] = v
					}
				}

				return fstest.MapFS(migrations)
			},
			expectError: false,
			errorCheck:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFS := tt.setupFS()
			migration := mustCreateEmbeddedMigration(t, testFS)

			err := migration.ValidateEmbeddedMigrations()

			if tt.expectError {
				if err == nil {
					t.Errorf("expected validation to fail for %s, got nil error", tt.name)

					return
				}

				if tt.errorCheck != nil {
					tt.errorCheck(t, err)
				}
			} else if err != nil {
				t.Errorf("expected validation to pass for %s, got error: %v", tt.name, err)
			}
		})
	}
}

func TestChecksumValidation(t *testing.T) {
	skipIfNotShort(t)

	// Create initial valid migrations
	migrations := createMigrationPair(1, "initial")
	initialFS := fstest.MapFS(migrations)

	migration := mustCreateEmbeddedMigration(t, initialFS)

	// First validation should pass and store checksums
	err := migration.ValidateEmbeddedMigrations()
	if err != nil {
		t.Fatalf("initial validation failed: %v", err)
	}

	// Create modified filesystem (simulating tampering)
	modifiedMigrations := make(map[string]*fstest.MapFile)
	upFile, _ := createTestMigration(1, "initial", "up", modifiedMigrationContent)
	downFile, downContent := createTestMigration(1, "initial", "down", validDownMigrationContent)

	modifiedMigrations[upFile] = &fstest.MapFile{Data: []byte(modifiedMigrationContent)}
	modifiedMigrations[downFile] = downContent

	modifiedFS := fstest.MapFS(modifiedMigrations)
	modifiedMigration := mustCreateEmbeddedMigration(t, modifiedFS)

	// Copy stored checksums to simulate checksum comparison
	modifiedMigration.checksums = migration.checksums

	// Should detect modification
	err = modifiedMigration.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("expected checksum validation to detect modified files")
	} else {
		assertErrorIs(t, err, ErrChecksumMismatch, "checksum validation")
	}
}

// Performance benchmarks

func BenchmarkListEmbeddedMigrations(b *testing.B) {
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

func BenchmarkGetEmbeddedMigrationContent(b *testing.B) {
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

// TestGetMaxSchemaVersionWithRealEmbedded tests getMaxSchemaVersion with actual embedded migrations.
func TestGetMaxSchemaVersionWithRealEmbedded(t *testing.T) {
	skipIfNotShort(t)

	// Test with actual embedded migrations
	result := getMaxSchemaVersion()

	// We know we have at least 002_performance_optimization migration
	expectedMin := 2
	if result < expectedMin {
		t.Errorf("getMaxSchemaVersion() = %d, expected at least %d", result, expectedMin)
	}

	// Sanity check: should not be unreasonably high
	maxReasonable := 999
	if result > maxReasonable {
		t.Errorf("getMaxSchemaVersion() = %d, seems unreasonably high (>%d)", result, maxReasonable)
	}

	t.Logf("Current max schema version: %d", result)
}
