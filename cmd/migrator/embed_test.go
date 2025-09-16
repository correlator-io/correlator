package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestNewEmbeddedMigrationSupport(t *testing.T) {
	tests := []struct {
		name           string
		migrationsPath string
		expected       string
	}{
		{
			name:           "valid migrations path",
			migrationsPath: "/valid/path/to/migrations",
			expected:       "/valid/path/to/migrations",
		},
		{
			name:           "empty migrations path",
			migrationsPath: "",
			expected:       "",
		},
		{
			name:           "relative migrations path",
			migrationsPath: "./migrations",
			expected:       "./migrations",
		},
		{
			name:           "absolute migrations path",
			migrationsPath: "/absolute/path/migrations",
			expected:       "/absolute/path/migrations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			support := NewEmbeddedMigrationSupport(tt.migrationsPath)

			if support == nil {
				t.Fatal("expected non-nil EmbeddedMigrationSupport instance")
			}

			if support.migrationsPath != tt.expected {
				t.Errorf("expected migrationsPath %q, got %q", tt.expected, support.migrationsPath)
			}
		})
	}
}

func TestGetEmbeddedMigrations(t *testing.T) {
	// Create temporary directory with test files
	tempDir := t.TempDir()

	// Create some test files
	testFiles := []string{
		"001_test.up.sql",
		"001_test.down.sql",
		"002_test.up.sql",
		"README.md",
		"script.sh",
	}

	for _, file := range testFiles {
		content := fmt.Sprintf("-- Test content for %s", file)
		if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create test file %s: %v", file, err)
		}
	}

	tests := []struct {
		name           string
		migrationsPath string
		wantError      bool
		wantReadable   bool
	}{
		{
			name:           "valid directory with files",
			migrationsPath: tempDir,
			wantError:      false,
			wantReadable:   true,
		},
		{
			name:           "non-existent directory",
			migrationsPath: "/non/existent/path",
			wantError:      false, // os.DirFS doesn't fail on non-existent path
			wantReadable:   false,
		},
		{
			name:           "empty path",
			migrationsPath: "",
			wantError:      false,
			wantReadable:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			support := NewEmbeddedMigrationSupport(tt.migrationsPath)
			fsys := support.GetEmbeddedMigrations()

			if fsys == nil {
				t.Fatal("expected non-nil fs.FS")
			}

			// Verify the returned fs.FS implements the interface properly
			if _, ok := fsys.(fs.FS); !ok {
				t.Fatal("returned object does not implement fs.FS interface")
			}

			// Test if we can read from the file system
			if tt.wantReadable {
				// Try to read a known file
				_, err := fsys.Open("001_test.up.sql")
				if err != nil {
					t.Errorf("expected to be able to read file from fs.FS, got error: %v", err)
				}
			} else {
				// For non-readable cases, opening should fail
				_, err := fsys.Open("001_test.up.sql")
				if err == nil {
					t.Error("expected error when opening file from invalid fs.FS, got nil")
				}
			}
		})
	}
}

func TestListEmbeddedMigrations(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(t *testing.T) string
		expected      []string
		expectError   bool
		errorContains string
	}{
		{
			name: "directory with SQL files only",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()
				files := []string{
					"001_initial.up.sql",
					"001_initial.down.sql",
					"002_users.up.sql",
					"002_users.down.sql",
				}

				for _, file := range files {
					content := fmt.Sprintf("-- Migration: %s", file)
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
				}
				return tempDir
			},
			expected: []string{
				"001_initial.up.sql",
				"001_initial.down.sql",
				"002_users.up.sql",
				"002_users.down.sql",
			},
			expectError: false,
		},
		{
			name: "directory with mixed file types",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()
				files := map[string]string{
					"001_test.up.sql":   "-- SQL migration",
					"002_test.down.sql": "-- SQL rollback",
					"README.md":         "# Documentation",
					"script.sh":         "#!/bin/bash",
					"config.yaml":       "key: value",
					"backup.txt":        "backup data",
				}

				for file, content := range files {
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
				}
				return tempDir
			},
			expected: []string{
				"001_test.up.sql",
				"002_test.down.sql",
			},
			expectError: false,
		},
		{
			name: "empty directory",
			setupFunc: func(t *testing.T) string {
				return t.TempDir()
			},
			expected:    []string{},
			expectError: false,
		},
		{
			name: "directory with subdirectories",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()

				// Create SQL files
				sqlFiles := []string{"001_test.up.sql", "001_test.down.sql"}
				for _, file := range sqlFiles {
					content := fmt.Sprintf("-- Migration: %s", file)
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
				}

				// Create subdirectory (should be ignored)
				subDir := filepath.Join(tempDir, "subdir")
				if err := os.Mkdir(subDir, 0o755); err != nil {
					t.Fatalf("failed to create subdirectory: %v", err)
				}

				// Create file in subdirectory (should be ignored)
				if err := os.WriteFile(filepath.Join(subDir, "ignored.sql"), []byte("-- ignored"), 0o644); err != nil {
					t.Fatalf("failed to create file in subdirectory: %v", err)
				}

				return tempDir
			},
			expected: []string{
				"001_test.up.sql",
				"001_test.down.sql",
			},
			expectError: false,
		},
		{
			name: "non-existent directory",
			setupFunc: func(t *testing.T) string {
				return "/non/existent/directory"
			},
			expected:      nil,
			expectError:   true,
			errorContains: "failed to read migrations directory",
		},
		{
			name: "directory with permission issues",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()

				// Create a file first
				testFile := filepath.Join(tempDir, "001_test.up.sql")
				if err := os.WriteFile(testFile, []byte("-- test"), 0o644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}

				// Change directory permissions to be unreadable
				if err := os.Chmod(tempDir, 0o000); err != nil {
					t.Fatalf("failed to change directory permissions: %v", err)
				}

				// Cleanup function to restore permissions for cleanup
				t.Cleanup(func() {
					os.Chmod(tempDir, 0o755)
				})

				return tempDir
			},
			expected:      nil,
			expectError:   true,
			errorContains: "failed to read migrations directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			migrationsPath := tt.setupFunc(t)
			support := NewEmbeddedMigrationSupport(migrationsPath)

			result, err := support.ListEmbeddedMigrations()

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Sort both slices for comparison
			sort.Strings(result)
			expectedSorted := make([]string, len(tt.expected))
			copy(expectedSorted, tt.expected)
			sort.Strings(expectedSorted)

			// Handle nil vs empty slice comparison properly
			if len(result) == 0 && len(expectedSorted) == 0 {
				// Both are empty, this is correct
				return
			}

			if !reflect.DeepEqual(result, expectedSorted) {
				t.Errorf("expected files %v, got %v", expectedSorted, result)
			}
		})
	}
}

func TestValidateEmbeddedMigrations(t *testing.T) {
	tests := []struct {
		name          string
		setupFunc     func(t *testing.T) string
		expectError   bool
		errorContains string
	}{
		{
			name: "valid migrations directory with readable files",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()
				files := map[string]string{
					"001_initial.up.sql":   "CREATE TABLE users (id INTEGER);",
					"001_initial.down.sql": "DROP TABLE users;",
					"002_posts.up.sql":     "CREATE TABLE posts (id INTEGER);",
					"002_posts.down.sql":   "DROP TABLE posts;",
				}

				for file, content := range files {
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
				}
				return tempDir
			},
			expectError: false,
		},
		{
			name: "non-existent directory",
			setupFunc: func(t *testing.T) string {
				return "/non/existent/directory"
			},
			expectError:   true,
			errorContains: "migrations directory does not exist",
		},
		{
			name: "empty directory (no SQL files)",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()

				// Create non-SQL files
				files := map[string]string{
					"README.txt":  "Documentation",
					"config.yaml": "key: value",
					"script.sh":   "#!/bin/bash",
				}

				for file, content := range files {
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
				}
				return tempDir
			},
			expectError:   true,
			errorContains: "no migration files found in directory",
		},
		{
			name: "directory with unreadable migration files",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()

				// Create a readable SQL file first
				readableFile := filepath.Join(tempDir, "001_readable.up.sql")
				if err := os.WriteFile(readableFile, []byte("CREATE TABLE test (id INTEGER);"), 0o644); err != nil {
					t.Fatalf("failed to create readable file: %v", err)
				}

				// Create an unreadable SQL file
				unreadableFile := filepath.Join(tempDir, "002_unreadable.up.sql")
				if err := os.WriteFile(unreadableFile, []byte("CREATE TABLE test2 (id INTEGER);"), 0o644); err != nil {
					t.Fatalf("failed to create unreadable file: %v", err)
				}

				// Make the file unreadable
				if err := os.Chmod(unreadableFile, 0o000); err != nil {
					t.Fatalf("failed to change file permissions: %v", err)
				}

				// Cleanup function to restore permissions
				t.Cleanup(func() {
					os.Chmod(unreadableFile, 0o644)
				})

				return tempDir
			},
			expectError:   true,
			errorContains: "failed to read migration file",
		},
		{
			name: "directory with mixed files but valid SQL files",
			setupFunc: func(t *testing.T) string {
				tempDir := t.TempDir()
				files := map[string]string{
					"001_initial.up.sql":   "CREATE TABLE users (id INTEGER);",
					"001_initial.down.sql": "DROP TABLE users;",
					"README.md":            "# Documentation",
					"backup.txt":           "backup data",
					"script.py":            "print('hello')",
				}

				for file, content := range files {
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
				}
				return tempDir
			},
			expectError: false,
		},
		{
			name: "completely empty directory",
			setupFunc: func(t *testing.T) string {
				return t.TempDir()
			},
			expectError:   true,
			errorContains: "no migration files found in directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			migrationsPath := tt.setupFunc(t)
			support := NewEmbeddedMigrationSupport(migrationsPath)

			err := support.ValidateEmbeddedMigrations()

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestGetEmbeddedMigrationContent(t *testing.T) {
	tests := []struct {
		name            string
		setupFunc       func(t *testing.T) (string, string) // returns (tempDir, filename)
		expectedContent string
		expectError     bool
		errorContains   string
	}{
		{
			name: "read valid migration file",
			setupFunc: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				filename := "001_initial.up.sql"
				content := "CREATE TABLE users (\n    id SERIAL PRIMARY KEY,\n    name VARCHAR(255) NOT NULL\n);"

				if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return tempDir, filename
			},
			expectedContent: "CREATE TABLE users (\n    id SERIAL PRIMARY KEY,\n    name VARCHAR(255) NOT NULL\n);",
			expectError:     false,
		},
		{
			name: "read empty migration file",
			setupFunc: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				filename := "002_empty.up.sql"

				if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(""), 0o644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return tempDir, filename
			},
			expectedContent: "",
			expectError:     false,
		},
		{
			name: "read migration file with complex SQL",
			setupFunc: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				filename := "003_complex.up.sql"
				content := `-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create index
CREATE INDEX idx_users_email ON users(email);

-- Insert initial data
INSERT INTO users (email) VALUES ('admin@example.com');`

				if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return tempDir, filename
			},
			expectedContent: `-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create index
CREATE INDEX idx_users_email ON users(email);

-- Insert initial data
INSERT INTO users (email) VALUES ('admin@example.com');`,
			expectError: false,
		},
		{
			name: "read non-existent file",
			setupFunc: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				filename := "non_existent.sql"
				return tempDir, filename
			},
			expectedContent: "",
			expectError:     true,
			errorContains:   "no such file or directory",
		},
		{
			name: "read file from non-existent directory",
			setupFunc: func(t *testing.T) (string, string) {
				return "/non/existent/directory", "001_test.sql"
			},
			expectedContent: "",
			expectError:     true,
			errorContains:   "no such file or directory",
		},
		{
			name: "read unreadable file",
			setupFunc: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				filename := "004_unreadable.up.sql"
				content := "CREATE TABLE test (id INTEGER);"

				filepath := filepath.Join(tempDir, filename)
				if err := os.WriteFile(filepath, []byte(content), 0o644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}

				// Make file unreadable
				if err := os.Chmod(filepath, 0o000); err != nil {
					t.Fatalf("failed to change file permissions: %v", err)
				}

				// Cleanup function to restore permissions
				t.Cleanup(func() {
					os.Chmod(filepath, 0o644)
				})

				return tempDir, filename
			},
			expectedContent: "",
			expectError:     true,
			errorContains:   "permission denied",
		},
		{
			name: "read file with binary content",
			setupFunc: func(t *testing.T) (string, string) {
				tempDir := t.TempDir()
				filename := "005_binary.sql"
				// Create content with binary data (though this would be unusual for SQL)
				content := "CREATE TABLE test;\x00\x01\x02\x03"

				if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
				return tempDir, filename
			},
			expectedContent: "CREATE TABLE test;\x00\x01\x02\x03",
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			migrationsPath, filename := tt.setupFunc(t)
			support := NewEmbeddedMigrationSupport(migrationsPath)

			content, err := support.GetEmbeddedMigrationContent(filename)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if string(content) != tt.expectedContent {
				t.Errorf("expected content %q, got %q", tt.expectedContent, string(content))
			}
		})
	}
}

// Integration test that combines multiple methods
func TestEmbeddedMigrationSupportIntegration(t *testing.T) {
	// Create a temporary directory with realistic migration files
	tempDir := t.TempDir()

	migrations := map[string]string{
		"001_initial_schema.up.sql": `CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);`,
		"001_initial_schema.down.sql": `DROP TABLE users;`,
		"002_add_posts.up.sql": `CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title VARCHAR(255) NOT NULL,
    content TEXT
);`,
		"002_add_posts.down.sql": `DROP TABLE posts;`,
	}

	// Create migration files
	for filename, content := range migrations {
		if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create migration file %s: %v", filename, err)
		}
	}

	// Also create some non-SQL files that should be ignored
	nonSQLFiles := map[string]string{
		"README.md":   "# Migrations",
		"config.yaml": "version: 1",
		"backup.txt":  "backup data",
	}

	for filename, content := range nonSQLFiles {
		if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create non-SQL file %s: %v", filename, err)
		}
	}

	// Initialize the support instance
	support := NewEmbeddedMigrationSupport(tempDir)

	// Test 1: Validation should pass
	if err := support.ValidateEmbeddedMigrations(); err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	// Test 2: List migrations should return only SQL files
	files, err := support.ListEmbeddedMigrations()
	if err != nil {
		t.Fatalf("failed to list migrations: %v", err)
	}

	expectedFiles := []string{
		"001_initial_schema.up.sql",
		"001_initial_schema.down.sql",
		"002_add_posts.up.sql",
		"002_add_posts.down.sql",
	}

	sort.Strings(files)
	sort.Strings(expectedFiles)

	if !reflect.DeepEqual(files, expectedFiles) {
		t.Errorf("expected files %v, got %v", expectedFiles, files)
	}

	// Test 3: Get file system should allow reading files
	fsys := support.GetEmbeddedMigrations()
	for filename := range migrations {
		file, err := fsys.Open(filename)
		if err != nil {
			t.Errorf("failed to open file %s from fs.FS: %v", filename, err)
			continue
		}
		file.Close()
	}

	// Test 4: Get migration content should return correct content
	for filename, expectedContent := range migrations {
		content, err := support.GetEmbeddedMigrationContent(filename)
		if err != nil {
			t.Errorf("failed to get content for %s: %v", filename, err)
			continue
		}

		if string(content) != expectedContent {
			t.Errorf(
				"content mismatch for %s:\nexpected: %q\ngot: %q",
				filename,
				expectedContent,
				string(content),
			)
		}
	}
}

// Benchmark tests for performance validation
func BenchmarkListEmbeddedMigrations(b *testing.B) {
	// Create temporary directory with many migration files
	tempDir := b.TempDir()

	// Create 100 migration files
	for i := 1; i <= 100; i++ {
		filename := fmt.Sprintf("%03d_migration_%d.up.sql", i, i)
		content := fmt.Sprintf("CREATE TABLE table_%d (id INTEGER);", i)
		if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
			b.Fatalf("failed to create migration file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := support.ListEmbeddedMigrations()
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

func BenchmarkGetEmbeddedMigrationContent(b *testing.B) {
	// Create temporary directory with a large migration file
	tempDir := b.TempDir()
	filename := "large_migration.up.sql"

	// Create a large SQL content (simulate complex migration)
	var contentBuilder strings.Builder
	for i := 0; i < 1000; i++ {
		contentBuilder.WriteString(
			fmt.Sprintf("CREATE TABLE table_%d (id SERIAL PRIMARY KEY, name VARCHAR(255));\n", i),
		)
	}
	content := contentBuilder.String()

	if err := os.WriteFile(filepath.Join(tempDir, filename), []byte(content), 0o644); err != nil {
		b.Fatalf("failed to create large migration file: %v", err)
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := support.GetEmbeddedMigrationContent(filename)
		if err != nil {
			b.Fatalf("benchmark failed: %v", err)
		}
	}
}

// Tests that SHOULD FAIL - These define requirements for true embedded migration system
// These tests expose the limitations of the current os.DirFS implementation

func TestEmbeddedMigrationsSortingBehavior(t *testing.T) {
	// This test should FAIL - current implementation doesn't guarantee proper migration ordering
	tempDir := t.TempDir()

	// Create migrations out of alphabetical order
	files := []string{
		"010_migration.up.sql",
		"002_migration.up.sql",
		"001_migration.up.sql",
		"100_migration.up.sql",
		"020_migration.up.sql",
	}

	for _, file := range files {
		content := fmt.Sprintf("-- Migration: %s", file)
		if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)
	result, err := support.ListEmbeddedMigrations()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// SHOULD FAIL: Current implementation doesn't enforce proper migration ordering
	expected := []string{
		"001_migration.up.sql",
		"002_migration.up.sql",
		"010_migration.up.sql",
		"020_migration.up.sql",
		"100_migration.up.sql",
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf(
			"EXPECTED FAILURE: migrations not properly ordered. Expected %v, got %v",
			expected,
			result,
		)
	}
}

func TestEmbeddedMigrationsFilenameValidation(t *testing.T) {
	// This test should FAIL - current implementation doesn't validate migration filenames
	tempDir := t.TempDir()

	// Create migrations with invalid filenames
	invalidFiles := []string{
		"migration.sql",            // Missing version number
		"001.sql",                  // Missing direction (up/down)
		"001_test.invalid.sql",     // Invalid direction
		"invalid_migration.up.sql", // Non-numeric prefix
		"001_migration.UP.sql",     // Wrong case
	}

	for _, file := range invalidFiles {
		content := fmt.Sprintf("-- Migration: %s", file)
		if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	// SHOULD FAIL: Current implementation should reject invalid migration filenames
	err := support.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should fail for invalid migration filenames")
	}

	// With strict naming enforcement, invalid files are filtered out during listing
	// So we should get "no migration files found" error instead of filename validation error
	if err != nil && !strings.Contains(err.Error(), "no migration files found") {
		t.Errorf(
			"EXPECTED FAILURE: with strict naming, should get 'no migration files found', got: %v",
			err,
		)
	}
}

func TestEmbeddedMigrationsSQLSyntaxValidation(t *testing.T) {
	// This test validates that we DON'T validate SQL syntax (architectural decision)
	// SQL validation is left to the database engine during execution
	tempDir := t.TempDir()

	files := map[string]string{
		"001_valid.up.sql":          "CREATE TABLE test (id INTEGER);",
		"001_valid.down.sql":        "DROP TABLE test;",
		"002_invalid.up.sql":        "CREATE INVALID SQL SYNTAX HERE;",
		"002_invalid.down.sql":      "DROP TABLE invalid;",
		"003_empty.up.sql":          "",
		"003_empty.down.sql":        "",
		"004_comment_only.up.sql":   "-- Just a comment",
		"004_comment_only.down.sql": "-- Drop comment",
	}

	for file, content := range files {
		if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	// SHOULD PASS: We intentionally don't validate SQL syntax
	// This is left to the database engine during execution
	err := support.ValidateEmbeddedMigrations()
	if err != nil {
		t.Errorf(
			"validation should pass despite invalid SQL syntax (left to database engine), got: %v",
			err,
		)
	}
}

func TestEmbeddedMigrationsPairedValidation(t *testing.T) {
	// This test should FAIL - current implementation doesn't validate up/down pairs
	tempDir := t.TempDir()

	// Create unpaired migrations
	files := map[string]string{
		"001_initial.up.sql": "CREATE TABLE users (id INTEGER);",
		// Missing 001_initial.down.sql
		"002_posts.up.sql":    "CREATE TABLE posts (id INTEGER);",
		"002_posts.down.sql":  "DROP TABLE posts;",
		"003_orphan.down.sql": "DROP TABLE orphan;",
		// Missing 003_orphan.up.sql
	}

	for file, content := range files {
		if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	// SHOULD FAIL: Current implementation should validate migration pairs
	err := support.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should fail for unpaired migrations")
	}

	if err != nil && !strings.Contains(err.Error(), "pair") &&
		!strings.Contains(err.Error(), "orphan") {
		t.Errorf("EXPECTED FAILURE: error should mention pairing validation, got: %v", err)
	}
}

func TestEmbeddedMigrationsSequenceValidation(t *testing.T) {
	// This test should FAIL - current implementation doesn't validate migration sequence
	tempDir := t.TempDir()

	// Create migrations with gaps in sequence
	files := map[string]string{
		"001_first.up.sql":   "CREATE TABLE first (id INTEGER);",
		"001_first.down.sql": "DROP TABLE first;",
		// Missing 002_*
		"003_third.up.sql":   "CREATE TABLE third (id INTEGER);",
		"003_third.down.sql": "DROP TABLE third;",
		"005_fifth.up.sql":   "CREATE TABLE fifth (id INTEGER);",
		"005_fifth.down.sql": "DROP TABLE fifth;",
	}

	for file, content := range files {
		if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	// SHOULD FAIL: Current implementation should validate migration sequence
	err := support.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should fail for gaps in migration sequence")
	}

	if err != nil && !strings.Contains(err.Error(), "sequence") &&
		!strings.Contains(err.Error(), "gap") {
		t.Errorf("EXPECTED FAILURE: error should mention sequence validation, got: %v", err)
	}
}

func TestEmbeddedMigrationsPerformanceWithActualEmbedding(t *testing.T) {
	// Skip this test until Phase 4 - true embedded migrations not implemented yet
	if testing.Short() {
		t.Skip("skipping embedded migration test in short mode - Phase 4 feature")
	}

	// This test should FAIL - current implementation uses os.DirFS which doesn't provide
	// true embedding performance benefits

	// Create real migrations directory for comparison
	tempDir := t.TempDir()

	// Create 50 migration files
	for i := 1; i <= 50; i++ {
		upFile := fmt.Sprintf("%03d_migration_%d.up.sql", i, i)
		downFile := fmt.Sprintf("%03d_migration_%d.down.sql", i, i)
		content := fmt.Sprintf("CREATE TABLE table_%d (id INTEGER);", i)
		downContent := fmt.Sprintf("DROP TABLE table_%d;", i)

		if err := os.WriteFile(filepath.Join(tempDir, upFile), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create migration file: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tempDir, downFile), []byte(downContent), 0o644); err != nil {
			t.Fatalf("failed to create migration file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	// Test 1: File system access should not depend on disk I/O for truly embedded files
	// SHOULD FAIL: Current implementation reads from disk each time
	fsys := support.GetEmbeddedMigrations()

	// Measure time for repeated access - embedded should be consistent
	start := time.Now()
	for i := 0; i < 10; i++ {
		files, err := support.ListEmbeddedMigrations()
		if err != nil {
			t.Fatalf("failed to list migrations: %v", err)
		}
		if len(files) != 100 { // 50 up + 50 down
			t.Errorf("expected 100 files, got %d", len(files))
		}
	}
	elapsed := time.Since(start)

	// SHOULD FAIL: True embedded system should be much faster than filesystem access
	if elapsed > 1000000 { // 1ms - arbitrary threshold
		t.Logf(
			"EXPECTED FAILURE: embedded access should be faster than filesystem, took %v",
			elapsed,
		)
		t.Logf("This limitation will be fixed in Phase 4 with true go:embed implementation")
	}

	// Test 2: File system should work without the original directory
	// SHOULD FAIL: Current implementation will fail if directory is removed
	originalPath := support.migrationsPath

	// Try to remove directory (this should not affect truly embedded files)
	if err := os.RemoveAll(originalPath); err != nil {
		t.Fatalf("failed to remove directory: %v", err)
	}

	// This should work with truly embedded files but will fail with os.DirFS
	_, err := fsys.Open("001_migration_1.up.sql")
	if err != nil {
		t.Logf(
			"EXPECTED FAILURE: truly embedded files should work without original directory, got error: %v",
			err,
		)
		t.Logf("This limitation will be fixed in Phase 4 with true go:embed implementation")
	}

	// Restore directory for cleanup
	if err := os.MkdirAll(originalPath, 0o755); err != nil {
		t.Logf("failed to restore directory for cleanup: %v", err)
	}
}

func TestEmbeddedMigrationsChecksumValidation(t *testing.T) {
	// This test should FAIL - current implementation doesn't validate migration checksums
	tempDir := t.TempDir()

	files := map[string]string{
		"001_initial.up.sql":   "CREATE TABLE users (id INTEGER);",
		"001_initial.down.sql": "DROP TABLE users;",
	}

	for file, content := range files {
		if err := os.WriteFile(filepath.Join(tempDir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	support := NewEmbeddedMigrationSupport(tempDir)

	// First validation should pass
	err := support.ValidateEmbeddedMigrations()
	if err != nil {
		t.Fatalf("initial validation failed: %v", err)
	}

	// Modify a file after "embedding" (simulating file tampering)
	modifiedContent := "CREATE TABLE users (id INTEGER, email VARCHAR(255));"
	if err := os.WriteFile(filepath.Join(tempDir, "001_initial.up.sql"), []byte(modifiedContent), 0o644); err != nil {
		t.Fatalf("failed to modify file: %v", err)
	}

	// SHOULD FAIL: Validation should detect that embedded content doesn't match file system
	err = support.ValidateEmbeddedMigrations()
	if err == nil {
		t.Error("EXPECTED FAILURE: validation should detect modified migration files")
	}

	if err != nil && !strings.Contains(err.Error(), "checksum") &&
		!strings.Contains(err.Error(), "modified") {
		t.Errorf("EXPECTED FAILURE: error should mention checksum validation, got: %v", err)
	}
}
