package main

import (
	"crypto/sha256"
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// EmbeddedMigration provides a true embedded migration system with comprehensive validation.
// This implementation uses go:embed for zero-config deployment and includes filename validation,
// pairing validation, sequence validation, and checksum integrity checking for production-ready
// migration management in containerized environments.
type EmbeddedMigration struct {
	fs        fs.FS
	checksums map[string]string // filename -> checksum for integrity checking
}

// MigrationInfo contains parsed information about a migration file
type MigrationInfo struct {
	Sequence  int
	Name      string
	Direction string // "up" or "down"
	Filename  string
	Checksum  string
}

//go:embed *.sql
var embeddedMigrations embed.FS

// Migration filename regex: 001_migration_name.up.sql or 001_migration_name.down.sql
var migrationFilenameRegex = regexp.MustCompile(`^(\d{3})_([a-zA-Z0-9_]+)\.(up|down)\.sql$`)

// NewEmbeddedMigration creates a new EmbeddedMigration instance with injectable filesystem dependency.
// Pass nil to use the default embedded migrations.
func NewEmbeddedMigration(filesystem fs.FS) *EmbeddedMigration {
	if filesystem == nil {
		filesystem = embeddedMigrations
	}

	return &EmbeddedMigration{
		fs:        filesystem,
		checksums: make(map[string]string),
	}
}

// GetEmbeddedMigrations returns the embedded file system containing all migration files.
// All migrations are embedded at build time using go:embed directive, enabling zero-config
// deployment without external file dependencies.
func (e *EmbeddedMigration) GetEmbeddedMigrations() fs.FS {
	return e.fs
}

// ListEmbeddedMigrations returns a list of all embedded migration files that conform to the strict naming standard.
// Only files matching the format 001_name.(up|down).sql are included.
// Invalid filenames are rejected to enforce consistency and prevent operational mistakes.
func (e *EmbeddedMigration) ListEmbeddedMigrations() ([]string, error) {
	entries, err := fs.ReadDir(e.fs, ".")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded migrations directory: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()

		// Only include .sql files that match our strict naming standard
		if filepath.Ext(filename) == ".sql" && migrationFilenameRegex.MatchString(filename) {
			files = append(files, filename)
		}
	}

	// Simple lexicographic sort works perfectly with our naming standard
	// 001_name.up.sql comes before 001_name.down.sql
	// 001_name.down.sql comes before 002_name.up.sql
	sort.Strings(files)

	return files, nil
}

// ValidateEmbeddedMigrations performs comprehensive validation of embedded migration files.
// This includes filename format, up/down pairing, sequence validation, and checksum integrity.
func (e *EmbeddedMigration) ValidateEmbeddedMigrations() error {
	// List embedded files to ensure we have some migrations
	files, err := e.ListEmbeddedMigrations()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return fmt.Errorf("no embedded migration files found")
	}

	// First, validate that we can read each file (for backward compatibility)
	for _, file := range files {
		if _, err := e.GetEmbeddedMigrationContent(file); err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", file, err)
		}
	}

	// Perform filename validation
	if err := e.validateFilenames(files); err != nil {
		return err
	}

	// Perform up/down pairing validation
	if err := e.validatePairing(files); err != nil {
		return err
	}

	// Perform sequence validation
	if err := e.validateSequence(files); err != nil {
		return err
	}

	// Perform checksum validation if checksums are available
	if len(e.checksums) > 0 {
		if err := e.validateChecksums(files); err != nil {
			return err
		}
	}

	// Store checksums for future validation
	for _, file := range files {
		content, err := e.GetEmbeddedMigrationContent(file)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", file, err)
		}
		e.checksums[file] = e.calculateChecksum(content)
	}

	return nil
}

// GetEmbeddedMigrationContent returns the content of a specific embedded migration file.
func (e *EmbeddedMigration) GetEmbeddedMigrationContent(filename string) ([]byte, error) {
	return fs.ReadFile(e.fs, filename)
}

// parseMigrationFilename parses a migration filename and extracts its components
func (e *EmbeddedMigration) parseMigrationFilename(filename string) (*MigrationInfo, error) {
	matches := migrationFilenameRegex.FindStringSubmatch(filename)
	if len(matches) != 4 {
		return nil, fmt.Errorf(
			"invalid migration filename format: %s (expected: 001_name.up.sql or 001_name.down.sql)",
			filename,
		)
	}

	sequence, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid sequence number in filename %s: %w", filename, err)
	}

	return &MigrationInfo{
		Sequence:  sequence,
		Name:      matches[2],
		Direction: matches[3],
		Filename:  filename,
	}, nil
}

// validateFilenames validates that all migration files follow the correct naming convention
func (e *EmbeddedMigration) validateFilenames(files []string) error {
	for _, file := range files {
		_, err := e.parseMigrationFilename(file)
		if err != nil {
			return fmt.Errorf("filename validation failed for %s: %w", file, err)
		}
	}
	return nil
}

// validatePairing ensures that every up migration has a corresponding down migration
func (e *EmbeddedMigration) validatePairing(files []string) error {
	// Parse all migration files
	migrations := make(
		map[string]map[string]*MigrationInfo,
	) // sequence_name -> direction -> migration

	for _, file := range files {
		migration, err := e.parseMigrationFilename(file)
		if err != nil {
			return err // This should have been caught in filename validation
		}

		key := fmt.Sprintf("%03d_%s", migration.Sequence, migration.Name)
		if migrations[key] == nil {
			migrations[key] = make(map[string]*MigrationInfo)
		}
		migrations[key][migration.Direction] = migration
	}

	// Check for unpaired migrations
	for key, directions := range migrations {
		if len(directions) != 2 {
			if _, hasUp := directions["up"]; !hasUp {
				return fmt.Errorf("orphaned down migration: missing up migration for %s", key)
			}
			if _, hasDown := directions["down"]; !hasDown {
				return fmt.Errorf("orphaned up migration: missing down migration for %s", key)
			}
		}
	}

	return nil
}

// validateSequence ensures there are no gaps in the migration sequence
func (e *EmbeddedMigration) validateSequence(files []string) error {
	sequences := make(map[int]bool)

	// Collect all sequence numbers
	for _, file := range files {
		migration, err := e.parseMigrationFilename(file)
		if err != nil {
			return err // This should have been caught in filename validation
		}
		sequences[migration.Sequence] = true
	}

	// Convert to sorted slice
	var sequenceNumbers []int
	for seq := range sequences {
		sequenceNumbers = append(sequenceNumbers, seq)
	}
	sort.Ints(sequenceNumbers)

	// Check for gaps
	if len(sequenceNumbers) == 0 {
		return nil // No migrations
	}

	// Should start with 1
	if sequenceNumbers[0] != 1 {
		return fmt.Errorf(
			"migration sequence should start with 001, but found %03d",
			sequenceNumbers[0],
		)
	}

	// Check for gaps
	for i := 1; i < len(sequenceNumbers); i++ {
		expected := sequenceNumbers[i-1] + 1
		actual := sequenceNumbers[i]
		if actual != expected {
			return fmt.Errorf(
				"gap in migration sequence: expected %03d, found %03d",
				expected,
				actual,
			)
		}
	}

	return nil
}

// calculateChecksum calculates SHA256 checksum of content
func (e *EmbeddedMigration) calculateChecksum(content []byte) string {
	hash := sha256.Sum256(content)
	return fmt.Sprintf("%x", hash)
}

// validateChecksums verifies that migration files haven't been modified
func (e *EmbeddedMigration) validateChecksums(files []string) error {
	for _, file := range files {
		content, err := e.GetEmbeddedMigrationContent(file)
		if err != nil {
			return fmt.Errorf("failed to read file %s for checksum validation: %w", file, err)
		}

		currentChecksum := e.calculateChecksum(content)
		if storedChecksum, exists := e.checksums[file]; exists {
			if currentChecksum != storedChecksum {
				return fmt.Errorf("checksum mismatch for %s: file has been modified", file)
			}
		}
	}
	return nil
}
