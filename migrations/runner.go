package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	_ "github.com/lib/pq" // PostgreSQL driver
)

type (
	// MigrationRunner defines the interface for running database migrations.
	MigrationRunner interface {
		// Up applies all pending migrations
		Up() error

		// Down rollbacks the last migration
		Down() error

		// Status shows the current migration status
		Status() error

		// Version shows the current migration version
		Version() error

		// Drop drops all tables (destructive operation)
		Drop() error

		// Close closes any open connections
		Close() error
	}

	// Runner implements MigrationRunner using golang-migrate.
	Runner struct {
		config            *Config
		migrate           *migrate.Migrate
		db                *sql.DB
		embeddedMigration *EmbeddedMigration // For embedded migration validation and access
	}

	// migrateLogger implements the migrate.Logger interface.
	migrateLogger struct{}
)

// Ensure we implement the interface at compile time.
var _ migrate.Logger = (*migrateLogger)(nil)

// Add io.Writer interface compliance for broader compatibility.
var _ io.Writer = (*migrateLogger)(nil)

// NewMigrationRunner creates a new migration runner with the given configuration.
func NewMigrationRunner(config *Config) (*Runner, error) {
	log.Printf("Initializing migration runner with config: %s", config.String())

	// Initialize embedded migration
	embeddedMigration := NewEmbeddedMigration(nil)

	// Perform startup validation of embedded migrations
	log.Println("Validating embedded migrations at startup...")

	err := embeddedMigration.ValidateEmbeddedMigrations()
	if err != nil {
		return nil, fmt.Errorf("embedded migration validation failed: %w", err)
	}

	log.Println("Embedded migration validation passed")

	// Open database connection
	db, err := sql.Open("postgres", config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test database connection
	err = db.PingContext(context.Background())
	if err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Database connection established successfully")

	// Create database driver
	driver, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: config.MigrationTable,
	})
	if err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("failed to create postgres driver: %w", err)
	}

	log.Println("Using embedded migrations")

	// Create iofs source driver from embedded file system
	sourceDriver, err := iofs.New(embeddedMigration.GetEmbeddedMigrations(), ".")
	if err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("failed to create embedded migration source: %w", err)
	}

	// Create migrate instance with embedded migrations
	m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", driver)
	if err != nil {
		_ = db.Close()

		return nil, fmt.Errorf(
			"failed to create migrate instance with embedded migrations: %w",
			err,
		)
	}

	// Set up logging for migrate
	m.Log = &migrateLogger{}

	log.Println("Migration runner initialized successfully")

	return &Runner{
		config:            config,
		migrate:           m,
		db:                db,
		embeddedMigration: embeddedMigration,
	}, nil
}

// Up applies all pending migrations.
func (r *Runner) Up() error {
	// Validate embedded migrations before state-changing operations
	log.Println("Pre-operation validation: checking embedded migrations...")

	err := r.embeddedMigration.ValidateEmbeddedMigrations()
	if err != nil {
		return fmt.Errorf("pre-operation validation failed: %w", err)
	}

	log.Println("Starting migration up...")

	err = r.migrate.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migration up failed: %w", err)
	}

	if errors.Is(err, migrate.ErrNoChange) {
		log.Println("No new migrations to apply")
	} else {
		log.Println("All migrations applied successfully")
	}

	return nil
}

// Down rollbacks the last migration.
func (r *Runner) Down() error {
	// Validate embedded migrations before state-changing operations
	log.Println("Pre-operation validation: checking embedded migrations...")

	err := r.embeddedMigration.ValidateEmbeddedMigrations()
	if err != nil {
		return fmt.Errorf("pre-operation validation failed: %w", err)
	}

	log.Println("Starting migration down...")

	err = r.migrate.Steps(-1)
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migration down failed: %w", err)
	}

	if errors.Is(err, migrate.ErrNoChange) {
		log.Println("No migrations to rollback")
	} else {
		log.Println("Last migration rolled back successfully")
	}

	return nil
}

// Status shows the current migration status with schema compatibility information.
func (r *Runner) Status() error {
	ver, dirty, err := r.migrate.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			log.Println("Migration Status: No migrations applied yet")
			r.showSchemaCompatibility(0)

			return nil
		}

		return fmt.Errorf("failed to get migration version: %w", err)
	}

	status := "clean"
	if dirty {
		status = "dirty (needs manual intervention)"
	}

	log.Printf("Migration Status: Version %d (%s)\n", ver, status)

	// Show schema compatibility information
	r.showSchemaCompatibility(int(ver)) // #nosec G115 - version numbers are safe to convert

	// Additional information about pending migrations
	err = r.showPendingMigrations()
	if err != nil {
		log.Printf("Warning: Could not determine pending migrations: %v", err)
	}

	return nil
}

// Version shows the current migration version with schema compatibility.
func (r *Runner) Version() error {
	ver, dirty, err := r.migrate.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			log.Println("Current Version: No migrations applied")
			r.showSchemaCompatibility(0)

			return nil
		}

		return fmt.Errorf("failed to get migration version: %w", err)
	}

	dirtyNote := ""
	if dirty {
		dirtyNote = " (dirty)"
	}

	log.Printf("Current Version: %d%s\n", ver, dirtyNote)

	// Show schema compatibility information
	r.showSchemaCompatibility(int(ver)) // #nosec G115 - version numbers are safe to convert

	return nil
}

// Drop drops all tables (destructive operation).
func (r *Runner) Drop() error {
	// Validate embedded migrations before state-changing operations
	log.Println("Pre-operation validation: checking embedded migrations...")

	err := r.embeddedMigration.ValidateEmbeddedMigrations()
	if err != nil {
		return fmt.Errorf("pre-operation validation failed: %w", err)
	}

	log.Println("WARNING: Dropping all tables...")

	err = r.migrate.Drop()
	if err != nil {
		return fmt.Errorf("drop operation failed: %w", err)
	}

	log.Println("All tables dropped successfully")

	return nil
}

// Close closes database connections.
func (r *Runner) Close() error {
	var errs []error

	if r.migrate != nil {
		sourceErr, dbErr := r.migrate.Close()
		if sourceErr != nil {
			errs = append(errs, fmt.Errorf("source close error: %w", sourceErr))
		}

		if dbErr != nil {
			errs = append(errs, fmt.Errorf("database close error: %w", dbErr))
		}
	}

	if r.db != nil {
		err := r.db.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("database connection close error: %w", err))
		}
	}

	return errors.Join(errs...)
}

// showPendingMigrations attempts to show information about pending migrations.
func (r *Runner) showPendingMigrations() error {
	// This is a best-effort attempt to show pending migrations
	// The golang-migrate library doesn't provide a direct way to list pending migrations
	// In a production system, you might want to implement this by reading the source
	// and comparing with the current version

	// For now, we'll just indicate that this feature could be enhanced
	log.Println("Note: Use 'up' command to apply any pending migrations")

	return nil
}

// showSchemaCompatibility displays schema version compatibility information
// between the migrator tool capabilities and current database state.
func (r *Runner) showSchemaCompatibility(currentVersion int) {
	maxSchemaVersion := r.getMaxEmbeddedSchemaVersion()

	log.Printf("Schema Compatibility:")
	log.Printf("  Database Schema: v%03d", currentVersion)
	log.Printf("  Migrator Supports: v%03d", maxSchemaVersion)

	switch {
	case currentVersion == maxSchemaVersion:
		log.Printf("  Status: ✅ Up to date")
	case currentVersion < maxSchemaVersion:
		pending := maxSchemaVersion - currentVersion
		log.Printf("  Status: ⬆️  %d migration(s) available", pending)
	default:
		log.Printf("  Status: ⚠️  Database schema newer than migrator supports")
		log.Printf(
			"  Warning: Please update migrator tool to handle schema v%03d",
			currentVersion,
		)
	}
}

// getMaxEmbeddedSchemaVersion returns the highest migration sequence number
// from embedded migration files in this migrator binary.
func (r *Runner) getMaxEmbeddedSchemaVersion() int {
	files, err := r.embeddedMigration.ListEmbeddedMigrations()
	if err != nil {
		return 0 // If we can't read migrations, assume no schema support
	}

	maxSequence := 0

	for _, filename := range files {
		if migration, err := r.embeddedMigration.parseMigrationFilename(filename); err == nil {
			if migration.Sequence > maxSequence {
				maxSequence = migration.Sequence
			}
		}
	}

	return maxSequence
}

func (l *migrateLogger) Printf(format string, v ...interface{}) {
	log.Printf("[MIGRATE] "+format, v...)
}

func (l *migrateLogger) Verbose() bool {
	return true
}

func (l *migrateLogger) Write(p []byte) (int, error) {
	log.Printf("[MIGRATE] %s", string(p))

	return len(p), nil
}
