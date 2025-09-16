package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/golang-migrate/migrate/v4/database/postgres"

	_ "github.com/golang-migrate/migrate/v4/source/file" // File source driver
	_ "github.com/lib/pq"                                // PostgreSQL driver

	migrate "github.com/golang-migrate/migrate/v4"
)

type (
	// MigrationRunner defines the interface for running database migrations
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

	// migrationRunner implements MigrationRunner using golang-migrate
	migrationRunner struct {
		config  *Config
		migrate *migrate.Migrate
		db      *sql.DB
	}

	// migrateLogger implements the migrate.Logger interface
	migrateLogger struct{}
)

// Ensure we implement the interface at compile time
var _ migrate.Logger = (*migrateLogger)(nil)

// Add io.Writer interface compliance for broader compatibility
var _ io.Writer = (*migrateLogger)(nil)

// NewMigrationRunner creates a new migration runner with the given configuration
func NewMigrationRunner(config *Config) (MigrationRunner, error) {
	log.Printf("Initializing migration runner with config: %s", config.String())

	// Open database connection
	db, err := sql.Open("postgres", config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test database connection
	if err := db.Ping(); err != nil {
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

	// Create source URL for file-based migrations
	// TODO: Add embedded migration support in Phase 4
	log.Printf("Using file system migrations from: %s", config.MigrationsPath)
	sourceURL := fmt.Sprintf("file://%s", config.MigrationsPath)

	// Create migrate instance with file-based migrations
	m, err := migrate.NewWithDatabaseInstance(sourceURL, "postgres", driver)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create migrate instance: %w", err)
	}

	// Set up logging for migrate
	m.Log = &migrateLogger{}

	log.Println("Migration runner initialized successfully")

	return &migrationRunner{
		config:  config,
		migrate: m,
		db:      db,
	}, nil
}

// Up applies all pending migrations
func (r *migrationRunner) Up() error {
	log.Println("Starting migration up...")

	err := r.migrate.Up()
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

// Down rollbacks the last migration
func (r *migrationRunner) Down() error {
	log.Println("Starting migration down...")

	err := r.migrate.Steps(-1)
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

// Status shows the current migration status
func (r *migrationRunner) Status() error {
	ver, dirty, err := r.migrate.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			fmt.Println("Migration Status: No migrations applied yet")
			return nil
		}
		return fmt.Errorf("failed to get migration version: %w", err)
	}

	status := "clean"
	if dirty {
		status = "dirty (needs manual intervention)"
	}

	fmt.Printf("Migration Status: Version %d (%s)\n", ver, status)

	// Additional information about pending migrations
	if err := r.showPendingMigrations(); err != nil {
		log.Printf("Warning: Could not determine pending migrations: %v", err)
	}

	return nil
}

// Version shows the current migration version
func (r *migrationRunner) Version() error {
	ver, dirty, err := r.migrate.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			fmt.Println("Current Version: No migrations applied")
			return nil
		}
		return fmt.Errorf("failed to get migration version: %w", err)
	}

	dirtyNote := ""
	if dirty {
		dirtyNote = " (dirty)"
	}

	fmt.Printf("Current Version: %d%s\n", ver, dirtyNote)
	return nil
}

// Drop drops all tables (destructive operation)
func (r *migrationRunner) Drop() error {
	log.Println("WARNING: Dropping all tables...")

	err := r.migrate.Drop()
	if err != nil {
		return fmt.Errorf("drop operation failed: %w", err)
	}

	log.Println("All tables dropped successfully")
	return nil
}

// Close closes database connections
func (r *migrationRunner) Close() error {
	var errs []error

	if r.migrate != nil {
		if sourceErr, dbErr := r.migrate.Close(); sourceErr != nil || dbErr != nil {
			if sourceErr != nil {
				errs = append(errs, fmt.Errorf("source close error: %w", sourceErr))
			}
			if dbErr != nil {
				errs = append(errs, fmt.Errorf("database close error: %w", dbErr))
			}
		}
	}

	if r.db != nil {
		if err := r.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("database connection close error: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}

	return nil
}

// showPendingMigrations attempts to show information about pending migrations
func (r *migrationRunner) showPendingMigrations() error {
	// This is a best-effort attempt to show pending migrations
	// The golang-migrate library doesn't provide a direct way to list pending migrations
	// In a production system, you might want to implement this by reading the source
	// and comparing with the current version

	// For now, we'll just indicate that this feature could be enhanced
	fmt.Println("Note: Use 'up' command to apply any pending migrations")
	return nil
}

func (l *migrateLogger) Printf(format string, v ...interface{}) {
	log.Printf("[MIGRATE] "+format, v...)
}

func (l *migrateLogger) Verbose() bool {
	return true
}

func (l *migrateLogger) Write(p []byte) (n int, err error) {
	log.Printf("[MIGRATE] %s", string(p))
	return len(p), nil
}
