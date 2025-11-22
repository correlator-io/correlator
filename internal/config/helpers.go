// Package config provides configuration and shared test utilities for the Correlator application.
package config

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file" // used to run migrations using source files
)

const (
	occurrenceCount = 2
	startUpTimeOut  = 120 * time.Second
)

// TestDatabase encapsulates test database resources for cleanup.
// Used by integration tests across multiple packages to maintain consistent test infrastructure.
type TestDatabase struct {
	Container  *postgres.PostgresContainer
	Connection *sql.DB
}

// SetupTestDatabase creates a PostgreSQL container and runs migrations.
// This is the standard way to set up integration test databases across all packages.
//
// Usage:
//
//	func TestMyFeature(t *testing.T) {
//		if testing.Short() {
//			t.Skip("skipping integration test in short mode")
//		}
//		ctx := context.Background()
//		testDB := config.SetupTestDatabase(ctx, t)
//		t.Cleanup(func() {
//			_ = testDB.Connection.Close()
//			_ = testcontainers.TerminateContainer(testDB.Container)
//		})
//		// ... your test code
//	}
//
// The function automatically:
//   - Creates a PostgreSQL 16-alpine container
//   - Waits for the database to be ready
//   - Runs all migrations from the migrations/ directory
//   - Returns a TestDatabase with an active connection
//
// Cleanup is the caller's responsibility using t.Cleanup().
func SetupTestDatabase(ctx context.Context, t *testing.T) *TestDatabase {
	t.Helper()

	// Create PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("correlator_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(occurrenceCount).
				WithStartupTimeout(startUpTimeOut),
		),
	)
	require.NoError(t, err, "Failed to start postgres container")
	require.NotNil(t, pgContainer, "postgres container is nil")

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err, "Failed to get connection string")

	// Create storage connection
	conn, err := sql.Open("postgres", connStr)
	require.NoError(t, err, "Failed to open database")

	// Run migrations
	if err := RunTestMigrations(conn); err != nil {
		_ = conn.Close()
		_ = testcontainers.TerminateContainer(pgContainer)

		t.Fatalf("Failed to run migrations: %v", err)
	}

	return &TestDatabase{
		Container:  pgContainer,
		Connection: conn,
	}
}

// RunTestMigrations applies all migrations from the migrations directory using golang-migrate.
// This function uses file:// source pointing to actual migrations directory (no duplication).
//
// The migration path is relative to the package calling this function:
//   - internal/config:    ../../migrations
//   - internal/api:       ../../migrations
//   - internal/storage:   ../../migrations
//   - internal/correlation: ../../migrations
//
// This works because all these packages are at the same depth relative to the project root.
//
// Returns:
//   - nil if migrations succeed or no changes needed
//   - error if migrations fail
func RunTestMigrations(db *sql.DB) error {
	// Create database driver
	driver, err := migratepg.WithInstance(db, &migratepg.Config{})
	if err != nil {
		return err
	}

	// Use file source pointing to migrations directory
	// Path is relative to project root: internal/config -> ../../migrations
	m, err := migrate.NewWithDatabaseInstance(
		"file://../../migrations",
		"postgres",
		driver,
	)
	if err != nil {
		return err
	}

	// Run all migrations up
	// ErrNoChange is not an error - it means migrations are already applied
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}
