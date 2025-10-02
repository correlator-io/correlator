package storage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestDatabase creates a PostgreSQL testcontainer and runs migrations.
func setupTestDatabase(ctx context.Context, t *testing.T) (*pgcontainer.PostgresContainer, *Connection) {
	t.Helper()

	// Create PostgreSQL container
	postgresContainer, err := pgcontainer.Run(ctx,
		"postgres:16-alpine",
		pgcontainer.WithDatabase("correlator_test"),
		pgcontainer.WithUsername("test"),
		pgcontainer.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(120*time.Second), // Extended timeout for dev containers
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	if postgresContainer == nil {
		t.Fatalf("postgres container is nil")
	}

	// Get connection string
	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Create connection
	config := &Config{
		databaseURL:     connStr,
		MaxOpenConns:    defaultMaxOpenConns,
		MaxIdleConns:    defaultMaxIdleConns,
		ConnMaxLifetime: defaultConnMaxLifetime,
		ConnMaxIdleTime: defaultConnMaxIdleTime,
	}

	conn, err := NewConnection(config) //nolint:contextcheck
	if err != nil {
		_ = postgresContainer.Terminate(ctx)

		t.Fatalf("failed to connect to test database: %v", err)
	}

	// Run migrations using golang-migrate
	if err := runTestMigrations(conn.DB); err != nil {
		_ = conn.Close()
		_ = postgresContainer.Terminate(ctx)

		t.Fatalf("failed to run test migrations: %v", err)
	}

	return postgresContainer, conn
}

// runTestMigrations applies all migrations from the migrations directory using golang-migrate.
func runTestMigrations(db *sql.DB) error {
	// Create migrate instance with PostgreSQL driver
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	// Use file source pointing to migrations directory (relative to project root)
	m, err := migrate.NewWithDatabaseInstance(
		"file://../../migrations", // Relative path from internal/storage to project root migrations/
		postgresDriver,
		driver,
	)
	if err != nil {
		return err
	}

	// Run all migrations up
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}

	return nil
}

func TestPersistentKeyStoreAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, t)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewPersistentKeyStore(conn)
	if err != nil {
		t.Fatalf("NewPersistentKeyStore() error = %v", err)
	}

	defer func() {
		_ = store.Close()
	}()

	tests := []struct {
		name      string
		apiKey    *APIKey
		expectErr bool
	}{
		{
			name: "successfully adds new API key with bcrypt hash",
			apiKey: &APIKey{
				ID:          "test-key-1",
				Key:         "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
				PluginID:    "dbt-plugin",
				Name:        "Test Key 1",
				Permissions: []string{"lineage:read", "lineage:write"},
				CreatedAt:   time.Now(),
				Active:      true,
			},
			expectErr: false,
		},
		{
			name: "successfully adds API key with expiration",
			apiKey: &APIKey{
				ID:          "test-key-2",
				Key:         "correlator_ak_abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				PluginID:    "airflow-plugin",
				Name:        "Test Key 2",
				Permissions: []string{"lineage:read"},
				CreatedAt:   time.Now(),
				ExpiresAt: func(t time.Time) *time.Time {
					return &t
				}(time.Now().Add(24 * time.Hour)),
				Active: true,
			},
			expectErr: false,
		},
		{
			name: "fails to add duplicate API key (same hash)",
			apiKey: &APIKey{
				ID:          "test-key-3",
				Key:         "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", // Same as test-key-1
				PluginID:    "dbt-plugin",
				Name:        "Duplicate Key",
				Permissions: []string{"lineage:read"},
				CreatedAt:   time.Now(),
				Active:      true,
			},
			expectErr: true,
		},
		{
			name:      "fails to add nil API key",
			apiKey:    nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Add(ctx, tt.apiKey)

			if tt.expectErr {
				if err == nil {
					t.Error("Add() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Add() unexpected error: %v", err)
				}
			}
		})
	}
}

func TestPersistentKeyStoreFindByKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, t)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewPersistentKeyStore(conn)
	if err != nil {
		t.Fatalf("NewPersistentKeyStore() error = %v", err)
	}

	defer func() {
		_ = store.Close()
	}()

	// Setup: Add test keys
	testKey := &APIKey{
		ID:          "find-test-1",
		Key:         "correlator_ak_findtest1234567890abcdef1234567890abcdef1234567890abcdef1234", // pragma: allowlist secret
		PluginID:    "test-plugin",
		Name:        "Find Test Key",
		Permissions: []string{"lineage:read"},
		CreatedAt:   time.Now(),
		Active:      true,
	}

	if err := store.Add(ctx, testKey); err != nil {
		t.Fatalf("failed to add test key: %v", err)
	}

	tests := []struct {
		name      string
		key       string
		wantFound bool
		wantID    string
	}{
		{
			name:      "finds existing active API key",
			key:       "correlator_ak_findtest1234567890abcdef1234567890abcdef1234567890abcdef1234", // pragma: allowlist secret
			wantFound: true,
			wantID:    "find-test-1",
		},
		{
			name:      "returns false for non-existent key",
			key:       "correlator_ak_nonexistent1234567890abcdef1234567890abcdef1234567890abcdef12", // pragma: allowlist secret
			wantFound: false,
		},
		{
			name:      "returns false for empty key",
			key:       "",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apiKey, found := store.FindByKey(ctx, tt.key)

			if found != tt.wantFound {
				t.Errorf("FindByKey() found = %v, want %v", found, tt.wantFound)
			}

			if tt.wantFound {
				if apiKey == nil { // pragma: allowlist secret
					t.Error("FindByKey() returned nil API key when found=true")
				} else if apiKey.ID != tt.wantID {
					t.Errorf("FindByKey() ID = %q, want %q", apiKey.ID, tt.wantID)
				}
			}
		})
	}
}

func TestPersistentKeyStoreUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, t)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewPersistentKeyStore(conn)
	if err != nil {
		t.Fatalf("NewPersistentKeyStore() error = %v", err)
	}

	defer func() {
		_ = store.Close()
	}()

	// Setup: Add test key
	testKey := &APIKey{
		ID:          "update-test-1",
		Key:         "correlator_ak_updatetest1234567890abcdef1234567890abcdef1234567890abcde1",
		PluginID:    "test-plugin",
		Name:        "Original Name",
		Permissions: []string{"lineage:read"},
		CreatedAt:   time.Now(),
		Active:      true,
	}

	if err := store.Add(ctx, testKey); err != nil {
		t.Fatalf("failed to add test key: %v", err)
	}

	tests := []struct {
		name      string
		apiKey    *APIKey
		expectErr bool
	}{
		{
			name: "successfully updates API key name",
			apiKey: &APIKey{
				ID:          "update-test-1",
				Key:         testKey.Key,
				PluginID:    "test-plugin",
				Name:        "Updated Name",
				Permissions: []string{"lineage:read"},
				Active:      true,
			},
			expectErr: false,
		},
		{
			name: "successfully updates permissions",
			apiKey: &APIKey{
				ID:          "update-test-1",
				Key:         testKey.Key,
				PluginID:    "test-plugin",
				Name:        "Updated Name",
				Permissions: []string{"lineage:read", "lineage:write", "admin"},
				Active:      true,
			},
			expectErr: false,
		},
		{
			name: "successfully deactivates API key",
			apiKey: &APIKey{
				ID:       "update-test-1",
				Key:      testKey.Key,
				PluginID: "test-plugin",
				Name:     "Updated Name",
				Active:   false,
			},
			expectErr: false,
		},
		{
			name: "fails to update non-existent key",
			apiKey: &APIKey{
				ID:       "non-existent",
				Key:      "correlator_ak_nonexistent1234567890abcdef1234567890abcdef1234567890abcde1", // pragma: allowlist secret
				PluginID: "test-plugin",
				Name:     "Ghost Key",
				Active:   true,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Update(ctx, tt.apiKey)

			if tt.expectErr {
				if err == nil {
					t.Error("Update() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Update() unexpected error: %v", err)
				}
			}
		})
	}
}

func TestPersistentKeyStoreDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, t)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewPersistentKeyStore(conn)
	if err != nil {
		t.Fatalf("NewPersistentKeyStore() error = %v", err)
	}

	defer func() {
		_ = store.Close()
	}()

	// Setup: Add test key
	testKey := &APIKey{
		ID:          "delete-test-1",
		Key:         "correlator_ak_deletetest1234567890abcdef1234567890abcdef1234567890abcde1",
		PluginID:    "test-plugin",
		Name:        "To Be Deleted",
		Permissions: []string{"lineage:read"},
		CreatedAt:   time.Now(),
		Active:      true,
	}

	if err := store.Add(ctx, testKey); err != nil {
		t.Fatalf("failed to add test key: %v", err)
	}

	tests := []struct {
		name      string
		keyID     string
		expectErr bool
	}{
		{
			name:      "successfully deletes existing API key",
			keyID:     "delete-test-1",
			expectErr: false,
		},
		{
			name:      "fails to delete non-existent key",
			keyID:     "non-existent-key",
			expectErr: true,
		},
		{
			name:      "fails to delete with empty key ID",
			keyID:     "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Delete(ctx, tt.keyID)

			if tt.expectErr {
				if err == nil {
					t.Error("Delete() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Delete() unexpected error: %v", err)
				}

				// Verify key is actually deleted
				_, found := store.FindByKey(ctx, testKey.Key)
				if found {
					t.Error("Delete() key still found after deletion")
				}
			}
		})
	}
}

func TestPersistentKeyStoreListByPlugin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	container, conn := setupTestDatabase(ctx, t)

	defer func() {
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}()

	store, err := NewPersistentKeyStore(conn)
	if err != nil {
		t.Fatalf("NewPersistentKeyStore() error = %v", err)
	}

	defer func() {
		_ = store.Close()
	}()

	// Setup: Add multiple test keys for different plugins
	testKeys := []*APIKey{
		{
			ID:          "list-test-1",
			Key:         "correlator_ak_listtest1234567890abcdef1234567890abcdef1234567890abcdef121",
			PluginID:    "dbt-plugin",
			Name:        "DBT Key 1",
			Permissions: []string{"lineage:read"},
			Active:      true,
		},
		{
			ID:          "list-test-2",
			Key:         "correlator_ak_listtest1234567890abcdef1234567890abcdef1234567890abcdef122",
			PluginID:    "dbt-plugin",
			Name:        "DBT Key 2",
			Permissions: []string{"lineage:read", "lineage:write"},
			Active:      true,
		},
		{
			ID:          "list-test-3",
			Key:         "correlator_ak_listtest1234567890abcdef1234567890abcdef1234567890abcdef123",
			PluginID:    "airflow-plugin",
			Name:        "Airflow Key 1",
			Permissions: []string{"lineage:read"},
			Active:      true,
		},
		{
			ID:          "list-test-4",
			Key:         "correlator_ak_listtest1234567890abcdef1234567890abcdef1234567890abcdef124",
			PluginID:    "dbt-plugin",
			Name:        "DBT Key 3 (Inactive)",
			Permissions: []string{"lineage:read"},
			Active:      false,
		},
	}

	for _, key := range testKeys {
		if err := store.Add(ctx, key); err != nil {
			t.Fatalf("failed to add test key %s: %v", key.ID, err)
		}
	}

	tests := []struct {
		name      string
		pluginID  string
		wantCount int
		expectErr bool
	}{
		{
			name:      "lists all active keys for dbt-plugin",
			pluginID:  "dbt-plugin",
			wantCount: 2, // Only active keys
			expectErr: false,
		},
		{
			name:      "lists all active keys for airflow-plugin",
			pluginID:  "airflow-plugin",
			wantCount: 1,
			expectErr: false,
		},
		{
			name:      "returns empty list for plugin with no keys",
			pluginID:  "non-existent-plugin",
			wantCount: 0,
			expectErr: false,
		},
		{
			name:      "fails with empty plugin ID",
			pluginID:  "",
			wantCount: 0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := store.ListByPlugin(ctx, tt.pluginID)

			if tt.expectErr {
				if err == nil {
					t.Error("ListByPlugin() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ListByPlugin() unexpected error: %v", err)
				}

				if len(keys) != tt.wantCount {
					t.Errorf("ListByPlugin() returned %d keys, want %d", len(keys), tt.wantCount)
				}
			}
		})
	}
}
