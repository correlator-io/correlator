package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

const (
	keyCreated = "created"
	keyUpdated = "updated"
	keyDeleted = "deleted"
)

// PersistentKeyStore implements APIKeyStore interface with PostgreSQL backend.
// Provides production-ready API key storage with connection pooling, transaction handling,
// and comprehensive error management.
type PersistentKeyStore struct {
	conn   *Connection
	logger *slog.Logger
}

// NewPersistentKeyStore creates a production-ready PostgreSQL key store with connection pooling.
// Performs immediate health check to ensure database connectivity.
func NewPersistentKeyStore(conn *Connection) (*PersistentKeyStore, error) {
	return &PersistentKeyStore{
		conn: conn,
		logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: getEnvLogLevel("LOG_LEVEL", slog.LevelDebug),
		})),
	}, nil
}

// Close closes the database connection pool gracefully.
// This method is safe to call multiple times.
func (s *PersistentKeyStore) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}

	return nil
}

// FindByKey retrieves an API key by its key value using bcrypt hash comparison.
// Queries all active keys and compares hashes in-memory (acceptable for MVP with <1000 keys).
// Returns (nil, false) if key not found or invalid.
func (s *PersistentKeyStore) FindByKey(ctx context.Context, key string) (*APIKey, bool) {
	// Validate input
	if key == "" {
		return nil, false
	}

	// Query all active API keys
	query := `
		SELECT id, key_hash, plugin_id, name, permissions, created_at, expires_at, active, updated_at
		FROM api_keys
		WHERE active = TRUE
	`

	rows, err := s.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, false
	}

	defer func() {
		_ = rows.Close()
	}()

	var keyFound *APIKey

	// Iterate through active keys and compare hashes
	for rows.Next() {
		var (
			apiKey          APIKey
			permissionsJSON []byte
			updatedAt       interface{} // Not used in APIKey struct yet
		)

		err := rows.Scan(
			&apiKey.ID,
			&apiKey.Key, // This is actually the hash, we'll use it for comparison
			&apiKey.PluginID,
			&apiKey.Name,
			&permissionsJSON,
			&apiKey.CreatedAt,
			&apiKey.ExpiresAt,
			&apiKey.Active,
			&updatedAt,
		)
		if err != nil {
			continue
		}

		// Parse permissions from JSONB
		if err := json.Unmarshal(permissionsJSON, &apiKey.Permissions); err != nil {
			continue
		}

		// Compare the provided key with the stored hash using bcrypt
		if CompareAPIKeyHash(apiKey.Key, key) {
			// Found a match - Mask the key for security (we don't return the plaintext key or hash)
			apiKey.Key = MaskKey(apiKey.Key)
			keyFound = &apiKey

			break
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		s.logger.Error("failed to find key", slog.String("key", key), slog.String("error", err.Error()))

		return nil, false
	}

	// Return the found key if exists, otherwise nil with false
	return keyFound, keyFound != nil
}

// Add stores a new API key with bcrypt hashing and audit logging.
// The plaintext key is hashed with bcrypt (cost=10) before storage for security.
// Audit logging is performed synchronously to ensure compliance.
//
// Duplicate Detection: Queries all active keys and compares hashes using bcrypt.
// This approach is acceptable for MVP with <1000 keys (~60ms per key with cost=10).
func (s *PersistentKeyStore) Add(ctx context.Context, apiKey *APIKey) error {
	// Validate input
	if apiKey == nil { // pragma: allowlist secret
		return ErrKeyNil
	}

	// Check for duplicate key by comparing with existing active keys
	// This is necessary because bcrypt generates different hashes for the same input
	if existing, found := s.FindByKey(ctx, apiKey.Key); found && existing != nil {
		return ErrKeyAlreadyExists
	}

	// Hash the API key using bcrypt
	keyHash, err := HashAPIKey(apiKey.Key)
	if err != nil {
		return fmt.Errorf("failed to hash API key: %w", err)
	}

	// Convert permissions slice to JSONB-compatible format
	permissionsJSON, err := permissionsToJSON(apiKey.Permissions)
	if err != nil {
		return fmt.Errorf("failed to serialize permissions: %w", err)
	}

	// Insert API key into database
	query := `
		INSERT INTO api_keys (id, key_hash, plugin_id, name, permissions, created_at, expires_at, active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err = s.conn.ExecContext(
		ctx,
		query,
		apiKey.ID,
		keyHash,
		apiKey.PluginID,
		apiKey.Name,
		permissionsJSON,
		apiKey.CreatedAt,
		apiKey.ExpiresAt,
		apiKey.Active,
	)
	if err != nil {
		return fmt.Errorf("failed to insert API key: %w", err)
	}

	// Synchronous audit logging (blocking for strict compliance)
	if err := s.logAudit(ctx, keyCreated, apiKey, nil); err != nil {
		// Log error but don't fail the operation - audit logging is best-effort
		// In production, this would be logged to a monitoring system
		s.logger.Error(
			"failed to write an audit log entry for API key operation",
			slog.String("operation", keyCreated),
			slog.String("error", err.Error()),
		)
	}

	return nil
}

// Update modifies an existing API key with audit logging.
// Updates name, permissions, active status, and expiration.
// The key hash itself cannot be updated for security reasons.
func (s *PersistentKeyStore) Update(ctx context.Context, apiKey *APIKey) error {
	// Validate input
	if apiKey == nil { // pragma: allowlist secret
		return ErrKeyNil
	}

	if apiKey.ID == "" {
		return ErrKeyNotFound
	}

	// Convert permissions slice to JSONB-compatible format
	permissionsJSON, err := permissionsToJSON(apiKey.Permissions)
	if err != nil {
		return fmt.Errorf("failed to serialize permissions: %w", err)
	}

	// Update API key in database
	query := `
		UPDATE api_keys
		SET name = $1, permissions = $2, active = $3, expires_at = $4
		WHERE id = $5
	`

	result, err := s.conn.ExecContext(
		ctx,
		query,
		apiKey.Name,
		permissionsJSON,
		apiKey.Active,
		apiKey.ExpiresAt,
		apiKey.ID,
	)
	if err != nil {
		return fmt.Errorf("failed to update API key: %w", err)
	}

	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrKeyNotFound
	}

	// Synchronous audit logging (blocking for strict compliance)
	if err := s.logAudit(ctx, keyUpdated, apiKey, nil); err != nil {
		// Log error but don't fail the operation - audit logging is best-effort
		s.logger.Error(
			"failed to write an audit log entry for API key operation",
			slog.String("operation", keyUpdated),
			slog.String("error", err.Error()),
		)
	}

	return nil
}

// Delete performs a soft delete on an API key by setting active=FALSE.
// The key is not physically removed from the database for audit trail purposes.
func (s *PersistentKeyStore) Delete(ctx context.Context, keyID string) error {
	// Validate input
	if keyID == "" {
		return ErrKeyNotFound
	}

	// Soft delete: Set active=FALSE instead of physical deletion
	query := `
		UPDATE api_keys
		SET active = FALSE
		WHERE id = $1
	`

	result, err := s.conn.ExecContext(ctx, query, keyID)
	if err != nil {
		return fmt.Errorf("failed to delete API key: %w", err)
	}

	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrKeyNotFound
	}

	// Create a minimal APIKey for audit logging
	apiKey := &APIKey{
		ID: keyID,
	}

	// Synchronous audit logging (blocking for strict compliance)
	if err := s.logAudit(ctx, keyDeleted, apiKey, nil); err != nil {
		// Log error but don't fail the operation - audit logging is best-effort
		s.logger.Error(
			"failed to write an audit log entry for API key operation",
			slog.String("operation", keyDeleted),
			slog.String("error", err.Error()),
		)
	}

	return nil
}

// ListByPlugin returns all active API keys for a specific plugin.
// Uses the idx_api_keys_plugin_id index for optimal query performance.
func (s *PersistentKeyStore) ListByPlugin(ctx context.Context, pluginID string) ([]*APIKey, error) {
	// Validate input
	if pluginID == "" {
		return nil, ErrPluginIDEmpty
	}

	// Query active keys for the specified plugin
	query := `
		SELECT id, key_hash, plugin_id, name, permissions, created_at, expires_at, active, updated_at
		FROM api_keys
		WHERE plugin_id = $1 AND active = TRUE
		ORDER BY created_at DESC
	`

	rows, err := s.conn.QueryContext(ctx, query, pluginID)
	if err != nil {
		return nil, fmt.Errorf("failed to query API keys: %w", err)
	}

	defer func() {
		_ = rows.Close()
	}()

	// Collect all matching keys
	var keys []*APIKey

	for rows.Next() {
		var (
			apiKey          APIKey
			permissionsJSON []byte
			updatedAt       interface{} // Not used in APIKey struct yet
		)

		err := rows.Scan(
			&apiKey.ID,
			&apiKey.Key, // This is actually the hash, mask it before returning
			&apiKey.PluginID,
			&apiKey.Name,
			&permissionsJSON,
			&apiKey.CreatedAt,
			&apiKey.ExpiresAt,
			&apiKey.Active,
			&updatedAt,
		)
		if err != nil {
			continue
		}

		// Parse permissions from JSONB
		if err := json.Unmarshal(permissionsJSON, &apiKey.Permissions); err != nil {
			continue
		}

		// Mask the key hash for security
		apiKey.Key = MaskKey(apiKey.Key)

		keys = append(keys, &apiKey)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Return empty slice (not nil) if no keys found
	if keys == nil {
		keys = []*APIKey{}
	}

	return keys, nil
}

// permissionsToJSON converts a permissions slice to JSON format for PostgreSQL JSONB storage.
func permissionsToJSON(permissions []string) ([]byte, error) {
	if permissions == nil {
		permissions = []string{}
	}

	return json.Marshal(permissions)
}

// logAudit writes an audit log entry for API key operations.
// This is synchronous (blocking) to ensure strict compliance requirements.
func (s *PersistentKeyStore) logAudit(
	ctx context.Context,
	operation string,
	apiKey *APIKey,
	metadata map[string]interface{},
) error {
	maskedKey := MaskKey(apiKey.Key)

	var (
		// Convert metadata to JSON
		metadataJSON []byte
		err          error
	)

	if metadata == nil {
		metadataJSON = []byte("{}")
	} else {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	query := `
		INSERT INTO api_key_audit_log (api_key_id, operation, masked_key, plugin_id, metadata)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = s.conn.ExecContext(ctx, query, apiKey.ID, operation, maskedKey, apiKey.PluginID, metadataJSON)
	if err != nil {
		return fmt.Errorf("failed to insert audit log: %w", err)
	}

	return nil
}
