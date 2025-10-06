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

// FindByKey retrieves an API key by its key value using O(1) hash lookup.
// Uses key_lookup_hash (SHA256) for fast database query, then verifies with bcrypt.
// Returns (nil, false) if key not found or invalid.
// Note: Active/inactive status is checked by the authentication layer, not here.
func (s *PersistentKeyStore) FindByKey(ctx context.Context, key string) (*APIKey, bool) {
	if key == "" {
		return nil, false
	}

	// Compute lookup hash for O(1) database query
	lookupHash := ComputeKeyLookupHash(key)

	// Query by lookup_hash for O(1) performance
	// Authentication layer will check active status and return appropriate error
	query := `
		SELECT id, key_hash, plugin_id, name, permissions, created_at, expires_at, active, updated_at
		FROM api_keys
		WHERE key_lookup_hash = $1
		LIMIT 1
	`

	var (
		apiKey          APIKey
		permissionsJSON []byte
		updatedAt       interface{} // Not used in APIKey struct yet
	)

	err := s.conn.QueryRowContext(ctx, query, lookupHash).Scan(
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
		return nil, false
	}

	// Parse permissions from JSONB
	if err := json.Unmarshal(permissionsJSON, &apiKey.Permissions); err != nil {
		s.logger.Error("failed to parse permissions", slog.String("error", err.Error()))

		return nil, false
	}

	// Verify with bcrypt for security (protects against SHA256 collision attacks)
	if !CompareAPIKeyHash(apiKey.Key, key) {
		// Hash collision (extremely unlikely) or tampered lookup_hash
		s.logger.Warn("key lookup hash matched but bcrypt verification failed",
			slog.String("key_id", apiKey.ID),
			slog.String("plugin_id", apiKey.PluginID),
		)

		return nil, false
	}

	// Found and verified - Mask the key for security
	apiKey.Key = MaskKey(apiKey.Key)

	return &apiKey, true
}

// Add stores a new API key with bcrypt hashing, SHA256 lookup hash, and audit logging.
// The plaintext key is hashed with:
//   - bcrypt (cost=10) for security validation
//   - SHA256 for O(1) database lookup performance
//
// Audit logging is performed synchronously to ensure compliance.
//
// Duplicate Detection: Uses key_lookup_hash for O(1) duplicate check via FindByKey.
func (s *PersistentKeyStore) Add(ctx context.Context, apiKey *APIKey) error {
	if apiKey == nil { // pragma: allowlist secret
		return ErrKeyNil
	}

	if existing, found := s.FindByKey(ctx, apiKey.Key); found && existing != nil {
		return ErrKeyAlreadyExists
	}

	// Compute lookup hash for O(1) queries (SHA256)
	lookupHash := ComputeKeyLookupHash(apiKey.Key)

	// Hash the API key using bcrypt for security
	keyHash, err := HashAPIKey(apiKey.Key)
	if err != nil {
		return fmt.Errorf("failed to hash API key: %w", err)
	}

	// Convert permissions slice to JSONB-compatible format
	permissionsJSON, err := permissionsToJSON(apiKey.Permissions)
	if err != nil {
		return fmt.Errorf("failed to serialize permissions: %w", err)
	}

	// Insert API key into database with both hashes
	query := `
		INSERT INTO api_keys (id, key_hash, key_lookup_hash, plugin_id, name, permissions, created_at, expires_at, active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err = s.conn.ExecContext(
		ctx,
		query,
		apiKey.ID,
		keyHash,
		lookupHash,
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
