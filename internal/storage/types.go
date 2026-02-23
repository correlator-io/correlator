// Package storage provides data storage interfaces and domain types for the Correlator API.
package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

const (
	// API key format constants.
	randomBytesSize = 32
	apiKeyLength    = 78
	prefixLen       = 18 // Show "correlator_ak_1234"
	suffixLen       = 4  // Show last 4 chars
	postgresDriver  = "postgres"
	ctxTimeout      = 5 * time.Second
)

var (
	// ErrKeyAlreadyExists is returned when attempting to add a key that already exists.
	ErrKeyAlreadyExists = errors.New("API key already exists")
	// ErrKeyNotFound is returned when attempting to operate on a non-existent key.
	ErrKeyNotFound = errors.New("API key not found")
	// ErrKeyNil is returned when a nil API key is provided.
	ErrKeyNil = errors.New("API key cannot be nil")
	// ErrPluginIDEmpty is returned when plugin ID is empty during key generation.
	ErrPluginIDEmpty = errors.New("plugin ID cannot be empty")
	// ErrKeyStringEmpty is returned when key string is empty during parsing.
	ErrKeyStringEmpty = errors.New("key string cannot be empty")
	// ErrInvalidKeyFormat is returned when API key doesn't match expected format.
	ErrInvalidKeyFormat = errors.New("invalid API key format")
	// ErrInvalidKeyLength is returned when API key length is incorrect.
	ErrInvalidKeyLength = errors.New("invalid API key length")
)

type (
	// Connection represents a database connection.
	Connection struct {
		*sql.DB
	}

	// APIKey represents an API key with plugin identification and permissions.
	// This is a storage domain model - not serialized to JSON directly.
	// For API responses, create a separate response type in the api package.
	APIKey struct {
		ID          string
		Key         string // bcrypt hash - never expose in API responses
		PluginID    string
		Name        string
		Permissions []string
		CreatedAt   time.Time
		ExpiresAt   *time.Time
		Active      bool
	}

	// APIKeyStore defines the interface for API key storage and retrieval.
	APIKeyStore interface {
		// FindByKey retrieves an API key by its key value
		FindByKey(ctx context.Context, key string) (*APIKey, bool)
		// Add stores a new API key
		Add(ctx context.Context, apiKey *APIKey) error
		// Update modifies an existing API key
		Update(ctx context.Context, apiKey *APIKey) error
		// Delete removes an API key
		Delete(ctx context.Context, keyID string) error
		// ListByPlugin returns all API keys for a specific plugin
		ListByPlugin(ctx context.Context, pluginID string) ([]*APIKey, error)
		// HealthCheck verifies the storage backend is healthy and ready to serve requests
		HealthCheck(ctx context.Context) error
	}

	// healthStats holds correlation health statistics.
	// All counts are based on DISTINCT dataset URNs, not row counts.
	healthStats struct {
		// totalFailedTestedDatasets is the count of distinct dataset URNs with failed/error test results.
		// This is the denominator for correlation rate calculation.
		totalFailedTestedDatasets int

		// correlatedFailedTestedDatasets is the count of distinct dataset URNs that have both:
		// 1. Failed/error test results, AND
		// 2. A producer output edge (lineage_edges with edge_type='output')
		// This is the numerator for correlation rate calculation.
		correlatedFailedTestedDatasets int

		// totalDatasets is the count of all distinct dataset URNs with any test results.
		totalDatasets int

		// producedDatasets is the count of distinct dataset URNs with output edges.
		producedDatasets int

		// correlatedDatasets is the count of distinct dataset URNs that have both
		// test results (any status) AND producer output edges.
		correlatedDatasets int
	}

	// correlatedTest holds a test ID and its pattern-resolved dataset URN.
	correlatedTest struct {
		TestID      int64
		ResolvedURN string
	}
)

// NewConnection returns a new Database Connection.
func NewConnection(config *Config) (*Connection, error) {
	db, err := sql.Open(postgresDriver, config.databaseURL)
	if err != nil {
		return nil, err
	}

	// Configure connection pool with production-ready settings
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	// Perform immediate health check with timeout
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("database health check failed: %w", err)
	}

	return &Connection{db}, nil
}

// HealthCheck checks if the database connection is healthy with timeout.
// This method is used for health checks and monitoring.
func (c *Connection) HealthCheck(ctx context.Context) error { //nolint: contextcheck
	if ctx == nil {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(context.Background(), ctxTimeout)

		defer cancel()
	}

	return c.PingContext(ctx)
}

// Close closes the database connection pool gracefully.
// This method is safe to call multiple times.
func (c *Connection) Close() error {
	return c.DB.Close()
}

// Stats returns database connection pool statistics for monitoring.
// Useful for observability and debugging connection pool issues.
func (c *Connection) Stats() sql.DBStats {
	return c.DB.Stats()
}

// ValidateKey performs constant-time comparison of the provided key against this API key.
func (ak *APIKey) ValidateKey(providedKey string) bool {
	// Validate inputs first
	if providedKey == "" || ak.Key == "" {
		return false
	}

	// Check if API key is active
	if !ak.Active {
		return false
	}

	// Check expiration
	if ak.ExpiresAt != nil && time.Now().After(*ak.ExpiresAt) {
		return false
	}

	// Constant-time comparison for security
	return SecureCompare(ak.Key, providedKey)
}

// HasPermission checks if the API key has a specific permission.
func (ak *APIKey) HasPermission(permission string) bool {
	for _, p := range ak.Permissions {
		if p == permission {
			return true
		}
	}

	return false
}

// SecureCompare performs constant-time comparison of two strings to prevent timing attacks.
func SecureCompare(a, b string) bool {
	// If lengths differ, still perform comparison to prevent timing attacks
	// but ensure we return false
	if len(a) != len(b) {
		// Compare against a dummy string of the same length as 'a' to maintain constant time
		dummy := make([]byte, len(a))
		subtle.ConstantTimeCompare([]byte(a), dummy)

		return false
	}

	// Perform constant-time comparison
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// MaskKey masks an API key for secure logging by showing only the prefix and suffix.
// Designed specifically for 78-character correlator API keys in format:
// "correlator_ak_" + 64 hex chars = 78 total chars.
func MaskKey(key string) string {
	if key == "" {
		return ""
	}

	keyLen := len(key)

	// For our standard 78-character API keys, show meaningful prefix and suffix
	if keyLen == apiKeyLength {
		maskedLen := keyLen - prefixLen - suffixLen // 78 - 18 - 4 = 56

		return key[:prefixLen] + strings.Repeat("*", maskedLen) + key[keyLen-suffixLen:]
	}

	// For any other key length (testing, development, etc.), mask completely
	return strings.Repeat("*", keyLen)
}

// ComputeKeyLookupHash computes the SHA256 hash of an API key for O(1) lookup.
// This hash is stored in the key_lookup_hash column and used for fast key retrieval.
// Note: This is separate from the bcrypt hash used for security validation.
//
// The lookup hash enables O(1) database queries:
//   - Input: plaintext API key (e.g., "correlator_ak_abc123...")
//   - Output: 64-character hex string (SHA256 hash)
//
// Security considerations:
//   - SHA256 is used for indexing only, NOT for password verification
//   - The bcrypt key_hash field remains the security boundary
//   - Rainbow tables are ineffective due to high-entropy API keys (256 bits)
func ComputeKeyLookupHash(key string) string {
	hash := sha256.Sum256([]byte(key))

	return hex.EncodeToString(hash[:])
}

// GenerateAPIKey creates a new secure API key for a plugin.
func GenerateAPIKey(pluginID string) (string, error) {
	if pluginID == "" {
		return "", ErrPluginIDEmpty
	}

	// Generate 32 random bytes (256 bits)
	randomBytes := make([]byte, randomBytesSize)

	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}

	// Convert to hex and add correlator prefix
	randomHex := hex.EncodeToString(randomBytes)
	apiKey := "correlator_ak_" + randomHex // pragma: allowlist secret

	return apiKey, nil
}

// ParseAPIKey extracts the API key from various header formats.
func ParseAPIKey(keyString string) (string, error) {
	if keyString == "" {
		return "", ErrKeyStringEmpty
	}

	// Remove "Bearer " prefix if present
	keyString = strings.TrimPrefix(keyString, "Bearer ")

	// Validate key format (should start with correlator_ak_)
	if !strings.HasPrefix(keyString, "correlator_ak_") {
		return "", ErrInvalidKeyFormat
	}

	// Ensure key has correct length (correlator_ak_ + 64 hex chars = 78 total)
	if len(keyString) != apiKeyLength {
		return "", ErrInvalidKeyLength
	}

	return keyString, nil
}
