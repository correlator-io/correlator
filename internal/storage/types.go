// Package storage provides data storage interfaces and domain types for the Correlator API.
package storage

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// API key format constants.
	randomBytesSize = 32
	apiKeyLength    = 78
	prefixLen       = 18 // Show "correlator_ak_1234"
	suffixLen       = 4  // Show last 4 chars
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

// Key represents an API key with plugin identification and permissions.
type Key struct {
	ID          string     `json:"id"`
	Key         string     `json:"key"`
	PluginID    string     `json:"pluginId"`
	Name        string     `json:"name"`
	Permissions []string   `json:"permissions"`
	CreatedAt   time.Time  `json:"createdAt"`
	ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
	Active      bool       `json:"active"`
}

// KeyStore defines the interface for API key storage and retrieval.
type KeyStore interface {
	// FindByKey retrieves an API key by its key value
	FindByKey(key string) (*Key, bool)
	// Add stores a new API key
	Add(apiKey *Key) error
	// Update modifies an existing API key
	Update(apiKey *Key) error
	// Delete removes an API key
	Delete(keyID string) error
	// ListByPlugin returns all API keys for a specific plugin
	ListByPlugin(pluginID string) ([]*Key, error)
}

// ValidateKey performs constant-time comparison of the provided key against this API key.
func (ak *Key) ValidateKey(providedKey string) bool {
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
func (ak *Key) HasPermission(permission string) bool {
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
