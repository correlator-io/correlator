package storage

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

const (
	// bcryptCost defines the computational cost for bcrypt hashing.
	// Cost 10 = ~60ms per hash (MVP performance vs security balance)
	// Can be increased to 12 (~250ms) for production security hardening.
	bcryptCost  = 10
	bcryptLimit = 72
)

// HashAPIKey generates a bcrypt hash of the API key for secure storage.
// The API key is never stored in plaintext - only the bcrypt hash is persisted.
//
// Performance: ~60ms per call with cost 10 (intentionally slow for security)
// Security: Each hash includes a random salt, so identical keys produce different hashes
//
// Note: Bcrypt has a 72-byte input limit. For longer keys, we pre-hash with SHA-256
// to ensure consistent behavior while maintaining security properties.
func HashAPIKey(apiKey string) (string, error) {
	if apiKey == "" {
		return "", ErrKeyNil
	}

	// Bcrypt input preparation
	var input []byte

	if len(apiKey) > bcryptLimit {
		// For keys longer than 72 bytes, pre-hash with SHA-256
		// This maintains security while working within bcrypt's limits
		hasher := sha256.New()
		hasher.Write([]byte(apiKey))
		input = hasher.Sum(nil)
	} else {
		input = []byte(apiKey)
	}

	hash, err := bcrypt.GenerateFromPassword(input, bcryptCost)
	if err != nil {
		return "", fmt.Errorf("failed to hash API key: %w", err)
	}

	return string(hash), nil
}

// CompareAPIKeyHash performs constant-time comparison of API key against bcrypt hash.
// This is the primary method for API key validation - never compare plaintext keys.
//
// Performance: ~60ms per call with cost 10 (intentionally slow to prevent brute force)
// Security: Uses constant-time comparison to prevent timing attacks
//
// Returns true if the API key matches the stored hash, false otherwise.
// Returns false for any error conditions (empty inputs, invalid hash format, etc.)
//
// Note: Must use same input preparation logic as HashAPIKey for long keys.
func CompareAPIKeyHash(hash, apiKey string) bool {
	if hash == "" || apiKey == "" {
		return false
	}

	// Prepare input using same logic as HashAPIKey
	var input []byte

	if len(apiKey) > bcryptLimit {
		// For keys longer than 72 bytes, pre-hash with SHA-256
		hasher := sha256.New()
		hasher.Write([]byte(apiKey))
		input = hasher.Sum(nil)
	} else {
		input = []byte(apiKey)
	}

	err := bcrypt.CompareHashAndPassword([]byte(hash), input)

	return err == nil
}
