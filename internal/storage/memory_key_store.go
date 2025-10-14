// Package storage provides data storage implementations for the Correlator API.
package storage

import (
	"context"
	"sync"
)

// InMemoryKeyStore provides thread-safe in-memory storage for API keys.
type InMemoryKeyStore struct {
	// keys maps key strings to Key structs for fast lookup
	keys map[string]*APIKey
	// keysByID maps key IDs to Key structs for ID-based operations
	keysByID map[string]*APIKey
	// keysByPlugin maps plugin IDs to slices of Key structs for plugin filtering
	keysByPlugin map[string][]*APIKey
	// mutex protects concurrent access to all maps
	mutex sync.RWMutex
}

// NewInMemoryKeyStore creates a new thread-safe in-memory key store.
func NewInMemoryKeyStore() *InMemoryKeyStore {
	return &InMemoryKeyStore{
		keys:         make(map[string]*APIKey),
		keysByID:     make(map[string]*APIKey),
		keysByPlugin: make(map[string][]*APIKey),
	}
}

// FindByKey retrieves an API key by its key value.
func (s *InMemoryKeyStore) FindByKey(_ context.Context, key string) (*APIKey, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	apiKey, exists := s.keys[key]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent external modification
	keyCopy := *apiKey

	return &keyCopy, true
}

// Add stores a new API key.
func (s *InMemoryKeyStore) Add(_ context.Context, apiKey *APIKey) error {
	if apiKey == nil { // pragma: allowlist secret
		return ErrKeyNil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if key already exists by ID or key string
	if _, exists := s.keysByID[apiKey.ID]; exists {
		return ErrKeyAlreadyExists
	}

	if _, exists := s.keys[apiKey.Key]; exists {
		return ErrKeyAlreadyExists
	}

	// Create a copy to prevent external modification
	keyCopy := *apiKey

	// Store in all maps
	s.keys[keyCopy.Key] = &keyCopy
	s.keysByID[keyCopy.ID] = &keyCopy

	// Add to plugin map
	s.keysByPlugin[keyCopy.PluginID] = append(s.keysByPlugin[keyCopy.PluginID], &keyCopy)

	return nil
}

// Update modifies an existing API key.
func (s *InMemoryKeyStore) Update(_ context.Context, apiKey *APIKey) error {
	if apiKey == nil { // pragma: allowlist secret
		return ErrKeyNil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if key exists
	existingKey, exists := s.keysByID[apiKey.ID]
	if !exists {
		return ErrKeyNotFound
	}

	// Remove from plugin map (old plugin)
	s.removeFromPluginMap(existingKey.PluginID, existingKey.ID)

	// Remove from key string map if key changed
	if existingKey.Key != apiKey.Key {
		delete(s.keys, existingKey.Key)
	}

	// Create a copy to prevent external modification
	keyCopy := *apiKey

	// Update all maps
	s.keys[keyCopy.Key] = &keyCopy
	s.keysByID[keyCopy.ID] = &keyCopy

	// Add to plugin map (new plugin)
	s.keysByPlugin[keyCopy.PluginID] = append(s.keysByPlugin[keyCopy.PluginID], &keyCopy)

	return nil
}

// Delete soft-deletes an API key by setting active=false.
// This matches PostgreSQL behavior for consistency.
func (s *InMemoryKeyStore) Delete(_ context.Context, keyID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if key exists
	existingKey, exists := s.keysByID[keyID]
	if !exists {
		return ErrKeyNotFound
	}

	// Soft delete: set active=false (matches PostgreSQL behavior)
	existingKey.Active = false

	// Update all references to point to the same modified key
	// (all maps point to the same instance, so modifying one updates all)
	return nil
}

// ListByPlugin returns all API keys for a specific plugin.
func (s *InMemoryKeyStore) ListByPlugin(_ context.Context, pluginID string) ([]*APIKey, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	keys, exists := s.keysByPlugin[pluginID]
	if !exists {
		return []*APIKey{}, nil // Return empty slice for non-existent plugins
	}

	// Return copies to prevent external modification
	result := make([]*APIKey, len(keys))
	for i, key := range keys {
		keyCopy := *key
		result[i] = &keyCopy
	}

	return result, nil
}

// removeFromPluginMap removes a key from the plugin map by key ID.
// Caller must hold write lock.
func (s *InMemoryKeyStore) removeFromPluginMap(pluginID, keyID string) {
	keys := s.keysByPlugin[pluginID]
	for i, key := range keys {
		if key.ID == keyID {
			// Remove element at index i
			s.keysByPlugin[pluginID] = append(keys[:i], keys[i+1:]...)

			break
		}
	}

	// Clean up empty plugin entries
	if len(s.keysByPlugin[pluginID]) == 0 {
		delete(s.keysByPlugin, pluginID)
	}
}
