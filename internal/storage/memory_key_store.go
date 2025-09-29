// Package storage provides data storage implementations for the Correlator API.
package storage

import (
	"sync"
)

// InMemoryKeyStore provides thread-safe in-memory storage for API keys.
type InMemoryKeyStore struct {
	// keys maps key strings to Key structs for fast lookup
	keys map[string]*Key
	// keysByID maps key IDs to Key structs for ID-based operations
	keysByID map[string]*Key
	// keysByPlugin maps plugin IDs to slices of Key structs for plugin filtering
	keysByPlugin map[string][]*Key
	// mutex protects concurrent access to all maps
	mutex sync.RWMutex
}

// NewInMemoryKeyStore creates a new thread-safe in-memory key store.
func NewInMemoryKeyStore() *InMemoryKeyStore {
	return &InMemoryKeyStore{
		keys:         make(map[string]*Key),
		keysByID:     make(map[string]*Key),
		keysByPlugin: make(map[string][]*Key),
	}
}

// FindByKey retrieves an API key by its key value.
func (s *InMemoryKeyStore) FindByKey(key string) (*Key, bool) {
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
func (s *InMemoryKeyStore) Add(apiKey *Key) error {
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
func (s *InMemoryKeyStore) Update(apiKey *Key) error {
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

// Delete removes an API key.
func (s *InMemoryKeyStore) Delete(keyID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if key exists
	existingKey, exists := s.keysByID[keyID]
	if !exists {
		return ErrKeyNotFound
	}

	// Remove from all maps
	delete(s.keys, existingKey.Key)
	delete(s.keysByID, keyID)

	// Remove from plugin map
	s.removeFromPluginMap(existingKey.PluginID, keyID)

	return nil
}

// ListByPlugin returns all API keys for a specific plugin.
func (s *InMemoryKeyStore) ListByPlugin(pluginID string) ([]*Key, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	keys, exists := s.keysByPlugin[pluginID]
	if !exists {
		return []*Key{}, nil // Return empty slice for non-existent plugins
	}

	// Return copies to prevent external modification
	result := make([]*Key, len(keys))
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
