// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"

	"github.com/correlator-io/correlator/internal/storage"
)

// MockAPIKeyStore is a mock implementation of storage.APIKeyStore for testing.
type MockAPIKeyStore struct {
	FindByKeyFunc   func(ctx context.Context, key string) (*storage.APIKey, bool)
	AddFunc         func(ctx context.Context, apiKey *storage.APIKey) error
	UpdateFunc      func(ctx context.Context, apiKey *storage.APIKey) error
	DeleteFunc      func(ctx context.Context, keyID string) error
	ListByPluginFunc func(ctx context.Context, pluginID string) ([]*storage.APIKey, error)
}

// FindByKey implements storage.APIKeyStore.FindByKey.
func (m *MockAPIKeyStore) FindByKey(ctx context.Context, key string) (*storage.APIKey, bool) {
	if m.FindByKeyFunc != nil {
		return m.FindByKeyFunc(ctx, key)
	}

	return nil, false
}

// Add implements storage.APIKeyStore.Add.
func (m *MockAPIKeyStore) Add(ctx context.Context, apiKey *storage.APIKey) error {
	if m.AddFunc != nil {
		return m.AddFunc(ctx, apiKey)
	}

	return nil
}

// Update implements storage.APIKeyStore.Update.
func (m *MockAPIKeyStore) Update(ctx context.Context, apiKey *storage.APIKey) error {
	if m.UpdateFunc != nil {
		return m.UpdateFunc(ctx, apiKey)
	}

	return nil
}

// Delete implements storage.APIKeyStore.Delete.
func (m *MockAPIKeyStore) Delete(ctx context.Context, keyID string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, keyID)
	}

	return nil
}

// ListByPlugin implements storage.APIKeyStore.ListByPlugin.
func (m *MockAPIKeyStore) ListByPlugin(ctx context.Context, pluginID string) ([]*storage.APIKey, error) {
	if m.ListByPluginFunc != nil {
		return m.ListByPluginFunc(ctx, pluginID)
	}

	return []*storage.APIKey{}, nil
}
