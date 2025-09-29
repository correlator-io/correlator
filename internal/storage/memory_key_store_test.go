package storage

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestInMemoryKeyStore(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Test data
	testKey := &Key{
		ID:          "key-1",
		Key:         "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		PluginID:    "dbt-plugin",
		Name:        "DBT Production Plugin",
		Permissions: []string{"lineage:write", "health:read"},
		CreatedAt:   time.Now(),
		Active:      true,
	}

	t.Run("add and find key", func(t *testing.T) {
		store := NewInMemoryKeyStore()

		err := store.Add(testKey)
		if err != nil {
			t.Errorf("Add() unexpected error: %v", err)
		}

		found, exists := store.FindByKey(testKey.Key)
		if !exists {
			t.Errorf("FindByKey() key not found")
		}

		if found.ID != testKey.ID {
			t.Errorf("FindByKey() ID = %v, want %v", found.ID, testKey.ID)
		}

		if found.PluginID != testKey.PluginID {
			t.Errorf("FindByKey() PluginID = %v, want %v", found.PluginID, testKey.PluginID)
		}
	})

	t.Run("find non-existent key", func(t *testing.T) {
		store := NewInMemoryKeyStore()

		found, exists := store.FindByKey("non-existent-key")
		if exists {
			t.Errorf("FindByKey() found non-existent key")
		}

		if found != nil {
			t.Errorf("FindByKey() returned non-nil for non-existent key")
		}
	})

	t.Run("update existing key", func(t *testing.T) {
		store := NewInMemoryKeyStore()
		// Add initial key
		err := store.Add(testKey)
		if err != nil {
			t.Errorf("Add() unexpected error: %v", err)
		}

		// Update key
		updatedKey := &Key{
			ID:          testKey.ID,
			Key:         testKey.Key,
			PluginID:    testKey.PluginID,
			Name:        "Updated DBT Plugin",
			Permissions: []string{"lineage:write", "health:read", "metrics:read"},
			CreatedAt:   testKey.CreatedAt,
			Active:      false, // Deactivate
		}

		err = store.Update(updatedKey)
		if err != nil {
			t.Errorf("Update() unexpected error: %v", err)
		}

		// Verify update
		found, exists := store.FindByKey(testKey.Key)
		if !exists {
			t.Errorf("FindByKey() updated key not found")
		}

		if found.Name != updatedKey.Name {
			t.Errorf("FindByKey() Name = %v, want %v", found.Name, updatedKey.Name)
		}

		if found.Active != false {
			t.Errorf("FindByKey() Active = %v, want false", found.Active)
		}

		if len(found.Permissions) != 3 {
			t.Errorf("FindByKey() Permissions length = %v, want 3", len(found.Permissions))
		}
	})

	t.Run("delete key", func(t *testing.T) {
		store := NewInMemoryKeyStore()
		// Add key first
		err := store.Add(testKey)
		if err != nil {
			t.Errorf("Add() unexpected error: %v", err)
		}

		err = store.Delete(testKey.ID)
		if err != nil {
			t.Errorf("Delete() unexpected error: %v", err)
		}

		// Verify deletion
		found, exists := store.FindByKey(testKey.Key)
		if exists {
			t.Errorf("FindByKey() found deleted key")
		}

		if found != nil {
			t.Errorf("FindByKey() returned non-nil for deleted key")
		}
	})

	t.Run("list by plugin", func(t *testing.T) {
		store := NewInMemoryKeyStore()
		// Add multiple keys for different plugins
		key1 := &Key{
			ID:       "key-1",
			Key:      "correlator_ak_1111111111111111111111111111111111111111111111111111111111111111",
			PluginID: "dbt-plugin",
			Name:     "DBT Key 1",
			Active:   true,
		}
		key2 := &Key{
			ID:       "key-2",
			Key:      "correlator_ak_2222222222222222222222222222222222222222222222222222222222222222",
			PluginID: "dbt-plugin",
			Name:     "DBT Key 2",
			Active:   true,
		}
		key3 := &Key{
			ID:       "key-3",
			Key:      "correlator_ak_3333333333333333333333333333333333333333333333333333333333333333",
			PluginID: "airflow-plugin",
			Name:     "Airflow Key 1",
			Active:   true,
		}

		err := store.Add(key1)
		if err != nil {
			t.Errorf("Add() unexpected error: %v", err)
		}

		err = store.Add(key2)
		if err != nil {
			t.Errorf("Add() unexpected error: %v", err)
		}

		err = store.Add(key3)
		if err != nil {
			t.Errorf("Add() unexpected error: %v", err)
		}

		dbtKeys, err := store.ListByPlugin("dbt-plugin")
		if err != nil {
			t.Errorf("ListByPlugin() unexpected error: %v", err)
		}

		if len(dbtKeys) != 2 {
			t.Errorf("ListByPlugin() returned %d keys, want 2", len(dbtKeys))
		}

		airflowKeys, err := store.ListByPlugin("airflow-plugin")
		if err != nil {
			t.Errorf("ListByPlugin() unexpected error: %v", err)
		}

		if len(airflowKeys) != 1 {
			t.Errorf("ListByPlugin() returned %d keys, want 1", len(airflowKeys))
		}

		// Test non-existent plugin
		nonKeys, err := store.ListByPlugin("non-existent-plugin")
		if err != nil {
			t.Errorf("ListByPlugin() unexpected error: %v", err)
		}

		if len(nonKeys) != 0 {
			t.Errorf("ListByPlugin() returned %d keys for non-existent plugin, want 0", len(nonKeys))
		}
	})
}

func TestInMemoryKeyStoreConcurrency(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	store := NewInMemoryKeyStore()

	// Test concurrent reads and writes
	t.Run("concurrent access", func(t *testing.T) {
		// This will test thread safety - multiple goroutines accessing store
		done := make(chan bool, 100)

		// Start multiple goroutines that add keys
		for i := 0; i < 50; i++ {
			go func(id int) {
				key := &Key{
					ID:       fmt.Sprintf("key-%d", id),
					Key:      fmt.Sprintf("correlator_ak_%064d", id), // 64 digit number padded with zeros
					PluginID: "test-plugin",
					Name:     fmt.Sprintf("Test Key %d", id),
					Active:   true,
				}

				err := store.Add(key)
				if err != nil {
					t.Errorf("Concurrent Add() unexpected error: %v", err)
				}

				done <- true
			}(i)
		}

		// Start multiple goroutines that read keys
		for i := 0; i < 50; i++ {
			go func(id int) {
				keyStr := fmt.Sprintf("correlator_ak_%064d", id)
				_, _ = store.FindByKey(keyStr) // Don't care about result, just testing concurrency

				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < 100; i++ {
			<-done
		}
	})
}

func TestInMemoryKeyStoreErrors(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	store := NewInMemoryKeyStore()

	t.Run("add duplicate key", func(t *testing.T) {
		key := &Key{
			ID:       "key-1",
			Key:      "correlator_ak_1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			PluginID: "test-plugin",
			Name:     "Test Key",
			Active:   true,
		}

		// Add key first time - should succeed
		err := store.Add(key)
		if err != nil {
			t.Errorf("Add() first time unexpected error: %v", err)
		}

		// Add same key again - should fail
		err = store.Add(key)
		if err == nil {
			t.Errorf("Add() duplicate key should return error")
		}
	})

	t.Run("update non-existent key", func(t *testing.T) {
		key := &Key{
			ID:       "non-existent-key",
			Key:      "correlator_ak_9999999999999999999999999999999999999999999999999999999999999999",
			PluginID: "test-plugin",
			Name:     "Non-existent Key",
			Active:   true,
		}

		err := store.Update(key)
		if err == nil {
			t.Errorf("Update() non-existent key should return error")
		}
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		err := store.Delete("non-existent-key")
		if err == nil {
			t.Errorf("Delete() non-existent key should return error")
		}
	})

	t.Run("add nil key", func(t *testing.T) {
		err := store.Add(nil)
		if !errors.Is(err, ErrKeyNil) {
			t.Errorf("Add() nil key should return ErrKeyNil, got %v", err)
		}
	})

	t.Run("update nil key", func(t *testing.T) {
		err := store.Update(nil)
		if !errors.Is(err, ErrKeyNil) {
			t.Errorf("Update() nil key should return ErrKeyNil, got %v", err)
		}
	})
}
