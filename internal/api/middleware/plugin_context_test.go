// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"testing"
	"time"
)

// TestGetPluginContext_NotFound verifies that GetPluginContext returns empty context and false
// when no plugin context exists in the request context.
func TestGetPluginContext_NotFound(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	pluginCtx, found := GetPluginContext(ctx)

	if found {
		t.Error("GetPluginContext should return false when context not found")
	}

	if pluginCtx.PluginID != "" {
		t.Errorf("Expected empty PluginID, got %q", pluginCtx.PluginID)
	}
}

// TestGetPluginContext_Found verifies that GetPluginContext returns the correct
// plugin context when it exists in the request context.
func TestGetPluginContext_Found(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	authTime := time.Now()

	expected := PluginContext{
		PluginID:    "dbt-plugin-v1",
		Name:        "dbt Core Plugin",
		Permissions: []string{"lineage:write", "metrics:read"},
		KeyID:       "key-123",
		AuthTime:    authTime,
	}

	ctx = SetPluginContext(ctx, expected)
	actual, found := GetPluginContext(ctx)

	if !found {
		t.Fatal("GetPluginContext should return true when context exists")
	}

	if actual.PluginID != expected.PluginID {
		t.Errorf("Expected PluginID %q, got %q", expected.PluginID, actual.PluginID)
	}

	if actual.Name != expected.Name {
		t.Errorf("Expected Name %q, got %q", expected.Name, actual.Name)
	}

	if len(actual.Permissions) != len(expected.Permissions) {
		t.Errorf("Expected %d permissions, got %d", len(expected.Permissions), len(actual.Permissions))
	}

	for i, perm := range expected.Permissions {
		if actual.Permissions[i] != perm {
			t.Errorf("Expected permission[%d] %q, got %q", i, perm, actual.Permissions[i])
		}
	}

	if actual.KeyID != expected.KeyID {
		t.Errorf("Expected KeyID %q, got %q", expected.KeyID, actual.KeyID)
	}

	if !actual.AuthTime.Equal(expected.AuthTime) {
		t.Errorf("Expected AuthTime %v, got %v", expected.AuthTime, actual.AuthTime)
	}
}

// TestSetPluginContext verifies that SetPluginContext correctly stores
// plugin context in the request context and can be retrieved.
func TestSetPluginContext(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	authTime := time.Now()

	pluginCtx := PluginContext{
		PluginID:    "airflow-plugin-v1",
		Name:        "Apache Airflow Plugin",
		Permissions: []string{"lineage:write"},
		KeyID:       "key-456",
		AuthTime:    authTime,
	}

	newCtx := SetPluginContext(ctx, pluginCtx)

	// Verify original context is not modified
	_, found := GetPluginContext(ctx)
	if found {
		t.Error("Original context should not contain plugin context")
	}

	// Verify new context contains plugin context
	retrieved, found := GetPluginContext(newCtx)
	if !found {
		t.Fatal("New context should contain plugin context")
	}

	if retrieved.PluginID != pluginCtx.PluginID {
		t.Errorf("Expected PluginID %q, got %q", pluginCtx.PluginID, retrieved.PluginID)
	}
}

// TestSetPluginContext_MultipleValues verifies that SetPluginContext can be called
// multiple times and the latest value is returned.
func TestSetPluginContext_MultipleValues(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	first := PluginContext{
		PluginID: "first-plugin",
		Name:     "First Plugin",
		KeyID:    "key-1",
		AuthTime: time.Now(),
	}

	second := PluginContext{
		PluginID: "second-plugin",
		Name:     "Second Plugin",
		KeyID:    "key-2",
		AuthTime: time.Now(),
	}

	// Set first value
	ctx = SetPluginContext(ctx, first)

	// Set second value (overwrites first)
	ctx = SetPluginContext(ctx, second)

	// Retrieve and verify second value is returned
	retrieved, found := GetPluginContext(ctx)
	if !found {
		t.Fatal("Context should contain plugin context")
	}

	if retrieved.PluginID != second.PluginID {
		t.Errorf("Expected PluginID %q, got %q", second.PluginID, retrieved.PluginID)
	}

	if retrieved.Name != second.Name {
		t.Errorf("Expected Name %q, got %q", second.Name, retrieved.Name)
	}
}

// TestPluginContext_EmptyPermissions verifies that PluginContext handles
// empty permissions slice correctly.
func TestPluginContext_EmptyPermissions(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	pluginCtx := PluginContext{
		PluginID:    "test-plugin",
		Name:        "Test Plugin",
		Permissions: []string{}, // Empty permissions
		KeyID:       "key-789",
		AuthTime:    time.Now(),
	}

	ctx = SetPluginContext(ctx, pluginCtx)
	retrieved, found := GetPluginContext(ctx)

	if !found {
		t.Fatal("Context should contain plugin context")
	}

	if retrieved.Permissions == nil {
		t.Error("Permissions should not be nil, expected empty slice")
	}

	if len(retrieved.Permissions) != 0 {
		t.Errorf("Expected 0 permissions, got %d", len(retrieved.Permissions))
	}
}
