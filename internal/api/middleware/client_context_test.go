// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"testing"
	"time"
)

// TestGetClientContext_NotFound verifies that GetClientContext returns empty context and false
// when no client context exists in the request context.
func TestGetClientContext_NotFound(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	clientCtx, found := GetClientContext(ctx)

	if found {
		t.Error("GetClientContext should return false when context not found")
	}

	if clientCtx.ClientID != "" {
		t.Errorf("Expected empty ClientID, got %q", clientCtx.ClientID)
	}
}

// TestGetClientContext_Found verifies that GetClientContext returns the correct
// client context when it exists in the request context.
func TestGetClientContext_Found(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	authTime := time.Now()

	expected := ClientContext{
		ClientID:    "dbt-ol-v1",
		Name:        "dbt ol",
		Permissions: []string{"lineage:write", "metrics:read"},
		KeyID:       "key-123",
		AuthTime:    authTime,
	}

	ctx = SetClientContext(ctx, expected)
	actual, found := GetClientContext(ctx)

	if !found {
		t.Fatal("GetClientContext should return true when context exists")
	}

	if actual.ClientID != expected.ClientID {
		t.Errorf("Expected ClientID %q, got %q", expected.ClientID, actual.ClientID)
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

// TestSetClientContext verifies that SetClientContext correctly stores
// client context in the request context and can be retrieved.
func TestSetClientContext(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()
	authTime := time.Now()

	clientCtx := ClientContext{
		ClientID:    "airflow-ol-v1",
		Name:        "Openlineage Airflow Integration",
		Permissions: []string{"lineage:write"},
		KeyID:       "key-456",
		AuthTime:    authTime,
	}

	newCtx := SetClientContext(ctx, clientCtx)

	// Verify original context is not modified
	_, found := GetClientContext(ctx)
	if found {
		t.Error("Original context should not contain client context")
	}

	// Verify new context contains client context
	retrieved, found := GetClientContext(newCtx)
	if !found {
		t.Fatal("New context should contain client context")
	}

	if retrieved.ClientID != clientCtx.ClientID {
		t.Errorf("Expected ClientID %q, got %q", clientCtx.ClientID, retrieved.ClientID)
	}
}

// TestSetClientContext_MultipleValues verifies that SetClientContext can be called
// multiple times and the latest value is returned.
func TestSetClientContext_MultipleValues(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	first := ClientContext{
		ClientID: "first-client",
		Name:     "First Client",
		KeyID:    "key-1",
		AuthTime: time.Now(),
	}

	second := ClientContext{
		ClientID: "second-client",
		Name:     "Second Client",
		KeyID:    "key-2",
		AuthTime: time.Now(),
	}

	// Set first value
	ctx = SetClientContext(ctx, first)

	// Set second value (overwrites first)
	ctx = SetClientContext(ctx, second)

	// Retrieve and verify second value is returned
	retrieved, found := GetClientContext(ctx)
	if !found {
		t.Fatal("Context should contain client context")
	}

	if retrieved.ClientID != second.ClientID {
		t.Errorf("Expected ClientID %q, got %q", second.ClientID, retrieved.ClientID)
	}

	if retrieved.Name != second.Name {
		t.Errorf("Expected Name %q, got %q", second.Name, retrieved.Name)
	}
}

// TestClientContext_EmptyPermissions verifies that ClientContext handles
// empty permissions slice correctly.
func TestClientContext_EmptyPermissions(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	ctx := context.Background()

	clientCtx := ClientContext{
		ClientID:    "test-client",
		Name:        "Test Client",
		Permissions: []string{}, // Empty permissions
		KeyID:       "key-789",
		AuthTime:    time.Now(),
	}

	ctx = SetClientContext(ctx, clientCtx)
	retrieved, found := GetClientContext(ctx)

	if !found {
		t.Fatal("Context should contain client context")
	}

	if retrieved.Permissions == nil {
		t.Error("Permissions should not be nil, expected empty slice")
	}

	if len(retrieved.Permissions) != 0 {
		t.Errorf("Expected 0 permissions, got %d", len(retrieved.Permissions))
	}
}
