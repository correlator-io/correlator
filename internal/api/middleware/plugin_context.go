// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"time"
)

// pluginContextKey is the context key for authenticated plugin information.
// Using a struct type ensures type safety and prevents collisions with other context keys.
type pluginContextKey struct{}

// PluginContext contains authenticated plugin information enriched in the request context.
// This context is added by the authentication middleware after successful API key validation.
type PluginContext struct {
	// PluginID is the unique identifier for the plugin (e.g., "dbt-plugin-v1")
	PluginID string

	// Name is the human-readable plugin name for logging and display
	Name string

	// Permissions are the authorization scopes granted to this plugin
	Permissions []string

	// KeyID is the API key ID used for authentication (for audit logging)
	KeyID string

	// AuthTime is the timestamp when authentication occurred (for latency tracking)
	AuthTime time.Time
}

// GetPluginContext extracts plugin context from the request context.
// Returns (context, true) if authenticated, (empty, false) if not found.
//
// Example usage:
//
//	pluginCtx, authenticated := middleware.GetPluginContext(r.Context())
//	if !authenticated {
//	    // Handle unauthenticated request
//	    return
//	}
//	log.Printf("Request from plugin: %s", pluginCtx.PluginID)
func GetPluginContext(ctx context.Context) (PluginContext, bool) {
	pluginCtx, ok := ctx.Value(pluginContextKey{}).(PluginContext)

	return pluginCtx, ok
}

// SetPluginContext adds plugin context to the request context.
// Returns a new context with the plugin context attached.
//
// This function is used by the authentication middleware to enrich the request context
// after successful API key validation.
//
// Example usage:
//
//	pluginCtx := middleware.PluginContext{
//	    PluginID:    "dbt-plugin-v1",
//	    Name:        "dbt Core Plugin",
//	    Permissions: []string{"lineage:write"},
//	    KeyID:       "key-123",
//	    AuthTime:    time.Now(),
//	}
//	newCtx := middleware.SetPluginContext(r.Context(), pluginCtx)
func SetPluginContext(ctx context.Context, pluginCtx PluginContext) context.Context {
	return context.WithValue(ctx, pluginContextKey{}, pluginCtx)
}
