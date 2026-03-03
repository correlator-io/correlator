// Package middleware provides HTTP middleware components for the Correlator API.
package middleware

import (
	"context"
	"time"
)

// clientContextKey is the context key for authenticated client information.
// Using a struct type ensures type safety and prevents collisions with other context keys.
type clientContextKey struct{}

// ClientContext contains authenticated client information enriched in the request context.
// This context is added by the authentication middleware after successful API key validation.
type ClientContext struct {
	// ClientID identifies the OpenLineage integration or team (e.g., "default", "dbt-ol")
	ClientID string

	// Name is the human-readable client name for logging and display
	Name string

	// Permissions are the authorization scopes granted to this client
	Permissions []string

	// KeyID is the API key ID used for authentication (for audit logging)
	KeyID string

	// AuthTime is the timestamp when authentication occurred (for latency tracking)
	AuthTime time.Time
}

// GetClientContext extracts client context from the request context.
// Returns (context, true) if authenticated, (empty, false) if not found.
//
// Example usage:
//
//	clientCtx, authenticated := middleware.GetClientContext(r.Context())
//	if !authenticated {
//	    // Handle unauthenticated request
//	    return
//	}
//	log.Printf("Request from client: %s", clientCtx.ClientID)
func GetClientContext(ctx context.Context) (ClientContext, bool) {
	clientCtx, ok := ctx.Value(clientContextKey{}).(ClientContext)

	return clientCtx, ok
}

// SetClientContext adds client context to the request context.
// Returns a new context with the client context attached.
//
// This function is used by the authentication middleware to enrich the request context
// after successful API key validation.
//
// Example usage:
//
//	clientCtx := middleware.ClientContext{
//	    ClientID:    "default",
//	    Name:        "Production API Key",
//	    Permissions: []string{"lineage:write"},
//	    KeyID:       "key-123",
//	    AuthTime:    time.Now(),
//	}
//	newCtx := middleware.SetClientContext(r.Context(), clientCtx)
func SetClientContext(ctx context.Context, clientCtx ClientContext) context.Context {
	return context.WithValue(ctx, clientContextKey{}, clientCtx)
}
