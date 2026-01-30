package aliasing

import (
	"log/slog"
	"sort"
	"strings"
)

// Resolver resolves namespace aliases to canonical namespaces.
// Thread-safe for concurrent use (immutable after construction).
//
// The resolver maps tool-specific namespaces to canonical namespaces,
// enabling cross-tool correlation when different data tools use different
// namespace formats for the same data source.
type Resolver struct {
	aliases map[string]string
}

// wouldCreateCycle checks if adding alias → canonical would create a cycle.
// A cycle exists if following the chain from canonical eventually reaches alias.
func wouldCreateCycle(alias, canonical string, validAliases map[string]string) bool {
	visited := make(map[string]bool)
	current := canonical

	for {
		// If we reach the alias we're trying to add, it's a cycle
		if current == alias {
			return true
		}

		// If we've seen this value before, there's already a cycle (shouldn't happen)
		if visited[current] {
			return false
		}

		visited[current] = true

		// Follow the chain
		next, exists := validAliases[current]
		if !exists {
			// End of chain, no cycle
			return false
		}

		current = next
	}
}

// NewResolver creates a resolver from config with validation.
//
// Validates:
//   - No self-referential aliases (A → A) - skipped with warning
//   - No circular aliases (A → B where B is also an alias) - skipped with warning
//   - No empty canonical values - skipped with warning
//   - Whitespace is trimmed from keys and values
//
// Processing order is deterministic (sorted by alias key) to ensure consistent
// behavior across server restarts when circular aliases are detected.
//
// Returns a resolver containing only valid aliases.
// If config is nil or has no aliases, returns a no-op resolver (passthrough).
func NewResolver(cfg *Config) *Resolver {
	if cfg == nil || len(cfg.NamespaceAliases) == 0 {
		return &Resolver{
			aliases: make(map[string]string),
		}
	}

	// Sort keys for deterministic processing order
	// This ensures consistent behavior when circular aliases are detected
	sortedKeys := make([]string, 0, len(cfg.NamespaceAliases))
	for k := range cfg.NamespaceAliases {
		sortedKeys = append(sortedKeys, k)
	}

	sort.Strings(sortedKeys)

	validAliases := make(map[string]string)

	for _, alias := range sortedKeys {
		canonical := cfg.NamespaceAliases[alias]
		// Trim whitespace from both key and value
		alias = strings.TrimSpace(alias)
		canonical = strings.TrimSpace(canonical)

		// Skip empty alias keys
		if alias == "" {
			slog.Warn("Skipping alias with empty key")

			continue
		}

		// Skip empty canonical values
		if canonical == "" {
			slog.Warn("Skipping alias with empty canonical value",
				slog.String("alias", alias))

			continue
		}

		// Skip self-referential aliases (A → A)
		if alias == canonical {
			slog.Warn("Skipping self-referential alias",
				slog.String("alias", alias))

			continue
		}

		// Skip circular aliases - check if adding this alias would create a cycle
		// A cycle exists if following the chain from canonical leads back to alias
		// This allows transitive chains (A → B → C) while preventing cycles (A → B → A)
		if wouldCreateCycle(alias, canonical, validAliases) {
			slog.Warn("Skipping circular alias",
				slog.String("alias", alias),
				slog.String("canonical", canonical))

			continue
		}

		validAliases[alias] = canonical
	}

	return &Resolver{
		aliases: validAliases,
	}
}

// Resolve returns the canonical namespace for an alias.
// If no alias exists for the given namespace, returns it unchanged (passthrough).
//
// Transitive resolution: If A → B and B → C, then Resolve("A") returns "C".
// This follows the alias chain until reaching a terminal (non-aliased) value.
//
// Loop detection: If a circular chain is detected (should not happen with
// proper validation), resolution stops and returns the last resolved value.
//
// This method is thread-safe as the resolver is immutable after construction.
func (r *Resolver) Resolve(namespace string) string {
	if r == nil || len(r.aliases) == 0 {
		return namespace
	}

	// Track visited namespaces to detect loops
	visited := make(map[string]bool)
	current := namespace

	for {
		// Check if we've seen this namespace before (loop detection)
		if visited[current] {
			slog.Warn("Circular alias chain detected during resolution",
				slog.String("original", namespace),
				slog.String("loop_at", current))

			return current
		}

		visited[current] = true

		// Try to resolve current namespace
		canonical, exists := r.aliases[current]
		if !exists {
			// No more aliases to follow - return current value
			return current
		}

		// Follow the chain
		current = canonical
	}
}

// HasAlias returns true if the namespace has a configured alias.
func (r *Resolver) HasAlias(namespace string) bool {
	if r == nil || len(r.aliases) == 0 {
		return false
	}

	_, exists := r.aliases[namespace]

	return exists
}

// AliasCount returns the number of configured aliases.
func (r *Resolver) AliasCount() int {
	if r == nil {
		return 0
	}

	return len(r.aliases)
}

// Aliases returns a copy of the alias map.
// Modifications to the returned map do not affect the resolver.
func (r *Resolver) Aliases() map[string]string {
	if r == nil || len(r.aliases) == 0 {
		return make(map[string]string)
	}

	// Return a copy to prevent external modification
	result := make(map[string]string, len(r.aliases))
	for k, v := range r.aliases {
		result[k] = v
	}

	return result
}

// AliasSlices returns aliases as parallel slices for SQL parameterization.
// Returns (aliasKeys, canonicalValues) for use with PostgreSQL unnest().
//
// Example usage:
//
//	keys, values := resolver.AliasSlices()
//	query := "SELECT unnest($1::text[]), unnest($2::text[])"
//	rows, err := db.Query(query, pq.Array(keys), pq.Array(values))
func (r *Resolver) AliasSlices() ([]string, []string) {
	if r == nil || len(r.aliases) == 0 {
		return []string{}, []string{}
	}

	keys := make([]string, 0, len(r.aliases))
	values := make([]string, 0, len(r.aliases))

	for k, v := range r.aliases {
		keys = append(keys, k)
		values = append(values, v)
	}

	return keys, values
}
