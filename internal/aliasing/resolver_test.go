package aliasing

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewResolver_WithValidConfig(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod": "postgresql://prod-db:5432/mydb",
			"mysql_prod":    "mysql://prod-db:3306/mydb",
		},
	}

	r := NewResolver(cfg)

	require.NotNil(t, r)
	assert.Equal(t, 2, r.AliasCount())
}

func TestNewResolver_WithNilConfig(t *testing.T) {
	r := NewResolver(nil)

	require.NotNil(t, r)
	assert.Equal(t, 0, r.AliasCount())
}

func TestNewResolver_WithEmptyAliases(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{},
	}

	r := NewResolver(cfg)

	require.NotNil(t, r)
	assert.Equal(t, 0, r.AliasCount())
}

func TestResolver_Resolve_KnownAlias(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("postgres_prod")

	assert.Equal(t, "postgresql://prod-db:5432/mydb", result)
}

func TestResolver_Resolve_UnknownNamespace(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	// Unknown namespace should pass through unchanged
	result := r.Resolve("unknown_namespace")

	assert.Equal(t, "unknown_namespace", result)
}

func TestResolver_Resolve_EmptyString(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("")

	assert.Empty(t, result)
}

func TestResolver_Resolve_WithNilConfig(t *testing.T) {
	r := NewResolver(nil)

	// Should pass through when no config
	result := r.Resolve("any_namespace")

	assert.Equal(t, "any_namespace", result)
}

func TestResolver_Resolve_CaseSensitive(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	// Case mismatch should not match - aliases are case-sensitive
	result := r.Resolve("POSTGRES_PROD")

	assert.Equal(t, "POSTGRES_PROD", result)
}

func TestResolver_Resolve_MultipleAliasesToSameCanonical(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod":           "postgresql://prod-db:5432/mydb",
			"postgres://prod-db:5432": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	// Both aliases should resolve to same canonical
	assert.Equal(t, "postgresql://prod-db:5432/mydb", r.Resolve("postgres_prod"))
	assert.Equal(t, "postgresql://prod-db:5432/mydb", r.Resolve("postgres://prod-db:5432"))
}

func TestResolver_Resolve_TransitiveChain(t *testing.T) {
	// A → B → C should resolve A to C
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod":           "postgres://prod-db:5432",
			"postgres://prod-db:5432": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	// Direct resolution
	assert.Equal(t, "postgresql://prod-db:5432/mydb", r.Resolve("postgres://prod-db:5432"))

	// Transitive resolution: postgres_prod → postgres://... → postgresql://...
	assert.Equal(t, "postgresql://prod-db:5432/mydb", r.Resolve("postgres_prod"))
}

func TestResolver_Resolve_LongTransitiveChain(t *testing.T) {
	// A → B → C → D should resolve A to D
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias1": "alias2",
			"alias2": "alias3",
			"alias3": "canonical",
		},
	}
	r := NewResolver(cfg)

	assert.Equal(t, "canonical", r.Resolve("alias1"))
	assert.Equal(t, "canonical", r.Resolve("alias2"))
	assert.Equal(t, "canonical", r.Resolve("alias3"))
	assert.Equal(t, "canonical", r.Resolve("canonical")) // Terminal, returns itself
}

func TestResolver_Resolve_CircularChainDetection(t *testing.T) {
	// Manually construct a resolver with a circular chain
	// (bypassing NewResolver validation for testing)
	r := &Resolver{
		aliases: map[string]string{
			"a": "b",
			"b": "c",
			"c": "a", // Creates cycle: a → b → c → a
		},
	}

	// Should detect the loop and return without infinite loop
	result := r.Resolve("a")

	// The exact result depends on where the loop is detected
	// but it should be one of the values in the chain
	assert.Contains(t, []string{"a", "b", "c"}, result)
}

func TestResolver_HasAlias(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	assert.True(t, r.HasAlias("postgres_prod"))
	assert.False(t, r.HasAlias("unknown"))
	assert.False(t, r.HasAlias(""))
}

func TestResolver_HasAlias_NilConfig(t *testing.T) {
	r := NewResolver(nil)

	assert.False(t, r.HasAlias("any"))
}

func TestResolver_AliasCount(t *testing.T) {
	tests := []struct {
		name     string
		aliases  map[string]string
		expected int
	}{
		{
			name:     "empty",
			aliases:  map[string]string{},
			expected: 0,
		},
		{
			name:     "one",
			aliases:  map[string]string{"a": "b"},
			expected: 1,
		},
		{
			name:     "multiple",
			aliases:  map[string]string{"a": "b", "c": "d", "e": "f"},
			expected: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewResolver(&Config{NamespaceAliases: tc.aliases})
			assert.Equal(t, tc.expected, r.AliasCount())
		})
	}
}

func TestResolver_Aliases_ReturnsCopy(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias1": "canonical1",
		},
	}
	r := NewResolver(cfg)

	// Get copy and modify it
	cp := r.Aliases()
	cp["alias2"] = "canonical2"

	// Original should be unchanged
	assert.Equal(t, 1, r.AliasCount())
	assert.False(t, r.HasAlias("alias2"))
}

func TestResolver_AliasSlices(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias1": "canonical1",
			"alias2": "canonical2",
		},
	}
	r := NewResolver(cfg)

	keys, values := r.AliasSlices()

	assert.Len(t, keys, 2)
	assert.Len(t, values, 2)

	// Build a map from slices to verify correctness (order is not guaranteed)
	resultMap := make(map[string]string)
	for i := range keys {
		resultMap[keys[i]] = values[i]
	}

	assert.Equal(t, "canonical1", resultMap["alias1"])
	assert.Equal(t, "canonical2", resultMap["alias2"])
}

func TestResolver_AliasSlices_Empty(t *testing.T) {
	r := NewResolver(nil)

	keys, values := r.AliasSlices()

	assert.Empty(t, keys)
	assert.Empty(t, values)
}

// Validation tests

func TestNewResolver_SkipsSelfReferentialAlias(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod": "postgres_prod", // Self-referential - should be skipped
			"mysql_prod":    "mysql://prod",  // Valid
		},
	}

	r := NewResolver(cfg)

	// Should only have the valid alias
	assert.Equal(t, 1, r.AliasCount())
	assert.False(t, r.HasAlias("postgres_prod"))
	assert.True(t, r.HasAlias("mysql_prod"))
}

func TestNewResolver_SkipsCircularAlias(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias_a": "alias_b", // First alias (processed first due to sorting)
			"alias_b": "alias_a", // Circular - skipped because alias_a already processed
		},
	}

	r := NewResolver(cfg)

	// Processing is deterministic (sorted by key):
	// 1. alias_a → alias_b is processed first (a < b alphabetically)
	// 2. alias_b → alias_a is skipped because alias_a is already a valid alias key
	assert.Equal(t, 1, r.AliasCount(), "Only one alias should be kept")
	assert.True(t, r.HasAlias("alias_a"), "alias_a should be kept (processed first)")
	assert.False(t, r.HasAlias("alias_b"), "alias_b should be skipped (circular)")
}

func TestNewResolver_DeterministicCircularHandling(t *testing.T) {
	// Run multiple times to verify determinism
	for i := 0; i < 10; i++ {
		cfg := &Config{
			NamespaceAliases: map[string]string{
				"zebra":  "apple",
				"apple":  "zebra",
				"banana": "cherry",
			},
		}

		r := NewResolver(cfg)

		// Sorted order: apple, banana, zebra
		// 1. apple → zebra: kept (zebra not yet processed)
		// 2. banana → cherry: kept (cherry is not an alias)
		// 3. zebra → apple: skipped (apple already in validAliases)
		assert.Equal(t, 2, r.AliasCount(), "Should have exactly 2 aliases")
		assert.True(t, r.HasAlias("apple"), "apple should be kept")
		assert.True(t, r.HasAlias("banana"), "banana should be kept")
		assert.False(t, r.HasAlias("zebra"), "zebra should be skipped (circular with apple)")
	}
}

func TestNewResolver_SkipsEmptyCanonical(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias1": "",      // Empty canonical - should be skipped
			"alias2": "   ",   // Whitespace only - should be skipped
			"alias3": "valid", // Valid
		},
	}

	r := NewResolver(cfg)

	// Should only have the valid alias
	assert.Equal(t, 1, r.AliasCount())
	assert.False(t, r.HasAlias("alias1"))
	assert.False(t, r.HasAlias("alias2"))
	assert.True(t, r.HasAlias("alias3"))
}

func TestNewResolver_TrimsWhitespace(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"  alias_with_spaces  ": "  canonical_with_spaces  ",
		},
	}

	r := NewResolver(cfg)

	// Keys and values should be trimmed
	assert.True(t, r.HasAlias("alias_with_spaces"))
	assert.Equal(t, "canonical_with_spaces", r.Resolve("alias_with_spaces"))
}

//nolint:gosmopolitan // testing unicode support intentionally
func TestResolver_Resolve_Unicode(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"生产数据库": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("生产数据库")

	assert.Equal(t, "postgresql://prod-db:5432/mydb", result)
}

func TestResolver_ConcurrentResolve(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias1": "canonical1",
			"alias2": "canonical2",
			"alias3": "canonical3",
		},
	}
	r := NewResolver(cfg)

	var wg sync.WaitGroup

	// Run 100 concurrent resolve operations
	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			// Mix of known aliases and passthrough
			switch i % 4 {
			case 0:
				assert.Equal(t, "canonical1", r.Resolve("alias1"))
			case 1:
				assert.Equal(t, "canonical2", r.Resolve("alias2"))
			case 2:
				assert.Equal(t, "canonical3", r.Resolve("alias3"))
			case 3:
				assert.Equal(t, "unknown", r.Resolve("unknown"))
			}
		}(i)
	}

	wg.Wait()
}
