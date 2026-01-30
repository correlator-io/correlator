package aliasing

import (
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
	assert.Equal(t, 2, r.GetAliasCount())
}

func TestNewResolver_WithNilConfig(t *testing.T) {
	r := NewResolver(nil)

	require.NotNil(t, r)
	assert.Equal(t, 0, r.GetAliasCount())
}

func TestNewResolver_WithEmptyAliases(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{},
	}

	r := NewResolver(cfg)

	require.NotNil(t, r)
	assert.Equal(t, 0, r.GetAliasCount())
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
			assert.Equal(t, tc.expected, r.GetAliasCount())
		})
	}
}

func TestResolver_AliasCount_NilResolver(t *testing.T) {
	var r *Resolver
	assert.Equal(t, 0, r.GetAliasCount())
}

func TestResolver_Aliases_ReturnsCopy(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias1": "canonical1",
		},
	}
	r := NewResolver(cfg)

	// Get copy and modify it
	cp := r.GetAliases()
	cp["alias2"] = "canonical2"

	// Original should be unchanged
	assert.Equal(t, 1, r.GetAliasCount())

	// Verify alias2 is not in the resolver's aliases
	aliases := r.GetAliases()
	_, exists := aliases["alias2"]
	assert.False(t, exists)
}

func TestResolver_Aliases_Empty(t *testing.T) {
	r := NewResolver(nil)

	aliases := r.GetAliases()

	assert.NotNil(t, aliases)
	assert.Empty(t, aliases)
}

func TestResolver_AliasSlices(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"alias1": "canonical1",
			"alias2": "canonical2",
		},
	}
	r := NewResolver(cfg)

	keys, values := r.GetAliasSlices()

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

	keys, values := r.GetAliasSlices()

	assert.Empty(t, keys)
	assert.Empty(t, values)
}

func TestResolver_AliasSlices_NilResolver(t *testing.T) {
	var r *Resolver

	keys, values := r.GetAliasSlices()

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
	assert.Equal(t, 1, r.GetAliasCount())

	aliases := r.GetAliases()
	_, hasSelfRef := aliases["postgres_prod"]
	_, hasValid := aliases["mysql_prod"]

	assert.False(t, hasSelfRef, "Self-referential alias should be skipped")
	assert.True(t, hasValid, "Valid alias should be kept")
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
	assert.Equal(t, 1, r.GetAliasCount(), "Only one alias should be kept")

	aliases := r.GetAliases()
	_, hasA := aliases["alias_a"]
	_, hasB := aliases["alias_b"]

	assert.True(t, hasA, "alias_a should be kept (processed first)")
	assert.False(t, hasB, "alias_b should be skipped (circular)")
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
		assert.Equal(t, 2, r.GetAliasCount(), "Should have exactly 2 aliases")

		aliases := r.GetAliases()
		_, hasApple := aliases["apple"]
		_, hasBanana := aliases["banana"]
		_, hasZebra := aliases["zebra"]

		assert.True(t, hasApple, "apple should be kept")
		assert.True(t, hasBanana, "banana should be kept")
		assert.False(t, hasZebra, "zebra should be skipped (circular with apple)")
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
	assert.Equal(t, 1, r.GetAliasCount())

	aliases := r.GetAliases()
	_, has1 := aliases["alias1"]
	_, has2 := aliases["alias2"]
	_, has3 := aliases["alias3"]

	assert.False(t, has1, "Empty canonical should be skipped")
	assert.False(t, has2, "Whitespace-only canonical should be skipped")
	assert.True(t, has3, "Valid alias should be kept")
}

func TestNewResolver_TrimsWhitespace(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"  alias_with_spaces  ": "  canonical_with_spaces  ",
		},
	}

	r := NewResolver(cfg)

	// Keys and values should be trimmed
	aliases := r.GetAliases()
	canonical, exists := aliases["alias_with_spaces"]

	assert.True(t, exists, "Trimmed alias key should exist")
	assert.Equal(t, "canonical_with_spaces", canonical, "Canonical value should be trimmed")
}

func TestNewResolver_MultipleAliasesToSameCanonical(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod":           "postgresql://prod-db:5432/mydb",
			"postgres://prod-db:5432": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	assert.Equal(t, 2, r.GetAliasCount())

	aliases := r.GetAliases()
	assert.Equal(t, "postgresql://prod-db:5432/mydb", aliases["postgres_prod"])
	assert.Equal(t, "postgresql://prod-db:5432/mydb", aliases["postgres://prod-db:5432"])
}

func TestNewResolver_TransitiveChainAllowed(t *testing.T) {
	// A → B → C should be allowed (not circular)
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"postgres_prod":           "postgres://prod-db:5432",
			"postgres://prod-db:5432": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	// Both aliases should be kept (transitive chains are valid)
	assert.Equal(t, 2, r.GetAliasCount())

	aliases := r.GetAliases()
	_, has1 := aliases["postgres_prod"]
	_, has2 := aliases["postgres://prod-db:5432"]

	assert.True(t, has1)
	assert.True(t, has2)
}

//nolint:gosmopolitan // testing unicode support intentionally
func TestNewResolver_UnicodeAliases(t *testing.T) {
	cfg := &Config{
		NamespaceAliases: map[string]string{
			"生产数据库": "postgresql://prod-db:5432/mydb",
		},
	}
	r := NewResolver(cfg)

	assert.Equal(t, 1, r.GetAliasCount())

	aliases := r.GetAliases()
	canonical, exists := aliases["生产数据库"]

	assert.True(t, exists)
	assert.Equal(t, "postgresql://prod-db:5432/mydb", canonical)
}
