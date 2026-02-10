package correlation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSuggestPatterns_SinglePattern tests pattern suggestion from orphans with same prefix.
func TestSuggestPatterns_SinglePattern(t *testing.T) {
	// TC-002 scenario: GE orphans with matching dbt datasets
	orphans := []OrphanDataset{
		{
			DatasetURN: "demo_postgres/customers",
			LikelyMatch: &DatasetMatch{
				DatasetURN:  "postgresql://demo/marts.customers",
				Confidence:  1.0,
				MatchReason: "exact_table_name",
			},
		},
		{
			DatasetURN: "demo_postgres/orders",
			LikelyMatch: &DatasetMatch{
				DatasetURN:  "postgresql://demo/marts.orders",
				Confidence:  1.0,
				MatchReason: "exact_table_name",
			},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1, "Should suggest 1 pattern")
	assert.Equal(t, "demo_postgres/{name}", patterns[0].Pattern)
	assert.Equal(t, "postgresql://demo/marts.{name}", patterns[0].Canonical)
	assert.Equal(t, 2, patterns[0].ResolvesCount)
	assert.ElementsMatch(t, []string{"demo_postgres/customers", "demo_postgres/orders"}, patterns[0].OrphansResolved)
}

// TestSuggestPatterns_MultiplePatterns tests multiple distinct pattern groups.
func TestSuggestPatterns_MultiplePatterns(t *testing.T) {
	orphans := []OrphanDataset{
		// Group 1: demo_postgres → postgresql://demo/marts
		{
			DatasetURN:  "demo_postgres/customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
		{
			DatasetURN:  "demo_postgres/orders",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.orders"},
		},
		// Group 2: staging_db → postgresql://staging/raw
		{
			DatasetURN:  "staging_db/events",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://staging/raw.events"},
		},
		{
			DatasetURN:  "staging_db/users",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://staging/raw.users"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 2, "Should suggest 2 patterns")

	// Build map for easier assertion
	patternMap := make(map[string]SuggestedPattern)
	for _, p := range patterns {
		patternMap[p.Pattern] = p
	}

	// Verify group 1
	p1, ok := patternMap["demo_postgres/{name}"]
	require.True(t, ok, "Should have demo_postgres pattern")
	assert.Equal(t, "postgresql://demo/marts.{name}", p1.Canonical)
	assert.Equal(t, 2, p1.ResolvesCount)

	// Verify group 2
	p2, ok := patternMap["staging_db/{name}"]
	require.True(t, ok, "Should have staging_db pattern")
	assert.Equal(t, "postgresql://staging/raw.{name}", p2.Canonical)
	assert.Equal(t, 2, p2.ResolvesCount)
}

// TestSuggestPatterns_NoMatches tests orphans without likely matches.
func TestSuggestPatterns_NoMatches(t *testing.T) {
	orphans := []OrphanDataset{
		{DatasetURN: "demo_postgres/customers", LikelyMatch: nil},
		{DatasetURN: "demo_postgres/orders", LikelyMatch: nil},
	}

	patterns := SuggestPatterns(orphans)

	assert.Empty(t, patterns, "Should return empty when no likely matches")
}

// TestSuggestPatterns_SingleOrphan tests single orphan (cannot derive pattern).
func TestSuggestPatterns_SingleOrphan(t *testing.T) {
	orphans := []OrphanDataset{
		{
			DatasetURN:  "demo_postgres/customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
	}

	patterns := SuggestPatterns(orphans)

	// Single orphan can still suggest a pattern (it resolves 1 orphan)
	require.Len(t, patterns, 1, "Should suggest pattern even for single orphan")
	assert.Equal(t, 1, patterns[0].ResolvesCount)
}

// TestSuggestPatterns_EmptyOrphans tests empty input.
func TestSuggestPatterns_EmptyOrphans(t *testing.T) {
	patterns := SuggestPatterns(nil)
	assert.Empty(t, patterns, "Should return empty for nil input")

	patterns = SuggestPatterns([]OrphanDataset{})
	assert.Empty(t, patterns, "Should return empty for empty slice")
}

// TestSuggestPatterns_MixedMatchAndNoMatch tests mix of matched and unmatched orphans.
func TestSuggestPatterns_MixedMatchAndNoMatch(t *testing.T) {
	orphans := []OrphanDataset{
		{
			DatasetURN:  "demo_postgres/customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
		{
			DatasetURN:  "demo_postgres/orders",
			LikelyMatch: nil, // No match for this one
		},
		{
			DatasetURN:  "demo_postgres/products",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.products"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1, "Should suggest 1 pattern")
	assert.Equal(t, 2, patterns[0].ResolvesCount, "Should resolve only matched orphans")
	assert.ElementsMatch(t, []string{"demo_postgres/customers", "demo_postgres/products"}, patterns[0].OrphansResolved)
}

// TestSuggestPatterns_DifferentTableNameFormats tests various URN format combinations.
func TestSuggestPatterns_DifferentTableNameFormats(t *testing.T) {
	// Orphan uses slash separator, canonical uses dot separator
	orphans := []OrphanDataset{
		{
			DatasetURN:  "mydb/schema/table1",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://host/schema.table1"},
		},
		{
			DatasetURN:  "mydb/schema/table2",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://host/schema.table2"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1)
	assert.Equal(t, "mydb/schema/{name}", patterns[0].Pattern)
	assert.Equal(t, "postgresql://host/schema.{name}", patterns[0].Canonical)
}

// TestSuggestPatterns_SortedByResolvesCount tests patterns are sorted by resolves count.
func TestSuggestPatterns_SortedByResolvesCount(t *testing.T) {
	orphans := []OrphanDataset{
		// Group 1: 3 orphans
		{DatasetURN: "big/a", LikelyMatch: &DatasetMatch{DatasetURN: "canonical/a"}},
		{DatasetURN: "big/b", LikelyMatch: &DatasetMatch{DatasetURN: "canonical/b"}},
		{DatasetURN: "big/c", LikelyMatch: &DatasetMatch{DatasetURN: "canonical/c"}},
		// Group 2: 1 orphan
		{DatasetURN: "small/x", LikelyMatch: &DatasetMatch{DatasetURN: "other/x"}},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 2)
	// First pattern should have higher resolves count
	assert.Equal(t, 3, patterns[0].ResolvesCount, "First pattern should resolve 3")
	assert.Equal(t, 1, patterns[1].ResolvesCount, "Second pattern should resolve 1")
}

// TestSuggestPatterns_ProtocolInCanonical tests patterns where canonical has protocol.
func TestSuggestPatterns_ProtocolInCanonical(t *testing.T) {
	orphans := []OrphanDataset{
		{
			DatasetURN:  "simple/customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://db:5432/schema.customers"},
		},
		{
			DatasetURN:  "simple/orders",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://db:5432/schema.orders"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1)
	assert.Equal(t, "simple/{name}", patterns[0].Pattern)
	assert.Equal(t, "postgresql://db:5432/schema.{name}", patterns[0].Canonical)
}
