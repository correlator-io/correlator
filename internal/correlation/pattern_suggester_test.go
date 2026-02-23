package correlation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSuggestPatterns_SinglePattern tests pattern suggestion from orphans with same namespace.
func TestSuggestPatterns_SinglePattern(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		{
			DatasetURN: "demo_postgres/marts.customers",
			LikelyMatch: &DatasetMatch{
				DatasetURN:  "postgresql://demo/marts.customers",
				Confidence:  1.0,
				MatchReason: "exact_table_name",
			},
		},
		{
			DatasetURN: "demo_postgres/marts.orders",
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
	assert.Equal(t, "postgresql://demo/{name}", patterns[0].Canonical)
	assert.Equal(t, 2, patterns[0].ResolvesCount)
	assert.ElementsMatch(
		t, []string{"demo_postgres/marts.customers", "demo_postgres/marts.orders"}, patterns[0].OrphansResolved,
	)
}

// TestSuggestPatterns_MultiplePatterns tests multiple distinct namespace groups.
func TestSuggestPatterns_MultiplePatterns(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		// Group 1: demo_postgres → postgresql://demo
		{
			DatasetURN:  "demo_postgres/marts.customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
		{
			DatasetURN:  "demo_postgres/marts.orders",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.orders"},
		},
		// Group 2: staging_db → postgresql://staging
		{
			DatasetURN:  "staging_db/raw.events",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://staging/raw.events"},
		},
		{
			DatasetURN:  "staging_db/raw.users",
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
	assert.Equal(t, "postgresql://demo/{name}", p1.Canonical)
	assert.Equal(t, 2, p1.ResolvesCount)

	// Verify group 2
	p2, ok := patternMap["staging_db/{name}"]
	require.True(t, ok, "Should have staging_db pattern")
	assert.Equal(t, "postgresql://staging/{name}", p2.Canonical)
	assert.Equal(t, 2, p2.ResolvesCount)
}

// TestSuggestPatterns_NoMatches tests orphans without likely matches.
func TestSuggestPatterns_NoMatches(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		{DatasetURN: "demo_postgres/marts.customers", LikelyMatch: nil},
		{DatasetURN: "demo_postgres/marts.orders", LikelyMatch: nil},
	}

	patterns := SuggestPatterns(orphans)

	assert.Empty(t, patterns, "Should return empty when no likely matches")
}

// TestSuggestPatterns_SingleOrphan tests single orphan.
func TestSuggestPatterns_SingleOrphan(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		{
			DatasetURN:  "demo_postgres/marts.customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1, "Should suggest pattern even for single orphan")
	assert.Equal(t, 1, patterns[0].ResolvesCount)
}

// TestSuggestPatterns_EmptyOrphans tests empty input.
func TestSuggestPatterns_EmptyOrphans(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	patterns := SuggestPatterns(nil)
	assert.Empty(t, patterns, "Should return empty for nil input")

	patterns = SuggestPatterns([]OrphanDataset{})
	assert.Empty(t, patterns, "Should return empty for empty slice")
}

// TestSuggestPatterns_MixedMatchAndNoMatch tests mix of matched and unmatched orphans.
func TestSuggestPatterns_MixedMatchAndNoMatch(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		{
			DatasetURN:  "demo_postgres/marts.customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
		{
			DatasetURN:  "demo_postgres/marts.orders",
			LikelyMatch: nil, // No match for this one
		},
		{
			DatasetURN:  "demo_postgres/marts.products",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.products"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1, "Should suggest 1 pattern")
	assert.Equal(t, 2, patterns[0].ResolvesCount, "Should resolve only matched orphans")
	assert.ElementsMatch(
		t, []string{"demo_postgres/marts.customers", "demo_postgres/marts.products"}, patterns[0].OrphansResolved,
	)
}

// TestSuggestPatterns_MultipleSchemasSameNamespace tests that datasets across
// different schemas but the same namespace are grouped into a single pattern.
func TestSuggestPatterns_MultipleSchemasSameNamespace(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		{
			DatasetURN:  "demo_postgres/marts.customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
		{
			DatasetURN:  "demo_postgres/staging.stg_customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/staging.stg_customers"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1, "Should suggest 1 pattern for both schemas")
	assert.Equal(t, "demo_postgres/{name}", patterns[0].Pattern)
	assert.Equal(t, "postgresql://demo/{name}", patterns[0].Canonical)
	assert.Equal(t, 2, patterns[0].ResolvesCount)
}

// TestSuggestPatterns_SortedByResolvesCount tests patterns are sorted by resolves count.
func TestSuggestPatterns_SortedByResolvesCount(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

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
	assert.Equal(t, 3, patterns[0].ResolvesCount, "First pattern should resolve 3")
	assert.Equal(t, 1, patterns[1].ResolvesCount, "Second pattern should resolve 1")
}

// TestSuggestPatterns_ProtocolInCanonical tests patterns where canonical has protocol.
func TestSuggestPatterns_ProtocolInCanonical(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		{
			DatasetURN:  "simple/schema.customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://db:5432/schema.customers"},
		},
		{
			DatasetURN:  "simple/schema.orders",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://db:5432/schema.orders"},
		},
	}

	patterns := SuggestPatterns(orphans)

	require.Len(t, patterns, 1)
	assert.Equal(t, "simple/{name}", patterns[0].Pattern)
	assert.Equal(t, "postgresql://db:5432/{name}", patterns[0].Canonical)
}

// TestSuggestPatterns_NameMismatchSkipped tests that orphan/match pairs with
// different dataset names are skipped (pattern substitution wouldn't work).
func TestSuggestPatterns_NameMismatchSkipped(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	orphans := []OrphanDataset{
		{
			DatasetURN:  "demo_postgres/customers",
			LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"},
		},
	}

	patterns := SuggestPatterns(orphans)

	assert.Empty(t, patterns, "Should not suggest pattern when names differ")
}
