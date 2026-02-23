package aliasing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==============================================================================
// Unit Tests: Resolver Construction
// ==============================================================================

func TestNewResolver_WithValidPatterns(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
			{Pattern: "old/{name}", Canonical: "new/{name}"},
		},
	}

	r := NewResolver(cfg)

	require.NotNil(t, r)
	assert.Equal(t, 2, r.GetPatternCount())
}

func TestNewResolver_WithNilConfig(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	r := NewResolver(nil)

	require.NotNil(t, r)
	assert.Equal(t, 0, r.GetPatternCount())
}

func TestNewResolver_WithEmptyPatterns(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{},
	}

	r := NewResolver(cfg)

	require.NotNil(t, r)
	assert.Equal(t, 0, r.GetPatternCount())
}

func TestNewResolver_SkipsEmptyPattern(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "", Canonical: "canonical/{name}"},
			{Pattern: "valid/{name}", Canonical: "output/{name}"},
		},
	}

	r := NewResolver(cfg)

	assert.Equal(t, 1, r.GetPatternCount())
}

func TestNewResolver_SkipsEmptyCanonical(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "input/{name}", Canonical: ""},
			{Pattern: "valid/{name}", Canonical: "output/{name}"},
		},
	}

	r := NewResolver(cfg)

	assert.Equal(t, 1, r.GetPatternCount())
}

func TestNewResolver_AllPatternsValid(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// Note: Most "special character" patterns become valid after QuoteMeta escaping
	// This test verifies patterns with regex-like characters work correctly
	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "prefix[test]/{name}", Canonical: "output/{name}"},
			{Pattern: "valid/{name}", Canonical: "output/{name}"},
		},
	}

	r := NewResolver(cfg)

	// Both patterns should be valid (special chars are escaped)
	assert.Equal(t, 2, r.GetPatternCount())
}

// ==============================================================================
// Unit Tests: Pattern Resolution
// ==============================================================================

func TestResolver_Resolve_SingleVariable(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("demo_postgres/customers")

	assert.Equal(t, "postgresql://demo/marts.customers", result)
}

func TestResolver_Resolve_MultipleVariables(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "{namespace}/{schema}/{table}", Canonical: "postgresql://prod/{schema}.{table}"},
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("mydb/public/users")

	assert.Equal(t, "postgresql://prod/public.users", result)
}

func TestResolver_Resolve_PathCapture(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "s3://old-bucket/{path*}", Canonical: "s3://new-bucket/{path*}"},
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("s3://old-bucket/data/warehouse/orders.parquet")

	assert.Equal(t, "s3://new-bucket/data/warehouse/orders.parquet", result)
}

func TestResolver_Resolve_NoMatch(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
		},
	}
	r := NewResolver(cfg)

	// Input doesn't match pattern - should return original
	result := r.Resolve("other_namespace/customers")

	assert.Equal(t, "other_namespace/customers", result)
}

func TestResolver_Resolve_FirstMatchWins(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "first/{name}"},
			{Pattern: "demo_postgres/{name}", Canonical: "second/{name}"},
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("demo_postgres/customers")

	// First pattern should match
	assert.Equal(t, "first/customers", result)
}

func TestResolver_Resolve_EmptyPatterns(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	r := NewResolver(nil)

	// No patterns - should return original
	result := r.Resolve("any/input")

	assert.Equal(t, "any/input", result)
}

func TestResolver_Resolve_EmptyInput(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("")

	assert.Empty(t, result)
}

func TestResolver_Resolve_NilResolver(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	var r *Resolver

	result := r.Resolve("any/input")

	assert.Equal(t, "any/input", result)
}

// ==============================================================================
// Unit Tests: Match Detection
// ==============================================================================

func TestResolver_Match_Found(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
		},
	}
	r := NewResolver(cfg)

	canonical, matched := r.Match("demo_postgres/customers")

	assert.True(t, matched)
	assert.Equal(t, "postgresql://demo/marts.customers", canonical)
}

func TestResolver_Match_NotFound(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
		},
	}
	r := NewResolver(cfg)

	canonical, matched := r.Match("other/customers")

	assert.False(t, matched)
	assert.Empty(t, canonical)
}

// ==============================================================================
// Unit Tests: Real-World Scenarios (TC-002 use case)
// ==============================================================================

func TestResolver_Resolve_DBTvsGE(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	// This is the exact use case from TC-002
	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
		},
	}
	r := NewResolver(cfg)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "GE customers → dbt format",
			input:    "demo_postgres/customers",
			expected: "postgresql://demo/marts.customers",
		},
		{
			name:     "GE orders → dbt format",
			input:    "demo_postgres/orders",
			expected: "postgresql://demo/marts.orders",
		},
		{
			name:     "dbt format unchanged (no match)",
			input:    "postgresql://demo/marts.customers",
			expected: "postgresql://demo/marts.customers",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := r.Resolve(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestResolver_Resolve_SpecialCharacters(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "postgres://host:5432/{name}", Canonical: "postgresql://host/{name}"},
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("postgres://host:5432/mydb.public.orders")

	assert.Equal(t, "postgresql://host/mydb.public.orders", result)
}

func TestResolver_Resolve_Underscores(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "raw_{env}/{name}", Canonical: "processed_{env}/{name}"},
		},
	}
	r := NewResolver(cfg)

	result := r.Resolve("raw_prod/customer_orders")

	assert.Equal(t, "processed_prod/customer_orders", result)
}

// ==============================================================================
// Unit Tests: Pattern Count
// ==============================================================================

func TestResolver_GetPatternCount(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	tests := []struct {
		name     string
		patterns []DatasetPattern
		expected int
	}{
		{
			name:     "empty",
			patterns: []DatasetPattern{},
			expected: 0,
		},
		{
			name: "one",
			patterns: []DatasetPattern{
				{Pattern: "a/{name}", Canonical: "b/{name}"},
			},
			expected: 1,
		},
		{
			name: "multiple",
			patterns: []DatasetPattern{
				{Pattern: "a/{name}", Canonical: "b/{name}"},
				{Pattern: "c/{name}", Canonical: "d/{name}"},
				{Pattern: "e/{name}", Canonical: "f/{name}"},
			},
			expected: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewResolver(&Config{DatasetPatterns: tc.patterns})
			assert.Equal(t, tc.expected, r.GetPatternCount())
		})
	}
}

func TestResolver_GetPatternCount_NilResolver(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	var r *Resolver
	assert.Equal(t, 0, r.GetPatternCount())
}

// ==============================================================================
// Concurrency Tests
// ==============================================================================

func TestResolver_ConcurrentAccess(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping unit test in non-short mode")
	}

	cfg := &Config{
		DatasetPatterns: []DatasetPattern{
			{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}"},
			{Pattern: "old_ns/{schema}/{table}", Canonical: "new_ns/{schema}.{table}"},
		},
	}
	r := NewResolver(cfg)

	// Test URNs
	testURNs := []string{
		"demo_postgres/customers",
		"demo_postgres/orders",
		"old_ns/public/users",
		"unmatched/dataset",
	}

	// Run concurrent Resolve calls
	const goroutines = 100

	const iterations = 100

	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				for _, urn := range testURNs {
					_ = r.Resolve(urn)
					_, _ = r.Match(urn)
					_ = r.GetPatternCount()
				}
			}

			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Verify resolver still works correctly after concurrent access
	assert.Equal(t, "postgresql://demo/marts.customers", r.Resolve("demo_postgres/customers"))
	assert.Equal(t, "new_ns/public.users", r.Resolve("old_ns/public/users"))
	assert.Equal(t, 2, r.GetPatternCount())
}
