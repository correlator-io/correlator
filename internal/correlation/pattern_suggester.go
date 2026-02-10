// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import (
	"sort"
	"strings"

	"github.com/correlator-io/correlator/internal/canonicalization"
)

type (
	// SuggestedPattern represents a pattern suggestion derived from orphan→match pairs.
	// These patterns can be added to .correlator.yaml to resolve Entity Resolution issues.
	SuggestedPattern struct {
		// Pattern is the source pattern to match orphan dataset URNs.
		// Example: "demo_postgres/{name}"
		Pattern string

		// Canonical is the target pattern to transform orphan URNs into canonical form.
		// Example: "postgresql://demo/marts.{name}"
		Canonical string

		// ResolvesCount is the number of orphan datasets this pattern would resolve.
		ResolvesCount int

		// OrphansResolved is the list of orphan dataset URNs this pattern would resolve.
		OrphansResolved []string
	}

	// patternGroup holds orphans with the same transformation pattern.
	patternGroup struct {
		orphanPrefix    string
		canonicalPrefix string
		orphans         []string
	}
)

// SuggestPatterns analyzes orphan datasets and suggests patterns to resolve them.
//
// Algorithm:
//  1. Filter orphans that have LikelyMatch (others can't be resolved)
//  2. Extract prefix and table name from each orphan/match pair
//  3. Group orphans by their transformation pattern (orphan_prefix → match_prefix)
//  4. Generate pattern strings with {name} placeholder
//  5. Sort by ResolvesCount descending (most impactful first)
//
// Example:
//
//	orphans := []OrphanDataset{
//	    {DatasetURN: "demo_postgres/customers", LikelyMatch: &DatasetMatch{...}},
//	    {DatasetURN: "demo_postgres/orders", LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.orders"}},
//	}
//	patterns := SuggestPatterns(orphans)
//	// → [{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/marts.{name}", ResolvesCount: 2}]
func SuggestPatterns(orphans []OrphanDataset) []SuggestedPattern {
	if len(orphans) == 0 {
		return nil
	}

	// Group orphans by transformation pattern
	// Key: "orphanPrefix|canonicalPrefix" (e.g., "demo_postgres|postgresql://demo/marts")
	groups := make(map[string]*patternGroup)

	for _, orphan := range orphans {
		if orphan.LikelyMatch == nil {
			continue
		}

		orphanPrefix, orphanTable := extractPrefixAndTable(orphan.DatasetURN)
		canonicalPrefix, canonicalTable := extractPrefixAndTable(orphan.LikelyMatch.DatasetURN)

		// Only suggest patterns where the table names match
		if orphanTable == "" || canonicalTable == "" || orphanTable != canonicalTable {
			continue
		}

		key := orphanPrefix + "|" + canonicalPrefix

		if groups[key] == nil {
			groups[key] = &patternGroup{
				orphanPrefix:    orphanPrefix,
				canonicalPrefix: canonicalPrefix,
				orphans:         make([]string, 0),
			}
		}

		groups[key].orphans = append(groups[key].orphans, orphan.DatasetURN)
	}

	// Convert groups to suggested patterns
	patterns := make([]SuggestedPattern, 0, len(groups))

	for _, group := range groups {
		patterns = append(patterns, SuggestedPattern{
			Pattern:         group.orphanPrefix + "/{name}",
			Canonical:       group.canonicalPrefix + ".{name}",
			ResolvesCount:   len(group.orphans),
			OrphansResolved: group.orphans,
		})
	}

	// Sort by ResolvesCount descending
	sort.Slice(patterns, func(i, j int) bool {
		return patterns[i].ResolvesCount > patterns[j].ResolvesCount
	})

	return patterns
}

// extractPrefixAndTable extracts the prefix (everything before table) and table name from a URN.
//
// Examples:
//   - "demo_postgres/customers" → ("demo_postgres", "customers")
//   - "postgresql://demo/marts.customers" → ("postgresql://demo/marts", "customers")
//   - "mydb/schema/table" → ("mydb/schema", "table")
func extractPrefixAndTable(urn string) (string, string) {
	table := canonicalization.ExtractTableName(urn)
	if table == "" {
		return urn, ""
	}

	// Find the table name in the URN and extract prefix
	// The table could be separated by "/" or "."
	idx := strings.LastIndex(urn, table)
	if idx <= 0 {
		return urn, table
	}

	// Get prefix (everything before table name, excluding the separator)
	prefix := urn[:idx-1]

	return prefix, table
}
