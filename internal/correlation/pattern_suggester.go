// Package correlation provides correlation engine functionality for linking incidents to job runs.
package correlation

import (
	"sort"

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
		Canonical string

		// ResolvesCount is the number of orphan datasets this pattern would resolve.
		ResolvesCount int

		// OrphansResolved is the list of orphan dataset URNs this pattern would resolve.
		OrphansResolved []string
	}

	// patternGroup holds orphans with the same namespace transformation.
	patternGroup struct {
		orphanNamespace    string
		canonicalNamespace string
		orphans            []string
	}
)

// SuggestPatterns analyzes orphan datasets and suggests patterns to resolve them.
//
// Algorithm:
//  1. Filter orphans that have LikelyMatch (others can't be resolved)
//  2. Parse each orphan/match URN at the namespace/name boundary (aligned with GenerateDatasetURN)
//  3. Group orphans by namespace transformation (orphan_namespace → canonical_namespace)
//  4. Generate pattern strings with {name} placeholder
//  5. Sort by ResolvesCount descending (most impactful first)
//
// Example:
//
//	orphans := []OrphanDataset{
//	    {
//	   		DatasetURN: "demo_postgres/marts.customers",
//	   		LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.customers"}
//	   	},
//	    {
//	   		DatasetURN: "demo_postgres/marts.orders",
//	   		LikelyMatch: &DatasetMatch{DatasetURN: "postgresql://demo/marts.orders"}
//	   	},
//	}
//	patterns := SuggestPatterns(orphans)
//	// → [{Pattern: "demo_postgres/{name}", Canonical: "postgresql://demo/{name}", ResolvesCount: 2}]
func SuggestPatterns(orphans []OrphanDataset) []SuggestedPattern {
	if len(orphans) == 0 {
		return nil
	}

	// Group orphans by namespace transformation
	// Key: "orphanNamespace|canonicalNamespace" (e.g., "demo_postgres|postgresql://demo")
	groups := make(map[string]*patternGroup)

	for _, orphan := range orphans {
		if orphan.LikelyMatch == nil {
			continue
		}

		orphanNamespace, orphanName, err := canonicalization.ParseDatasetURN(orphan.DatasetURN)
		if err != nil {
			continue
		}

		canonicalNamespace, canonicalName, err := canonicalization.ParseDatasetURN(orphan.LikelyMatch.DatasetURN)
		if err != nil {
			continue
		}

		// Only suggest patterns where the dataset names match exactly.
		// This ensures the {name} placeholder substitution produces correct results.
		if orphanName != canonicalName {
			continue
		}

		key := orphanNamespace + "|" + canonicalNamespace

		if groups[key] == nil {
			groups[key] = &patternGroup{
				orphanNamespace:    orphanNamespace,
				canonicalNamespace: canonicalNamespace,
				orphans:            make([]string, 0),
			}
		}

		groups[key].orphans = append(groups[key].orphans, orphan.DatasetURN)
	}

	// Convert groups to suggested patterns
	patterns := make([]SuggestedPattern, 0, len(groups))

	for _, group := range groups {
		patterns = append(patterns, SuggestedPattern{
			Pattern:         group.orphanNamespace + "/{name}",
			Canonical:       group.canonicalNamespace + "/{name}",
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
