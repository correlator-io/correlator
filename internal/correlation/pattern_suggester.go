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
		Canonical string

		// ResolvesCount is the number of orphan datasets this pattern would resolve.
		ResolvesCount int

		// OrphansResolved is the list of orphan dataset URNs this pattern would resolve.
		OrphansResolved []string
	}

	// patternGroup collects orphans that share the same pattern transformation.
	patternGroup struct {
		pattern   string
		canonical string
		orphans   []string
	}
)

// SuggestPatterns analyzes orphan datasets and suggests patterns to resolve them.
//
// The algorithm handles two categories of orphan→match relationships:
//
// Category 1 — Namespace transformation (different namespaces, same name):
//
//	Orphan:     "demo_postgres/marts.customers"
//	Canonical:  "postgresql://demo/marts.customers"
//	Pattern:    "demo_postgres/{name}" → "postgresql://demo/{name}"
//
// Category 2 — Name transformation (same namespace, different name depth):
//
//	Orphan:     "postgresql://demo-postgres/marts.customers"
//	Canonical:  "postgresql://demo-postgres/demo.marts.customers"
//	Pattern:    "postgresql://demo-postgres/marts.{table}" → "postgresql://demo-postgres/demo.marts.{table}"
//
// Name-transformation patterns use an anchored {table} variable instead of {name} to avoid
// the double-prefix problem (see implementation plan for details). When the orphan name has
// only one dot-separated segment, an exact URN-to-URN mapping is generated as a fallback.
//
// All patterns are sorted by ResolvesCount descending (most impactful first).
func SuggestPatterns(orphans []OrphanDataset) []SuggestedPattern {
	if len(orphans) == 0 {
		return nil
	}

	// Groups keyed with type-discriminator prefix to prevent collisions:
	//   "ns:orphanNS|canonicalNS"              — namespace transformation
	//   "name:namespace|orphanAnchor|canonicalAnchor" — name transformation
	//   "exact:orphanURN"                       — exact mapping fallback
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

		switch {
		case orphanNamespace != canonicalNamespace && orphanName == canonicalName:
			// Category 1: namespace transformation
			key := "ns:" + orphanNamespace + "|" + canonicalNamespace
			if groups[key] == nil {
				groups[key] = &patternGroup{
					pattern:   orphanNamespace + "/{name}",
					canonical: canonicalNamespace + "/{name}",
					orphans:   make([]string, 0),
				}
			}

			groups[key].orphans = append(groups[key].orphans, orphan.DatasetURN)

		case orphanNamespace == canonicalNamespace && orphanName != canonicalName:
			// Category 2: name transformation (same namespace, different name depth)
			addNameTransformationGroup(groups, orphanNamespace, orphanName, canonicalName, orphan.DatasetURN)
		}
	}

	// Convert groups to suggested patterns
	patterns := make([]SuggestedPattern, 0, len(groups))

	for _, group := range groups {
		patterns = append(patterns, SuggestedPattern{
			Pattern:         group.pattern,
			Canonical:       group.canonical,
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

// addNameTransformationGroup handles Category 2 (same namespace, different name depth).
// It detects dot-boundary suffix relationships and generates anchored patterns or
// exact mappings depending on orphan segment count.
func addNameTransformationGroup(
	groups map[string]*patternGroup,
	namespace, orphanName, canonicalName, orphanURN string,
) {
	orphanSegments := strings.Split(orphanName, ".")
	canonicalSegments := strings.Split(canonicalName, ".")

	// Determine which name is shorter and which is longer
	shorter, longer := orphanSegments, canonicalSegments
	if len(orphanSegments) > len(canonicalSegments) {
		shorter, longer = canonicalSegments, orphanSegments
	}

	// Names must differ in length for a suffix relationship
	if len(shorter) >= len(longer) {
		return
	}

	// Verify dot-boundary suffix: shorter segments must match the tail of longer segments
	tail := longer[len(longer)-len(shorter):]
	if !segmentsEqual(shorter, tail) {
		return
	}

	if len(orphanSegments) < 2 { //nolint:mnd
		// Single-segment orphan name: no anchor possible, use exact mapping
		key := "exact:" + orphanURN
		groups[key] = &patternGroup{
			pattern:   namespace + "/" + orphanName,
			canonical: namespace + "/" + canonicalName,
			orphans:   []string{orphanURN},
		}

		return
	}

	// Multi-segment orphan name: generate anchored pattern using {table}
	// Anchor = all segments except the last (provides specificity to avoid double-prefix)
	orphanAnchor := strings.Join(orphanSegments[:len(orphanSegments)-1], ".")
	canonicalAnchor := strings.Join(canonicalSegments[:len(canonicalSegments)-1], ".")

	key := "name:" + namespace + "|" + orphanAnchor + "|" + canonicalAnchor
	if groups[key] == nil {
		groups[key] = &patternGroup{
			pattern:   namespace + "/" + orphanAnchor + ".{table}",
			canonical: namespace + "/" + canonicalAnchor + ".{table}",
			orphans:   make([]string, 0),
		}
	}

	groups[key].orphans = append(groups[key].orphans, orphanURN)
}

// segmentsEqual returns true if two string slices are element-wise equal.
func segmentsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
