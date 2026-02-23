package aliasing

import (
	"log/slog"
	"regexp"
	"strings"
)

type (
	// compiledPattern holds a pre-compiled regex pattern and its canonical template.
	compiledPattern struct {
		regex     *regexp.Regexp
		canonical string
		variables []string
	}

	// Resolver resolves dataset URNs using pattern-based aliasing.
	// Thread-safe for concurrent use (immutable after construction).
	//
	// The resolver transforms tool-specific dataset URNs to canonical URNs,
	// enabling cross-tool correlation when different data tools use different
	// URN formats for the same dataset.
	//
	// Pattern syntax:
	//   - {variable} captures any characters except "/"
	//   - {variable*} captures any characters including "/" (for paths)
	//   - Literal characters match exactly
	//   - First matching pattern wins (order matters)
	Resolver struct {
		patterns []compiledPattern
	}
)

// variableRegex matches {name} or {name*} patterns in the pattern string.
var variableRegex = regexp.MustCompile(`\{([a-zA-Z_][a-zA-Z0-9_]*)\*?\}`)

// compilePattern converts a pattern string to a compiled regex.
//
// Pattern: "demo_postgres/{name}" → Regex: ^demo_postgres/(?P<name>[^/]+)$.
// Pattern: "s3://bucket/{path*}" → Regex: ^s3://bucket/(?P<path>.+)$.
func compilePattern(pattern string) (*regexp.Regexp, []string, error) {
	variables := make([]string, 0, 4) //nolint:mnd // preallocate for typical pattern

	// Escape regex special characters in literal parts
	escaped := regexp.QuoteMeta(pattern)

	// Replace escaped variable placeholders with capture groups
	// QuoteMeta escapes { and }, so we look for \{...\}
	result := escaped

	// Find all variables in original pattern
	matches := variableRegex.FindAllStringSubmatch(pattern, -1)
	for _, match := range matches {
		fullMatch := match[0] // e.g., "{name}" or "{path*}"
		varName := match[1]   // e.g., "name" or "path"
		isGreedy := strings.HasSuffix(fullMatch, "*}")

		variables = append(variables, varName)

		// Build the capture group
		var captureGroup string
		if isGreedy {
			// {var*} captures anything including slashes
			captureGroup = "(?P<" + varName + ">.+)"
		} else {
			// {var} captures anything except slashes
			captureGroup = "(?P<" + varName + ">[^/]+)"
		}

		// Replace the escaped version in the result
		escapedVar := regexp.QuoteMeta(fullMatch)
		result = strings.Replace(result, escapedVar, captureGroup, 1)
	}

	// Anchor the regex to match the entire string
	result = "^" + result + "$"

	regex, err := regexp.Compile(result)
	if err != nil {
		return nil, nil, err
	}

	return regex, variables, nil
}

// substituteVariables replaces {var} placeholders in canonical with captured values.
func substituteVariables(canonical string, captures map[string]string) string {
	result := canonical

	for varName, value := range captures {
		// Replace both {var} and {var*} forms
		result = strings.ReplaceAll(result, "{"+varName+"}", value)
		result = strings.ReplaceAll(result, "{"+varName+"*}", value)
	}

	return result
}

// NewResolver creates a resolver from config with validation.
//
// Validates:
//   - Patterns with empty pattern or canonical are skipped with warning
//   - Patterns with invalid regex are skipped with warning
//
// Returns a resolver containing only valid patterns.
// If config is nil or has no patterns, returns a no-op resolver (passthrough).
func NewResolver(cfg *Config) *Resolver {
	if cfg == nil || len(cfg.DatasetPatterns) == 0 {
		return &Resolver{
			patterns: []compiledPattern{},
		}
	}

	validPatterns := make([]compiledPattern, 0, len(cfg.DatasetPatterns))

	for _, dp := range cfg.DatasetPatterns {
		pattern := strings.TrimSpace(dp.Pattern)
		canonical := strings.TrimSpace(dp.Canonical)

		// Skip empty patterns
		if pattern == "" {
			slog.Warn("Skipping pattern with empty pattern string")

			continue
		}

		// Skip empty canonical
		if canonical == "" {
			slog.Warn("Skipping pattern with empty canonical",
				slog.String("pattern", pattern))

			continue
		}

		// Compile the pattern
		regex, variables, err := compilePattern(pattern)
		if err != nil {
			slog.Warn("Skipping pattern with invalid regex",
				slog.String("pattern", pattern),
				slog.String("error", err.Error()))

			continue
		}

		validPatterns = append(validPatterns, compiledPattern{
			regex:     regex,
			canonical: canonical,
			variables: variables,
		})

		slog.Debug("Compiled dataset pattern",
			slog.String("pattern", pattern),
			slog.String("canonical", canonical),
			slog.Int("variables", len(variables)))
	}

	return &Resolver{
		patterns: validPatterns,
	}
}

// GetPatternCount returns the number of compiled patterns.
func (r *Resolver) GetPatternCount() int {
	if r == nil {
		return 0
	}

	return len(r.patterns)
}

// Resolve applies patterns to transform a dataset URN to its canonical form.
// Returns the canonical URN if a pattern matches, otherwise returns the original.
//
// Patterns are evaluated in order; first match wins.
func (r *Resolver) Resolve(datasetURN string) string {
	if r == nil || len(r.patterns) == 0 || datasetURN == "" {
		return datasetURN
	}

	for _, cp := range r.patterns {
		match := cp.regex.FindStringSubmatch(datasetURN)
		if match == nil {
			continue
		}

		// Extract captured values
		captures := make(map[string]string)

		for i, name := range cp.regex.SubexpNames() {
			if i > 0 && name != "" && i < len(match) {
				captures[name] = match[i]
			}
		}

		// Substitute variables in canonical template
		return substituteVariables(cp.canonical, captures)
	}

	// No pattern matched - return original
	return datasetURN
}

// Match checks if a URN matches any pattern and returns match details.
// Returns (canonical, true) if matched, ("", false) if no match.
func (r *Resolver) Match(datasetURN string) (string, bool) {
	if r == nil || len(r.patterns) == 0 || datasetURN == "" {
		return "", false
	}

	for _, cp := range r.patterns {
		match := cp.regex.FindStringSubmatch(datasetURN)
		if match == nil {
			continue
		}

		// Extract captured values
		captures := make(map[string]string)

		for i, name := range cp.regex.SubexpNames() {
			if i > 0 && name != "" && i < len(match) {
				captures[name] = match[i]
			}
		}

		// Substitute variables in canonical template
		return substituteVariables(cp.canonical, captures), true
	}

	return "", false
}
