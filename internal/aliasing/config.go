// Package aliasing provides dataset pattern aliasing for cross-tool correlation.
//
// Different data tools (dbt, Airflow, Great Expectations) emit different URN
// formats for the same dataset, breaking cross-tool correlation. This package
// provides configuration loading and pattern-based resolution to map tool-specific
// dataset URNs to canonical URNs.
//
// Example configuration (.correlator.yaml):
//
//	dataset_patterns:
//	  - pattern: "demo_postgres/{name}"
//	    canonical: "postgresql://demo/marts.{name}"
//
// This transforms "demo_postgres/customers" → "postgresql://demo/marts.customers"
package aliasing

import (
	"errors"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/correlator-io/correlator/internal/config"
)

type (
	// DatasetPattern defines a pattern-based transformation rule for dataset URNs.
	//
	// Patterns are evaluated in order; first match wins.
	// Pattern syntax:
	//   - {variable} captures any characters except "/"
	//   - {variable*} captures any characters including "/" (for paths)
	//   - Literal characters match exactly
	//
	// Examples:
	//
	//	Pattern: "demo_postgres/{name}"
	//	Canonical: "postgresql://demo/marts.{name}"
	//	Input: "demo_postgres/customers" → Output: "postgresql://demo/marts.customers"
	DatasetPattern struct {
		Pattern   string `yaml:"pattern"`
		Canonical string `yaml:"canonical"`
	}

	// Config holds dataset pattern configuration loaded from .correlator.yaml.
	Config struct {
		//nolint:tagliatelle // snake_case is intentional for YAML config files
		DatasetPatterns []DatasetPattern `yaml:"dataset_patterns"`
	}
)

const (
	// DefaultConfigPath is the default location for the correlator configuration file.
	// Uses hidden file format following common tool conventions (.eslintrc, .prettierrc, etc.).
	DefaultConfigPath = ".correlator.yaml"

	// ConfigPathEnvVar is the environment variable name for custom config path.
	ConfigPathEnvVar = "CORRELATOR_CONFIG_PATH"
)

// LoadConfig loads pattern configuration from a YAML file at the given path.
//
// Behavior:
//   - Returns empty config (not error) if file doesn't exist - patterns are optional
//   - Returns empty config + logs warning if YAML is invalid (graceful degradation)
//   - Returns populated config on success
//
// This graceful degradation ensures the server can start even without patterns
// configured, as dataset pattern aliasing is an optional feature.
func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		DatasetPatterns: []DatasetPattern{},
	}

	data, err := os.ReadFile(path) //nolint:gosec // path is from trusted config source
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Missing file is OK - patterns are optional
			slog.Debug("Config file not found, continuing without patterns",
				slog.String("path", path))

			return cfg, nil
		}

		// Other read errors (permissions, etc.) - log warning and continue
		slog.Warn("Failed to read config file, continuing without patterns",
			slog.String("path", path),
			slog.String("error", err.Error()))

		return cfg, nil
	}

	// Empty file is valid - just no patterns
	if len(data) == 0 {
		return cfg, nil
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		// Invalid YAML - log warning and continue with empty config
		slog.Warn("Failed to parse config file, continuing without patterns",
			slog.String("path", path),
			slog.String("error", err.Error()))

		return &Config{DatasetPatterns: []DatasetPattern{}}, nil
	}

	// Ensure slice is initialized even if YAML had nil/empty section
	if cfg.DatasetPatterns == nil {
		cfg.DatasetPatterns = []DatasetPattern{}
	}

	return cfg, nil
}

// LoadConfigFromEnv loads config from the path specified in CORRELATOR_CONFIG_PATH
// environment variable. Falls back to ".correlator.yaml" in current directory if not set.
func LoadConfigFromEnv() (*Config, error) {
	path := config.GetEnvStr(ConfigPathEnvVar, DefaultConfigPath)

	return LoadConfig(path)
}
