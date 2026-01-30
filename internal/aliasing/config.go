// Package aliasing provides namespace alias resolution for cross-tool correlation.
//
// Different data tools (dbt, Airflow, Great Expectations) emit different namespace
// formats for the same data source, breaking cross-tool correlation. This package
// provides configuration loading and resolution to map tool-specific namespaces
// to canonical namespaces.
package aliasing

import (
	"errors"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/correlator-io/correlator/internal/config"
)

// Config holds namespace alias configuration loaded from .correlator.yaml.
type Config struct {
	// NamespaceAliases maps tool-specific namespaces to canonical namespaces.
	// Key is the alias (tool-specific), value is the canonical namespace.
	//nolint:tagliatelle // snake_case is intentional for YAML config files
	NamespaceAliases map[string]string `yaml:"namespace_aliases"`
}

// DefaultConfigPath is the default location for the correlator configuration file.
// Uses hidden file format following common tool conventions (.eslintrc, .prettierrc, etc.).
const DefaultConfigPath = ".correlator.yaml"

// ConfigPathEnvVar is the environment variable name for custom config path.
const ConfigPathEnvVar = "CORRELATOR_CONFIG_PATH"

// LoadConfig loads alias configuration from a YAML file at the given path.
//
// Behavior:
//   - Returns empty config (not error) if file doesn't exist - aliases are optional
//   - Returns empty config + logs warning if YAML is invalid (graceful degradation)
//   - Returns populated config on success
//
// This graceful degradation ensures the server can start even without aliases
// configured, as namespace aliasing is an optional feature.
func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		NamespaceAliases: make(map[string]string),
	}

	data, err := os.ReadFile(path) //nolint:gosec // path is from trusted config source
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Missing file is OK - aliases are optional
			slog.Debug("Config file not found, continuing without aliases",
				slog.String("path", path))

			return cfg, nil
		}

		// Other read errors (permissions, etc.) - log warning and continue
		slog.Warn("Failed to read config file, continuing without aliases",
			slog.String("path", path),
			slog.String("error", err.Error()))

		return cfg, nil
	}

	// Empty file is valid - just no aliases
	if len(data) == 0 {
		return cfg, nil
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		// Invalid YAML - log warning and continue with empty config
		slog.Warn("Failed to parse config file, continuing without aliases",
			slog.String("path", path),
			slog.String("error", err.Error()))

		return &Config{NamespaceAliases: make(map[string]string)}, nil
	}

	// Ensure map is initialized even if YAML had nil/empty section
	if cfg.NamespaceAliases == nil {
		cfg.NamespaceAliases = make(map[string]string)
	}

	return cfg, nil
}

// LoadConfigFromEnv loads config from the path specified in CORRELATOR_CONFIG_PATH
// environment variable. Falls back to ".correlator.yaml" in current directory if not set.
func LoadConfigFromEnv() (*Config, error) {
	path := config.GetEnvStr(ConfigPathEnvVar, DefaultConfigPath)

	return LoadConfig(path)
}
