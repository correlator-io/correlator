package aliasing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_ValidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
dataset_patterns:
  - pattern: "demo_postgres/{name}"
    canonical: "postgresql://demo/marts.{name}"
  - pattern: "old_namespace/{name}"
    canonical: "new_namespace/{name}"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.DatasetPatterns, 2)
	assert.Equal(t, "demo_postgres/{name}", cfg.DatasetPatterns[0].Pattern)
	assert.Equal(t, "postgresql://demo/marts.{name}", cfg.DatasetPatterns[0].Canonical)
	assert.Equal(t, "old_namespace/{name}", cfg.DatasetPatterns[1].Pattern)
	assert.Equal(t, "new_namespace/{name}", cfg.DatasetPatterns[1].Canonical)
}

func TestLoadConfig_EmptyPatternsSection(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
dataset_patterns:
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.DatasetPatterns)
}

func TestLoadConfig_MissingFile(t *testing.T) {
	cfg, err := LoadConfig("/nonexistent/path/correlator.yaml")

	// Missing file should return empty config, no error (graceful degradation)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.DatasetPatterns)
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	// Invalid YAML
	content := `
dataset_patterns:
  - pattern: [invalid yaml
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	// Invalid YAML should return empty config with no error (graceful degradation)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.DatasetPatterns)
}

func TestLoadConfig_YAMLWithOnlyComments(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
# This is a comment
# Another comment
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.DatasetPatterns)
}

func TestLoadConfig_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	err := os.WriteFile(configPath, []byte(""), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.DatasetPatterns)
}

func TestLoadConfig_NoPatternsKey(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	// Valid YAML but no dataset_patterns key
	content := `
some_other_config:
  key: value
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.DatasetPatterns)
}

func TestLoadConfigFromEnv_DefaultPath(t *testing.T) {
	// Unset env var to use default
	os.Unsetenv("CORRELATOR_CONFIG_PATH")

	// This will try to load from ./correlator.yaml which likely doesn't exist
	cfg, err := LoadConfigFromEnv()

	// Should gracefully return empty config
	require.NoError(t, err)
	require.NotNil(t, cfg)
}

func TestLoadConfigFromEnv_CustomPath(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "custom-config.yaml")

	content := `
dataset_patterns:
  - pattern: "test/{name}"
    canonical: "canonical/{name}"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	// Set env var to custom path
	t.Setenv("CORRELATOR_CONFIG_PATH", configPath)

	cfg, err := LoadConfigFromEnv()

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.DatasetPatterns, 1)
	assert.Equal(t, "test/{name}", cfg.DatasetPatterns[0].Pattern)
	assert.Equal(t, "canonical/{name}", cfg.DatasetPatterns[0].Canonical)
}

func TestLoadConfig_MultipleVariables(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
dataset_patterns:
  - pattern: "{namespace}/{schema}/{table}"
    canonical: "postgresql://prod/{schema}.{table}"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.DatasetPatterns, 1)
	assert.Equal(t, "{namespace}/{schema}/{table}", cfg.DatasetPatterns[0].Pattern)
}

func TestLoadConfig_PathCapture(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
dataset_patterns:
  - pattern: "s3://old-bucket/{path*}"
    canonical: "s3://new-bucket/{path*}"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.DatasetPatterns, 1)
	assert.Equal(t, "s3://old-bucket/{path*}", cfg.DatasetPatterns[0].Pattern)
	assert.Equal(t, "s3://new-bucket/{path*}", cfg.DatasetPatterns[0].Canonical)
}

func TestLoadConfig_SpecialCharactersInPatterns(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	// Test patterns with special characters (common in URNs)
	content := `
dataset_patterns:
  - pattern: "postgres://host:5432/{name}"
    canonical: "postgresql://host/{name}"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.DatasetPatterns, 1)
	assert.Equal(t, "postgres://host:5432/{name}", cfg.DatasetPatterns[0].Pattern)
}
