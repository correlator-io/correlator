package aliasing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig_ValidYAML(t *testing.T) {
	// Create temp file with valid YAML
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
namespace_aliases:
  postgres_prod: "postgresql://prod-db:5432/mydb"
  postgres://prod-db:5432: "postgresql://prod-db:5432/mydb"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.NamespaceAliases, 2)
	assert.Equal(t, "postgresql://prod-db:5432/mydb", cfg.NamespaceAliases["postgres_prod"])
	assert.Equal(t, "postgresql://prod-db:5432/mydb", cfg.NamespaceAliases["postgres://prod-db:5432"])
}

func TestLoadConfig_EmptyAliasesSection(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
namespace_aliases:
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.NamespaceAliases)
}

func TestLoadConfig_MissingFile(t *testing.T) {
	cfg, err := LoadConfig("/nonexistent/path/correlator.yaml")

	// Missing file should return empty config, no error (graceful degradation)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.NamespaceAliases)
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	// Invalid YAML - tabs used for indentation incorrectly
	content := `
namespace_aliases:
  key: [invalid yaml
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	// Invalid YAML should return empty config with no error (graceful degradation)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.NamespaceAliases)
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
	assert.Empty(t, cfg.NamespaceAliases)
}

//nolint:gosmopolitan // testing unicode support intentionally
func TestLoadConfig_UnicodeInNamespaces(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	content := `
namespace_aliases:
  生产数据库: "postgresql://prod-db:5432/mydb"
  données_prod: "postgresql://prod-db:5432/mydb"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.NamespaceAliases, 2)
	assert.Equal(t, "postgresql://prod-db:5432/mydb", cfg.NamespaceAliases["生产数据库"])
	assert.Equal(t, "postgresql://prod-db:5432/mydb", cfg.NamespaceAliases["données_prod"])
}

func TestLoadConfig_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	err := os.WriteFile(configPath, []byte(""), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.NamespaceAliases)
}

func TestLoadConfig_NoAliasesKey(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	// Valid YAML but no namespace_aliases key
	content := `
some_other_config:
  key: value
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Empty(t, cfg.NamespaceAliases)
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
namespace_aliases:
  test_alias: "canonical_value"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	// Set env var to custom path
	t.Setenv("CORRELATOR_CONFIG_PATH", configPath)

	cfg, err := LoadConfigFromEnv()

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.NamespaceAliases, 1)
	assert.Equal(t, "canonical_value", cfg.NamespaceAliases["test_alias"])
}

func TestLoadConfig_SpecialCharactersInNamespaces(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "correlator.yaml")

	// Test namespaces with special characters (common in connection strings)
	content := `
namespace_aliases:
  "postgres://user:pass@host:5432/db": "postgresql://prod-db:5432/mydb" # pragma: allowlist secret
  "snowflake://account.snowflakecomputing.com/warehouse/prod": "canonical_snowflake"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	require.NoError(t, err)

	cfg, err := LoadConfig(configPath)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Len(t, cfg.NamespaceAliases, 2)
	assert.Equal(t, "postgresql://prod-db:5432/mydb",
		cfg.NamespaceAliases["postgres://user:pass@host:5432/db"]) // pragma: allowlist secret
	assert.Equal(t, "canonical_snowflake",
		cfg.NamespaceAliases["snowflake://account.snowflakecomputing.com/warehouse/prod"])
}
