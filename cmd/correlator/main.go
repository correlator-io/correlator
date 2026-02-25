// Package main provides the Correlator incident correlation service.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/correlator-io/correlator/internal/aliasing"
	"github.com/correlator-io/correlator/internal/api"
	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/storage"
)

// Build-time information variables (set via -ldflags during compilation).
//
//nolint:gochecknoglobals // Required for build-time version injection via -ldflags -X
var (
	version   = "0.0.1-dev" // Version of correlator (set at build time)
	gitCommit = "unknown"   // Git commit hash (set at build time)
	buildTime = "unknown"   // Build timestamp (set at build time)
)

const name = "correlator"

// Version returns the build version.
func Version() string { return version }

// GitCommit returns the git commit hash.
func GitCommit() string { return gitCommit }

// BuildTime returns the build timestamp.
func BuildTime() string { return buildTime }

//nolint:funlen // main function sets up dependencies sequentially, extracting would reduce clarity
func main() {
	versionFlag := flag.Bool("version", false, "show version information")
	flag.Parse()

	//nolint:forbidigo
	if *versionFlag {
		fmt.Printf("%s v%s\n", name, Version())
		fmt.Printf("Git Commit: %s\n", GitCommit())
		fmt.Printf("Build Time: %s\n", BuildTime())
		os.Exit(0)
	}

	serverConfig := api.LoadServerConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: serverConfig.LogLevel,
	}))

	logger.Info("Starting Correlator service",
		slog.String("service", name),
		slog.String("version", Version()),
		slog.String("git_commit", GitCommit()),
		slog.String("build_time", BuildTime()),
	)

	logger.Info("Loaded server configuration",
		slog.String("host", serverConfig.Host),
		slog.Int("port", serverConfig.Port),
		slog.Duration("read_timeout", serverConfig.ReadTimeout),
		slog.Duration("write_timeout", serverConfig.WriteTimeout),
		slog.Duration("shutdown_timeout", serverConfig.ShutdownTimeout),
		slog.String("log_level", serverConfig.LogLevel.String()),
	)

	// Load rate limiter configuration
	middlewareConfig := middleware.LoadConfig()

	// Create rate limiter instance (graceful shutdown handled by server.shutdown())
	rateLimiter := middleware.NewInMemoryRateLimiter(middlewareConfig)

	logger.Info("Rate limiter initialized",
		slog.Int("global_rps", middlewareConfig.GlobalRPS),
		slog.Int("global_burst", middlewareConfig.GlobalBurst),
		slog.Int("plugin_rps", middlewareConfig.PluginRPS),
		slog.Int("plugin_burst", middlewareConfig.PluginBurst),
		slog.Int("unauth_rps", middlewareConfig.UnAuthRPS),
		slog.Int("unauth_burst", middlewareConfig.UnAuthBurst),
	)

	// Load storage configuration
	storageConfig := storage.LoadConfig()

	dbConn, err := storage.NewConnection(storageConfig)
	if err != nil {
		logger.Error("Failed to connect to database", slog.String("error", err.Error()))
		os.Exit(1)
	}

	defer func() {
		_ = dbConn.Close() // Ensure connection closes on normal shutdown
	}()

	var apiKeyStore storage.APIKeyStore

	authEnabled := config.GetEnvBool("CORRELATOR_AUTH_ENABLED", false)
	if authEnabled {
		apiKeyStore, err = storage.NewPersistentKeyStore(dbConn)
		if err != nil {
			logger.Error("Failed to connect to persistent key store", slog.String("error", err.Error()))

			_ = dbConn.Close()
			//nolint:gocritic // Explicit cleanup before os.Exit is intentional (defer won't run)
			os.Exit(1)
		}

		logger.Info("Plugin authentication enabled",
			slog.String("database_url", storageConfig.MaskDatabaseURL()),
		)
	} else {
		logger.Warn("Plugin authentication disabled",
			slog.String("security", "Only use in trusted networks (localhost, VPN, internal)"),
			slog.String("note", "Set CORRELATOR_AUTH_ENABLED=true to enable API key authentication"),
		)
	}

	// Load dataset pattern configuration (optional - graceful degradation)
	patternConfig, err := aliasing.LoadConfigFromEnv()
	if err != nil {
		logger.Warn("Failed to load dataset pattern config, continuing without dataset patterns",
			slog.String("error", err.Error()))

		patternConfig = &aliasing.Config{}
	}

	resolver := aliasing.NewResolver(patternConfig)

	logger.Info("Dataset pattern configuration loaded",
		slog.Int("pattern_count", resolver.GetPatternCount()))

	// Log individual patterns at debug level for troubleshooting
	for _, pattern := range patternConfig.DatasetPatterns {
		logger.Debug("Configured dataset pattern",
			slog.String("pattern", pattern.Pattern),
			slog.String("canonical", pattern.Canonical))
	}

	lineageStore, err := storage.NewLineageStore(
		dbConn, storageConfig.CleanupInterval,
		storage.WithAliasResolver(resolver),
		storage.WithViewRefreshDelay(storageConfig.ViewRefreshDelay),
	)
	if err != nil {
		logger.Error("Failed to connect to lineage store", slog.String("error", err.Error()))
		// Close database connection before exit (defer won't run with os.Exit)
		_ = dbConn.Close()
		// Fail-fast: exit immediately to prevent the server creation process from panicking. LineageStore is required!
		os.Exit(1)
	}

	logger.Info("Lineage store initialized",
		slog.String("database_url", storageConfig.MaskDatabaseURL()),
		slog.Duration("cleanup_interval", storageConfig.CleanupInterval),
		slog.Duration("view_refresh_delay", storageConfig.ViewRefreshDelay),
		slog.Int("database_max_open_conns", storageConfig.MaxOpenConns),
		slog.Int("database_max_idle_conns", storageConfig.MaxIdleConns),
		slog.Duration("database_conn_max_lifetime", storageConfig.ConnMaxLifetime),
		slog.Duration("database_conn_max_idle_time", storageConfig.ConnMaxIdleTime),
	)

	// lineageStore implements both ingestion.Store and correlation.Store interfaces
	server := api.NewServer(serverConfig, apiKeyStore, rateLimiter, lineageStore, lineageStore)

	if err := server.Start(); err != nil {
		logger.Error("Server failed to start",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	logger.Info("Correlator service stopped")
}
