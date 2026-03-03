package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/correlator-io/correlator/internal/aliasing"
	"github.com/correlator-io/correlator/internal/api"
	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/storage"
)

const (
	serviceName = "correlator"
	initTimeout = 30 * time.Second
)

//nolint:funlen // start command sets up dependencies sequentially, extracting would reduce clarity
func runStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	host := fs.String("host", "", "override CORRELATOR_HOST")
	port := fs.Int("port", 0, "override CORRELATOR_PORT")

	_ = fs.Parse(args)

	serverConfig := api.LoadServerConfig()

	// Apply CLI overrides
	if *host != "" {
		serverConfig.Host = *host
	}

	if *port != 0 {
		serverConfig.Port = *port
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: serverConfig.LogLevel,
	}))

	logger.Info("Starting Correlator service",
		slog.String("service", serviceName),
		slog.String("version", version),
		slog.String("git_commit", gitCommit),
		slog.String("build_time", buildTime),
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
		slog.Int("client_rps", middlewareConfig.ClientRPS),
		slog.Int("client_burst", middlewareConfig.ClientBurst),
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

		logger.Info("API key authentication enabled",
			slog.String("database_url", storageConfig.MaskDatabaseURL()),
		)
	} else {
		logger.Warn("API key authentication disabled",
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

	// Initialize resolved_datasets lookup table (must run before serving traffic)
	initCtx, initCancel := context.WithTimeout(context.Background(), initTimeout)

	if err := lineageStore.InitResolvedDatasets(initCtx); err != nil {
		initCancel()

		logger.Error("Failed to initialize resolved_datasets", slog.String("error", err.Error()))

		_ = lineageStore.Close()
		_ = dbConn.Close()

		os.Exit(1)
	}

	initCancel()

	logger.Info("Resolved datasets lookup table initialized")

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
