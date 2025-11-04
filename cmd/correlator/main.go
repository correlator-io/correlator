// Package main provides the Correlator incident correlation service.
//
// This is the main correlation engine service that processes OpenLineage events
// and correlates test failures with job runs to provide < 5minute incident response.
package main

import (
	"flag"
	"log"
	"log/slog"
	"os"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/correlator-io/correlator/internal/api"
	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/storage"
)

// Version information.
const (
	version = "1.0.0-dev"
	name    = "correlator"
)

func main() {
	versionFlag := flag.Bool("version", false, "show version information")
	flag.Parse()

	if *versionFlag {
		log.Printf("%s v%s\n", name, version)
		os.Exit(0)
	}

	serverConfig := api.LoadServerConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: serverConfig.LogLevel,
	}))

	logger.Info("Starting Correlator service",
		slog.String("service", name),
		slog.String("version", version),
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
	}

	apiKeyStore, err := storage.NewPersistentKeyStore(dbConn)
	if err != nil {
		logger.Error("Failed to connect to persistent key store", slog.String("error", err.Error()))
	}

	logger.Info("Persistent key store initialized",
		slog.String("database_url", storageConfig.MaskDatabaseURL()),
		slog.Int("database_max_open_conns", storageConfig.MaxOpenConns),
		slog.Int("database_max_idle_conns", storageConfig.MaxIdleConns),
		slog.Duration("database_conn_max_lifetime", storageConfig.ConnMaxLifetime),
		slog.Duration("database_conn_max_idle_time", storageConfig.ConnMaxIdleTime),
	)

	lineageStore, err := storage.NewLineageStore(dbConn)
	if err != nil {
		logger.Error("Failed to connect to lineage store", slog.String("error", err.Error()))
		// Fail-fast: exit immediately to prevent the server creation process from panicking. LineageStore is required!
		os.Exit(1)
	}

	logger.Info("Lineage store initialized",
		slog.String("database_url", storageConfig.MaskDatabaseURL()),
		slog.Int("database_max_open_conns", storageConfig.MaxOpenConns),
		slog.Int("database_max_idle_conns", storageConfig.MaxIdleConns),
		slog.Duration("database_conn_max_lifetime", storageConfig.ConnMaxLifetime),
		slog.Duration("database_conn_max_idle_time", storageConfig.ConnMaxIdleTime),
	)

	server := api.NewServer(serverConfig, apiKeyStore, rateLimiter, lineageStore)

	if err := server.Start(); err != nil {
		logger.Error("Server failed to start",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	logger.Info("Correlator service stopped")
}
