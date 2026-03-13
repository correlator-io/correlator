package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/correlator-io/correlator/internal/aliasing"
	"github.com/correlator-io/correlator/internal/api"
	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/ingestion"
	"github.com/correlator-io/correlator/internal/kafka"
	"github.com/correlator-io/correlator/internal/storage"
)

const (
	serviceName = "correlator"
	initTimeout = 30 * time.Second
)

func runStart(args []string) {
	if err := start(args); err != nil {
		fmt.Fprintf(os.Stderr, "correlator: %v\n", err)
		os.Exit(1)
	}
}

//nolint:funlen,gocognit,cyclop,maintidx // Sequential setup — splitting adds indirection without clarity.
func start(args []string) error {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	host := fs.String("host", "", "override CORRELATOR_SERVER_HOST")
	port := fs.Int("port", 0, "override CORRELATOR_SERVER_PORT")

	_ = fs.Parse(args)

	serverConfig := api.LoadServerConfig()

	// Apply CLI overrides
	if *host != "" {
		serverConfig.Host = *host
	}

	if *port != 0 {
		serverConfig.Port = *port
	}

	if err := serverConfig.Validate(); err != nil {
		return fmt.Errorf("invalid server configuration: %w", err)
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

	// Create rate limiter instance (graceful shutdown handled by server.Shutdown())
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
		return fmt.Errorf("database connection: %w", err)
	}

	defer func() { _ = dbConn.Close() }()

	var apiKeyStore storage.APIKeyStore

	authEnabled := config.GetEnvBool("CORRELATOR_AUTH_ENABLED", false)
	if authEnabled {
		apiKeyStore, err = storage.NewPersistentKeyStore(dbConn)
		if err != nil {
			return fmt.Errorf("persistent key store: %w", err)
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
		return fmt.Errorf("lineage store: %w", err)
	}

	defer func() { _ = lineageStore.Close() }()

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
	defer initCancel()

	if err := lineageStore.InitResolvedDatasets(initCtx); err != nil {
		return fmt.Errorf("resolved datasets initialization: %w", err)
	}

	logger.Info("Resolved datasets lookup table initialized")

	// Load Kafka consumer configuration (optional — disabled by default)
	kafkaConfig := kafka.LoadConfig()
	if err := kafkaConfig.Validate(); err != nil {
		return fmt.Errorf("kafka configuration: %w", err)
	}

	// Create shared validator for all transports (thread-safe, no mutable state)
	validator := ingestion.NewValidator()

	// Create Kafka consumer (if enabled)
	var consumer *kafka.Consumer

	if kafkaConfig.Enabled {
		consumer = kafka.NewConsumer(kafkaConfig, lineageStore, validator, logger)

		logger.Info("Kafka consumer configured",
			slog.Any("brokers", kafkaConfig.Brokers),
			slog.String("topic", kafkaConfig.Topic),
			slog.String("group_id", kafkaConfig.GroupID),
		)
	} else {
		logger.Info("Kafka consumer disabled (set CORRELATOR_KAFKA_ENABLED=true to enable)")
	}

	var kafkaHealthChecker api.KafkaHealthChecker
	if consumer != nil {
		kafkaHealthChecker = consumer
	}

	server := api.NewServer(serverConfig, api.Dependencies{
		APIKeyStore:      apiKeyStore,
		RateLimiter:      rateLimiter,
		IngestionStore:   lineageStore,
		CorrelationStore: lineageStore,
		ResolutionStore:  lineageStore,
		KafkaHealth:      kafkaHealthChecker,
	})

	// --- Signal handling: orchestrate all subsystems ---

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start subsystems (non-blocking)
	// A nil channel blocks forever on select — disabled subsystems are simply silent.
	var consumerErrors <-chan error
	if consumer != nil {
		consumerErrors = consumer.Start(ctx)
	}

	serverErrors := server.ListenAndServe()

	// Block until shutdown signal or fatal subsystem error
	select {
	case err := <-consumerErrors:
		return fmt.Errorf("kafka consumer failed: %w", err)
	case err := <-serverErrors:
		return fmt.Errorf("server failed: %w", err)
	case sig := <-stop:
		logger.Info("Received shutdown signal", slog.String("signal", sig.String()))
	}

	// Ordered shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), serverConfig.ShutdownTimeout)
	defer shutdownCancel()

	// 1. Cancel context (stops Kafka consumer loop)
	cancel()

	// 2. Kafka consumer drain (wait for in-flight message)
	if consumer != nil {
		if err := consumer.Close(); err != nil {
			logger.Error("Kafka consumer shutdown failed", slog.String("error", err.Error()))
		}
	}

	// 3. HTTP server drain + dependency cleanup
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown failed", slog.String("error", err.Error()))
	}

	logger.Info("Correlator service stopped")

	return nil
}
