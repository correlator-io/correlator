// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/correlation"
	"github.com/correlator-io/correlator/internal/ingestion"
	"github.com/correlator-io/correlator/internal/storage"
)

// Server represents the HTTP API server.
type Server struct {
	httpServer       *http.Server
	logger           *slog.Logger
	config           *ServerConfig
	startTime        time.Time
	apiKeyStore      storage.APIKeyStore
	rateLimiter      middleware.RateLimiter
	ingestionStore   ingestion.Store
	correlationStore correlation.Store    // Optional: enables correlation API endpoints (nil = disabled)
	validator        *ingestion.Validator // Shared validator (thread-safe, created once)
}

// NewServer creates a new HTTP server instance with structured logging and middleware stack.
//
// Dependencies are injected explicitly rather than being part of ServerConfig.
// This follows the dependency injection pattern where configuration (what) is
// separated from dependencies (how).
//
// Parameters:
//   - cfg: Pure server configuration (ports, timeouts, CORS settings)
//   - apiKeyStore: API key storage implementation (nil disables authentication)
//   - rateLimiter: Rate limiter implementation (nil disables rate limiting)
//   - ingestionStore: open lineage events store (REQUIRED - panics if nil)
//   - correlationStore: Correlation query implementation (nil disables correlation endpoints)
func NewServer(
	cfg *ServerConfig,
	apiKeyStore storage.APIKeyStore,
	rateLimiter middleware.RateLimiter,
	ingestionStore ingestion.Store,
	correlationStore correlation.Store,
) *Server {
	// Create structured logger with configured log level
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))

	if ingestionStore == nil || correlationStore == nil {
		logger.Error("LineageStore is required - cannot start server without core functionality")
		panic("correlator: LineageStore cannot be nil - this indicates a configuration error")
	}

	// Create base HTTP mux
	mux := http.NewServeMux()

	// Create validator once (thread-safe, no mutable state)
	validator := ingestion.NewValidator()

	// Create server instance for route setup
	server := &Server{
		logger:           logger,
		config:           cfg,
		apiKeyStore:      apiKeyStore,
		rateLimiter:      rateLimiter,
		ingestionStore:   ingestionStore,
		correlationStore: correlationStore,
		validator:        validator,
	}

	// Set up all API routes
	server.setupRoutes(mux)

	// Log middleware configuration
	if apiKeyStore != nil { // pragma: allowlist secret
		logger.Info("Plugin authentication middleware enabled")
	} else {
		logger.Warn("APIKeyStore not configured - plugin authentication middleware disabled")
	}

	if rateLimiter != nil {
		logger.Info("Rate limiting middleware enabled")
	} else {
		logger.Warn("RateLimiter not configured - rate limiting middleware disabled")
	}

	// LineageStore is always configured (we panic if nil above)
	logger.Info("Lineage store configured - all api endpoints enabled")

	// Apply middleware chain using functional options pattern.
	// Middleware executes in the order listed (top-to-bottom):
	//   1. CorrelationID - generate correlation ID for all responses
	//   2. Recovery - catch panics in all downstream middleware
	//   3. Plugin Auth - identify plugin and set PluginContext (optional)
	//   4. RateLimit - block requests before expensive operations (optional)
	//   5. RequestLogger - log only legitimate requests (not rate-limited spam)
	//   6. CORS - lightweight header manipulation
	handler := middleware.Apply(mux,
		middleware.WithCorrelationID(),
		middleware.WithRecovery(logger),
		middleware.WithAuthPlugin(apiKeyStore, logger),
		middleware.WithRateLimit(rateLimiter, logger),
		middleware.WithRequestLogger(logger),
		middleware.WithCORS(cfg.ToCORSConfig()),
	)

	httpServer := &http.Server{
		Addr:         cfg.Address(),
		Handler:      handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	// Set the httpServer field for the existing server instance
	server.httpServer = httpServer

	return server
}

// Start starts the HTTP server and blocks until shutdown.
// It handles graceful shutdown on SIGINT and SIGTERM signals.
func (s *Server) Start() error {
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid server configuration: %w", err)
	}

	// Record server start time for uptime calculation
	s.startTime = time.Now()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	serverErrors := make(chan error, 1)

	// Start server in a goroutine
	go func() {
		s.logger.Info("Starting Correlator API server",
			slog.String("address", s.config.Address()),
			slog.Duration("read_timeout", s.config.ReadTimeout),
			slog.Duration("write_timeout", s.config.WriteTimeout),
			slog.Duration("shutdown_timeout", s.config.ShutdownTimeout),
		)

		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("Server failed to start",
				slog.String("address", s.config.Address()),
				slog.String("error", err.Error()),
			)

			serverErrors <- fmt.Errorf("server failed to start: %w", err)
		}
	}()

	// Block until we receive a signal or server error
	select {
	case err := <-serverErrors:
		return err
	case sig := <-stop:
		s.logger.Info("Received shutdown signal",
			slog.String("signal", sig.String()),
		)

		return s.shutdown()
	}
}

// shutdown gracefully shuts down the server.
func (s *Server) shutdown() error {
	// Create context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	s.logger.Info("Initiating server shutdown",
		slog.Duration("shutdown_timeout", s.config.ShutdownTimeout),
	)

	// Attempt graceful shutdown of HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Error("Server shutdown failed",
			slog.String("error", err.Error()),
			slog.Duration("shutdown_timeout", s.config.ShutdownTimeout),
		)

		return fmt.Errorf("server shutdown failed: %w", err)
	}

	// Close all dependencies (best-effort - log failures but continue shutdown)
	s.closeDependency("API key store", s.apiKeyStore)
	s.closeDependency("rate limiter", s.rateLimiter)
	s.closeDependency("ingestion store", s.ingestionStore)
	// Note: correlationStore is typically the same instance as ingestionStore,
	// so we don't close it separately to avoid double-close

	s.logger.Info("Server shutdown completed successfully")

	return nil
}

// closeDependency attempts to close a server dependency that implements io.Closer.
// Logs the operation and its result. Errors are logged but don't stop shutdown (best-effort).
func (s *Server) closeDependency(name string, store interface{}) {
	// Skip if store is nil
	if store == nil {
		return
	}

	s.logger.Info("Closing " + name)

	// Check if store implements io.Closer
	closer, ok := store.(io.Closer)
	if !ok {
		// Dependency doesn't implement io.Closer, nothing to close
		return
	}

	// Attempt to close (log error but continue)
	if err := closer.Close(); err != nil {
		s.logger.Error("Failed to close "+name, slog.String("error", err.Error()))

		return
	}

	s.logger.Info(name + " closed successfully")
}
