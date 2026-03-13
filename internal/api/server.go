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
	correlationStore correlation.Store           // Optional: enables correlation API endpoints (nil = disabled)
	resolutionStore  correlation.ResolutionStore // Optional: enables resolution write endpoints (nil = disabled)
	validator        *ingestion.Validator        // Shared validator (thread-safe, created once)
	healthChecker    *HealthChecker              // Dependency health checker for /health endpoint
}

// Dependencies holds the runtime dependencies injected into the server.
// Configuration (ports, timeouts) is passed separately via ServerConfig.
//
// Required fields panic on NewServer if nil:
//   - IngestionStore
//   - CorrelationStore
//
// Optional fields are nil-safe (feature is disabled when nil).
type Dependencies struct {
	APIKeyStore      storage.APIKeyStore         // nil = auth disabled
	RateLimiter      middleware.RateLimiter      // nil = rate limiting disabled
	IngestionStore   ingestion.Store             // REQUIRED — panics if nil
	CorrelationStore correlation.Store           // REQUIRED — panics if nil
	ResolutionStore  correlation.ResolutionStore // nil = resolution endpoints disabled
	KafkaHealth      KafkaHealthChecker          // nil = Kafka disabled in /health
}

// NewServer creates a new HTTP server instance with structured logging and middleware stack.
//
// Configuration (what) is separated from dependencies (how):
//   - cfg: Pure server configuration (ports, timeouts, CORS settings)
//   - deps: Runtime dependencies (stores, middleware, health checkers)
func NewServer(cfg *ServerConfig, deps Dependencies) *Server {
	// Create structured logger with configured log level
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))

	if deps.IngestionStore == nil || deps.CorrelationStore == nil {
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
		apiKeyStore:      deps.APIKeyStore,
		rateLimiter:      deps.RateLimiter,
		ingestionStore:   deps.IngestionStore,
		correlationStore: deps.CorrelationStore,
		resolutionStore:  deps.ResolutionStore,
		validator:        validator,
		healthChecker:    NewHealthChecker(deps.IngestionStore, deps.KafkaHealth),
	}

	// Set up all API routes
	server.setupRoutes(mux)

	// Log middleware configuration
	if deps.APIKeyStore != nil { // pragma: allowlist secret
		logger.Info("API key authentication middleware enabled")
	} else {
		logger.Warn("APIKeyStore not configured - API key authentication middleware disabled")
	}

	if deps.RateLimiter != nil {
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
	//   3. Auth - identify client and set ClientContext (optional)
	//   4. RateLimit - block requests before expensive operations (optional)
	//   5. RequestLogger - log only legitimate requests (not rate-limited spam)
	//   6. CORS - lightweight header manipulation
	handler := middleware.Apply(mux,
		middleware.WithCorrelationID(),
		middleware.WithRecovery(logger),
		middleware.WithAuth(deps.APIKeyStore, logger),
		middleware.WithRateLimit(deps.RateLimiter, logger),
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

// ListenAndServe starts the HTTP server in a background goroutine and returns
// an error channel. The server runs until Shutdown is called or a fatal error
// occurs (e.g., port already in use).
//
// Callers should select on the returned channel and call Shutdown for cleanup.
// This method is non-blocking — it returns immediately after starting the server.
//
// Usage:
//
//	serverErrors := server.ListenAndServe()
//	select {
//	case err := <-serverErrors:
//	    // fatal server error (port in use, etc.)
//	case <-stopSignal:
//	    server.Shutdown(ctx)
//	}
func (s *Server) ListenAndServe() <-chan error {
	s.startTime = time.Now()

	serverErrors := make(chan error, 1)

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

	return serverErrors
}

// Shutdown gracefully shuts down the HTTP server and closes all dependencies.
// The provided context controls the shutdown deadline. If the context expires
// before shutdown completes, in-flight requests are forcibly terminated.
//
// Shutdown order:
//  1. HTTP server drain (stop accepting new connections, finish in-flight requests)
//  2. Close API key store
//  3. Close rate limiter
//  4. Close ingestion store
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Initiating server shutdown")

	// Attempt graceful shutdown of HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Error("Server shutdown failed",
			slog.String("error", err.Error()),
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

// Start starts the HTTP server and blocks until shutdown signal (SIGINT/SIGTERM).
// This is a convenience wrapper around ListenAndServe + Shutdown for simple
// single-subsystem deployments. When running multiple subsystems (e.g., HTTP + Kafka),
// use ListenAndServe and Shutdown directly with shared signal handling.
func (s *Server) Start() error {
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid server configuration: %w", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	serverErrors := s.ListenAndServe()

	// Block until we receive a signal or server error
	select {
	case err := <-serverErrors:
		return err
	case sig := <-stop:
		s.logger.Info("Received shutdown signal",
			slog.String("signal", sig.String()),
		)

		ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancel()

		return s.Shutdown(ctx)
	}
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
