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
)

// Server represents the HTTP API server.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
	config     ServerConfig
	startTime  time.Time
}

// NewServer creates a new HTTP server instance with structured logging and middleware stack.
func NewServer(cfg ServerConfig) *Server {
	// Create structured logger with configured log level
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))

	// Create base HTTP mux
	mux := http.NewServeMux()

	// Create server instance for route setup
	server := &Server{
		logger: logger,
		config: cfg,
	}

	// Set up all API routes
	server.setupRoutes(mux)

	// Create middleware stack (applied in reverse order)
	var handler http.Handler = mux

	// Apply middleware stack (innermost to outermost)
	// Execution order: CorrelationID → Recovery → Auth → RateLimit → Logger → CORS → Handler

	// 1. CORS - lightweight header manipulation (innermost)
	handler = middleware.CORS(cfg.ToCORSConfig())(handler)

	// 2. Request logger - logs only legitimate requests (not rate-limited spam)
	handler = middleware.RequestLogger(logger)(handler)

	// 3. Rate limiting - block before expensive operations (if configured)
	if cfg.RateLimiter != nil {
		handler = middleware.RateLimit(cfg.RateLimiter, logger)(handler)
		logger.Info("Rate limiting middleware enabled")
	} else {
		logger.Warn("RateLimiter not configured - rate limiting middleware disabled")
	}

	// 4. Plugin Authentication - identifies plugin and sets PluginContext (if configured)
	if cfg.APIKeyStore != nil { // pragma: allowlist secret
		handler = middleware.AuthenticatePlugin(cfg.APIKeyStore, logger)(handler)
		logger.Info("Plugin authentication middleware enabled")
	} else {
		logger.Warn("APIKeyStore not configured - plugin authentication middleware disabled")
	}

	// 5. Recovery - catches panics in ALL downstream middleware
	handler = middleware.Recovery(logger)(handler)

	// 6. CorrelationID - generate correlation ID for all responses (outermost)
	handler = middleware.CorrelationID()(handler)

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

	// Close API key store to release database connections
	if s.config.APIKeyStore != nil { // pragma: allowlist secret
		s.logger.Info("Closing API key store")

		if store, ok := s.config.APIKeyStore.(io.Closer); ok {
			if err := store.Close(); err != nil {
				s.logger.Error("Failed to close API key store", slog.String("error", err.Error()))
			} else {
				s.logger.Info("API key store closed successfully")
			}
		}
	}

	s.logger.Info("Server shutdown completed successfully")

	return nil
}
