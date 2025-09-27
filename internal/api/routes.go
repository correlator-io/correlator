// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
)

type (
	// Version represents the API version response structure.
	Version struct {
		Version     string `json:"version"`
		ServiceName string `json:"serviceName"`
		BuildInfo   string `json:"buildInfo,omitempty"`
	}
	// HealthStatus represents the health check response structure.
	HealthStatus struct {
		Status      string `json:"status"`
		ServiceName string `json:"serviceName"`
		Version     string `json:"version"`
		Uptime      string `json:"uptime,omitempty"`
	}
)

// Routes sets up all HTTP routes for the API server.
func (s *Server) setupRoutes(mux *http.ServeMux) {
	// Health check endpoint (no versioning for basic ping)
	mux.HandleFunc("/ping", s.handlePing)

	mux.HandleFunc("/api/v1/version", s.handleVersion)
	mux.HandleFunc("/api/v1/health", s.handleHealth)

	// Catch-all handler for 404 responses
	mux.HandleFunc("/", s.handleNotFound)
}

// handlePing responds to ping requests for basic server validation.
func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)

	_, err := w.Write([]byte("pong"))
	if err != nil {
		s.logger.Error("Failed to write ping response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)
	}
}

// handleVersion returns API version information for client compatibility.
func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	version := Version{
		Version:     "v1.0.0",
		ServiceName: "correlator",
		BuildInfo:   "development",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(version); err != nil {
		s.logger.Error("Failed to encode version response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode version response"))
	}
}

// handleHealth returns detailed health status information.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	correlationID := middleware.GetCorrelationID(r.Context())

	// Calculate uptime if server has started
	var uptime string

	if !s.startTime.IsZero() {
		duration := time.Since(s.startTime)
		uptime = duration.Round(time.Second).String()
	}

	health := HealthStatus{
		Status:      "healthy",
		ServiceName: "correlator",
		Version:     "v1.0.0",
		Uptime:      uptime,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(health); err != nil {
		s.logger.Error("Failed to encode health response",
			slog.String("correlation_id", correlationID),
			slog.String("error", err.Error()),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode health response"))
	}
}

// handleNotFound returns RFC 7807 compliant 404 responses for unknown endpoints.
func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	WriteErrorResponse(w, r, s.logger, NotFound("The requested resource was not found"))
}
