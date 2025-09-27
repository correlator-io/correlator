// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/correlator-io/correlator/internal/api/middleware"
)

// ProblemDetail represents an RFC 7807 Problem Details structure.
// See https://tools.ietf.org/html/rfc7807 for specification.
type ProblemDetail struct {
	Type         string `json:"type"`
	Title        string `json:"title"`
	Status       int    `json:"status"`
	Detail       string `json:"detail,omitempty"`
	Instance     string `json:"instance,omitempty"`
	CorrelationID string `json:"correlationId,omitempty"`
}

// NewProblemDetail creates a new RFC 7807 Problem Detail.
func NewProblemDetail(status int, title, detail string) *ProblemDetail {
	return &ProblemDetail{
		Type:   fmt.Sprintf("https://correlator.io/problems/%d", status),
		Title:  title,
		Status: status,
		Detail: detail,
	}
}

// WithInstance adds an instance URI to the problem detail.
func (p *ProblemDetail) WithInstance(instance string) *ProblemDetail {
	p.Instance = instance

	return p
}

// WithCorrelationID adds a correlation ID to the problem detail.
func (p *ProblemDetail) WithCorrelationID(correlationID string) *ProblemDetail {
	p.CorrelationID = correlationID

	return p
}

// WriteErrorResponse writes an RFC 7807 compliant error response.
func WriteErrorResponse(w http.ResponseWriter, r *http.Request, logger *slog.Logger, problem *ProblemDetail) {
	correlationID := middleware.GetCorrelationID(r.Context())

	// Add correlation ID if not already set
	if problem.CorrelationID == "" {
		problem.CorrelationID = correlationID
	}

	// Add instance if not already set
	if problem.Instance == "" {
		problem.Instance = r.URL.Path
	}

	// Set proper content type for RFC 7807
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(problem.Status)

	if err := json.NewEncoder(w).Encode(problem); err != nil {
		logger.Error("Failed to encode error response",
			slog.String("correlation_id", correlationID),
			slog.String("path", r.URL.Path),
			slog.String("method", r.Method),
			slog.Any("encode_error", err),
			slog.Int("status", problem.Status),
		)

		// Fallback to basic error response
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// Common error constructors for frequently used errors.

// InternalServerError creates a 500 Internal Server Error problem.
func InternalServerError(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusInternalServerError,
		"Internal Server Error",
		detail,
	)
}

// BadRequest creates a 400 Bad Request problem.
func BadRequest(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusBadRequest,
		"Bad Request",
		detail,
	)
}

// NotFound creates a 404 Not Found problem.
func NotFound(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusNotFound,
		"Not Found",
		detail,
	)
}

// MethodNotAllowed creates a 405 Method Not Allowed problem.
func MethodNotAllowed(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusMethodNotAllowed,
		"Method Not Allowed",
		detail,
	)
}
