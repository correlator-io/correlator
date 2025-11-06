// Package api provides HTTP API server implementation for the Correlator service.
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/correlator-io/correlator/internal/api/middleware"
)

// commonProblemTypes pre-computes type URIs for frequently used HTTP status codes
// to avoid repeated string formatting allocations.
//
//nolint:gochecknoglobals // Global map is justified for memory optimization
var commonProblemTypes = map[int]string{
	http.StatusBadRequest:            "https://getcorrelator.io/problems/400",
	http.StatusUnauthorized:          "https://getcorrelator.io/problems/401",
	http.StatusForbidden:             "https://getcorrelator.io/problems/403",
	http.StatusNotFound:              "https://getcorrelator.io/problems/404",
	http.StatusMethodNotAllowed:      "https://getcorrelator.io/problems/405",
	http.StatusRequestEntityTooLarge: "https://getcorrelator.io/problems/413",
	http.StatusUnsupportedMediaType:  "https://getcorrelator.io/problems/415",
	http.StatusUnprocessableEntity:   "https://getcorrelator.io/problems/422",
	http.StatusInternalServerError:   "https://getcorrelator.io/problems/500",
}

// ProblemDetail represents an RFC 7807 Problem Details structure.
// See https://tools.ietf.org/html/rfc7807 for specification.
type ProblemDetail struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail,omitempty"`
	Instance      string `json:"instance,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"` //nolint: tagliatelle
}

// NewProblemDetail creates a new RFC 7807 Problem Detail.
func NewProblemDetail(status int, title, detail string) *ProblemDetail {
	// Use pre-computed type URI for common status codes to avoid allocation
	problemType, exists := commonProblemTypes[status]
	if !exists {
		problemType = fmt.Sprintf("https://getcorrelator.io/problems/%d", status)
	}

	return &ProblemDetail{
		Type:   problemType,
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
// Uses marshal-first pattern to ensure encoding errors are caught before headers are sent.
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

	// Marshal FIRST (before writing anything) - fail fast if encoding fails
	body, err := json.Marshal(problem)
	if err != nil {
		logger.Error("Failed to marshal error response",
			slog.String("correlation_id", correlationID),
			slog.String("path", r.URL.Path),
			slog.String("method", r.Method),
			slog.Any("marshal_error", err),
			slog.Int("status", problem.Status),
		)

		// Safe fallback: no headers sent yet, can call http.Error
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	// Now write headers and body atomically
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(problem.Status)

	if _, err := w.Write(body); err != nil {
		// Headers already sent, response is corrupted, can only log
		logger.Error("Failed to write error response",
			slog.String("correlation_id", correlationID),
			slog.String("path", r.URL.Path),
			slog.String("method", r.Method),
			slog.Any("write_error", err),
			slog.Int("status", problem.Status),
		)
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

// Unauthorized creates a 401 Unauthorized problem.
func Unauthorized(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusUnauthorized,
		"Unauthorized",
		detail,
	)
}

// Forbidden creates a 403 Forbidden problem.
func Forbidden(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusForbidden,
		"Forbidden",
		detail,
	)
}

// PayloadTooLarge creates a 413 Payload Too Large problem.
func PayloadTooLarge(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusRequestEntityTooLarge,
		"Payload Too Large",
		detail,
	)
}

// UnsupportedMediaType creates a 415 Unsupported Media Type problem.
func UnsupportedMediaType(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusUnsupportedMediaType,
		"Unsupported Media Type",
		detail,
	)
}

// UnprocessableEntity creates a 422 Unprocessable Entity problem.
func UnprocessableEntity(detail string) *ProblemDetail {
	return NewProblemDetail(
		http.StatusUnprocessableEntity,
		"Unprocessable Entity",
		detail,
	)
}
