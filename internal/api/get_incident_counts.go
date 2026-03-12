package api

import (
	"encoding/json"
	"net/http"

	"github.com/correlator-io/correlator/internal/api/middleware"
)

const defaultCountsWindowDays = 30

type incidentCountsResponse struct {
	Active   int `json:"active"`
	Resolved int `json:"resolved"`
	Muted    int `json:"muted"`
}

// handleGetIncidentCounts handles GET /api/v1/incidents/counts.
// Returns the number of active, resolved, and muted incidents.
// Resolved/muted counts use a 30-day window.
func (s *Server) handleGetIncidentCounts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	correlationID := middleware.GetCorrelationID(ctx)

	counts, err := s.correlationStore.QueryIncidentCounts(ctx, defaultCountsWindowDays)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query incident counts",
			"correlation_id", correlationID,
			"error", err.Error(),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to query incident counts"))

		return
	}

	response := incidentCountsResponse{
		Active:   counts.Active,
		Resolved: counts.Resolved,
		Muted:    counts.Muted,
	}

	data, err := json.Marshal(response)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to marshal counts response",
			"correlation_id", correlationID,
			"error", err.Error(),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode response"))

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
