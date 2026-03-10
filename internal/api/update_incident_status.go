package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/correlation"
	"github.com/correlator-io/correlator/internal/storage"
)

const (
	defaultMuteDays = 30
	maxMuteDays     = 365
)

type (
	// updateStatusRequest is the request body for PATCH /api/v1/incidents/{id}/status.
	updateStatusRequest struct {
		Status   string `json:"status"`
		Reason   string `json:"reason,omitempty"`
		Note     string `json:"note,omitempty"`
		MuteDays int    `json:"mute_days,omitempty"` //nolint:tagliatelle
	}

	// updateStatusResponse is the minimal response from PATCH /api/v1/incidents/{id}/status.
	updateStatusResponse struct {
		ID               string     `json:"id"`
		ResolutionStatus string     `json:"resolution_status"` //nolint:tagliatelle
		ResolvedBy       string     `json:"resolved_by"`       //nolint:tagliatelle
		ResolvedAt       *time.Time `json:"resolved_at"`       //nolint:tagliatelle
		ResolutionReason *string    `json:"resolution_reason"` //nolint:tagliatelle
		MuteExpiresAt    *time.Time `json:"mute_expires_at"`   //nolint:tagliatelle
	}
)

// handleUpdateIncidentStatus handles PATCH /api/v1/incidents/{id}/status.
// Validates the requested state transition and applies it via the resolution store.
func (s *Server) handleUpdateIncidentStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	correlationID := middleware.GetCorrelationID(ctx)

	testResultID, idStr, problem := parseIncidentID(r)
	if problem != nil {
		WriteErrorResponse(w, r, s.logger, problem)

		return
	}

	req, problem := parseAndValidateStatusBody(r)
	if problem != nil {
		WriteErrorResponse(w, r, s.logger, problem)

		return
	}

	incident, err := s.correlationStore.QueryIncidentByID(ctx, testResultID)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query incident for status update",
			"correlation_id", correlationID,
			"incident_id", testResultID,
			"error", err.Error(),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to query incident"))

		return
	}

	if incident == nil {
		WriteErrorResponse(w, r, s.logger, NotFound("Incident not found"))

		return
	}

	resolvedBy := "user"

	if clientCtx, ok := middleware.GetClientContext(ctx); ok && clientCtx.ClientID != "" {
		resolvedBy = clientCtx.ClientID
	}

	resolution, err := s.resolutionStore.SetResolution(ctx, testResultID, *req, resolvedBy)
	if err != nil {
		if errors.Is(err, storage.ErrInvalidResolutionTransition) {
			WriteErrorResponse(w, r, s.logger, Conflict(err.Error()))

			return
		}

		s.logger.ErrorContext(ctx, "Failed to set resolution",
			"correlation_id", correlationID,
			"incident_id", testResultID,
			"target_status", string(req.Status),
			"error", err.Error(),
		)

		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to update incident status"))

		return
	}

	resp := mapResolutionToResponse(idStr, resolution)

	data, err := json.Marshal(resp)
	if err != nil {
		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode response"))

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// parseIncidentID extracts and validates the incident ID from the request path.
func parseIncidentID(r *http.Request) (int64, string, *ProblemDetail) {
	idStr := r.PathValue("id")
	if idStr == "" {
		return 0, "", BadRequest("Missing incident ID")
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, "", BadRequest("Invalid incident ID: must be a numeric value")
	}

	return id, idStr, nil
}

// parseAndValidateStatusBody reads and validates the PATCH request body.
func parseAndValidateStatusBody(r *http.Request) (*correlation.ResolutionRequest, *ProblemDetail) {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		return nil, UnsupportedMediaType("Content-Type must be application/json")
	}

	var body updateStatusRequest

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, BadRequest("Invalid JSON request body")
	}

	return validateStatusRequest(body)
}

// validateStatusRequest validates the PATCH request body and returns a domain ResolutionRequest.
func validateStatusRequest(body updateStatusRequest) (*correlation.ResolutionRequest, *ProblemDetail) {
	if body.Status == "" {
		return nil, UnprocessableEntity("status is required")
	}

	status := correlation.ResolutionStatus(body.Status)
	if !correlation.IsValidResolutionStatus(status) {
		return nil, UnprocessableEntity("status must be one of: acknowledged, resolved, muted")
	}

	if status == correlation.ResolutionOpen {
		return nil, UnprocessableEntity("cannot transition to open; use acknowledged, resolved, or muted")
	}

	req := &correlation.ResolutionRequest{
		Status: status,
		Note:   body.Note,
	}

	switch status { //nolint:exhaustive
	case correlation.ResolutionResolved:
		req.Reason = body.Reason
		if req.Reason == "" {
			req.Reason = "manual"
		}
	case correlation.ResolutionMuted:
		if body.Reason == "" {
			return nil, UnprocessableEntity("reason is required when muting (e.g., false_positive, expected)")
		}

		req.Reason = body.Reason

		req.MuteDays = body.MuteDays
		if req.MuteDays <= 0 {
			req.MuteDays = defaultMuteDays
		}

		if req.MuteDays > maxMuteDays {
			return nil, UnprocessableEntity("mute_days must be <= 365")
		}
	}

	return req, nil
}

func mapResolutionToResponse(id string, r *correlation.IncidentResolution) updateStatusResponse {
	resp := updateStatusResponse{
		ID:               id,
		ResolutionStatus: string(r.Status),
		ResolvedBy:       r.ResolvedBy,
		ResolvedAt:       &r.UpdatedAt,
	}

	if r.ResolutionReason != "" {
		resp.ResolutionReason = &r.ResolutionReason
	}

	if r.MuteExpiresAt != nil {
		resp.MuteExpiresAt = r.MuteExpiresAt
	}

	return resp
}
