package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/correlation"
)

type (
	// incidentListParams holds parsed query parameters for incident list.
	incidentListParams struct {
		since  *time.Time
		limit  int
		offset int
	}

	// paramError represents a parameter validation error.
	paramError struct {
		param string
		msg   string
	}
)

const (
	// Pagination defaults and limits.
	defaultLimit = 20
	maxLimit     = 100
	minLimit     = 1
)

func (e *paramError) Error() string {
	return "Invalid parameter '" + e.param + "': " + e.msg
}

// handleListIncidents handles GET /api/v1/incidents.
// Returns a paginated list of incidents with optional filtering.
//
// Query Parameters:
//   - status: "failed" | "all" (default: "all") - Note: view already filters to failed/error
//   - since: ISO8601 timestamp (filter incidents after this time)
//   - limit: 1-100 (default: 20)
//   - offset: >= 0 (default: 0)
//
// Response: IncidentListResponse with incidents sorted by executed_at DESC.
func (s *Server) handleListIncidents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	correlationID := middleware.GetCorrelationID(ctx)

	// Parse query parameters
	params, err := parseIncidentListParams(r)
	if err != nil {
		WriteErrorResponse(w, r, s.logger, BadRequest(err.Error()))

		return
	}

	// Build filter and pagination from query parameters
	filter := buildIncidentFilter(params)
	pagination := &correlation.Pagination{
		Limit:  params.limit,
		Offset: params.offset,
	}

	// Query incidents from store (with database-level pagination)
	result, err := s.correlationStore.QueryIncidents(ctx, filter, pagination)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query incidents",
			"correlation_id", correlationID,
			"error", err.Error(),
		)
		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to query incidents"))

		return
	}

	downstreamCounts, err := s.correlationStore.QueryDownstreamCounts(ctx, extractJobRunIDs(result.Incidents))
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query downstream counts",
			"correlation_id", correlationID,
			"error", err.Error(),
		)
		// Non-fatal: continue with zero counts
		downstreamCounts = map[string]int{}
	}

	orphanDatasets, err := s.correlationStore.QueryOrphanDatasets(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query orphan datasets",
			"correlation_id", correlationID,
			"error", err.Error(),
		)
		// Non-fatal: continue with empty orphan set
		orphanDatasets = nil
	}

	orphanDatasetSet := buildOrphanDatasetSet(orphanDatasets)

	summaries := make([]IncidentSummary, 0, len(result.Incidents))
	for _, inc := range result.Incidents {
		summaries = append(summaries, mapIncidentToSummary(inc, downstreamCounts, orphanDatasetSet))
	}

	response := IncidentListResponse{
		Incidents:   summaries,
		Total:       result.Total,
		Limit:       params.limit,
		Offset:      params.offset,
		OrphanCount: len(orphanDatasetSet),
	}

	data, err := json.Marshal(response)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to marshal incidents response",
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

// parseIncidentListParams parses and validates query parameters.
func parseIncidentListParams(r *http.Request) (*incidentListParams, error) {
	q := r.URL.Query()

	params := &incidentListParams{
		limit:  defaultLimit,
		offset: 0,
	}

	// Parse since (ISO8601 timestamp)
	if since := q.Get("since"); since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return nil, &paramError{param: "since", msg: "must be valid ISO8601 timestamp"}
		}

		params.since = &t
	}

	// Parse limit
	if limitStr := q.Get("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil {
			return nil, &paramError{param: "limit", msg: "must be a valid integer"}
		}

		if limit < minLimit || limit > maxLimit {
			return nil, &paramError{param: "limit", msg: "must be between 1 and 100"}
		}

		params.limit = limit
	}

	// Parse offset
	if offsetStr := q.Get("offset"); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil {
			return nil, &paramError{param: "offset", msg: "must be a valid integer"}
		}

		if offset < 0 {
			return nil, &paramError{param: "offset", msg: "must be >= 0"}
		}

		params.offset = offset
	}

	return params, nil
}

// buildIncidentFilter creates a correlation.IncidentFilter from parsed parameters.
func buildIncidentFilter(params *incidentListParams) *correlation.IncidentFilter {
	if params.since == nil {
		return nil // No filter needed
	}

	return &correlation.IncidentFilter{
		TestExecutedAfter: params.since,
	}
}

// extractJobRunIDs returns unique job run IDs from a slice of incidents.
func extractJobRunIDs(incidents []correlation.Incident) []string {
	seen := make(map[string]struct{})

	for _, inc := range incidents {
		seen[inc.JobRunID] = struct{}{}
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}

	return ids
}

// mapIncidentToSummary converts a domain Incident to an API IncidentSummary.
// The orphanDatasetSet parameter is used to determine if the incident has a correlation issue.
func mapIncidentToSummary(
	inc correlation.Incident,
	downstreamCounts map[string]int,
	orphanDatasetSet map[string]bool,
) IncidentSummary {
	return IncidentSummary{
		ID:                  strconv.FormatInt(inc.TestResultID, 10),
		TestName:            inc.TestName,
		TestType:            inc.TestType,
		TestStatus:          inc.TestStatus,
		DatasetURN:          inc.DatasetURN,
		DatasetName:         inc.DatasetName,
		Producer:            inc.ProducerName,
		JobName:             inc.JobName,
		JobRunID:            inc.JobRunID,
		DownstreamCount:     downstreamCounts[inc.JobRunID],
		HasCorrelationIssue: orphanDatasetSet[inc.DatasetURN],
		ExecutedAt:          inc.TestExecutedAt,
	}
}

// buildOrphanDatasetSet creates a set of orphan dataset URNs for O(1) lookup.
func buildOrphanDatasetSet(orphans []correlation.OrphanDataset) map[string]bool {
	set := make(map[string]bool, len(orphans))

	for _, o := range orphans {
		set[o.DatasetURN] = true
	}

	return set
}
