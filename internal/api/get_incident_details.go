package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/correlation"
)

// Default max depth for downstream lineage traversal.
const defaultMaxDepth = 10

// handleGetIncidentDetails handles GET /api/v1/incidents/{id}.
// Returns detailed incident information with downstream impact.
//
// Path Parameters:
//   - id: Test result ID (numeric string)
//
// Response: IncidentDetailResponse with test, dataset, job, and downstream info.
func (s *Server) handleGetIncidentDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	correlationID := middleware.GetCorrelationID(ctx)

	idStr := r.PathValue("id")
	if idStr == "" {
		WriteErrorResponse(w, r, s.logger, BadRequest("Missing incident ID"))

		return
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		WriteErrorResponse(w, r, s.logger, BadRequest("Invalid incident ID: must be a numeric value"))

		return
	}

	incident, err := s.correlationStore.QueryIncidentByID(ctx, id)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query incident",
			"correlation_id", correlationID,
			"incident_id", id,
			"error", err.Error(),
		)
		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to query incident"))

		return
	}

	if incident == nil {
		WriteErrorResponse(w, r, s.logger, NotFound("Incident not found"))

		return
	}

	downstream, err := s.correlationStore.QueryDownstreamWithParents(ctx, incident.JobRunID, defaultMaxDepth)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query downstream",
			"correlation_id", correlationID,
			"incident_id", id,
			"job_run_id", incident.JobRunID,
			"error", err.Error(),
		)
		// Non-fatal: continue with empty downstream
		downstream = nil
	}

	orphanNamespaces, err := s.correlationStore.QueryOrphanNamespaces(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query orphan namespaces",
			"correlation_id", correlationID,
			"incident_id", id,
			"error", err.Error(),
		)
		// Non-fatal: continue with empty orphan set
		orphanNamespaces = nil
	}

	orphanNSSet := buildOrphanNamespaceSet(orphanNamespaces)

	response := mapIncidentToDetail(incident, downstream, orphanNSSet)

	data, err := json.Marshal(response)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to marshal incident response",
			"correlation_id", correlationID,
			"incident_id", id,
			"error", err.Error(),
		)
		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to encode response"))

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// mapIncidentToDetail converts a domain Incident and downstream results to API response.
// The orphanNSSet is used to determine the correlation status.
func mapIncidentToDetail(
	inc *correlation.Incident,
	downstream []correlation.DownstreamResult,
	orphanNSSet map[string]bool,
) IncidentDetailResponse {
	response := IncidentDetailResponse{
		ID: strconv.FormatInt(inc.TestResultID, 10),
		Test: TestDetail{
			Name:       inc.TestName,
			Type:       inc.TestType,
			Status:     inc.TestStatus,
			Message:    inc.TestMessage,
			ExecutedAt: inc.TestExecutedAt,
			DurationMs: inc.TestDurationMs,
		},
		Dataset: DatasetDetail{
			URN:       inc.DatasetURN,
			Name:      inc.DatasetName,
			Namespace: inc.DatasetNS,
		},
		Downstream:        mapDownstreamResults(downstream),
		CorrelationStatus: determineCorrelationStatus(inc, orphanNSSet),
	}

	// Include job details if correlated
	if inc.JobRunID != "" {
		response.Job = &JobDetail{
			Name:        inc.JobName,
			Namespace:   inc.JobNamespace,
			RunID:       inc.JobRunID,
			Producer:    inc.ProducerName,
			Status:      inc.JobStatus,
			StartedAt:   inc.JobStartedAt,
			CompletedAt: inc.JobCompletedAt,
		}
	}

	return response
}

// mapDownstreamResults converts domain DownstreamResult slice to API response slice.
func mapDownstreamResults(results []correlation.DownstreamResult) []DownstreamDataset {
	if len(results) == 0 {
		return []DownstreamDataset{}
	}

	datasets := make([]DownstreamDataset, 0, len(results))
	for _, r := range results {
		datasets = append(datasets, DownstreamDataset{
			URN:       r.DatasetURN,
			Name:      r.DatasetName,
			Depth:     r.Depth,
			ParentURN: r.ParentURN,
		})
	}

	return datasets
}

// determineCorrelationStatus determines the correlation status of an incident.
//
// Status Logic:
//   - "unknown": No job correlation (JobRunID is empty)
//   - "orphan": Namespace aliasing issue (namespace in orphan set)
//   - "correlated": Fully correlated with data-producing job
//
// Note: "unknown" takes priority over "orphan" because it indicates a more
// fundamental correlation failure (no job association at all).
func determineCorrelationStatus(inc *correlation.Incident, orphanNSSet map[string]bool) string {
	// No job correlation at all
	if inc.JobRunID == "" {
		return CorrelationStatusUnknown
	}

	// Namespace aliasing issue
	if orphanNSSet[inc.DatasetNS] {
		return CorrelationStatusOrphan
	}

	// Fully correlated
	return CorrelationStatusCorrelated
}
