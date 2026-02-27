package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/correlation"
)

// Default max depth for downstream lineage traversal.
const defaultMaxDepth = 10

// handleGetIncidentDetails handles GET /api/v1/incidents/{id}.
// Returns detailed incident information with upstream and downstream lineage.
//
// Path Parameters:
//   - id: Test result ID (numeric string)
//
// Response: IncidentDetailResponse with test, dataset, job, upstream, and downstream info.
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

	response := s.assembleIncidentDetailResponse(ctx, id, correlationID, incident)

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

// assembleIncidentDetailResponse gathers upstream, downstream, orphan, and orchestration
// data for a single incident and composes the full API response. All sub-queries are
// non-fatal; partial results are returned rather than failing the request.
func (s *Server) assembleIncidentDetailResponse(
	ctx context.Context,
	id int64,
	correlationID string,
	incident *correlation.Incident,
) IncidentDetailResponse {
	downstream, err := s.correlationStore.QueryDownstreamWithParents(ctx, incident.RunID, defaultMaxDepth)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query downstream",
			"correlation_id", correlationID,
			"incident_id", id,
			"run_id", incident.RunID,
			"error", err.Error(),
		)
		// Non-fatal: continue with empty downstream
		downstream = nil
	}

	upstream, err := s.correlationStore.QueryUpstreamWithChildren(
		ctx, incident.DatasetURN, incident.RunID, defaultMaxDepth)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query upstream",
			"correlation_id", correlationID,
			"incident_id", id,
			"dataset_urn", incident.DatasetURN,
			"run_id", incident.RunID,
			"error", err.Error(),
		)
		// Non-fatal: continue with empty upstream
		upstream = nil
	}

	orphanDatasets, err := s.correlationStore.QueryOrphanDatasets(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query orphan datasets",
			"correlation_id", correlationID,
			"incident_id", id,
			"error", err.Error(),
		)
		// Non-fatal: continue with empty orphan set
		orphanDatasets = nil
	}

	orphanDatasetSet := buildOrphanDatasetSet(orphanDatasets)

	// Query orchestration chain (ancestors from root to immediate parent)
	var orchestrationChain []correlation.OrchestrationNode

	if incident.RunID != "" {
		chain, err := s.correlationStore.QueryOrchestrationChain(ctx, incident.RunID, defaultMaxDepth)
		if err != nil {
			s.logger.ErrorContext(ctx, "Failed to query orchestration chain",
				"correlation_id", correlationID,
				"incident_id", id,
				"run_id", incident.RunID,
				"error", err.Error(),
			)
			// Non-fatal: continue without orchestration chain
		} else {
			orchestrationChain = chain
		}
	}

	return mapIncidentToDetail(incident, upstream, downstream, orphanDatasetSet, orchestrationChain)
}

// mapIncidentToDetail converts a domain Incident with lineage results to API response.
// The orphanDatasetSet is used to determine the correlation status.
func mapIncidentToDetail(
	inc *correlation.Incident,
	upstream []correlation.UpstreamResult,
	downstream []correlation.DownstreamResult,
	orphanDatasetSet map[string]bool,
	orchestrationChain []correlation.OrchestrationNode,
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
			Producer:   inc.TestProducerName,
		},
		Dataset: DatasetDetail{
			URN:       inc.DatasetURN,
			Name:      inc.DatasetName,
			Namespace: inc.DatasetNS,
		},
		Upstream:          mapUpstreamResults(upstream),
		Downstream:        mapDownstreamResults(downstream),
		CorrelationStatus: determineCorrelationStatus(inc, orphanDatasetSet),
	}

	if inc.RunID != "" {
		jobStatus := inc.JobStatus
		jobCompletedAt := inc.JobCompletedAt

		// Resolve status from immediate parent when job is stuck in non-terminal state
		if isNonTerminalStatus(jobStatus) && inc.ParentJobStatus != "" && isTerminalStatus(inc.ParentJobStatus) {
			jobStatus = inc.ParentJobStatus
			jobCompletedAt = inc.ParentJobCompletedAt
		}

		response.Job = &JobDetail{
			Name:        inc.JobName,
			Namespace:   inc.JobNamespace,
			RunID:       inc.RunID,
			Producer:    inc.ProducerName,
			Status:      jobStatus,
			StartedAt:   inc.JobStartedAt,
			CompletedAt: jobCompletedAt,
		}

		if inc.ParentRunID != "" {
			response.Job.Parent = &ParentJob{
				Name:        inc.ParentJobName,
				Namespace:   inc.ParentJobNamespace,
				RunID:       inc.ParentRunID,
				Producer:    inc.ParentProducerName,
				Status:      inc.ParentJobStatus,
				CompletedAt: inc.ParentJobCompletedAt,
			}
		}

		if len(orchestrationChain) > 0 {
			response.Job.Orchestration = mapOrchestrationChain(orchestrationChain)
		}
	}

	return response
}

// isNonTerminalStatus returns true if the job status indicates it hasn't reached a final state.
func isNonTerminalStatus(status string) bool {
	return status == "RUNNING" || status == "START" || status == "OTHER" || status == ""
}

// isTerminalStatus returns true if the job status indicates it has reached a final state.
func isTerminalStatus(status string) bool {
	return status == "COMPLETE" || status == "FAIL" || status == "ABORT"
}

// mapOrchestrationChain converts domain OrchestrationNode slice to API OrchestrationNode slice.
func mapOrchestrationChain(chain []correlation.OrchestrationNode) []OrchestrationNode {
	nodes := make([]OrchestrationNode, 0, len(chain))
	for _, n := range chain {
		nodes = append(nodes, OrchestrationNode{
			Name:      n.JobName,
			Namespace: n.JobNamespace,
			RunID:     n.RunID,
			Producer:  n.ProducerName,
			Status:    n.Status,
		})
	}

	return nodes
}

// mapUpstreamResults converts domain UpstreamResult slice to API response slice.
func mapUpstreamResults(results []correlation.UpstreamResult) []UpstreamDataset {
	if len(results) == 0 {
		return []UpstreamDataset{}
	}

	datasets := make([]UpstreamDataset, 0, len(results))
	for _, r := range results {
		datasets = append(datasets, UpstreamDataset{
			URN:      r.DatasetURN,
			Name:     r.DatasetName,
			Depth:    r.Depth,
			ChildURN: r.ChildURN,
			Producer: r.Producer,
		})
	}

	return datasets
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
			Producer:  r.Producer,
		})
	}

	return datasets
}

// determineCorrelationStatus determines the correlation status of an incident.
//
// Status Logic:
//   - "unknown": No job correlation (RunID is empty)
//   - "orphan": Dataset URN aliasing issue (dataset in orphan set)
//   - "correlated": Fully correlated with data-producing job
//
// Note: "unknown" takes priority over "orphan" because it indicates a more
// fundamental correlation failure (no job association at all).
func determineCorrelationStatus(inc *correlation.Incident, orphanDatasetSet map[string]bool) string {
	// No job correlation at all
	if inc.RunID == "" {
		return CorrelationStatusUnknown
	}

	// Dataset URN aliasing issue
	if orphanDatasetSet[inc.DatasetURN] {
		return CorrelationStatusOrphan
	}

	// Fully correlated
	return CorrelationStatusCorrelated
}
