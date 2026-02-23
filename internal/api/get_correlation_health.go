package api

import (
	"encoding/json"
	"net/http"

	"github.com/correlator-io/correlator/internal/api/middleware"
	"github.com/correlator-io/correlator/internal/correlation"
)

// handleGetCorrelationHealth handles GET /api/v1/health/correlation.
// Returns correlation system health metrics including orphan namespaces.
//
// Response: CorrelationHealthResponse with:
//   - correlation_rate: 0.0-1.0 (correlated incidents / total incidents)
//   - total_datasets: Count of distinct datasets with test results
//   - orphan_namespaces: List of namespaces requiring alias configuration
//
// Correlation Rate Calculation:
//   - correlated_incidents = incidents with lineage edges (in incident_correlation_view)
//   - total_incidents = ALL failed/error test results
//   - If total_incidents = 0, returns 1.0 (no incidents = healthy)
func (s *Server) handleGetCorrelationHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	correlationID := middleware.GetCorrelationID(ctx)

	health, err := s.correlationStore.QueryCorrelationHealth(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to query correlation health",
			"correlation_id", correlationID,
			"error", err.Error(),
		)
		WriteErrorResponse(w, r, s.logger, InternalServerError("Failed to query correlation health"))

		return
	}

	response := mapHealthToResponse(health)

	data, err := json.Marshal(response)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to marshal correlation health response",
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

// mapHealthToResponse converts a domain Health to an API CorrelationHealthResponse.
func mapHealthToResponse(health *correlation.Health) CorrelationHealthResponse {
	orphans := make([]OrphanDatasetResponse, 0, len(health.OrphanDatasets))

	for _, o := range health.OrphanDatasets {
		response := OrphanDatasetResponse{
			DatasetURN: o.DatasetURN,
			TestCount:  o.TestCount,
			LastSeen:   o.LastSeen,
		}

		if o.LikelyMatch != nil {
			response.LikelyMatch = &DatasetMatchResponse{
				DatasetURN:  o.LikelyMatch.DatasetURN,
				Confidence:  o.LikelyMatch.Confidence,
				MatchReason: o.LikelyMatch.MatchReason,
			}
		}

		orphans = append(orphans, response)
	}

	patterns := make([]SuggestedPatternResponse, 0, len(health.SuggestedPatterns))

	for _, p := range health.SuggestedPatterns {
		patterns = append(patterns, SuggestedPatternResponse{
			Pattern:         p.Pattern,
			Canonical:       p.Canonical,
			ResolvesCount:   p.ResolvesCount,
			OrphansResolved: p.OrphansResolved,
		})
	}

	return CorrelationHealthResponse{
		CorrelationRate:    health.CorrelationRate,
		TotalDatasets:      health.TotalDatasets,
		ProducedDatasets:   health.ProducedDatasets,
		CorrelatedDatasets: health.CorrelatedDatasets,
		OrphanDatasets:     orphans,
		SuggestedPatterns:  patterns,
	}
}
