package api

import (
	"context"
	"time"

	"github.com/correlator-io/correlator/internal/health"
	"github.com/correlator-io/correlator/internal/ingestion"
)

// KafkaHealthChecker is the interface the API layer uses to check Kafka health.
// Defined here (consumer in api package) following the Dependency Inversion Principle:
// the consumer defines what it needs, the infrastructure provides it.
//
// Implemented by: kafka.Consumer.
type KafkaHealthChecker interface {
	HealthCheck(ctx context.Context) *health.ComponentResult
}

const (
	statusHealthy   = "healthy"
	statusDegraded  = "degraded"
	statusUnhealthy = "unhealthy"
	statusDisabled  = "disabled"

	componentPostgres = "postgres"
	componentKafka    = "kafka"
)

// JSON response types for GET /health — transport-layer only.

// systemHealthResponse is the top-level JSON response for GET /health.
type systemHealthResponse struct {
	Status      string                             `json:"status"`
	ServiceName string                             `json:"serviceName"`
	Version     string                             `json:"version"`
	Uptime      string                             `json:"uptime,omitempty"`
	Checks      map[string]*componentCheckResponse `json:"checks"`
}

// componentCheckResponse is the JSON representation of a single dependency check.
type componentCheckResponse struct {
	Status    string                `json:"status"`
	LatencyMs int64                 `json:"latency_ms"` //nolint:tagliatelle
	Error     string                `json:"error,omitempty"`
	Details   *kafkaDetailsResponse `json:"details,omitempty"`
}

// kafkaDetailsResponse is the JSON representation of Kafka-specific diagnostics.
type kafkaDetailsResponse struct {
	Brokers       string `json:"brokers"`
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumer_group"`    //nolint:tagliatelle
	Messages      int64  `json:"messages_consumed"` //nolint:tagliatelle
	Errors        int64  `json:"errors"`
	Rebalances    int64  `json:"rebalances"`
}

// SystemHealth is the internal (non-JSON) aggregated health result.
type SystemHealth struct {
	Status      string
	ServiceName string
	Version     string
	Uptime      string
	Checks      map[string]*health.ComponentResult
}

// HealthChecker aggregates health checks for all critical-path dependencies.
type HealthChecker struct {
	store ingestion.Store
	kafka KafkaHealthChecker
}

// NewHealthChecker creates a health checker. kafkaChecker may be nil when Kafka is disabled.
func NewHealthChecker(store ingestion.Store, kafkaChecker KafkaHealthChecker) *HealthChecker {
	return &HealthChecker{
		store: store,
		kafka: kafkaChecker,
	}
}

// Check runs all dependency health checks and returns the aggregated result.
func (h *HealthChecker) Check(ctx context.Context) *SystemHealth {
	checks := make(map[string]*health.ComponentResult, 2) //nolint:mnd

	checks[componentPostgres] = h.checkPostgres(ctx)
	checks[componentKafka] = h.checkKafka(ctx)

	status := h.deriveStatus(checks)

	return &SystemHealth{
		Status:      status,
		ServiceName: "correlator",
		Checks:      checks,
	}
}

// toResponse maps the internal SystemHealth to a JSON-serializable response.
func (s *SystemHealth) toResponse() *systemHealthResponse {
	checks := make(map[string]*componentCheckResponse, len(s.Checks))

	for name, result := range s.Checks {
		checks[name] = mapComponentResult(result)
	}

	return &systemHealthResponse{
		Status:      s.Status,
		ServiceName: s.ServiceName,
		Version:     s.Version,
		Uptime:      s.Uptime,
		Checks:      checks,
	}
}

func mapComponentResult(r *health.ComponentResult) *componentCheckResponse {
	resp := &componentCheckResponse{
		Status:    r.Status,
		LatencyMs: r.LatencyMs,
		Error:     r.Error,
	}

	if d, ok := r.Details.(*health.KafkaDetails); ok {
		resp.Details = &kafkaDetailsResponse{
			Brokers:       d.Brokers,
			Topic:         d.Topic,
			ConsumerGroup: d.ConsumerGroup,
			Messages:      d.Messages,
			Errors:        d.Errors,
			Rebalances:    d.Rebalances,
		}
	}

	return resp
}

func (h *HealthChecker) checkPostgres(ctx context.Context) *health.ComponentResult {
	start := time.Now()

	err := h.store.HealthCheck(ctx)
	latencyMs := time.Since(start).Milliseconds()

	if err != nil {
		return &health.ComponentResult{
			Status:    statusUnhealthy,
			LatencyMs: latencyMs,
			Error:     err.Error(),
		}
	}

	return &health.ComponentResult{
		Status:    statusHealthy,
		LatencyMs: latencyMs,
	}
}

func (h *HealthChecker) checkKafka(ctx context.Context) *health.ComponentResult {
	if h.kafka == nil {
		return &health.ComponentResult{
			Status: statusDisabled,
		}
	}

	return h.kafka.HealthCheck(ctx)
}

// deriveStatus computes the overall system status from individual checks.
//
// Rules:
//   - DB unhealthy → "unhealthy" (nothing works without the database)
//   - Any non-disabled check unhealthy but DB up → "degraded" (HTTP ingestion still works)
//   - All checks healthy or disabled → "healthy"
func (h *HealthChecker) deriveStatus(checks map[string]*health.ComponentResult) string {
	pgCheck, pgExists := checks[componentPostgres]
	if pgExists && pgCheck.Status == statusUnhealthy {
		return statusUnhealthy
	}

	for _, check := range checks {
		if check.Status == statusUnhealthy {
			return statusDegraded
		}
	}

	return statusHealthy
}
