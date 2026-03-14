package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/health"
	"github.com/correlator-io/correlator/internal/storage"
)

// mockKafkaHealthChecker implements KafkaHealthChecker for testing without a real Kafka broker.
type mockKafkaHealthChecker struct {
	result *health.ComponentResult
}

func (m *mockKafkaHealthChecker) HealthCheck(_ context.Context) *health.ComponentResult {
	return m.result
}

func setupHealthTestServer(
	ctx context.Context, t *testing.T, kafkaChecker KafkaHealthChecker,
) (*Server, *config.TestDatabase) {
	t.Helper()

	testDB := config.SetupTestDatabase(ctx, t)
	storageConn := storage.WrapConnection(testDB.Connection)

	lineageStore, err := storage.NewLineageStore(storageConn, 1*time.Hour) //nolint:contextcheck
	require.NoError(t, err, "Failed to create lineage store")

	serverConfig := &ServerConfig{
		Port:               8080,
		Host:               "localhost",
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		ShutdownTimeout:    30 * time.Second,
		LogLevel:           slog.LevelInfo,
		MaxRequestSize:     defaultMaxRequestSize,
		CORSAllowedOrigins: []string{"*"},
		CORSAllowedMethods: []string{"GET"},
		CORSAllowedHeaders: []string{"Content-Type"},
		CORSMaxAge:         86400,
	}

	server := NewServer(serverConfig, Dependencies{
		IngestionStore:   lineageStore,
		CorrelationStore: lineageStore,
		ResolutionStore:  lineageStore,
		KafkaHealth:      kafkaChecker,
	})

	t.Cleanup(func() {
		_ = lineageStore.Close()
		_ = testDB.Connection.Close()
		_ = testcontainers.TerminateContainer(testDB.Container)
	})

	return server, testDB
}

func TestHealthEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Run("Healthy With Postgres Up And Kafka Disabled", func(t *testing.T) {
		server, _ := setupHealthTestServer(ctx, t, nil)
		server.startTime = time.Now().Add(-1 * time.Hour)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp systemHealthResponse

		err := json.Unmarshal(rr.Body.Bytes(), &resp)
		require.NoError(t, err)

		assert.Equal(t, statusHealthy, resp.Status)
		assert.Equal(t, "correlator", resp.ServiceName)
		assert.NotEmpty(t, resp.Version)
		assert.NotEmpty(t, resp.Uptime)

		pgCheck := resp.Checks[componentPostgres]
		require.NotNil(t, pgCheck, "Expected postgres check")
		assert.Equal(t, statusHealthy, pgCheck.Status)
		assert.Positive(t, pgCheck.LatencyMs+1)
		assert.Empty(t, pgCheck.Error)

		kafkaCheck := resp.Checks[componentKafka]
		require.NotNil(t, kafkaCheck, "Expected kafka check")
		assert.Equal(t, statusDisabled, kafkaCheck.Status)
	})

	t.Run("Degraded When Kafka Unhealthy", func(t *testing.T) {
		mockKafka := &mockKafkaHealthChecker{
			result: &health.ComponentResult{
				Status:    "unhealthy",
				LatencyMs: 2001,
				Error:     "broker unreachable: dial tcp kafka:9092: connect: connection refused",
				Details: &health.KafkaDetails{
					Brokers:       "kafka:9092",
					Topic:         "openlineage.events",
					ConsumerGroup: "correlator",
					Messages:      0,
					Errors:        3,
				},
			},
		}
		server, _ := setupHealthTestServer(ctx, t, mockKafka)
		server.startTime = time.Now().Add(-10 * time.Minute)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp systemHealthResponse

		err := json.Unmarshal(rr.Body.Bytes(), &resp)
		require.NoError(t, err)

		assert.Equal(t, statusDegraded, resp.Status)

		pgCheck := resp.Checks[componentPostgres]
		assert.Equal(t, statusHealthy, pgCheck.Status)

		kafkaCheck := resp.Checks[componentKafka]
		assert.Equal(t, "unhealthy", kafkaCheck.Status)
		assert.Contains(t, kafkaCheck.Error, "broker unreachable")
		require.NotNil(t, kafkaCheck.Details, "Expected kafka details")
		assert.Equal(t, "openlineage.events", kafkaCheck.Details.Topic)
		assert.Equal(t, int64(3), kafkaCheck.Details.Errors)
	})

	t.Run("Unhealthy Returns 503 When Postgres Down", func(t *testing.T) {
		server, testDB := setupHealthTestServer(ctx, t, nil)
		server.startTime = time.Now().Add(-5 * time.Minute)

		err := testDB.Connection.Close()
		require.NoError(t, err, "Failed to close test database")

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)

		var resp systemHealthResponse

		err = json.Unmarshal(rr.Body.Bytes(), &resp)
		require.NoError(t, err)

		assert.Equal(t, statusUnhealthy, resp.Status)

		pgCheck := resp.Checks[componentPostgres]
		assert.Equal(t, "unhealthy", pgCheck.Status)
		assert.NotEmpty(t, pgCheck.Error)
	})

	t.Run("Healthy With Kafka Healthy", func(t *testing.T) {
		mockKafka := &mockKafkaHealthChecker{
			result: &health.ComponentResult{
				Status:    "healthy",
				LatencyMs: 3,
				Details: &health.KafkaDetails{
					Brokers:       "kafka:9092",
					Topic:         "openlineage.events",
					ConsumerGroup: "correlator",
					Messages:      142,
					Errors:        0,
				},
			},
		}
		server, _ := setupHealthTestServer(ctx, t, mockKafka)
		server.startTime = time.Now().Add(-4 * time.Hour)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)

		var resp systemHealthResponse

		err := json.Unmarshal(rr.Body.Bytes(), &resp)
		require.NoError(t, err)

		assert.Equal(t, statusHealthy, resp.Status)

		pgCheck := resp.Checks[componentPostgres]
		assert.Equal(t, statusHealthy, pgCheck.Status)

		kafkaCheck := resp.Checks[componentKafka]
		assert.Equal(t, statusHealthy, kafkaCheck.Status)
		assert.Equal(t, int64(3), kafkaCheck.LatencyMs)
		require.NotNil(t, kafkaCheck.Details, "Expected kafka details")
		assert.Equal(t, int64(142), kafkaCheck.Details.Messages)
	})
}

func TestReadyEndpointUsesIngestionStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	t.Run("Ready Returns 200 Without Auth Configured", func(t *testing.T) {
		server, _ := setupHealthTestServer(ctx, t, nil)

		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "ready", rr.Body.String())
	})

	t.Run("Ready Returns 503 Without Auth When DB Down", func(t *testing.T) {
		server, testDB := setupHealthTestServer(ctx, t, nil)

		err := testDB.Connection.Close()
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rr := httptest.NewRecorder()
		server.httpServer.Handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
		assert.Equal(t, "storage unavailable", rr.Body.String())
	})
}
