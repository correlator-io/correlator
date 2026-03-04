package kafka_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/kafka"

	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/correlator-io/correlator/internal/config"
	"github.com/correlator-io/correlator/internal/ingestion"
	correlatorKafka "github.com/correlator-io/correlator/internal/kafka"
	"github.com/correlator-io/correlator/internal/storage"
)

const (
	testTopic   = "openlineage.events.test"
	testGroupID = "correlator-test"
)

func TestKafkaConsumerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container with migrations
	testDB := config.SetupTestDatabase(ctx, t)
	t.Cleanup(func() {
		_ = testDB.Connection.Close()
		_ = testDB.Container.Terminate(ctx)
	})

	// Start Kafka container
	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:7.6.0")
	require.NoError(t, err, "Failed to start Kafka container")

	t.Cleanup(func() {
		_ = kafkaContainer.Terminate(ctx)
	})

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err, "Failed to get Kafka brokers")

	// Create topic
	createTestTopic(t, brokers)

	// Create LineageStore
	storageConn := wrapDBConnection(t, testDB.Connection)
	lineageStore := createLineageStore(t, storageConn)

	// Run integration tests
	t.Run(
		"RunEvent_StoredSuccessfully", testRunEventStoredSuccessfully(
			ctx, t, brokers, lineageStore, testDB.Connection,
		),
	)
	t.Run(
		"DuplicateRunEvent_Idempotent", testDuplicateRunEventIdempotent(
			ctx, t, brokers, lineageStore, testDB.Connection,
		),
	)
	t.Run(
		"DatasetEvent_Skipped", testDatasetEventSkipped(
			ctx, t, brokers, lineageStore, testDB.Connection,
		),
	)
	t.Run(
		"MalformedMessage_Skipped", testMalformedMessageSkipped(
			ctx, t, brokers, lineageStore,
		),
	)
	t.Run(
		"MultipleRunEvents_AllStored", testMultipleRunEventsAllStored(
			ctx, t, brokers, lineageStore, testDB.Connection,
		),
	)
}

func testRunEventStoredSuccessfully(
	ctx context.Context,
	_ *testing.T,
	brokers []string,
	store *storage.LineageStore,
	db *sql.DB,
) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		runID := uuid.New().String()
		event := makeRunEvent(runID, "START", "test-namespace", "test-job")

		// Publish to Kafka
		publishMessage(ctx, t, brokers, event)

		// Start consumer with timeout
		consumer := createConsumer(t, brokers, store)
		waitForConsumption(ctx, t, consumer, db, runID)
		stopConsumer(t, consumer)

		// Verify event stored in PostgreSQL
		assertJobRunExists(ctx, t, db, runID, "START")
	}
}

func testDuplicateRunEventIdempotent(
	ctx context.Context,
	_ *testing.T,
	brokers []string,
	store *storage.LineageStore,
	db *sql.DB,
) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		runID := uuid.New().String()
		event := makeRunEvent(runID, "COMPLETE", "dup-namespace", "dup-job")

		// Publish same event twice
		publishMessage(ctx, t, brokers, event)
		publishMessage(ctx, t, brokers, event)

		// Start consumer — both messages should be processed (one stored, one deduped)
		consumer := createConsumer(t, brokers, store)
		waitForConsumption(ctx, t, consumer, db, runID)
		stopConsumer(t, consumer)

		// Verify only one row exists
		count := countJobRuns(ctx, t, db, runID)
		assert.Equal(t, 1, count, "Expected exactly 1 row for duplicate events")
	}
}

func testDatasetEventSkipped(
	ctx context.Context,
	_ *testing.T,
	brokers []string,
	store *storage.LineageStore,
	db *sql.DB,
) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		// DatasetEvent has no eventType field
		datasetEvent := map[string]interface{}{
			"dataset": map[string]interface{}{
				"namespace": "postgresql://db:5432",
				"name":      "public.users",
			},
			"producer":  "test",
			"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		}

		// Publish DatasetEvent followed by a RunEvent (sentinel)
		publishMessage(ctx, t, brokers, datasetEvent)

		sentinelRunID := uuid.New().String()
		sentinelEvent := makeRunEvent(
			sentinelRunID, "START", "sentinel-ns", "sentinel-job",
		)
		publishMessage(ctx, t, brokers, sentinelEvent)

		// Start consumer — should skip DatasetEvent, process sentinel
		consumer := createConsumer(t, brokers, store)
		waitForConsumption(ctx, t, consumer, db, sentinelRunID)
		stopConsumer(t, consumer)

		// Sentinel should be stored
		assertJobRunExists(ctx, t, db, sentinelRunID, "START")
	}
}

func testMalformedMessageSkipped(
	ctx context.Context,
	_ *testing.T,
	brokers []string,
	store *storage.LineageStore,
) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		// Publish malformed JSON
		writer := &kafkago.Writer{
			Addr:  kafkago.TCP(brokers...),
			Topic: testTopic,
		}

		err := writer.WriteMessages(ctx, kafkago.Message{
			Value: []byte(`this is not valid JSON`),
		})
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		// Consumer should not crash — just start and stop
		consumer := createConsumer(t, brokers, store)

		// Give it a moment to process the malformed message
		time.Sleep(2 * time.Second)
		stopConsumer(t, consumer)
	}
}

func testMultipleRunEventsAllStored(
	ctx context.Context,
	_ *testing.T,
	brokers []string,
	store *storage.LineageStore,
	db *sql.DB,
) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		const eventCount = 5

		runIDs := make([]string, eventCount)

		for i := range eventCount {
			runIDs[i] = uuid.New().String()
			event := makeRunEvent(runIDs[i], "START", "multi-ns", "multi-job")
			publishMessage(ctx, t, brokers, event)
		}

		// Start consumer and wait for last event
		consumer := createConsumer(t, brokers, store)
		waitForConsumption(ctx, t, consumer, db, runIDs[eventCount-1])
		stopConsumer(t, consumer)

		// Verify all events stored
		for _, runID := range runIDs {
			assertJobRunExists(ctx, t, db, runID, "START")
		}
	}
}

// --- Helpers ---

func makeRunEvent(runID, eventType, namespace, jobName string) map[string]interface{} {
	return map[string]interface{}{
		"eventType": eventType,
		"eventTime": time.Now().UTC().Format(time.RFC3339),
		"producer":  "https://github.com/OpenLineage/OpenLineage/tree/1.39.0/integration/dbt",
		"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
		"run":       map[string]interface{}{"runId": runID},
		"job":       map[string]interface{}{"namespace": namespace, "name": jobName},
		"inputs":    []interface{}{},
		"outputs":   []interface{}{},
	}
}

func publishMessage(ctx context.Context, t *testing.T, brokers []string, event map[string]interface{}) {
	t.Helper()

	data, err := json.Marshal(event)
	require.NoError(t, err)

	writer := &kafkago.Writer{
		Addr:  kafkago.TCP(brokers...),
		Topic: testTopic,
	}

	err = writer.WriteMessages(ctx, kafkago.Message{Value: data})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

func createTestTopic(t *testing.T, brokers []string) {
	t.Helper()

	conn, err := kafkago.Dial("tcp", brokers[0])
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	err = conn.CreateTopics(kafkago.TopicConfig{
		Topic:             testTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
}

func createConsumer(t *testing.T, brokers []string, store *storage.LineageStore) *correlatorKafka.Consumer {
	t.Helper()

	cfg := &correlatorKafka.Config{
		Enabled: true,
		Brokers: brokers,
		Topic:   testTopic,
		GroupID: testGroupID + "-" + uuid.New().String()[:8], // unique group per test
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	validator := ingestion.NewValidator()

	consumer := correlatorKafka.NewConsumer(cfg, store, validator, logger)

	return consumer
}

func waitForConsumption(
	ctx context.Context,
	t *testing.T,
	consumer *correlatorKafka.Consumer,
	db *sql.DB, runID string,
) {
	t.Helper()

	consumerCtx, cancel := context.WithCancel(ctx)

	// Start consumer (non-blocking, mirrors production usage)
	_ = consumer.Start(consumerCtx)

	// Poll for the sentinel run to appear in the DB
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)

	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("Timed out waiting for run_id %s to appear in database", runID)
		case <-ticker.C:
			if countJobRuns(ctx, t, db, runID) > 0 {
				cancel()

				return
			}
		}
	}
}

func stopConsumer(t *testing.T, consumer *correlatorKafka.Consumer) {
	t.Helper()

	err := consumer.Close()
	assert.NoError(t, err, "Consumer close should not error")
}

func assertJobRunExists(ctx context.Context, t *testing.T, db *sql.DB, runID, expectedEventType string) {
	t.Helper()

	var eventType string

	err := db.QueryRowContext(
		ctx, "SELECT event_type FROM job_runs WHERE run_id = $1", runID).Scan(&eventType)
	require.NoError(t, err, "Expected job_run with run_id %s to exist", runID)
	assert.Equal(t, expectedEventType, eventType)
}

func countJobRuns(ctx context.Context, t *testing.T, db *sql.DB, runID string) int {
	t.Helper()

	var count int

	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE run_id = $1", runID).Scan(&count)
	require.NoError(t, err)

	return count
}

// wrapDBConnection wraps a *sql.DB into a storage.Connection for LineageStore.
func wrapDBConnection(t *testing.T, db *sql.DB) *storage.Connection {
	t.Helper()

	conn := storage.WrapConnection(db)

	return conn
}

func createLineageStore(t *testing.T, conn *storage.Connection) *storage.LineageStore {
	t.Helper()

	store, err := storage.NewLineageStore(conn, 1*time.Hour)
	require.NoError(t, err)

	return store
}
