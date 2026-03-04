package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/correlator-io/correlator/internal/canonicalization"
	"github.com/correlator-io/correlator/internal/ingestion"
)

// Consumer reads OpenLineage events from a Kafka topic and stores them
// via the shared ingestion.Store pipeline. It is a transport adapter
// that sits alongside the HTTP API — both transports share the same
// validation, canonicalization, and storage logic.
//
// Only RunEvents are processed. DatasetEvents and JobEvents (design-time
// metadata without correlation value) are detected and skipped.
type Consumer struct {
	reader    *kafkago.Reader
	store     ingestion.Store
	validator *ingestion.Validator
	logger    *slog.Logger
	wg        sync.WaitGroup
}

// NewConsumer creates a Kafka consumer that reads from the configured topic
// and stores events via the ingestion pipeline.
//
//nolint:mnd
func NewConsumer(cfg *Config, store ingestion.Store, validator *ingestion.Validator, logger *slog.Logger) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,    // fetch even a single message
		MaxBytes: 10e6, // 10 MB max fetch
	})

	return &Consumer{
		reader:    reader,
		store:     store,
		validator: validator,
		logger:    logger,
	}
}

// Start starts the Kafka consumer in a background goroutine and returns an
// error channel. This mirrors api.Server.ListenAndServe() for a symmetric
// subsystem lifecycle pattern. The consumer runs until the context is cancelled.
//
// Usage:
//
//	consumerErrors := consumer.Start(ctx)
//	select {
//	case err := <-consumerErrors:
//	    // fatal consumer error
//	case <-stopSignal:
//	    consumer.Close()
//	}
func (c *Consumer) Start(ctx context.Context) <-chan error {
	consumerErrors := make(chan error, 1)

	go func() {
		if err := c.run(ctx); err != nil {
			consumerErrors <- err
		}
	}()

	return consumerErrors
}

// Close waits for in-flight message processing to complete, then closes
// the Kafka reader (which commits final offsets).
func (c *Consumer) Close() error {
	c.wg.Wait()

	if err := c.reader.Close(); err != nil {
		c.logger.Error("Failed to close Kafka reader", slog.String("error", err.Error()))

		return err
	}

	c.logger.Info("Kafka consumer closed")

	return nil
}

// run is the internal consumer loop. It blocks until the context is cancelled
// or an unrecoverable error occurs. Individual message failures (malformed
// JSON, validation errors) are logged and skipped — the consumer continues
// processing subsequent messages.
//
// Returns nil on graceful shutdown (context cancelled), or an error if
// FetchMessage fails for non-context reasons (broker auth failure, etc.).
func (c *Consumer) run(ctx context.Context) error {
	c.logger.Info("Kafka consumer started",
		slog.String("topic", c.reader.Config().Topic),
		slog.String("group_id", c.reader.Config().GroupID),
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// Context cancelled = graceful shutdown
			if ctx.Err() != nil {
				c.logger.Info("Kafka consumer stopping (context cancelled)")

				return nil //nolint:nilerr
			}

			return fmt.Errorf("fetch message: %w", err)
		}

		func() {
			c.wg.Add(1)
			defer c.wg.Done()

			c.processMessage(ctx, msg)
		}()
	}
}

// processMessage handles a single Kafka message: detects event type,
// deserializes RunEvents, validates, and stores. Non-RunEvents and
// invalid messages are skipped.
func (c *Consumer) processMessage(ctx context.Context, msg kafkago.Message) {
	// Event type detection: only process RunEvents
	if !isRunEvent(msg.Value) {
		c.logger.Debug("Skipping non-RunEvent message",
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
		)

		c.commitMessage(ctx, msg)

		return
	}

	// Deserialize to domain model
	event, err := parseRunEvent(msg.Value)
	if err != nil {
		c.logger.Warn("Failed to parse RunEvent from Kafka message",
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("error", err.Error()),
		)

		c.commitMessage(ctx, msg)

		return
	}

	// Validate
	if err := c.validator.ValidateRunEvent(event); err != nil {
		c.logger.Warn("RunEvent validation failed",
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("run_id", event.Run.ID),
			slog.String("error", err.Error()),
		)

		c.commitMessage(ctx, msg)

		return
	}

	// Store via shared ingestion pipeline
	stored, duplicate, err := c.store.StoreEvent(ctx, event)
	if err != nil {
		c.logger.Error("Failed to store Kafka event",
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("run_id", event.Run.ID),
			slog.String("error", err.Error()),
		)

		// Don't commit — message will be redelivered (at-least-once semantics).
		return
	}

	c.logger.Info("Kafka event processed",
		slog.String("run_id", event.Run.ID),
		slog.String("event_type", string(event.EventType)),
		slog.String("job", event.Job.Namespace+"/"+event.Job.Name),
		slog.Bool("stored", stored),
		slog.Bool("duplicate", duplicate),
	)

	c.commitMessage(ctx, msg)
}

// commitMessage commits the message offset. Errors are logged but do not
// stop the consumer — the message will be redelivered on restart (at-least-once).
func (c *Consumer) commitMessage(ctx context.Context, msg kafkago.Message) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("Failed to commit Kafka offset",
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("error", err.Error()),
		)
	}
}

// eventTypeProbe is a minimal struct for detecting whether a Kafka message
// is a RunEvent. Only RunEvents have an "eventType" field in the OL spec.
// DatasetEvents and JobEvents lack this field.
type eventTypeProbe struct {
	EventType *string `json:"eventType"`
}

// isRunEvent checks if a raw JSON message is an OpenLineage RunEvent
// by looking for the "eventType" field. This is a cheap partial unmarshal
// that avoids full deserialization for non-RunEvent messages.
//
// Returns false for DatasetEvents, JobEvents, malformed JSON, or nil input.
func isRunEvent(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	var probe eventTypeProbe
	if err := json.Unmarshal(data, &probe); err != nil {
		return false
	}

	return probe.EventType != nil
}

// rawRunEvent is a JSON-tagged struct matching the OpenLineage RunEvent wire format.
// This is local to the kafka package (Option A from the implementation plan) to
// avoid importing api types and keep transport adapters independent.
type rawRunEvent struct {
	EventType string       `json:"eventType"`
	EventTime string       `json:"eventTime"`
	Producer  string       `json:"producer"`
	SchemaURL string       `json:"schemaURL"` //nolint:tagliatelle // OpenLineage spec uses schemaURL
	Run       rawRun       `json:"run"`
	Job       rawJob       `json:"job"`
	Inputs    []rawDataset `json:"inputs,omitempty"`
	Outputs   []rawDataset `json:"outputs,omitempty"`
}

type rawRun struct {
	ID     string                 `json:"runId"`
	Facets map[string]interface{} `json:"facets,omitempty"`
}

type rawJob struct {
	Namespace string                 `json:"namespace"`
	Name      string                 `json:"name"`
	Facets    map[string]interface{} `json:"facets,omitempty"`
}

type rawDataset struct {
	Namespace    string                 `json:"namespace"`
	Name         string                 `json:"name"`
	Facets       map[string]interface{} `json:"facets,omitempty"`
	InputFacets  map[string]interface{} `json:"inputFacets,omitempty"`
	OutputFacets map[string]interface{} `json:"outputFacets,omitempty"`
}

// parseRunEvent deserializes a raw JSON Kafka message into an ingestion.RunEvent.
// Performs the same mapping as the HTTP handler's mapLineageRequest: whitespace
// trimming, dataset URN normalization, and nil facet initialization.
func parseRunEvent(data []byte) (*ingestion.RunEvent, error) {
	var raw rawRunEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	event := &ingestion.RunEvent{
		EventType: ingestion.EventType(strings.TrimSpace(raw.EventType)),
		EventTime: ingestion.ParseEventTime(raw.EventTime),
		Producer:  strings.TrimSpace(raw.Producer),
		SchemaURL: strings.TrimSpace(raw.SchemaURL),
		Run:       mapRun(&raw.Run),
		Job:       mapJob(&raw.Job),
		Inputs:    mapDatasets(raw.Inputs),
		Outputs:   mapDatasets(raw.Outputs),
	}

	return event, nil
}

func mapRun(r *rawRun) ingestion.Run {
	facets := r.Facets
	if facets == nil {
		facets = make(map[string]interface{})
	}

	return ingestion.Run{
		ID:     strings.TrimSpace(r.ID),
		Facets: facets,
	}
}

func mapJob(j *rawJob) ingestion.Job {
	facets := j.Facets
	if facets == nil {
		facets = make(map[string]interface{})
	}

	return ingestion.Job{
		Namespace: strings.TrimSpace(j.Namespace),
		Name:      strings.TrimSpace(j.Name),
		Facets:    facets,
	}
}

func mapDatasets(raw []rawDataset) []ingestion.Dataset {
	if raw == nil {
		return []ingestion.Dataset{}
	}

	datasets := make([]ingestion.Dataset, len(raw))

	for i, r := range raw {
		namespace := strings.TrimSpace(r.Namespace)
		name := strings.TrimSpace(r.Name)

		// Normalize dataset URN (postgres:// → postgresql://, remove default ports, etc.)
		if namespace != "" && name != "" {
			normalizedURN := canonicalization.GenerateDatasetURN(namespace, name)

			normalizedNamespace, normalizedName, err := canonicalization.ParseDatasetURN(normalizedURN)
			if err == nil {
				namespace = normalizedNamespace
				name = normalizedName
			}
		}

		facets := r.Facets
		if facets == nil {
			facets = make(map[string]interface{})
		}

		inputFacets := r.InputFacets
		if inputFacets == nil {
			inputFacets = make(map[string]interface{})
		}

		outputFacets := r.OutputFacets
		if outputFacets == nil {
			outputFacets = make(map[string]interface{})
		}

		datasets[i] = ingestion.Dataset{
			Namespace:    namespace,
			Name:         name,
			Facets:       facets,
			InputFacets:  inputFacets,
			OutputFacets: outputFacets,
		}
	}

	return datasets
}
