// Package kafka provides a Kafka consumer adapter for ingesting standard OpenLineage events.
//
// This package is a transport adapter that sits alongside internal/api (HTTP transport).
// Both transports share the same ingestion pipeline: validate → store via ingestion.Store.
//
// OpenLineage integrations (dbt-ol, Airflow provider, GE action) can publish events
// to Kafka using the OL Kafka transport. This consumer reads those events and feeds
// them into Correlator's existing storage pipeline.
package kafka

import (
	"errors"

	"github.com/correlator-io/correlator/internal/config"
)

const (
	defaultTopic   = "openlineage.events"
	defaultGroupID = "correlator"
)

// Config holds Kafka consumer configuration.
// All fields are required when Enabled is true. When Enabled is false,
// the Kafka consumer is not started and other fields are ignored.
type Config struct {
	// Enabled controls whether the Kafka consumer is started.
	Enabled bool

	// Brokers is the list of Kafka broker addresses.
	// Required when Enabled is true.
	Brokers []string

	// Topic is the Kafka topic to consume from.
	// Required when Enabled is true.
	// Note: OpenLineage has no standard default topic. This is Correlator's convention.
	Topic string

	// GroupID is the Kafka consumer group ID.
	// Required when Enabled is true.
	GroupID string
}

// Sentinel errors for Kafka configuration validation.
var (
	// ErrBrokersRequired indicates that Kafka brokers must be configured when Kafka is enabled.
	ErrBrokersRequired = errors.New("CORRELATOR_KAFKA_BROKERS is required when Kafka is enabled")

	// ErrTopicRequired indicates that a Kafka topic must be configured when Kafka is enabled.
	ErrTopicRequired = errors.New("CORRELATOR_KAFKA_TOPIC is required when Kafka is enabled")

	// ErrGroupIDRequired indicates that a Kafka consumer group ID must be configured when Kafka is enabled.
	ErrGroupIDRequired = errors.New("CORRELATOR_KAFKA_GROUP is required when Kafka is enabled")
)

// LoadConfig loads Kafka consumer configuration from environment variables.
//
// Environment variables:
//   - CORRELATOR_KAFKA_ENABLED: Enable Kafka consumer (default: false)
//   - CORRELATOR_KAFKA_BROKERS: Comma-separated broker addresses (required if enabled)
//   - CORRELATOR_KAFKA_TOPIC: Topic to consume from (default: openlineage.events)
//   - CORRELATOR_KAFKA_GROUP: Consumer group ID (default: correlator)
func LoadConfig() *Config {
	return &Config{
		Enabled: config.GetEnvBool("CORRELATOR_KAFKA_ENABLED", false),
		Brokers: config.ParseCommaSeparatedList(config.GetEnvStr("CORRELATOR_KAFKA_BROKERS", "")),
		Topic:   config.GetEnvStr("CORRELATOR_KAFKA_TOPIC", defaultTopic),
		GroupID: config.GetEnvStr("CORRELATOR_KAFKA_GROUP", defaultGroupID),
	}
}

// Validate checks that the configuration is valid.
// When Enabled is false, validation is skipped (all fields are ignored).
// When Enabled is true, brokers, topic, and group ID are all required.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if len(c.Brokers) == 0 {
		return ErrBrokersRequired
	}

	if c.Topic == "" {
		return ErrTopicRequired
	}

	if c.GroupID == "" {
		return ErrGroupIDRequired
	}

	return nil
}
