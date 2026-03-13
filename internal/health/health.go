// Package health provides shared health check types used by both the API layer
// and infrastructure adapters (Kafka consumer, PostgreSQL storage).
//
// These types are transport-agnostic — they carry no JSON tags or serialization
// concerns. The API layer maps them to its own JSON-tagged response structs.
//
// Dependency graph:
//
//	api  ──imports──▶  health  ◀──imports──  kafka
package health

// ComponentResult reports the health of a single subsystem (e.g., PostgreSQL, Kafka).
type ComponentResult struct {
	Status    string
	LatencyMs int64
	Error     string
	Details   any
}

// KafkaDetails provides Kafka-specific diagnostic information for troubleshooting
// consumer connectivity and partition assignment issues.
type KafkaDetails struct {
	Brokers       string
	Topic         string
	ConsumerGroup string
	Messages      int64
	Errors        int64
	Rebalances    int64
}
