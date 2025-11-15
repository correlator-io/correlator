package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/correlator-io/correlator/internal/ingestion"
)

// Benchmark Tests
//
// Run benchmarks:
//   go test -bench=. -benchmem -run=^$ ./internal/storage
//
// Run with CPU profiling:
//   go test -bench=. -cpuprofile=cpu.prof ./internal/storage
//   go tool pprof cpu.prof
//
// Run with memory profiling:
//   go test -bench=. -memprofile=mem.prof ./internal/storage
//   go tool pprof mem.prof

const (
	// benchmarkBatchSize is the number of events in batch benchmarks.
	// Set to 100 to match typical production batch sizes and <500ms target.
	benchmarkBatchSize = 100
)

// setupBenchmarkStore creates a test database and lineage store for benchmarks.
// Caller is responsible for calling cleanup function.
func setupBenchmarkStore(ctx context.Context, b *testing.B) (*LineageStore, func()) {
	b.Helper()

	container, conn := setupTestDatabase(ctx, b)

	store, err := NewLineageStore(conn, 1*time.Hour) //nolint: contextcheck
	if err != nil {
		b.Fatalf("NewLineageStore() error = %v", err)
	}

	cleanup := func() {
		_ = store.Close()
		_ = conn.Close()
		_ = container.Terminate(ctx)
	}

	return store, cleanup
}

// BenchmarkLineageStore_StoreEvent_Single benchmarks single event storage.
// Target: <100ms per event.
//
// This benchmark measures the performance of storing individual OpenLineage events,
// including idempotency checks, job run upserts, dataset upserts, and edge creation.
func BenchmarkLineageStore_StoreEvent_Single(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()

	store, cleanup := setupBenchmarkStore(ctx, b)
	defer cleanup()

	// Pre-allocate events to exclude allocation time from benchmark
	events := make([]*ingestion.RunEvent, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = createTestEvent(
			fmt.Sprintf("bench-single-%d", i),
			ingestion.EventTypeStart,
			1, // 1 input dataset
			1, // 1 output dataset
		)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, err := store.StoreEvent(ctx, events[i])
		if err != nil {
			b.Fatalf("StoreEvent() error = %v", err)
		}
	}
}

// BenchmarkLineageStore_StoreEvents_Batch benchmarks batch event storage.
// Target: <500ms for 100 events.
//
// This benchmark measures the performance of storing batches of OpenLineage events.
// Current implementation uses per-event transactions (no batch optimization).
func BenchmarkLineageStore_StoreEvents_Batch(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()

	store, cleanup := setupBenchmarkStore(ctx, b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()

	// Create batch on-demand (avoid memory exhaustion for large b.N)
	for i := 0; i < b.N; i++ {
		batch := make([]*ingestion.RunEvent, benchmarkBatchSize)
		for j := 0; j < benchmarkBatchSize; j++ {
			batch[j] = createTestEvent(
				fmt.Sprintf("bench-batch-%d-%d", i, j),
				ingestion.EventTypeStart,
				1, 1,
			)
		}

		_, err := store.StoreEvents(ctx, batch)
		if err != nil {
			b.Fatalf("StoreEvents() error = %v", err)
		}
	}
}

// BenchmarkLineageStore_StoreEvent_Duplicate benchmarks duplicate event handling.
// Target: <10ms per duplicate (idempotency check only, no storage).
//
// This benchmark measures the performance of the idempotency layer when
// duplicate events are submitted within the 24-hour TTL window.
func BenchmarkLineageStore_StoreEvent_Duplicate(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	ctx := context.Background()

	store, cleanup := setupBenchmarkStore(ctx, b)
	defer cleanup()

	// Store initial event (establishes idempotency key)
	event := createTestEvent(
		"bench-duplicate",
		ingestion.EventTypeStart,
		1, 1,
	)

	_, _, err := store.StoreEvent(ctx, event)
	if err != nil {
		b.Fatalf("StoreEvent() setup error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	// Benchmark duplicate detection
	for i := 0; i < b.N; i++ {
		stored, duplicate, err := store.StoreEvent(ctx, event)
		if err != nil {
			b.Fatalf("StoreEvent() error = %v", err)
		}

		if stored || !duplicate {
			b.Fatalf("Expected duplicate=true, stored=false, got duplicate=%v, stored=%v", duplicate, stored)
		}
	}
}
