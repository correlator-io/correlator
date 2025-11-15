# Idempotency Cleanup - Operational Runbook

**Last Updated:** November 15, 2025

---

## Overview

The idempotency cleanup system is a background goroutine that automatically removes expired idempotency keys from the `lineage_event_idempotency` table. This prevents unbounded table growth while maintaining a 24-hour deduplication window for OpenLineage events.

### Key Characteristics

- **TTL**: 24 hours (configurable via database schema)
- **Cleanup Interval**: 1 hour (configurable via environment variable)
- **Execution**: Asynchronous background goroutine with graceful shutdown
- **Failure Handling**: Errors logged but do not crash the server
- **Performance Impact**: Minimal (cleanup runs during low-traffic periods)

---

## Configuration

### Environment Variables

| Variable | Default | Description | Example |
|----------|---------|-------------|---------|
| `IDEMPOTENCY_CLEANUP_INTERVAL` | `1h` | Interval between cleanup runs | `30m`, `2h`, `15m` |

### Configuration Examples

**Development (frequent cleanup for testing):**
```bash
export IDEMPOTENCY_CLEANUP_INTERVAL=5m
```

**Production (default interval):**
```bash
export IDEMPOTENCY_CLEANUP_INTERVAL=1h
```

**High-volume environments (more frequent cleanup):**
```bash
export IDEMPOTENCY_CLEANUP_INTERVAL=30m
```

### Startup Logging

The cleanup goroutine logs its configuration on startup:

```json
{
  "time": "2025-01-15T12:00:00Z",
  "level": "INFO",
  "msg": "Started idempotency cleanup goroutine",
  "interval": 3600000000000
}
```

**Note:** The `interval` field is in nanoseconds (3600000000000ns = 1 hour).

---

## Monitoring

### Success Logs

Cleanup operations that remove expired keys generate INFO-level logs:

```json
{
  "time": "2025-01-15T13:00:00Z",
  "level": "INFO",
  "msg": "Cleaned up expired idempotency keys",
  "rows_deleted": 1234,
  "batches_completed": 2,
  "duration": "350ms",
  "status": "success"
}
```

**Key Metrics:**
- `rows_deleted`: Number of expired keys removed
- `batches_completed`: Number of batch iterations executed
- `duration`: Time taken to execute the cleanup operation
- `status`: "success" (cleanup completed successfully)

**Log Behavior:**
- **Info logs** appear when `rows_deleted > 0` (successful cleanup with deletions)
- **Debug logs** appear when `rows_deleted = 0` (no expired keys found)
- Set `LOG_LEVEL=info` (default) to avoid log spam from zero-row cleanups
- Set `LOG_LEVEL=debug` to see all cleanup executions (even when no rows deleted)

### Error Logs

Cleanup failures generate ERROR-level logs but do not crash the server:

```json
{
  "time": "2025-01-15T13:00:00Z",
  "level": "ERROR",
  "msg": "Failed to cleanup expired idempotency keys",
  "error": "context deadline exceeded",
  "rows_deleted_before_error": 50000,
  "batches_completed": 5,
  "status": "failed"
}
```

**Common Errors:**
- `context deadline exceeded` - Database query took >30 seconds (timeout)
- `database is closed` - Server shutting down (expected during graceful shutdown)
- `connection refused` - Database connection lost

---

## Graceful Shutdown

### Shutdown Behavior

When the Correlator server shuts down:

1. **Channel Cancellation** - `Close()` closes `cleanupStop` channel, signaling goroutine to stop
2. **Context Propagation** - Parent context cancelled, propagates to in-flight cleanup queries
3. **In-Flight Cleanup** - Current batch completes or times out (30-second query timeout)
4. **Graceful Wait** - Server waits up to 5 seconds for goroutine to exit
5. **Force Termination** - After 5-second timeout, server proceeds with shutdown (non-blocking)
6. **Clean Exit** - Goroutine stops without leaking resources

### Shutdown Logs

```json
{
  "time": "2025-01-15T14:00:00Z",
  "level": "INFO",
  "msg": "Stopping idempotency cleanup goroutine"
}
```

### Force Termination

If cleanup doesn't stop within 5 seconds:
- Server proceeds with shutdown (non-blocking)
- Database connection closed externally (managed by `main.go`)
- No resource leaks (context cancellation handles cleanup)

---

## Cleanup Strategy

### Batching Approach

The cleanup operation uses **batching** to avoid long-running table locks and enable graceful shutdown:

**Batch Configuration:**
- **Batch size**: 10,000 rows per batch
- **Sleep between batches**: 100ms
- **Maximum batches**: Unlimited (loops until no more expired rows)
- **Query timeout**: 30 seconds per batch
- **Index used**: `idx_idempotency_expires` (on `expires_at` column)

**Batch Deletion Query:**
```sql
DELETE FROM lineage_event_idempotency
WHERE idempotency_key IN (
    SELECT idempotency_key
    FROM lineage_event_idempotency
    WHERE expires_at < NOW()
    ORDER BY expires_at ASC
    LIMIT 10000
);
```

**Example: Cleanup 150,000 Expired Keys**
```
Batch 1:  DELETE 10,000 rows (~150ms)
Sleep:    100ms
Batch 2:  DELETE 10,000 rows (~150ms)
Sleep:    100ms
...
Batch 15: DELETE 10,000 rows (~150ms)
-------------------------------------------
Total:    ~3.75 seconds (15 batches)
```

**Why Batching:**
- ✅ Prevents long-running locks (each batch ~150ms vs unbounded DELETE taking 5+ seconds)
- ✅ Allows other queries to proceed between batches (100ms interleave window)
- ✅ Enables graceful cancellation during server shutdown (checked between batches)
- ✅ Avoids overwhelming database (small, frequent deletes vs large batch)

**Context Cancellation:**
- Context checked before each batch
- Context checked during sleep between batches
- In-flight batch completes before cancellation (up to 30-second timeout)

---

## Manual Cleanup (Emergency Use)

### When to Use Manual Cleanup

**Use manual cleanup when:**
- Table growth is unbounded (automated cleanup failing)
- Database performance degrading due to large `lineage_event_idempotency` table
- Emergency space reclamation needed

**Do NOT use manual cleanup for:**
- Normal operations (automated cleanup is sufficient)
- Testing (use `IDEMPOTENCY_CLEANUP_INTERVAL=1m` instead)

### Manual Cleanup SQL

**Simple Cleanup (Small Tables):**
```sql
-- Delete all expired idempotency keys (use for <50,000 expired rows)
DELETE FROM lineage_event_idempotency
WHERE expires_at < NOW();
```

**Batched Cleanup (Large Tables):**
```sql
-- For >50,000 expired rows, use batched approach to avoid long locks
-- Run this query repeatedly until it deletes 0 rows
DELETE FROM lineage_event_idempotency
WHERE idempotency_key IN (
    SELECT idempotency_key
    FROM lineage_event_idempotency
    WHERE expires_at < NOW()
    ORDER BY expires_at ASC
    LIMIT 10000
);

-- Check progress:
SELECT COUNT(*) AS remaining_expired
FROM lineage_event_idempotency
WHERE expires_at < NOW();
```

**Expected Output:**
```
-- First run (10,000 expired keys exist):
DELETE 1234

-- Subsequent runs:
DELETE 0  (cleanup complete)
```

**Estimated Time:**
- Small cleanup (<10K rows): ~1 second
- Medium cleanup (10K-50K rows): ~5-10 seconds
- Large cleanup (50K-100K rows): ~15-20 seconds (use batched approach)
- Very large (>100K rows): ~30-60 seconds (use batched approach, run during low-traffic period)

### Verify Cleanup

```sql
-- Check remaining idempotency keys
SELECT COUNT(*) AS total_keys,
       COUNT(*) FILTER (WHERE expires_at < NOW()) AS expired_keys
FROM lineage_event_idempotency;
```

**Expected Output:**
```
 total_keys | expired_keys
------------+--------------
       5678 |            0
```

---

## Troubleshooting

### Issue: Table Growing Unbounded

**Symptoms:**
- `lineage_event_idempotency` table size increasing continuously
- No cleanup logs appearing in server logs

**Diagnosis:**
```sql
-- Check table size and expired keys
SELECT
  pg_size_pretty(pg_total_relation_size('lineage_event_idempotency')) AS table_size,
  COUNT(*) AS total_keys,
  COUNT(*) FILTER (WHERE expires_at < NOW()) AS expired_keys,
  MIN(expires_at) AS oldest_expiration,
  MAX(expires_at) AS newest_expiration
FROM lineage_event_idempotency;
```

**Possible Causes:**
1. Cleanup goroutine not running (check startup logs for "Started idempotency cleanup goroutine")
2. Database connection lost (check error logs)
3. Cleanup interval too long (reduce `IDEMPOTENCY_CLEANUP_INTERVAL`)

**Resolution:**
1. Restart Correlator server
2. Verify cleanup goroutine starts successfully
3. Wait for next cleanup interval
4. If issue persists, run manual cleanup (see above)

---

### Issue: Cleanup Taking Too Long

**Symptoms:**
- Cleanup duration >30 seconds (timeout exceeded)
- Error logs: "context deadline exceeded"

**Diagnosis:**
```sql
-- Check table size and index health
SELECT
  pg_size_pretty(pg_total_relation_size('lineage_event_idempotency')) AS table_size,
  COUNT(*) AS total_keys
FROM lineage_event_idempotency;

-- Check if index is being used
EXPLAIN ANALYZE
DELETE FROM lineage_event_idempotency
WHERE expires_at < NOW();
```

**Possible Causes:**
1. Missing index on `expires_at` column (check migration 005)
2. Table too large (millions of expired keys)
3. Database under heavy load

**Resolution:**
1. **Short-term:** Run manual cleanup during low-traffic period
2. **Long-term:** Reduce `IDEMPOTENCY_CLEANUP_INTERVAL` (e.g., from `1h` to `30m`)
3. **Verify index:** Ensure migration 005 applied correctly

---

### Issue: High CPU/Memory During Cleanup

**Symptoms:**
- CPU spikes every cleanup interval
- High memory usage during cleanup

**Diagnosis:**
```sql
-- Check how many keys are being deleted per cleanup
SELECT COUNT(*) AS keys_to_delete
FROM lineage_event_idempotency
WHERE expires_at < NOW();
```

**Possible Causes:**
1. Cleanup interval too long (large batch deletions)
2. High event ingestion rate (many duplicates)

**Resolution:**
1. **Reduce cleanup interval** - Smaller, more frequent deletions
   ```bash
   export IDEMPOTENCY_CLEANUP_INTERVAL=30m
   ```
2. **Monitor cleanup metrics** - Track `rows_deleted` in logs
3. **Adjust TTL** - If business requirements allow, reduce 24-hour TTL (requires migration change)

---

### Issue: Cleanup Failing After Database Restart

**Symptoms:**
- Error logs: "connection refused" or "database is closed"
- Cleanup stops working after database maintenance

**Diagnosis:**
- Check database connectivity: `psql $DATABASE_URL -c "SELECT 1"`
- Check Correlator server logs for connection errors

**Resolution:**
1. **Restart Correlator server** - Re-establishes database connection
2. **Verify database credentials** - Ensure `DATABASE_URL` is correct
3. **Check network connectivity** - Database accessible from server?

---

## Performance Characteristics

### Cleanup Query Performance

**Benchmark Results (Session 20):**
- **Target:** <30 seconds (query timeout)
- **Typical:** <1 second for <10,000 expired keys
- **Large batch:** ~5 seconds for 100,000 expired keys

### Database Impact

- **Lock type:** Row-level locks (non-blocking for reads)
- **Concurrency:** Does NOT block event ingestion
- **I/O impact:** Minimal (indexed DELETE operation)

### Recommended Tuning

| Event Rate | Cleanup Interval | Expected Expired Keys per Cleanup |
|------------|------------------|-----------------------------------|
| <100 events/sec | `1h` (default) | <1,000 keys |
| 100-500 events/sec | `30m` | <5,000 keys |
| 500-1000 events/sec | `15m` | <10,000 keys |
| >1000 events/sec | `10m` | <20,000 keys |

---

## Architecture Notes

### Design Decisions

1. **Background goroutine vs cron job** - Goroutine chosen for simplicity (no external dependencies)
2. **Ticker pattern** - Standard Go approach for periodic tasks
3. **Context-based cancellation** - Enables graceful shutdown without leaks
4. **Selective logging** - Only log when `rows_deleted > 0` (reduces log spam)
5. **30-second query timeout** - Prevents hanging queries from blocking shutdown

### Future Enhancements (Deferred to Phase 2+)

- **Metrics export** - Prometheus metrics for cleanup performance
- **Adaptive intervals** - Adjust cleanup frequency based on table growth rate
- **Partitioning** - Time-based partitioning for `lineage_event_idempotency` table
- **Vacuum coordination** - Coordinate cleanup with PostgreSQL VACUUM operations

---

## Related Documentation

- **OpenLineage Ingestion API**: `docs/api/openapi.yaml` - `/lineage/events` endpoint
- **Event Validation**: `docs/EVENT-VALIDATION.md` - Validation rules and idempotency logic
- **Database Schema**: `migrations/006_lineage_idempotency.up.sql` - Idempotency table and indexes
- **Implementation**: `internal/storage/lineage_store.go` - Cleanup implementation
  - `runCleanup()` - Background goroutine with ticker
  - `cleanupExpiredIdempotencyKeys()` - Batched deletion logic

---

## Support

**Questions or Issues?**
- Check Correlator logs: `docker logs correlator`
- Review database health: `SELECT * FROM pg_stat_activity WHERE application_name = 'correlator';`
- Open issue: https://github.com/correlator-io/correlator/issues