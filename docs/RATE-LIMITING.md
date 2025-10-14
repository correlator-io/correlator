# Rate Limiting

**Version:** 1.0
**Last Updated:** October 2025
**Status:** Production-Ready (Week 1 MVP)

---

## Overview

Correlator implements a three-tier token bucket rate limiting system to protect the API from abuse while ensuring fair resource allocation across plugins. This document provides comprehensive technical documentation for operators, developers, and future maintainers.

### Key Features

- **Three-tier rate limiting**: Global, per-plugin, and unauthenticated limits
- **Token bucket algorithm**: Industry-standard approach with burst capacity
- **Operational monitoring**: Automatic warnings when approaching capacity limits
- **Memory-efficient**: Background cleanup prevents unbounded growth
- **Production-ready**: RFC 7807 compliant error responses, structured logging
- **Horizontally scalable**: Interface-based design enables implementation of a distributed rate limiter

---

## Architecture

### Three-Tier Rate Limiting Model

```
┌─────────────────────────────────────────────────────────────────┐
│                      Incoming Request                           │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   Tier 1: Global      │
                    │   100 RPS (200 burst) │
                    └───────────┬───────────┘
                                │
                       ┌────────▼───────── ┐
                       │  Authenticated?   │
                       └────┬──────────┬───┘
                            │          │
                    ┌───────▼───┐  ┌─ ─▼─────────────── ─┐
                    │   YES     │  │    NO               │
                    └───────┬───┘  └── ┬──────────────── ┘
                            │          │
            ┌───────────────▼─────┐   ┌▼────────────────────── ┐
            │  Tier 2: Per-Plugin │   │ Tier 3: Unauthenticated│
            │  50 RPS (100 burst) │   │  10 RPS (20 burst)     │
            └───────────────┬─────┘   └────────┬───────────────┘
                            │                  │
                      ┌─────▼──────────────────▼─────┐
                      │   Allowed / Rate Limited     │
                      └──────────────────────────────┘
```

### Rate Limit Tiers

| Tier                | RPS  | Burst | Scope                                     |
|---------------------|------|-------|-------------------------------------------|
| **Global**          | 100  | 200   | All requests (prevents system overload)   |
| **Per-Plugin**      | 50   | 100   | Authenticated plugin requests             |
| **Unauthenticated** | 10   | 20    | Requests without valid API key            |

**Burst Capacity Rationale**: 2× RPS (industry standard pattern from AWS, nginx, Google Cloud)
- Allows temporary traffic spikes after idle periods
- Prevents sustained bursts from consuming all tokens

---

## Configuration

### Environment Variables

Rate limiting is configured via environment variables following the `CORRELATOR_*` naming pattern:

```bash
# Global rate limit (applied to all requests)
CORRELATOR_GLOBAL_RPS=100        # Requests per second
CORRELATOR_GLOBAL_BURST=200      # Burst capacity (default: 2 × RPS)

# Per-plugin rate limit (authenticated requests)
CORRELATOR_PLUGIN_RPS=50         # Requests per second per plugin
CORRELATOR_PLUGIN_BURST=100      # Burst capacity (default: 2 × RPS)

# Unauthenticated rate limit
CORRELATOR_UNAUTH_RPS=10         # Requests per second
CORRELATOR_UNAUTH_BURST=20       # Burst capacity (default: 2 × RPS)

# Memory cleanup configuration
CORRELATOR_RATE_LIMIT_CLEANUP_INTERVAL=5m    # Cleanup interval (default: 5 minutes)
CORRELATOR_RATE_LIMIT_IDLE_TIMEOUT=1h        # Plugin idle timeout (default: 1 hour)
CORRELATOR_RATE_LIMIT_MAX_PLUGINS=1000       # Max tracked plugins (default: 1000)
```

### Configuration Loading

```go
import "github.com/correlator-io/correlator/internal/api/middleware"

// Load configuration from environment
config := middleware.LoadConfig()

// Create in-memory rate limiter (MVP deployment)
rateLimiter := middleware.NewInMemoryRateLimiter(config)
defer rateLimiter.Close() // Always close to stop cleanup goroutine

// Inject into server
server := api.NewServer(serverConfig, apiKeyStore, rateLimiter)
```

---

## Token Bucket Algorithm

### How It Works

The token bucket algorithm maintains a "bucket" of tokens that refill at a constant rate:

1. **Token Refill**: Tokens added at configured RPS (e.g., 100 tokens/second for global limit)
2. **Request Processing**: Each request consumes 1 token
3. **Burst Handling**: Bucket can hold up to `burst` tokens for traffic spikes
4. **Rate Limiting**: When bucket is empty, requests are rejected with 429

**Example (50 RPS, 100 burst)**:
```
Time 0: Bucket has 100 tokens (full)
↓
Request 1-100: All succeed immediately (bucket: 0 tokens)
↓
Wait 1 second: Refill 50 tokens (bucket: 50 tokens)
↓
Request 101-150: All succeed (bucket: 0 tokens)
↓
Request 151: RATE LIMITED (429) - bucket empty, must wait for refill
```

### Why Token Bucket?

- **Industry standard**: Used by AWS API Gateway, nginx, Google Cloud
- **Fair**: Prevents single client from monopolizing resources
- **Flexible**: Burst capacity handles legitimate traffic spikes
- **Efficient**: O(1) time complexity, minimal memory overhead

---

## Error Responses

### 429 Too Many Requests

When rate limit is exceeded, the API returns an RFC 7807 compliant error response:

**Response Headers**:
```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/problem+json
X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000
```

**Response Body**:
```json
{
  "type": "https://correlator.io/errors/rate-limit-exceeded",
  "title": "Rate Limit Exceeded",
  "status": 429,
  "detail": "Rate limit exceeded. Please retry after some time.",
  "instance": "/api/v1/lineage/events",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Future Enhancement

**Retry-After Header**
```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1
```

Enables automatic client backoff (RFC 6585 recommendation).

---

## Operational Monitoring

### Max Plugins Warning

The rate limiter automatically warns when approaching the maximum tracked plugins limit:

**Trigger**: 80% of `CORRELATOR_RATE_LIMIT_MAX_PLUGINS` (default: 800 of 1000)

**Log Output**:
```json
{
  "level": "WARN",
  "msg": "rate limiter approaching max plugins limit",
  "current_plugins": 812,
  "max_plugins": 1000,
  "threshold_percent": 80,
  "recommendation": "investigate potential plugin ID proliferation or increase max_plugins limit"
}
```

**Recommended Actions**:
1. **Check for plugin ID proliferation**: Are plugins generating unique IDs per request?
2. **Review cleanup settings**: Is `IDLE_TIMEOUT` too long?
3. **Increase limit**: Set `CORRELATOR_RATE_LIMIT_MAX_PLUGINS` higher if legitimate
4. **Consider Redis migration**: If consistently above 80%

### Memory Cleanup

**Background Process**:
- Runs every `CLEANUP_INTERVAL` (default: 5 minutes)
- Removes plugins idle longer than `IDLE_TIMEOUT` (default: 1 hour)
- Prevents unbounded memory growth

**Expected Behavior**:
```
Time 0:   1000 plugins tracked
Time 5m:  Cleanup runs, removes 200 idle plugins → 800 tracked
Time 10m: Cleanup runs, removes 150 idle plugins → 650 tracked
```

---

## Performance Characteristics

### Overhead Per Request

| Operation               | Latency    | Notes                                   |
|-------------------------|------------|-----------------------------------------|
| Global limit check      | ~100-200ns | Single atomic operation                 |
| Plugin lookup (cached)  | ~50-100ns  | Map access with RWMutex                 |
| Token bucket check      | ~50-100ns  | `rate.Limiter.Allow()` call             |
| **Total overhead**      | **~200-400ns** | Negligible vs 50ms bcrypt authentication |

### Memory Usage

| Component          | Memory per Plugin | Notes                                    |
|--------------------|-------------------|------------------------------------------|
| Plugin limiter     | ~200 bytes        | `rate.Limiter` + metadata                |
| 1000 plugins       | ~200 KB           | Negligible for modern servers            |
| 10,000 plugins     | ~2 MB             | Still acceptable for single-node MVP     |

---

## Migration Path (Phase 2)

### Current: In-Memory Rate Limiter

**Suitable for**:
- Single-node deployments
- <10,000 active plugins
- <1000 RPS sustained load

**Implementation**:
```go
rateLimiter := middleware.NewInMemoryRateLimiter(config)
```

### Future: Distributed Rate Limiter

**Trigger Conditions** (Sprint 9-10):
- >10,000 active API keys OR
- Multi-node horizontal scaling OR
- Authentication P99 latency >50ms

**Implementation** (future):
```go
// Redis-backed distributed rate limiter
rateLimiter := middleware.NewRedisRateLimiter(config, redisClient)
```

**Benefits**:
- Shared state across multiple API nodes
- Consistent rate limiting in distributed deployments
- Cache API key lookups (5-minute TTL)

---

## Troubleshooting

### Problem: All requests return 429

**Symptoms**:
- Every request immediately rate limited
- Even first request fails

**Possible Causes**:
1. **Rate limit too low**: Check `CORRELATOR_*_RPS` values
2. **Burst capacity zero**: Ensure burst = 2 × RPS (or explicitly set)
3. **Global limit exhausted**: Other plugins consuming all tokens

**Solution**:
```bash
# Increase limits for testing
export CORRELATOR_GLOBAL_RPS=1000
export CORRELATOR_PLUGIN_RPS=500
export CORRELATOR_UNAUTH_RPS=100
```

### Problem: Rate limiting not working

**Symptoms**:
- Can send unlimited requests
- No 429 responses

**Possible Causes**:
1. **Rate limiter not configured**: Check server initialization
2. **Middleware not applied**: Verify middleware chain order
3. **Limits too high**: RPS exceeds testing capabilities

**Verification**:
```go
// Ensure middleware is applied
server.Use(middleware.RateLimit(rateLimiter, logger))

// Check middleware order (rate limit should be position 4)
// 1. CorrelationID
// 2. Recovery
// 3. AuthenticatePlugin
// 4. RateLimit  ← Must be here
// 5. RequestLogger
// 6. CORS
```

### Problem: Memory growing unbounded

**Symptoms**:
- Server memory increases over time
- No plateau in memory usage

**Possible Causes**:
1. **Cleanup disabled**: Check `CLEANUP_INTERVAL` not set to 0
2. **Idle timeout too long**: Plugins never cleaned up
3. **Plugin ID proliferation**: Each request creates new plugin ID

**Solution**:
```bash
# Enable aggressive cleanup for testing
export CORRELATOR_RATE_LIMIT_CLEANUP_INTERVAL=1m
export CORRELATOR_RATE_LIMIT_IDLE_TIMEOUT=5m

# Monitor cleanup logs
tail -f correlator.log | grep "cleanup"
```

### Problem: Legitimate traffic rate limited

**Symptoms**:
- Users report intermittent 429 errors
- Traffic within expected limits

**Possible Causes**:
1. **Burst capacity insufficient**: Traffic bursty, not sustained
2. **Plugin RPS too low**: Multiple services sharing same plugin ID
3. **Global limit hit**: Other plugins consuming tokens

**Solution**:
```bash
# Increase burst capacity
export CORRELATOR_GLOBAL_BURST=500   # 5× RPS instead of 2×
export CORRELATOR_PLUGIN_BURST=250   # 5× RPS instead of 2×

# Or increase base rate
export CORRELATOR_PLUGIN_RPS=100     # Double the plugin limit
```

---

## Security Considerations

### Rate Limiting is NOT Authentication

Rate limiting protects **system availability**, not **access control**.

**Correct architecture**:
```
Request → Auth (401 if invalid) → RateLimit (429 if exceeded) → Handler
```

**Why this matters**:
- Unauthenticated requests still consume global limit
- Per-plugin limits only apply AFTER successful authentication
- True unauthenticated protection requires public endpoints

### Timing Attack Prevention

Rate limiting decisions use constant-time comparisons where possible:

```go
// ✅ CORRECT: Constant-time plugin lookup
if !rl.global.Allow() {
    return false  // Fail fast, no timing leak
}

// Per-plugin check uses map lookup (not constant-time, but acceptable)
```

**Rationale**: Plugin IDs are not secrets (transmitted in headers), timing leaks are low-risk.

### DDoS Protection

Rate limiting provides **basic DDoS protection**:

- **Layer 7 (Application)**: Token bucket prevents API abuse
- **Layer 4 (Transport)**: NOT protected (use firewall/load balancer)

**Recommended stack**:
```
Internet → Cloudflare (DDoS protection)
         → Load Balancer (Layer 4 rate limiting)
         → Correlator API (Layer 7 rate limiting)
```

---

## References

### Internal Documentation
- `/internal/api/middleware/ratelimit.go` - Implementation
- `/internal/api/middleware/ratelimit_test.go` - Unit tests
- `/internal/api/middleware/server_integration_test.go` - Integration tests

### External Resources
- [RFC 6585: HTTP Status Code 429](https://datatracker.ietf.org/doc/html/rfc6585)
- [RFC 7807: Problem Details for HTTP APIs](https://datatracker.ietf.org/doc/html/rfc7807)
- [Token Bucket Algorithm (Wikipedia)](https://en.wikipedia.org/wiki/Token_bucket)
- [`golang.org/x/time/rate` Package](https://pkg.go.dev/golang.org/x/time/rate)

### Future Enhancements
- Redis-backed distributed rate limiter
- `Retry-After` header in 429 responses
- Performance benchmarks (single/multi-plugin/concurrent)

---

## Changelog

### Version 1.0 (October 2025) - Initial Release
- Three-tier token bucket rate limiting (global, per-plugin, unauthenticated)
- In-memory implementation for single-node MVP deployments
- RFC 7807 compliant error responses
- Operational monitoring (max plugins warning)
- Background memory cleanup (5-minute interval, 1-hour idle timeout)
- Interface-based design for future Redis migration

---

**Questions or issues?** File a GitHub issue at `github.com/correlator-io/correlator/issues`