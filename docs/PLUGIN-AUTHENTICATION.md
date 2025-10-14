# Authentication

**Version:** 1.0
**Last Updated:** October 2025
**Status:** Production-Ready (Week 1 MVP)

---

## Overview

Correlator implements API key-based authentication with plugin identification to secure the API and enable per-plugin rate limiting and permissions. This document provides comprehensive technical documentation for operators, plugin developers, and future maintainers.

### Key Features

- **API key authentication**: Secure bearer token authentication for all API endpoints
- **Plugin identification**: Each API key is associated with a specific plugin
- **Dual header support**: `X-Api-Key` (primary) and `Authorization: Bearer` (fallback)
- **Bcrypt hashing**: Industry-standard password hashing (cost factor 10)
- **Timing attack prevention**: Constant-time comparison + dummy operations
- **RFC 7807 error responses**: Standardized error format for client integration
- **Security-first logging**: API keys masked in logs, no sensitive data in errors
- **PostgreSQL persistence**: Full audit trail with created/updated timestamps

---

## Architecture

### Authentication Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    Incoming Request                          │
│  Headers: X-Api-Key: plugin_abc123...                        │
└────────────────────────────┬─────────────────────────────────┘
                             │
                ┌────────────▼─────────────┐
                │  Extract API Key         │
                │  (X-Api-Key or Bearer)   │
                └────────────┬─────────────┘
                             │
                  ┌──────────▼──────────┐
                  │  Key present?       │
                  └──┬──────────────┬───┘
                     │ NO           │ YES
          ┌──────────▼───┐     ┌────▼────────────────────┐
          │ 401 Missing  │     │ Lookup in Database      │
          │ Credentials  │     │ (SHA256 index lookup)   │
          └──────────────┘     └────┬────────────────────┘
                                    │
                         ┌──────────▼──────────┐
                         │  Key found?         │
                         └──┬──────────────┬───┘
                            │ NO           │ YES
                 ┌──────────▼───┐     ┌────▼────────────────┐
                 │ Dummy bcrypt │     │ Verify bcrypt hash  │
                 │ (timing)     │     │ (constant-time)     │
                 └──┬───────────┘     └────┬────────────────┘
                    │                      │
                    │ FAIL        ┌────────▼────────── ┐
                    └────────────►│ Hash valid?        │
                                  └──┬──────────────┬──┘
                                     │ NO           │ YES
                          ┌──────────▼───┐     ┌────▼────────────────┐
                          │ 401 Invalid  │     │ Create PluginContext│
                          │ Credentials  │     │ (ID, Name, Perms)   │
                          └──────────────┘     └────┬────────────────┘
                                                    │
                                         ┌──────────▼─────────────┐
                                         │ Add to Request Context │
                                         │ Continue to Handler    │
                                         └────────────────────────┘
```

### Security Guarantees

| Security Feature               | Implementation                          | Purpose                                    |
|--------------------------------|-----------------------------------------|--------------------------------------------|
| **Bcrypt hashing**             | Cost factor 10 (~60ms)                  | Prevents rainbow table attacks             |
| **Timing attack prevention**   | Dummy bcrypt on missing keys            | Prevents user enumeration                  |
| **Constant-time comparison**   | `subtle.ConstantTimeCompare()`          | Prevents timing-based key extraction       |
| **SHA256 lookup index**        | O(1) database query                     | Performance without compromising security  |
| **Secure logging**             | Mask API keys (prefix + suffix only)   | Prevents key leakage in logs               |
| **No key in error responses**  | Generic "Invalid credentials" message   | Prevents information disclosure            |

---

## API Key Format

### Key Structure

```
plugin_[40 character random string]
```

**Example**:
```
plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0
```

### Generation (PostgreSQL)

API keys are generated server-side during key creation:

```sql
INSERT INTO api_keys (plugin_id, key_hash, lookup_hash, name, permissions, active)
VALUES (
    'dbt-plugin-prod',
    -- Bcrypt hash of full key (for verification)
    '$2a$10$N9qo8uLOickgx2ZMRZoMye...',
    -- SHA256 hash of full key (for O(1) lookup)
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', // pragma: allowlist secret
    'dbt Production Plugin',
    '["lineage:write", "incidents:read"]',
    true
);
```

### Key Properties

- **Prefix**: `plugin_` for easy identification in headers
- **Length**: 47 characters total (prefix + 40 char random)
- **Randomness**: Cryptographically secure random generation
- **Uniqueness**: Enforced by database unique constraint on `lookup_hash`

---

## Configuration

### API Key Storage

Authentication requires an `APIKeyStore` implementation injected into the server:

```go
import (
    "github.com/correlator-io/correlator/internal/api"
    "github.com/correlator-io/correlator/internal/storage"
)

// PostgreSQL-backed API key store (production)
apiKeyStore, err := storage.NewPersistentKeyStore(dbConn)
if err != nil {
    logger.Error("Failed to connect to persistent key store", slog.String("error", err.Error()))
}

// In-memory store (testing only)
apiKeyStore := storage.NewInMemoryKeyStore()
...
// Inject into server
server := api.NewServer(serverConfig, apiKeyStore, rateLimiter)
```

### Bcrypt Configuration

**Cost Factor**: 10 (hardcoded in MVP)

```go
// internal/storage/hash.go
const bcryptCost = 10 // ~60ms latency
```

**Rationale**:
- **Security**: Sufficient for MVP threat model (prevents brute force)
- **Performance**: 60ms authentication latency acceptable for MVP
- **Future**: Configurable via environment variable

---

## Request Headers

### Primary Header (Recommended)

```http
X-Api-Key: plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0
```

### Fallback Header (OAuth2 Compatible)

```http
Authorization: Bearer plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0
```

### Example Request

```bash
curl -X POST https://api.correlator.io/api/v1/lineage/events \
  -H "X-Api-Key: plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0" \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "START",
    "job": {
      "namespace": "dbt",
      "name": "my_model"
    }
  }'
```

---

## Error Responses

### 401 Missing Credentials

**Trigger**: No `X-Api-Key` or `Authorization` header present

**Response**:
```http
HTTP/1.1 401 Unauthorized
Content-Type: application/problem+json
X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000
```

```json
{
  "type": "https://correlator.io/errors/authentication-failed",
  "title": "Authentication Failed",
  "status": 401,
  "detail": "Missing authentication credentials",
  "instance": "/api/v1/lineage/events",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 401 Invalid Credentials

**Trigger**: API key not found or bcrypt hash verification failed

**Response**:
```http
HTTP/1.1 401 Unauthorized
Content-Type: application/problem+json
X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000
```

```json
{
  "type": "https://correlator.io/errors/authentication-failed",
  "title": "Authentication Failed",
  "status": 401,
  "detail": "Invalid authentication credentials",
  "instance": "/api/v1/lineage/events",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Security Note**: Both "key not found" and "invalid hash" return the same generic message to prevent user enumeration.

---

## Plugin Context

### PluginContext Structure

After successful authentication, a `PluginContext` is added to the request context:

```go
type PluginContext struct {
    PluginID    string    // e.g., "dbt-plugin-prod"
    Name        string    // e.g., "dbt Production Plugin"
    Permissions []string  // e.g., ["lineage:write", "incidents:read"]
    KeyID       string    // Database primary key (UUID)
    AuthTime    time.Time // When authentication succeeded
}
```

### Accessing Plugin Context

```go
import "github.com/correlator-io/correlator/internal/api/middleware"

func myHandler(w http.ResponseWriter, r *http.Request) {
    // Extract plugin context (safe, always present after auth middleware)
    pluginCtx, ok := middleware.GetPluginContext(r.Context())
    if !ok {
        // Should never happen - auth middleware runs first
        http.Error(w, "Internal error", http.StatusInternalServerError)
        return
    }

    // Use plugin information
    logger.Info("Processing request",
        "plugin_id", pluginCtx.PluginID,
        "plugin_name", pluginCtx.Name,
        "permissions", pluginCtx.Permissions,
    )

    // Check permissions
    if !hasPermission(pluginCtx.Permissions, "lineage:write") {
        http.Error(w, "Forbidden", http.StatusForbidden)
        return
    }

    // Process request...
}
```

---

## Performance Characteristics

### Authentication Latency

| Operation                      | Latency      | Notes                                         |
|--------------------------------|--------------|-----------------------------------------------|
| SHA256 lookup                  | ~1-5ms       | O(1) database query with index                |
| Bcrypt verification            | ~60ms        | Cost factor 10 (security vs performance)      |
| Context creation               | ~10-50µs     | Struct allocation + JSON unmarshal            |
| **Total authentication time**  | **~65ms**    | Acceptable for MVP, cacheable in Phase 2      |

### Database Queries

**Authentication flow** (single request):
```sql
-- 1. Lookup by SHA256 hash (O(1) with index)
SELECT id, plugin_id, key_hash, name, permissions, active
FROM api_keys
WHERE lookup_hash = $1 AND active = true;

-- Bcrypt comparison happens in application code (not DB)
```

**Query plan** (with `lookup_hash` index):
```
Index Scan using idx_api_keys_lookup_hash on api_keys
  Index Cond: (lookup_hash = 'e3b0c44...')
  Filter: (active = true)
Planning time: 0.123 ms
Execution time: 0.456 ms
```

---

## Security Best Practices

### Key Rotation

**Recommendation**: Rotate API keys every 90 days

```bash
# 1. Generate new key via API
curl -X POST https://api.correlator.io/api/v1/admin/api-keys \
  -H "X-Api-Key: $ADMIN_KEY" \
  -d '{"plugin_id": "dbt-plugin-prod", "name": "dbt Production (rotated)"}'

# 2. Update plugin configuration with new key
export CORRELATOR_API_KEY="plugin_new_key_here" // pragma: allowlist secret

# 3. Verify new key works
curl -X GET https://api.correlator.io/api/v1/health \
  -H "X-Api-Key: $CORRELATOR_API_KEY"

# 4. Deactivate old key
curl -X DELETE https://api.correlator.io/api/v1/admin/api-keys/$OLD_KEY_ID \
  -H "X-Api-Key: $ADMIN_KEY"
```

### Key Storage

**✅ Correct**:
- Environment variables: `export CORRELATOR_API_KEY=plugin_...`
- Secret management: AWS Secrets Manager, HashiCorp Vault
- Kubernetes secrets: `kubectl create secret generic correlator-api-key --from-literal=key=plugin_... // pragma: allowlist secret`

**❌ Incorrect**:
- Hardcoded in source code
- Committed to version control
- Plain text configuration files
- Shared via email/Slack

### Secure Transmission

**Always use HTTPS**:
```bash
# ✅ CORRECT
curl https://api.correlator.io/api/v1/health

# ❌ WRONG (API key transmitted in plaintext!)
curl http://api.correlator.io/api/v1/health
```

### Least Privilege Permissions

Assign minimum required permissions per plugin:

```json
{
  "plugin_id": "dbt-read-only",
  "permissions": ["incidents:read"]  // Read-only, no write access
}
```

```json
{
  "plugin_id": "dbt-full-access",
  "permissions": ["lineage:write", "incidents:read", "incidents:write"]
}
```

---

## Logging

### Authentication Success

```json
{
  "level": "INFO",
  "msg": "authentication successful",
  "plugin_id": "dbt-plugin-prod",
  "plugin_name": "dbt Production Plugin",
  "key_id": "550e8400-e29b-41d4-a716-446655440000",
  "latency_ms": 62
}
```

### Authentication Failure (Key Not Found)

```json
{
  "level": "ERROR",
  "msg": "authentication failed",
  "failure_type": "key_not_found",
  "lookup_hash": "e3b0c44298fc...",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Note**: API key is NOT logged (only lookup hash for debugging)

### Authentication Failure (Invalid Hash)

```json
{
  "level": "ERROR",
  "msg": "authentication failed",
  "failure_type": "invalid_hash",
  "plugin_id": "dbt-plugin-prod",
  "key_id": "550e8400-e29b-41d4-a716-446655440000",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Log Masking

API keys are automatically masked in logs:

```go
// Input:  "plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0" // pragma: allowlist secret
// Output: "plugin_a1b2...s9t0" (prefix + last 4 chars)
```

---

## Database Schema

### API Keys Table

```sql
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plugin_id TEXT NOT NULL,
    key_hash TEXT NOT NULL,              -- Bcrypt hash for verification
    lookup_hash TEXT NOT NULL UNIQUE,    -- SHA256 hash for O(1) lookup
    name TEXT NOT NULL,
    permissions JSONB DEFAULT '[]'::jsonb,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Performance: O(1) lookup by SHA256 hash
CREATE INDEX idx_api_keys_lookup_hash ON api_keys(lookup_hash);

-- Audit: Find all keys for a plugin
CREATE INDEX idx_api_keys_plugin_id ON api_keys(plugin_id);
```

### Example Data

```sql
SELECT id, plugin_id, name, active, created_at
FROM api_keys
WHERE active = true
ORDER BY created_at DESC;
```

| id                                   | plugin_id        | name                 | active | created_at          |
|--------------------------------------|------------------|----------------------|--------|---------------------|
| 550e8400-e29b-41d4-a716-446655440000 | dbt-plugin-prod  | dbt Production       | true   | 2025-10-14 10:23:45 |
| 6ba7b810-9dad-11d1-80b4-00c04fd430c8 | airflow-prod     | Airflow Production   | true   | 2025-10-13 15:42:11 |
| 7c9e6679-7425-40de-944b-e07fc1f90ae7 | ge-staging       | GE Staging           | true   | 2025-10-12 08:15:33 |

---

## Troubleshooting

### Problem: All requests return 401

**Symptoms**:
- Every request fails with "Missing authentication credentials"
- Valid API key provided

**Possible Causes**:
1. **Wrong header name**: Using `Api-Key` instead of `X-Api-Key`
2. **Header not sent**: Client library not configured
3. **Middleware not applied**: Authentication middleware missing from chain

**Solution**:
```bash
# Verify header is sent correctly
curl -v https://api.correlator.io/api/v1/health \
  -H "X-Api-Key: plugin_..."

# Check for "X-Api-Key" in request headers output:
> GET /api/v1/health HTTP/1.1
> Host: api.correlator.io
> X-Api-Key: plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0  ← Must be present
```

### Problem: Intermittent authentication failures

**Symptoms**:
- Authentication works sometimes, fails other times
- Same API key used

**Possible Causes**:
1. **Database connection issues**: Lookup query timing out
2. **Key deactivated**: `active=false` in database
3. **Clock skew**: Server time mismatch (affects token generation)

**Solution**:
```sql
-- Check if key is active
SELECT id, plugin_id, name, active, created_at, updated_at
FROM api_keys
WHERE lookup_hash = encode(sha256('your_key_here'::bytea), 'hex');

-- Reactivate if needed
UPDATE api_keys
SET active = true, updated_at = CURRENT_TIMESTAMP
WHERE id = 'key-uuid-here';
```

### Problem: Slow authentication (>100ms)

**Symptoms**:
- Authentication takes longer than expected
- P99 latency >100ms

**Possible Causes**:
1. **Missing database index**: `lookup_hash` not indexed
2. **Database connection pool exhausted**: Too many concurrent requests
3. **Bcrypt cost too high**: Cost factor >10

**Solution**:
```sql
-- Verify index exists and is being used
EXPLAIN ANALYZE
SELECT id, plugin_id, key_hash, name, permissions, active
FROM api_keys
WHERE lookup_hash = 'e3b0c44298fc...' AND active = true;

-- Should show "Index Scan using idx_api_keys_lookup_hash"
-- If showing "Seq Scan", index is missing:
CREATE INDEX IF NOT EXISTS idx_api_keys_lookup_hash ON api_keys(lookup_hash);
```

### Problem: "Invalid credentials" for valid key

**Symptoms**:
- API key is valid in database
- Authentication still fails

**Possible Causes**:
1. **Key copied incorrectly**: Extra spaces, newlines, or truncation
2. **Wrong key used**: Using old/revoked key
3. **Database key hash mismatch**: Key regenerated but hash not updated

**Solution**:
```bash
# Verify exact key (no whitespace)
echo -n "plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0" | wc -c // pragma: allowlist secret
# Should output: 47 (7 char prefix + 40 char random)

# Test with explicit key
CORRELATOR_API_KEY="plugin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0" // pragma: allowlist secret
curl -H "X-Api-Key: $CORRELATOR_API_KEY" https://api.correlator.io/api/v1/health
```

---

## Future Enhancements

### Phase 2: Authentication Caching

**Trigger Conditions** (Sprint 9-10):
- >10,000 active API keys OR
- Authentication P99 latency >50ms

**Implementation**:
```go
// Redis-backed authentication cache
authCache := middleware.NewRedisAuthCache(redisClient, 5*time.Minute)
middleware.AuthenticatePlugin(apiKeyStore, logger, middleware.WithCache(authCache))
```

**Benefits**:
- Reduce database load (cache hit rate >95%)
- Lower authentication latency (<10ms vs 65ms)
- Shared cache across API nodes

### Phase 2: JWT Tokens

**Use Case**: Machine-to-machine authentication with short-lived tokens

**Implementation** (future):
```http
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Phase 2: OAuth2 Support

**Use Case**: Third-party plugin marketplace integration

### Phase 3: RBAC Enhancement

**Use Case**: Fine-grained permissions (e.g., `lineage:dbt:write`, `incidents:critical:read`)

---

## References

### Internal Documentation
- `/internal/api/middleware/auth.go` - Authentication middleware implementation
- `/internal/api/middleware/auth_test.go` - Unit tests
- `/internal/storage/api_keys.go` - PostgreSQL API key store
- `/internal/storage/api_keys_test.go` - Storage layer tests
- `/internal/storage/api_keys_integration_test.go` - Integration tests

### External Resources
- [RFC 7807: Problem Details for HTTP APIs](https://datatracker.ietf.org/doc/html/rfc7807)
- [RFC 7617: The 'Basic' HTTP Authentication Scheme](https://datatracker.ietf.org/doc/html/rfc7617)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Bcrypt (Wikipedia)](https://en.wikipedia.org/wiki/Bcrypt)
- [Timing Attack (Wikipedia)](https://en.wikipedia.org/wiki/Timing_attack)

---

## Changelog

### Version 1.0 (October 2025) - Initial Release
- API key-based authentication with plugin identification
- Dual header support (`X-Api-Key` and `Authorization: Bearer`)
- Bcrypt hashing (cost factor 10) with SHA256 lookup index
- Timing attack prevention (dummy bcrypt + constant-time comparison)
- RFC 7807 compliant error responses
- Security-first logging (masked API keys)
- PostgreSQL persistence with full audit trail
- PluginContext for downstream handlers

---

**Questions or issues?** File a GitHub issue at `github.com/correlator-io/correlator/issues`