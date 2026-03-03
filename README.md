# 🔗 Correlator

**Incident correlation engine for data teams**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.23+-blue.svg)](https://golang.org)
[![Status](https://img.shields.io/badge/Status-Pre--Alpha-orange.svg)]()

---

## What It Does

Connects test failures to the job runs that caused them:

- Automatically correlates lineage events with test results
- Links upstream failures to downstream impact
- Provides a unified view across your data stack
- Works with your existing OpenLineage infrastructure

---

## Quick Start

```bash
# Start the server (Docker)
docker-compose -f deployments/docker/docker-compose.yml up -d

# Or build from source
make build
./build/correlator

# Ingest OpenLineage events (single event - standard OL API)
curl -X POST http://localhost:8080/api/v1/lineage \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-api-key" \
  -d @event.json

# Ingest batch events
curl -X POST http://localhost:8080/api/v1/lineage/batch \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-api-key" \
  -d @events.json
```

Standard OpenLineage integrations (dbt, Airflow, GE) work out of the box:

```bash
export OPENLINEAGE_URL=http://localhost:8080
export OPENLINEAGE_API_KEY=your-api-key
```

---

## How It Works

Correlator receives OpenLineage events and builds correlation views:

1. **Ingest** - Receives OpenLineage events via HTTP API
2. **Canonicalize** - Normalizes dataset URNs for consistent matching
3. **Store** - Persists events with idempotency handling
4. **Correlate** - Builds materialized views linking tests → datasets → jobs
5. **Query** - Exposes correlation data via API

---

## Why It Matters

**The Problem:**
When data tests fail, teams spend significant time manually searching through logs, lineage graphs, and job histories to
find the root cause.

**What You Get:**
Automated correlation that connects test failures to their source, reducing time spent investigating incidents.

**Key Benefits:**

- **Faster incident resolution**: Automated correlation instead of manual investigation
- **Unified view**: One place to see test results, lineage, and job runs
- **Standards-based**: Built on OpenLineage — no vendor lock-in

---

## Configuration

### Dataset Pattern Aliasing

Different tools can emit different dataset URNs for the same underlying table. For example, when tools use different
schema prefixes or naming conventions, Correlator sees them as separate datasets and cannot correlate test failures
across tools.

**Solution:** Configure dataset patterns in `.correlator.yaml` to map orphan URNs to their canonical form:

```yaml
# .correlator.yaml
dataset_patterns:
  # GE emits:  postgresql://prod-db/marts.customers
  # dbt emits: postgresql://prod-db/analytics.marts.customers
  - pattern: "postgresql://prod-db/marts.{table}"
    canonical: "postgresql://prod-db/analytics.marts.{table}"
```

**Pattern syntax:**

- `{variable}` — captures any characters except `/`
- `{variable*}` — captures any characters including `/` (for paths)
- Literal characters match exactly
- First matching pattern wins (order matters)

**Workflow:**

1. Deploy Correlator and run your data tools (dbt, GE, Airflow)
2. Check the Correlation Health page for orphan datasets
3. Apply suggested patterns or write custom ones in `.correlator.yaml`
4. Restart Correlator — historical data is immediately resolved

See `.correlator.yaml.example` for a full configuration template.

### Environment Variables

| Variable                      | Description                          | Default            |
|-------------------------------|--------------------------------------|--------------------|
| `CORRELATOR_CONFIG_PATH`      | Path to YAML config file             | `.correlator.yaml` |
| `CORRELATOR_AUTH_ENABLED`     | Enable API key authentication        | `false`            |
| `CORRELATOR_SERVER_PORT`      | HTTP server port                     | `8080`             |
| `CORRELATOR_SERVER_LOG_LEVEL` | Log level (debug, info, warn, error) | `info`             |

See `.env.example` for all available configuration options.

---

## Versioning

This project follows [Semantic Versioning](https://semver.org/) with the following guidelines:

- **0.x.y versions** (e.g., 0.1.0, 0.2.0) indicate **initial development phase**:
    - The API is not yet stable and may change between minor versions
    - Features may be added, modified, or removed without major version changes
    - For production-critical systems, please pin a version that works in your environment

- **1.0.0 and above** will indicate a **stable API** with semantic versioning guarantees:
    - MAJOR version for incompatible API changes
    - MINOR version for backwards-compatible functionality additions
    - PATCH version for backwards-compatible bug fixes

The current version is in early development stage, so expect possible API changes until the 1.0.0 release.

---

## Documentation

- **Development**: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) - Local setup, testing, architecture
- **Contributing**: [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) - Contribution guidelines

---

## Requirements

- Go 1.23+
- PostgreSQL 15+
- Docker (for local development)

---

## Links

- **OpenLineage**: https://openlineage.io/
- **Issues**: https://github.com/correlator-io/correlator/issues
- **Discussions**: https://github.com/correlator-io/correlator/discussions

---

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
