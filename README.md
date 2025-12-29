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

# Ingest OpenLineage events
curl -X POST http://localhost:8080/api/v1/lineage/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d @event.json
```

For dbt projects, use [dbt-correlator](https://github.com/correlator-io/correlator-dbt):

```bash
pip install correlator-dbt
export CORRELATOR_ENDPOINT=http://localhost:8080/api/v1/lineage/events
dbt-correlator test
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
- **Extensible**: Plugin architecture for custom integrations

---

## Versioning

This project follows [Semantic Versioning](https://semver.org/) with the following guidelines:

- **0.x.y versions** (e.g., 0.1.0, 0.2.0) indicate **initial development phase**:
    - The API is not yet stable and may change between minor versions
    - Features may be added, modified, or removed without major version changes
    - Not recommended for production-critical systems without pinned versions

- **1.0.0 and above** will indicate a **stable API** with semantic versioning guarantees:
    - MAJOR version for incompatible API changes
    - MINOR version for backwards-compatible functionality additions
    - PATCH version for backwards-compatible bug fixes

The current version is in early development stage, so expect possible API changes until the 1.0.0 release.

---

## Documentation

- **Development**: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) - Local setup, testing, architecture
- **Contributing**: [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) - Contribution guidelines
- **OpenLineage**: [docs/OPENLINEAGE.md](docs/OPENLINEAGE.md) - Event format and validation
- **API Reference**: [docs/api/openapi.yaml](docs/api/openapi.yaml) - OpenAPI specification
- **Migrations**: [docs/MIGRATIONS.md](docs/MIGRATIONS.md) - Database schema management

---

## Plugins

| Plugin                                                            | Description                  | Status      |
|-------------------------------------------------------------------|------------------------------|-------------|
| [dbt-correlator](https://github.com/correlator-io/correlator-dbt) | dbt test results and lineage | ✅ Available |
| airflow-correlator                                                | Airflow DAG run correlation  | 🔲 Planned  |
| great-expectations-correlator                                     | GX validation results        | 🔲 Planned  |

---

## Requirements

- Go 1.23+
- PostgreSQL 15+
- Docker (for local development)

---

## Links

- **dbt Plugin**: https://github.com/correlator-io/correlator-dbt
- **OpenLineage**: https://openlineage.io/
- **Issues**: https://github.com/correlator-io/correlator/issues
- **Discussions**: https://github.com/correlator-io/correlator/discussions

---

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
