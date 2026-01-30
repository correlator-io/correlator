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

## Configuration

### Namespace Aliasing

Different data tools emit different namespace formats for the same data source:

| Tool               | Example Namespace                |
|--------------------|----------------------------------|
| Great Expectations | `postgres_prod`                  |
| dbt                | `postgresql://prod-db:5432/mydb` |
| Airflow            | `postgres://prod-db:5432`        |

Without aliasing, Correlator sees these as separate datasets and cannot correlate test failures across tools.

**Solution:** Configure namespace aliases in `.correlator.yaml`:

```yaml
# .correlator.yaml
namespace_aliases:
  # Map GE namespace to dbt's canonical format
  postgres_prod: "postgresql://prod-db:5432/mydb"
  postgres://prod-db:5432: "postgresql://prod-db:5432/mydb"
```

**Workflow:**

1. Deploy Correlator without aliases
2. Check Correlation Health page for orphan namespaces
3. Configure aliases based on discovered orphans
4. Restart Correlator - historical data is immediately resolved

See `.correlator.yaml.example` for a full configuration template.

### Environment Variables

| Variable                  | Description                          | Default            |
|---------------------------|--------------------------------------|--------------------|
| `CORRELATOR_CONFIG_PATH`  | Path to YAML config file             | `.correlator.yaml` |
| `CORRELATOR_AUTH_ENABLED` | Enable API key authentication        | `false`            |
| `CORRELATOR_PORT`         | HTTP server port                     | `8080`             |
| `LOG_LEVEL`               | Log level (debug, info, warn, error) | `info`             |

See `.env.example` for all available configuration options.

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
