# 🔗 Correlator

**Incident correlation engine for data teams**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.25+-blue.svg)](https://golang.org)
[![Status](https://img.shields.io/badge/Status-Alpha-yellow.svg)]()
[![Discord](https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white)](https://discord.gg/rGysCFnt)

---

## What It Does

Connects test failures to the job runs that caused them:

- Automatically correlates lineage events with test results
- Links upstream failures to downstream impact
- Provides a unified view across your data stack
- Works with your existing OpenLineage infrastructure

---

## Quick Start

Prerequisites: [Docker](https://docs.docker.com/get-docker/) installed and running.

```bash
# Download the quickstart compose file
curl -O https://raw.githubusercontent.com/correlator-io/correlator/main/deployments/quickstart/docker-compose.yml

# Start Correlator
docker compose up -d
```

Once all services are healthy:

- **Web UI**: [http://localhost:3000](http://localhost:3000)
- **API**: [http://localhost:8080](http://localhost:8080)

Point your OpenLineage-enabled tools at Correlator:

```bash
export OPENLINEAGE_URL=http://localhost:8080
```

Standard integrations ([dbt-ol](https://openlineage.io/docs/integrations/dbt),
[Airflow](https://openlineage.io/docs/integrations/airflow),
[Great Expectations](https://openlineage.io/docs/integrations/great-expectations))
work out of the box.

To stop: `docker compose down` (add `-v` to delete data).

> **Using Kafka?** If your OpenLineage events are on a Kafka topic (common with Airflow),
> add these environment variables to the `correlator` service in the compose file:
> ```yaml
> CORRELATOR_KAFKA_ENABLED: "true"
> CORRELATOR_KAFKA_BROKERS: your-broker:9092
> CORRELATOR_KAFKA_TOPIC: openlineage.events
> ```

> **Want to see Correlator with real data?** Check out the
> [correlator-demo](https://github.com/correlator-io/correlator-demo) — a complete
> environment with Airflow, dbt, Great Expectations, and sample pipelines that generate
> incidents you can investigate.

---

## How It Works

Correlator receives OpenLineage events and builds correlation views:

1. **Ingest** - Receives OpenLineage events via HTTP API or Kafka
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

Different tools can emit different dataset URNs for the same underlying table (e.g., GE omits the schema prefix
that dbt includes). Correlator detects these mismatches automatically and **suggests patterns in the UI** — you
don't need to figure them out yourself.

**Workflow:**

1. Deploy Correlator and run your data tools (dbt, GE, Airflow)
2. Open the Correlation Health page — orphan datasets and suggested patterns appear automatically
3. Copy the suggested pattern into `.correlator.yaml`
4. Restart Correlator — historical data is immediately resolved

```yaml
# .correlator.yaml
dataset_patterns:
  # Suggested by Correlator UI:
  # GE emits:  postgresql://prod-db/marts.customers
  # dbt emits: postgresql://prod-db/analytics.marts.customers
  - pattern: "postgresql://prod-db/marts.{table}"
    canonical: "postgresql://prod-db/analytics.marts.{table}"
```

See `.correlator.yaml.example` for a full configuration template.

### Environment Variables

| Variable                      | Description                            | Default               |
|-------------------------------|----------------------------------------|-----------------------|
| `CORRELATOR_CONFIG_PATH`      | Path to YAML config file               | `.correlator.yaml`    |
| `CORRELATOR_AUTH_ENABLED`     | Enable API key authentication          | `false`               |
| `CORRELATOR_SERVER_PORT`      | HTTP server port                       | `8080`                |
| `CORRELATOR_SERVER_LOG_LEVEL` | Log level (debug, info, warn, error)   | `info`                |
| `CORRELATOR_KAFKA_ENABLED`    | Enable Kafka consumer for OL events    | `false`               |
| `CORRELATOR_KAFKA_BROKERS`    | Comma-separated Kafka broker addresses | (required if enabled) |
| `CORRELATOR_KAFKA_TOPIC`      | Kafka topic to consume from            | `openlineage.events`  |
| `CORRELATOR_KAFKA_GROUP`      | Kafka consumer group ID                | `correlator`          |

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

- **Changelog**: [CHANGELOG.md](CHANGELOG.md) - Release history
- **Development**: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) - Local setup, testing, architecture
- **Contributing**: [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) - Contribution guidelines

---

## Requirements

- Docker (for running Correlator)
- Go 1.25+ (for development only)
- PostgreSQL 15+ (included in Docker setup)

---

## Links

- **Discord**: https://discord.gg/rGysCFnt — feedback, questions, and discussion
- **OpenLineage**: https://openlineage.io/
- **Issues**: https://github.com/correlator-io/correlator/issues
- **Discussions**: https://github.com/correlator-io/correlator/discussions

---

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
