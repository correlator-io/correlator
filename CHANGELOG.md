# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-alpha] - 2026-03-15

First public alpha release. Core incident correlation engine with OpenLineage-native ingestion,
automated test-failure-to-job-run correlation, and a web UI for incident investigation.

### Added

#### Ingestion
- OpenLineage-compliant HTTP API (`POST /api/v1/lineage` and `POST /api/v1/lineage/batch`)
- Kafka consumer for OpenLineage events (configurable topic and consumer group)
- Support for dbt, Airflow, and Great Expectations as event producers
- Idempotent event processing with TTL-based cleanup
- Dataset URN canonicalization (port stripping, regex-based pattern aliasing)
- Data quality assertion extraction from OpenLineage facets
- Parent run facet extraction for orchestration chain visibility

#### Correlation
- Materialized-view-based correlation of test failures to producing job runs
- Downstream impact analysis via lineage edges
- Orphan dataset detection (test failures with no known producer)
- Correlation health metrics (correlation rate, orphan count)
- Debounced view refresh after ingestion

#### Incident Management
- Incident list with severity, status, and correlation context
- Incident detail view with upstream root cause and downstream blast radius
- Incident resolution (manual resolve/unresolve)
- Cascade resolution to sibling incidents on the same dataset
- Auto-resolve on new passing ingestion events

#### API
- Bearer token authentication with bcrypt-hashed API keys
- Per-client rate limiting middleware
- CORS support
- Health and readiness endpoints (`/health`, `/ready`)
- RFC 7807 problem detail error responses
- PostgreSQL with embedded migrations (auto-apply on startup)
- CLI: `correlator start`, `correlator generate-key`, `correlator version`

#### Web UI
- Incidents list page with filtering and severity indicators
- Incident detail page with test results, job context, and lineage graph
- Interactive lineage graph (upstream/downstream visualization)
- Correlation health dashboard with tool attribution
- System health check page
- Build version display

#### Deployment
- Container images on GHCR: `correlator`, `correlator-migrator`, `correlator-ui`
- Multi-architecture Docker images (linux/amd64, linux/arm64)
- Demo environment with Airflow, dbt, Great Expectations, and Kafka

[0.1.0-alpha]: https://github.com/correlator-io/correlator/releases/tag/v0.1.0-alpha
