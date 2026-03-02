# Great Expectations Dockerfile for Correlator Demo
# Includes GE 0.15.x with standard OpenLineage integration
#
# Version pinning rationale (March 2026):
#
# openlineage-integration-common version determines compatible GE range:
#   OL <= 1.39.0  requires  great_expectations>=0.13.26,<0.15.35  (WORKING)
#   OL >= 1.40.0  requires  great_expectations>=1.0.0             (BROKEN — see below)
#
# We pin OL==1.39.0 (last version with tested GE integration):
#   - GE resolves to 0.15.34 (pip skips 0.15.33 which has broken metadata)
#   - The OL action (OpenLineageValidationAction) uses the GE 0.15 _run() API
#   - GE 0.15 SimpleCheckpoint calls _run() correctly for each validation
#
# Why OL >= 1.40.0 + GE 1.x is broken:
#   1. OL action implements _run() but GE 1.x ValidationAction.run() raises
#      NotImplementedError — subclasses must override run(), not _run().
#   2. OL team suspended GE tests in Sept 2024 (PR #3078).
#   3. sqlalchemy<2.0.0 constraint from OL conflicts with GE 1.x.
#
# GE 0.15.33 metadata bug: pydantic specifier reads "pydantic (>=1.0<2.0)"
# (missing comma). pip >= 24.1 rejects this. Pinning OL 1.39.0 constrains
# to <0.15.35, so pip resolves to 0.15.34 (valid metadata).

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /ge

RUN pip install --upgrade pip

# Pin OL 1.39.0 — see compatibility notes above.
# OL 1.39.0 pulls in great_expectations>=0.13.26,<0.15.35 automatically.
RUN pip install --no-cache-dir \
    "openlineage-integration-common[great_expectations]==1.39.0" \
    psycopg2-binary

# Standard OpenLineage configuration
ENV OPENLINEAGE_URL=""

# Default command
ENTRYPOINT ["python"]
CMD ["--help"]
