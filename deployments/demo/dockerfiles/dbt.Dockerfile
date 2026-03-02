# dbt Dockerfile for Correlator Demo
# Includes dbt-postgres and openlineage-dbt (standard OL integration)

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /dbt

RUN pip install --upgrade pip

# Install dbt-postgres and openlineage-dbt from PyPI
# CLI command is dbt-ol (replaces dbt-correlator)
RUN pip install --no-cache-dir \
    dbt-postgres \
    openlineage-dbt

# Standard OpenLineage configuration
# OPENLINEAGE_URL set via docker-compose; OPENLINEAGE_NAMESPACE left unset (uses tool defaults)
ENV OPENLINEAGE_URL=""

# Default command
ENTRYPOINT ["dbt-ol"]
CMD ["--help"]
