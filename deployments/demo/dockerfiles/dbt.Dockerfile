# dbt Dockerfile for Correlator Demo
# Includes dbt-postgres and dbt-correlator plugin from TestPyPI

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /dbt

RUN pip install --upgrade pip

# Install dbt-postgres and correlator-dbt from TestPyPI
# Package name is correlator-dbt (not dbt-correlator), CLI command is dbt-correlator
# --pre allows pre-release/dev versions, -i sets TestPyPI as primary index
# --extra-index-url ensures dependencies not on TestPyPI are fetched from PyPI
RUN pip install --no-cache-dir \
    dbt-postgres \
    && pip install --no-cache-dir --pre \
    -i https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    correlator-dbt

# print the version correlator-dbt plugin
RUN pip show correlator-dbt

# Environment variables for dbt-correlator
ENV CORRELATOR_URL=""
ENV OPENLINEAGE_NAMESPACE=""

# Default command
ENTRYPOINT ["dbt"]
CMD ["--help"]
