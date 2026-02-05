# Airflow Dockerfile for Correlator Demo
# Includes Airflow 2.11.0+ with all three Correlator plugins
#
# This container runs the demo DAG which executes dbt and GE commands.
# Therefore, it needs dbt, GE, and all Correlator plugins installed.
#
# IMPORTANT: airflow-correlator requires Airflow 2.11.0+ (older versions NOT supported)

FROM apache/airflow:2.11.0-python3.11

USER root

# Install system dependencies for building Python packages
# libpq-dev is needed for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install base tools and Airflow providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-openlineage>=2.0.0 \
    dbt-postgres \
    "great_expectations>=1.3.0" \
    psycopg2-binary \
    sqlalchemy

# Install all Correlator plugins from TestPyPI
# Package names follow the convention: correlator-{tool} (not {tool}-correlator)
# --extra-index-url ensures dependencies not on TestPyPI are fetched from PyPI
RUN pip install --no-cache-dir \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    correlator-airflow \
    correlator-dbt \
    correlator-ge

# OpenLineage configuration is mounted at runtime via openlineage.yml
# See: deployments/demo/airflow/openlineage.yml
ENV OPENLINEAGE_CONFIG=/opt/airflow/openlineage.yml

# dbt-correlator configuration
ENV CORRELATOR_URL=http://demo-correlator:8080
ENV OPENLINEAGE_NAMESPACE=dbt://demo
