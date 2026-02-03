# Airflow Dockerfile for Correlator Demo
# Includes Airflow 2.11.0+ with airflow-correlator plugin from TestPyPI
#
# IMPORTANT: airflow-correlator requires Airflow 2.11.0+ (older versions NOT supported)

FROM apache/airflow:2.11.0-python3.11

USER root

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Airflow providers and airflow-correlator from TestPyPI
# --extra-index-url ensures dependencies not on TestPyPI are fetched from PyPI
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-openlineage>=2.0.0 \
    && pip install --no-cache-dir \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    airflow-correlator

# OpenLineage configuration is mounted at runtime via openlineage.yml
# See: deployments/demo/airflow/openlineage.yml
ENV OPENLINEAGE_CONFIG=/opt/airflow/openlineage.yml
