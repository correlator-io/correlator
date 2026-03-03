# Airflow Dockerfile for Correlator Demo
# Includes Airflow 2.11.0+ with standard OpenLineage integrations
#
# This container runs the demo DAG which executes dbt and GE commands.
# Therefore, it needs dbt, GE, and standard OL integrations installed.

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

RUN pip install --upgrade pip

# Split installs to avoid pip resolution-too-deep with the Airflow base image.
# The Airflow image pins hundreds of packages via constraints.txt; resolving
# dbt + GE + OL + Airflow providers all at once exceeds pip's depth limit.

RUN pip install --no-cache-dir apache-airflow-providers-postgres psycopg2-binary

RUN pip install --no-cache-dir dbt-postgres openlineage-dbt

# OL integration pinned to 1.39.0 — see ge.Dockerfile for version rationale.
RUN pip install --no-cache-dir "openlineage-integration-common[great_expectations]==1.39.0"

# Upgrade the OL provider separately — the Airflow base image pins 2.3.0 via
# constraints.txt, which the bulk install above doesn't override. Version 2.4.0+
# adds lineage_root_parent_id macro needed for DAG-level parent run correlation.
RUN pip install --no-cache-dir --upgrade "apache-airflow-providers-openlineage>=2.4.0"

# Force-upgrade protobuf AFTER all other installs. GE 0.15.34 downgrades protobuf
# to 4.x, but dbt-core 1.11+ uses MessageToJson(always_print_fields_with_no_presence=...)
# which requires protobuf >= 5.26.0. Must be the last pip install to avoid being reverted.
RUN pip install --no-cache-dir "protobuf>=5.26.0,<6"

# OpenLineage configuration is mounted at runtime via openlineage.yml
# See: deployments/demo/airflow/openlineage.yml
ENV OPENLINEAGE_CONFIG=/opt/airflow/openlineage.yml

# Standard OpenLineage configuration
# OPENLINEAGE_URL is the base URL for the OL HTTP transport (used by dbt-ol, GE OL action)
# OPENLINEAGE_NAMESPACE is intentionally NOT set — each tool uses its own default
ENV OPENLINEAGE_URL=http://demo-correlator:8080
