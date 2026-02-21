"""
Correlator Demo Pipeline DAG

This DAG orchestrates the demo data pipeline:
1. dbt seed - Load raw Jaffle Shop data (no lineage events)
2. dbt-correlator run - Transform data and emit lineage events
3. dbt-correlator test - Run data quality tests and emit test result events
4. GE validate - Run Great Expectations checkpoint and emit validation events

The pipeline emits OpenLineage events to Correlator via:
- airflow-correlator: Airflow task lifecycle events
- dbt-correlator: dbt model lineage and test results
- ge-correlator: Great Expectations validation results
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for all tasks
default_args = {
    "owner": "correlator-demo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Correlator endpoint for dbt-correlator
CORRELATOR_ENDPOINT = "http://demo-correlator:8080/api/v1/lineage/events"

# DAG definition
with DAG(
        dag_id="demo_pipeline",
        default_args=default_args,
        description="Correlator demo pipeline: dbt + Great Expectations",
        schedule_interval=None,  # Manual trigger only
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["correlator", "demo", "dbt", "great-expectations"],
) as dag:
    # Task 1: dbt seed - Load raw data
    # Note: dbt-correlator doesn't support seed, so we use plain dbt
    # No lineage events are emitted for this task (only Airflow task events)
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="""
            cd /dbt && \
            dbt seed --profiles-dir . --project-dir .
        """,
        env={
            "OPENLINEAGE_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}"
            ),
            "OPENLINEAGE_ROOT_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
            ),
        },
        append_env=True,
        doc_md="""
        ### dbt Seed
        Loads raw Jaffle Shop data (customers, orders) into PostgreSQL.
        This creates the source tables that staging models will reference.

        Note: Uses plain dbt (not dbt-correlator) as seed is not supported.
        Only Airflow task events are emitted for this step.
        """,
    )

    # Task 2: dbt-correlator run - Transform data with lineage emission
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            cd /dbt && \
            dbt-correlator run \
                --project-dir . \
                --profiles-dir . \
                --correlator-endpoint {CORRELATOR_ENDPOINT} \
                --openlineage-namespace dbt://demo
        """,
        env={
            "OPENLINEAGE_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}"
            ),
            "OPENLINEAGE_ROOT_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
            ),
        },
        append_env=True,
        doc_md="""
        ### dbt Run (with dbt-correlator)
        Executes all dbt models and emits OpenLineage lineage events:
        - Staging: stg_customers, stg_orders (views)
        - Marts: customers, orders (tables with aggregations)

        Emits lineage events with input/output datasets and runtime metrics.
        """,
    )

    # Task 3: dbt-correlator test - Data quality tests with event emission
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd /dbt && \
            dbt-correlator test \
                --project-dir . \
                --profiles-dir . \
                --correlator-endpoint {CORRELATOR_ENDPOINT} \
                --openlineage-namespace dbt://demo
        """,
        env={
            "OPENLINEAGE_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}"
            ),
            "OPENLINEAGE_ROOT_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
            ),
        },
        append_env=True,
        doc_md="""
        ### dbt Test (with dbt-correlator)
        Runs schema tests defined in schema.yml and emits test result events:
        - Uniqueness constraints
        - Not null constraints
        - Referential integrity

        Emits dataQualityAssertions facet with test pass/fail status.
        """,
    )

    # Task 4: Great Expectations validation
    ge_validate = BashOperator(
        task_id="ge_validate",
        bash_command="""
            cd /ge && \
            python checkpoints/demo_checkpoint.py
        """,
        env={
            "OPENLINEAGE_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}"
            ),
            "OPENLINEAGE_ROOT_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
            ),
        },
        append_env=True,
        doc_md="""
        ### Great Expectations Validate
        Runs the demo checkpoint to validate:
        - customers mart: row counts, uniqueness, data types
        - orders mart: row counts, value ranges, referential integrity

        Emits OpenLineage events to Correlator via ge-correlator.
        """,
    )

    # Define task dependencies
    # Linear pipeline: seed -> run -> test -> validate
    dbt_seed >> dbt_run >> dbt_test >> ge_validate
