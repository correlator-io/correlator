"""
Correlator Demo Pipeline DAG

This DAG orchestrates the demo data pipeline:
1. dbt-ol seed - Load raw Jaffle Shop data (wrapper job events only, no dataset lineage)
2. dbt-ol run - Transform data and emit lineage events
3. dbt-ol test - Run data quality tests and emit test result events
4. GE validate - Run Great Expectations checkpoint and emit validation events

Dual transport design (proves both ingestion paths correlate):
- Airflow task events → Kafka (via openlineage.yml with Kafka transport)
- dbt-ol events       → HTTP  (BashOperators override OPENLINEAGE_CONFIG to openlineage-http.yml)
- GE events           → HTTP  (BashOperators override OPENLINEAGE_CONFIG to openlineage-http.yml)
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
    # Task 1: dbt-ol seed - Load raw data
    # dbt-ol wraps dbt seed but parse_execution() skips seed. nodes, so only
    # bare START/COMPLETE job events are emitted (no input/output datasets).
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="""
            cd /dbt && \
            dbt-ol seed --profiles-dir . --project-dir .
        """,
        env={
            # Override OPENLINEAGE_CONFIG so dbt-ol uses HTTP (not the Kafka config
            # that the Airflow OL provider reads from openlineage.yml).
            "OPENLINEAGE_CONFIG": "/opt/airflow/openlineage-http.yml",
            # "OPENLINEAGE_NAMESPACE": "dbt",
            "OPENLINEAGE_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}"
            ),
            "OPENLINEAGE_ROOT_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
            ),
        },
        append_env=True,
        doc_md="""
        ### dbt Seed (with dbt-ol)
        Loads raw Jaffle Shop data (customers, orders) into PostgreSQL.
        This creates the source tables that staging models will reference.

        Note: dbt-ol accepts seed but its parse_execution() skips seed nodes,
        so only wrapper job lifecycle events are emitted (no dataset lineage).
        Airflow task-level OL events are still emitted by the provider.
        """,
    )

    # Task 2: dbt-ol run - Transform data with lineage emission
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /dbt && \
            dbt-ol run \
                --project-dir . \
                --profiles-dir .
        """,
        env={
            "OPENLINEAGE_CONFIG": "/opt/airflow/openlineage-http.yml",
            # "OPENLINEAGE_NAMESPACE": "dbt",
            "OPENLINEAGE_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}"
            ),
            "OPENLINEAGE_ROOT_PARENT_ID": (
                "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
            ),
        },
        append_env=True,
        doc_md="""
        ### dbt Run (with dbt-ol)
        Executes all dbt models and emits OpenLineage lineage events:
        - Staging: stg_customers, stg_orders (views)
        - Marts: customers, orders (tables with aggregations)

        Emits lineage events with input/output datasets and runtime metrics.
        """,
    )

    # Task 3: dbt-ol test - Data quality tests with event emission
    # dbt_test = BashOperator(
    #     task_id="dbt_test",
    #     bash_command="""
    #         cd /dbt && \
    #         dbt-ol test \
    #             --project-dir . \
    #             --profiles-dir .
    #     """,
    #     env={
    #         "OPENLINEAGE_CONFIG": "/opt/airflow/openlineage-http.yml",
    #         # "OPENLINEAGE_NAMESPACE": "dbt",
    #         "OPENLINEAGE_PARENT_ID": (
    #             "{{ macros.OpenLineageProviderPlugin.lineage_parent_id(task_instance) }}"
    #         ),
    #         "OPENLINEAGE_ROOT_PARENT_ID": (
    #             "{{ macros.OpenLineageProviderPlugin.lineage_root_parent_id(task_instance) }}"
    #         ),
    #     },
    #     append_env=True,
    #     doc_md="""
    #     ### dbt Test (with dbt-ol)
    #     Runs schema tests defined in schema.yml and emits test result events:
    #     - Uniqueness constraints
    #     - Not null constraints
    #     - Referential integrity
    #
    #     Emits dataQualityAssertions facet with test pass/fail status.
    #     """,
    # )

    # Task 4: Great Expectations validation
    #
    # OPENLINEAGE_CONFIG must be overridden to the HTTP config. The container-level env
    # points to the Kafka config (openlineage.yml), and the OL Python client's transport
    # resolution evaluates config-file transport BEFORE the url argument:
    #
    #   1. OPENLINEAGE_DISABLED → noop
    #   2. transport= constructor arg → use it
    #   3. YAML config (OPENLINEAGE_CONFIG / ./openlineage.yml / ~/.openlineage/) → use it
    #   4. url= constructor arg (what GE's openlineage_host maps to) → HTTP
    #   5. OPENLINEAGE_URL env var → HTTP
    #   6. fallback → console
    #
    # Source: OpenLineage client/python/src/openlineage/client/client.py:_resolve_transport
    #
    # demo_checkpoint.py passes OPENLINEAGE_URL as openlineage_host to the GE action,
    # which calls OpenLineageClient(url=...). But step 3 wins over step 4 — the Kafka
    # transport from the container's config file would be used, not the URL argument.
    # This override points OPENLINEAGE_CONFIG to an HTTP-only YAML so GE events go via HTTP.
    ge_validate = BashOperator(
        task_id="ge_validate",
        bash_command="""
            cd /ge && \
            python checkpoints/demo_checkpoint.py
        """,
        env={
            "OPENLINEAGE_CONFIG": "/opt/airflow/openlineage-http.yml",
            # "OPENLINEAGE_NAMESPACE": "great_expectations",
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

        Emits OpenLineage events to Correlator via standard OL GE action.
        """,
    )

    # Define task dependencies
    # Linear pipeline: seed -> run -> test -> validate
    # dbt_seed >> dbt_run >> dbt_test >> ge_validate
    dbt_seed >> dbt_run >> ge_validate
