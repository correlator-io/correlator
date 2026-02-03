"""
Correlator Demo Pipeline DAG

This DAG orchestrates the demo data pipeline:
1. dbt seed - Load raw Jaffle Shop data
2. dbt run - Transform data into staging and mart models
3. dbt test - Run data quality tests
4. GE validate - Run Great Expectations checkpoint

The pipeline emits OpenLineage events to Correlator via airflow-correlator,
enabling cross-tool correlation between dbt, Airflow, and Great Expectations.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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

    # Task 1: dbt seed - Load raw data
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="""
            cd /dbt && \
            dbt seed --profiles-dir . --project-dir .
        """,
        doc_md="""
        ### dbt Seed
        Loads raw Jaffle Shop data (customers, orders) into PostgreSQL.
        This creates the source tables that staging models will reference.
        """,
    )

    # Task 2: dbt run - Transform data
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /dbt && \
            dbt run --profiles-dir . --project-dir .
        """,
        doc_md="""
        ### dbt Run
        Executes all dbt models:
        - Staging: stg_customers, stg_orders (views)
        - Marts: customers, orders (tables with aggregations)
        """,
    )

    # Task 3: dbt test - Data quality tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /dbt && \
            dbt test --profiles-dir . --project-dir .
        """,
        doc_md="""
        ### dbt Test
        Runs schema tests defined in schema.yml:
        - Uniqueness constraints
        - Not null constraints
        - Referential integrity
        """,
    )

    # Task 4: Great Expectations validation
    ge_validate = BashOperator(
        task_id="ge_validate",
        bash_command="""
            cd /gx && \
            python checkpoints/demo_checkpoint.py
        """,
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
