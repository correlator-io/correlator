#!/usr/bin/env python3
"""
Correlator Demo - Great Expectations Checkpoint Runner

Runs GE validations on the customers and orders mart tables and emits
OpenLineage events to Correlator via the standard OL validation action.

Uses the GE 0.15 API (compatible with openlineage-integration-common).
See: https://openlineage.io/docs/integrations/great-expectations/

Usage:
    python checkpoints/demo_checkpoint.py

Environment variables:
    OPENLINEAGE_URL              - Correlator base URL (e.g. http://demo-correlator:8080)
    OPENLINEAGE_NAMESPACE        - Job namespace (optional)
    GE_POSTGRES_URL              - PostgreSQL connection string
    OPENLINEAGE_PARENT_ID        - Airflow parent run (namespace/job_name/run_id)
    OPENLINEAGE_ROOT_PARENT_ID   - Airflow root parent run (namespace/job_name/run_id)
"""

import logging
import os
import sys

# Enable logging so OL client and GE action errors are visible
# logging.basicConfig(level=logging.INFO, format="%(name)s %(levelname)s: %(message)s")

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults


def parse_parent_id(composite):
    """Parse Airflow's composite parent ID into (namespace, job_name, run_id).

    Format: {namespace}/{job_name}/{run_id}
    Namespace can contain slashes (e.g. airflow://demo), so split from the right.
    """
    if not composite:
        return None, None, None
    parts = composite.rsplit("/", 2)
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None, None, None


def build_ol_action_config():
    """Build the OpenLineage action config dict for the checkpoint action_list.

    See: https://openlineage.io/docs/integrations/great-expectations/
    """
    action = {
        "class_name": "OpenLineageValidationAction",
        "module_name": "openlineage.common.provider.great_expectations",
    }

    namespace = os.environ.get("OPENLINEAGE_NAMESPACE")
    if namespace:
        action["openlineage_namespace"] = namespace

    openlineage_url = os.environ.get("OPENLINEAGE_URL")
    if openlineage_url:
        action["openlineage_host"] = openlineage_url

    parent_ns, parent_job, parent_run = parse_parent_id(
        os.environ.get("OPENLINEAGE_PARENT_ID")
    )
    if parent_run:
        action["openlineage_parent_run_id"] = parent_run
        action["openlineage_parent_job_namespace"] = parent_ns
        action["openlineage_parent_job_name"] = parent_job

    root_ns, root_job, root_run = parse_parent_id(
        os.environ.get("OPENLINEAGE_ROOT_PARENT_ID")
    )
    if root_run:
        action["openlineage_root_parent_run_id"] = root_run
        action["openlineage_root_parent_job_namespace"] = root_ns
        action["openlineage_root_parent_job_name"] = root_job

    return {"name": "openlineage", "action": action}


def add_expectations(suite, expectations):
    """Add a list of (expectation_type, kwargs) to a suite."""
    for expectation_type, kwargs in expectations:
        suite.add_expectation(
            ExpectationConfiguration(expectation_type=expectation_type, kwargs=kwargs)
        )


def main():
    """Run GE checkpoint with OpenLineage integration."""
    print("Starting Great Expectations validation...")

    # The OL validation action reflects tables via SQLAlchemy Table(name, autoload_with=engine)
    # but doesn't pass the schema, so tables must be findable via search_path.
    # We include 'marts' so that mart tables (customers, orders) resolve correctly.
    postgres_url = os.environ.get(
        "GE_POSTGRES_URL",
        "postgresql://correlator:correlator_dev_password@demo-postgres:5432/demo"  # pragma: allowlist secret
        "?options=-csearch_path%3Dmarts",
    )

    context = BaseDataContext(project_config=DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
    ))

    # Configure PostgreSQL datasource
    context.add_datasource(
        name="demo_postgres",
        class_name="Datasource",
        execution_engine={
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": postgres_url,
        },
        data_connectors={
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
        },
    )

    # Customers expectation suite
    customers_suite = context.create_expectation_suite("customers_suite", overwrite_existing=True)
    add_expectations(customers_suite, [
        ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": 10000}),
        ("expect_column_to_exist", {"column": "customer_id"}),
        ("expect_column_to_exist", {"column": "customer_name"}),
        ("expect_column_values_to_be_unique", {"column": "customer_id"}),
        ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
        ("expect_column_values_to_not_be_null", {"column": "customer_name"}),
        ("expect_column_values_to_be_between", {"column": "order_count", "min_value": 0}),
        ("expect_column_values_to_be_between", {"column": "total_amount", "min_value": 0}),
    ])
    context.save_expectation_suite(customers_suite)

    # Orders expectation suite
    orders_suite = context.create_expectation_suite("orders_suite", overwrite_existing=True)
    add_expectations(orders_suite, [
        ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": 100000}),
        ("expect_column_to_exist", {"column": "order_id"}),
        ("expect_column_to_exist", {"column": "customer_id"}),
        ("expect_column_to_exist", {"column": "order_total"}),
        ("expect_column_values_to_be_unique", {"column": "order_id"}),
        ("expect_column_values_to_not_be_null", {"column": "order_id"}),
        ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
        ("expect_column_values_to_not_be_null", {"column": "order_total"}),
        ("expect_column_values_to_be_between", {"column": "order_total", "min_value": 0}),
        ("expect_column_values_to_be_between", {"column": "subtotal", "min_value": 0}),
        ("expect_column_values_to_be_between", {"column": "tax_paid", "min_value": 0}),
    ])
    context.save_expectation_suite(orders_suite)

    # Run checkpoint with OpenLineage action.
    # NOTE: We use Checkpoint (not SimpleCheckpoint) because SimpleCheckpoint's
    # configurator always overwrites the user's action_list with its own defaults,
    # silently dropping custom actions like the OL validation action.
    result = context.add_checkpoint(
        name="demo_checkpoint",
        class_name="Checkpoint",
        config_version=1.0,
        validations=[
            {
                "batch_request": RuntimeBatchRequest(
                    datasource_name="demo_postgres",
                    data_connector_name="default_runtime_data_connector",
                    data_asset_name="customers",
                    runtime_parameters={"query": "SELECT * FROM marts.customers"},
                    batch_identifiers={"default_identifier_name": "customers"},
                ),
                "expectation_suite_name": "customers_suite",
            },
            {
                "batch_request": RuntimeBatchRequest(
                    datasource_name="demo_postgres",
                    data_connector_name="default_runtime_data_connector",
                    data_asset_name="orders",
                    runtime_parameters={"query": "SELECT * FROM marts.orders"},
                    batch_identifiers={"default_identifier_name": "orders"},
                ),
                "expectation_suite_name": "orders_suite",
            },
        ],
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            build_ol_action_config(),
        ],
    ).run()

    # Print action results to diagnose OL emission
    for validation_id, validation_result in result.run_results.items():
        actions = validation_result.get("actions_results", {})
        for action_name, action_result in actions.items():
            if isinstance(action_result, dict) and "exception" in action_result:
                print(f"\n   ACTION ERROR [{action_name}]: {action_result['exception']}")
            elif action_name == "openlineage":
                print(f"\n   OL event emitted for {validation_id}")

    if result.success:
        print("\nAll validations passed!")
        sys.exit(0)
    else:
        print("\nValidation failed!")
        for validation_id, validation_result in result.run_results.items():
            status = "PASS" if validation_result["validation_result"].success else "FAIL"
            print(f"   {status} {validation_id}")
        sys.exit(1)


if __name__ == "__main__":
    main()
