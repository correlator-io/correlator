#!/usr/bin/env python3
"""
Correlator Demo - Great Expectations Checkpoint Runner

This script runs GE validations with ge-correlator integration.
It validates the customers and orders mart tables and emits
OpenLineage events to Correlator.

Usage:
    python checkpoints/demo_checkpoint.py
"""

import os
import sys

import great_expectations as ge
from great_expectations.expectations import (
    ExpectColumnToExist,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectTableRowCountToBeBetween,
)

from ge_correlator.action import CorrelatorValidationAction


def create_context():
    """Create GE context with PostgreSQL datasource."""
    context = ge.get_context(mode="ephemeral")

    # Add PostgreSQL datasource
    datasource = context.data_sources.add_postgres(
        name="demo_postgres",
        connection_string=os.environ.get(
            "GE_POSTGRES_URL",
            "postgresql://correlator:correlator_dev_password@demo-postgres:5432/demo", # pragma: allowlist secret
        ),
    )

    return context, datasource


def create_customers_suite(context):
    """Create expectation suite for customers mart."""
    suite = context.suites.add(
        ge.ExpectationSuite(name="customers_suite")
    )

    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=1, max_value=10000))
    suite.add_expectation(ExpectColumnToExist(column="customer_id"))
    suite.add_expectation(ExpectColumnToExist(column="customer_name"))
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="customer_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="customer_name"))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="order_count", min_value=0))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="total_amount", min_value=0))

    return suite


def create_orders_suite(context):
    """Create expectation suite for orders mart."""
    suite = context.suites.add(
        ge.ExpectationSuite(name="orders_suite")
    )

    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=1, max_value=100000))
    suite.add_expectation(ExpectColumnToExist(column="order_id"))
    suite.add_expectation(ExpectColumnToExist(column="customer_id"))
    suite.add_expectation(ExpectColumnToExist(column="order_total"))
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="order_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="order_total"))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="order_total", min_value=0))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="subtotal", min_value=0))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="tax_paid", min_value=0))

    return suite


def main():
    """Run GE checkpoint with Correlator integration."""
    print("Starting Great Expectations validation...")

    # Create context and datasource
    context, datasource = create_context()

    # Create expectation suites
    customers_suite = create_customers_suite(context)
    orders_suite = create_orders_suite(context)

    # Create batch definitions for mart tables
    customers_asset = datasource.add_table_asset(
        name="customers",
        schema_name="marts",
        table_name="customers",
    )
    customers_batch = customers_asset.add_batch_definition_whole_table("customers_batch")

    orders_asset = datasource.add_table_asset(
        name="orders",
        schema_name="marts",
        table_name="orders",
    )
    orders_batch = orders_asset.add_batch_definition_whole_table("orders_batch")

    # Create validation definitions
    customers_validation = context.validation_definitions.add(
        ge.ValidationDefinition(
            name="customers_validation",
            data=customers_batch,
            suite=customers_suite,
        )
    )

    orders_validation = context.validation_definitions.add(
        ge.ValidationDefinition(
            name="orders_validation",
            data=orders_batch,
            suite=orders_suite,
        )
    )

    # Create Correlator action
    correlator_endpoint = os.environ.get(
        "CORRELATOR_ENDPOINT",
        "http://demo-correlator:8080/api/v1/lineage/events",
    )
    job_namespace = os.environ.get(
        "GE_JOB_NAMESPACE",
        "great_expectations://demo-postgres/demo",
    )

    correlator_action = CorrelatorValidationAction(
        correlator_endpoint=correlator_endpoint,
        job_namespace=job_namespace,
        emit_on="all",
        timeout=30,
    )

    # Create and run checkpoint
    checkpoint = ge.Checkpoint(
        name="demo_checkpoint",
        validation_definitions=[customers_validation, orders_validation],
        actions=[correlator_action],
    )

    result = checkpoint.run()

    # Report results
    if result.success:
        print("\n✅ All validations passed!")
        print(f"   - Customers: {result.run_results[customers_validation].success}")
        print(f"   - Orders: {result.run_results[orders_validation].success}")
        sys.exit(0)
    else:
        print("\n❌ Validation failed!")
        for validation_def, run_result in result.run_results.items():
            status = "✅" if run_result.success else "❌"
            print(f"   {status} {validation_def.name}: {run_result.success}")
        sys.exit(1)


if __name__ == "__main__":
    main()
