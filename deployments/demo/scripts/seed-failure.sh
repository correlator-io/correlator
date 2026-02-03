#!/bin/bash
# seed-failure.sh - Switch to bad data for failure scenario demonstration
#
# This script copies bad Jaffle Shop data to the active seed location,
# causing dbt tests and GE validation to fail. This demonstrates
# Correlator's ability to correlate failures across tools.
#
# Bad data includes:
# - Duplicate customer IDs
# - Null customer names
# - Invalid customer references in orders
# - Negative order amounts
# - Duplicate order IDs
#
# Usage: ./scripts/seed-failure.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
DBT_DIR="$DEMO_DIR/dbt"

echo "Switching to FAILURE scenario (bad data)..."

# Backup good data if not already backed up
if [ ! -f "$DBT_DIR/seeds/raw_customers_good.csv" ]; then
    cp "$DBT_DIR/seeds/raw_customers.csv" "$DBT_DIR/seeds/raw_customers_good.csv"
    echo "Backed up good customers data"
fi

if [ ! -f "$DBT_DIR/seeds/raw_orders_good.csv" ]; then
    cp "$DBT_DIR/seeds/raw_orders.csv" "$DBT_DIR/seeds/raw_orders_good.csv"
    echo "Backed up good orders data"
fi

# Copy bad data to active location
cp "$DBT_DIR/seeds/bad_data/raw_customers_bad.csv" "$DBT_DIR/seeds/raw_customers.csv"
cp "$DBT_DIR/seeds/bad_data/raw_orders_bad.csv" "$DBT_DIR/seeds/raw_orders.csv"

echo ""
echo "Bad data now active at:"
echo "  - $DBT_DIR/seeds/raw_customers.csv (duplicates, nulls)"
echo "  - $DBT_DIR/seeds/raw_orders.csv (invalid refs, negative amounts)"
echo ""
echo "Ready for failure scenario demonstration!"
echo "Run: make run demo"
echo ""
echo "To restore good data, run: ./scripts/seed-success.sh"
