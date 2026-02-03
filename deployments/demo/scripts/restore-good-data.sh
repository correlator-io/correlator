#!/bin/bash
# restore-good-data.sh - Restore good data after failure scenario
#
# This script restores the original good Jaffle Shop data after
# running the failure scenario demonstration.
#
# Usage: ./scripts/restore-good-data.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
DBT_DIR="$DEMO_DIR/dbt"

echo "Restoring good data..."

# Restore from backup if available
if [ -f "$DBT_DIR/seeds/raw_customers_good.csv" ]; then
    cp "$DBT_DIR/seeds/raw_customers_good.csv" "$DBT_DIR/seeds/raw_customers.csv"
    echo "Restored good customers data"
else
    echo "Warning: No backup found for customers data"
fi

if [ -f "$DBT_DIR/seeds/raw_orders_good.csv" ]; then
    cp "$DBT_DIR/seeds/raw_orders_good.csv" "$DBT_DIR/seeds/raw_orders.csv"
    echo "Restored good orders data"
else
    echo "Warning: No backup found for orders data"
fi

echo ""
echo "Good data restored!"
echo "Run: make run demo"
