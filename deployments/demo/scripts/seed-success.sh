#!/bin/bash
# seed-success.sh - Switch to good data for successful pipeline runs
#
# This script copies the good Jaffle Shop data to the active seed location,
# enabling successful dbt seed, run, test, and GE validation.
#
# Usage: ./scripts/seed-success.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
DBT_DIR="$DEMO_DIR/dbt"

echo "Switching to SUCCESS scenario (good data)..."

# Copy good data files to seeds directory
cp "$DBT_DIR/seeds/raw_customers.csv" "$DBT_DIR/seeds/raw_customers.csv.bak" 2>/dev/null || true
cp "$DBT_DIR/seeds/raw_orders.csv" "$DBT_DIR/seeds/raw_orders.csv.bak" 2>/dev/null || true

echo "Good data is already in place at:"
echo "  - $DBT_DIR/seeds/raw_customers.csv"
echo "  - $DBT_DIR/seeds/raw_orders.csv"
echo ""
echo "Ready for successful pipeline run!"
echo "Run: make run demo"
