#!/usr/bin/env bash
#
# Smoke tests for OpenLineage ingestion endpoint
# Tests basic functionality of POST /api/v1/lineage/events
#
# Prerequisites:
#   - Correlator server running (make run)
#   - PostgreSQL running (make docker)
#   - Migrations applied (make run migrate up)
#   - jq installed (brew install jq)
#
# Usage:
#   ./scripts/smoketest-lineage.sh
#   SERVER_URL=http://localhost:8080 API_KEY=my-key ./scripts/smoketest-lineage.sh
#
# Scope: Basic smoke tests (6 scenarios)
# - Single events (dbt, Airflow, Spark)
# - Duplicate detection (idempotency)
# - Invalid input (missing field)
# - Empty body
#
# Note: API expects array format even for single events: [event]
#
# Out of Scope (covered by integration tests):
# - Batch events (207 Multi-Status) - See internal/api/lineage_handler_integration_test.go
# - Authentication (401) - See internal/api/auth_test.go
# - Performance baselines (<100ms) - See benchmark tests
#
# Future Enhancements to smoke tests (Optional, Not Needed Now):
# - Add batch test if production reveals batch-specific bugs
# - Add auth test if middleware changes frequently
# - Add more smoke tests for correlation endpoints
#

set -e

# Configuration
SERVER_URL="${SERVER_URL:-http://localhost:8080}"
API_KEY="${API_KEY:-test-api-key}"
ENDPOINT="${SERVER_URL}/api/v1/lineage/events"
AUTH_ENABLED="${CORRELATOR_AUTH_ENABLED:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Helper function to print test results
print_test_result() {
    local test_name="$1"
    local status="$2"
    local message="$3"

    TESTS_RUN=$((TESTS_RUN + 1))

    if [ "$status" = "PASS" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "${GREEN}✓${NC} ${test_name}"
        [ -n "$message" ] && echo "  ${message}"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "${RED}✗${NC} ${test_name}"
        echo -e "  ${RED}${message}${NC}"
    fi
}

# Helper function to make API request
make_request() {
    local data="$1"

    # Use echo to pipe data to curl (ensures proper Content-Length)
    # This avoids shell quoting issues with -d "$data"
    if [ "$AUTH_ENABLED" = "true" ] || [ "$AUTH_ENABLED" = "1" ]; then
        local response=$(echo "$data" | curl -s -w "\n%{http_code}" -X POST "$ENDPOINT" \
            -H "Content-Type: application/json" \
            -H "X-API-Key: $API_KEY" \
            -d @-)
    else
        local response=$(echo "$data" | curl -s -w "\n%{http_code}" -X POST "$ENDPOINT" \
            -H "Content-Type: application/json" \
            -d @-)
    fi

    # Extract status code (last line) and body (everything else)
    local http_code=$(echo "$response" | tail -n 1)
    local body=$(echo "$response" | sed '$d')

    # Return both as JSON for easy parsing
    echo "{\"status\": $http_code, \"body\": $body}"
}

# Check dependencies
echo "🔍 Checking dependencies..."
if ! command -v jq &> /dev/null; then
    echo -e "${RED}❌ jq is not installed${NC}"
    echo "   Install with: brew install jq (macOS) or apt-get install jq (Linux)"
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo -e "${RED}❌ curl is not installed${NC}"
    exit 1
fi

echo "✅ Dependencies installed"
echo ""

# Check server availability
echo "🔍 Checking server availability..."
if ! curl -s -f "${SERVER_URL}/ping" > /dev/null; then
    echo -e "${RED}❌ Server not available at ${SERVER_URL}${NC}"
    echo "   Start server with: make run"
    exit 1
fi
echo "✅ Server available at ${SERVER_URL}"
echo ""

echo "🧪 Running smoke tests for OpenLineage ingestion endpoint"
echo "=================================================="
echo ""

#===============================================================================
# Test 1: Single dbt COMPLETE event
#===============================================================================
echo "Test 1: Single dbt COMPLETE event (200 OK)"
DBT_EVENT='{
  "eventTime": "2025-10-21T10:05:00Z",
  "eventType": "COMPLETE",
  "producer": "https://github.com/dbt-labs/dbt-core/tree/1.5.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000",
    "facets": {}
  },
  "job": {
    "namespace": "dbt://analytics",
    "name": "transform_orders",
    "facets": {}
  },
  "inputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "raw.public.orders",
      "facets": {}
    }
  ],
  "outputs": [
    {
      "namespace": "postgres://prod-db:5432",
      "name": "analytics.public.orders",
      "facets": {}
    }
  ]
}'

RESPONSE=$(make_request "[$DBT_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "200" ]; then
    # Validate response structure
    RESULT_COUNT=$(echo "$BODY" | jq -r '.results | length')
    if [ "$RESULT_COUNT" = "1" ]; then
        MESSAGE=$(echo "$BODY" | jq -r '.results[0].message')
        if [ "$MESSAGE" = "stored" ]; then
            print_test_result "Test 1" "PASS" "Status: $STATUS, Event stored successfully"
        else
            print_test_result "Test 1" "FAIL" "Status: $STATUS, but message='$MESSAGE' (expected 'stored')"
        fi
    else
        print_test_result "Test 1" "FAIL" "Status: $STATUS, but expected 1 result, got $RESULT_COUNT"
    fi
else
    print_test_result "Test 1" "FAIL" "Expected status 200, got $STATUS"
fi
echo ""

#===============================================================================
# Test 2: Single Airflow START event
#===============================================================================
echo "Test 2: Single Airflow START event (200 OK)"
AIRFLOW_EVENT='{
  "eventTime": "2025-10-21T10:01:00Z",
  "eventType": "START",
  "producer": "https://github.com/OpenLineage/OpenLineage/tree/0.30.0/integration/airflow",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440001",
    "facets": {}
  },
  "job": {
    "namespace": "airflow://production",
    "name": "daily_etl.load_users",
    "facets": {}
  },
  "inputs": [],
  "outputs": []
}'

RESPONSE=$(make_request "[$AIRFLOW_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "200" ]; then
    MESSAGE=$(echo "$BODY" | jq -r '.results[0].message')
    if [ "$MESSAGE" = "stored" ]; then
        print_test_result "Test 2" "PASS" "Status: $STATUS, Event stored successfully"
    else
        print_test_result "Test 2" "FAIL" "Status: $STATUS, but message='$MESSAGE' (expected 'stored')"
    fi
else
    print_test_result "Test 2" "FAIL" "Expected status 200, got $STATUS"
fi
echo ""

#===============================================================================
# Test 3: Single Spark FAIL event
#===============================================================================
echo "Test 3: Single Spark FAIL event (200 OK)"
SPARK_EVENT='{
  "eventTime": "2025-10-21T10:15:00Z",
  "eventType": "FAIL",
  "producer": "https://github.com/OpenLineage/OpenLineage/tree/0.30.0/integration/spark",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440002",
    "facets": {}
  },
  "job": {
    "namespace": "spark://prod-cluster",
    "name": "recommendation.train_model",
    "facets": {}
  },
  "inputs": [
    {
      "namespace": "hdfs://namenode:8020",
      "name": "/data/training/features.parquet",
      "facets": {}
    }
  ]
}'

RESPONSE=$(make_request "[$SPARK_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "200" ]; then
    MESSAGE=$(echo "$BODY" | jq -r '.results[0].message')
    if [ "$MESSAGE" = "stored" ]; then
        print_test_result "Test 3" "PASS" "Status: $STATUS, Event stored successfully"
    else
        print_test_result "Test 3" "FAIL" "Status: $STATUS, but message='$MESSAGE' (expected 'stored')"
    fi
else
    print_test_result "Test 3" "FAIL" "Expected status 200, got $STATUS"
fi
echo ""

#===============================================================================
# Test 4: Duplicate event (idempotency check)
#===============================================================================
echo "Test 4: Duplicate dbt event (200 OK, duplicate=true)"

# Submit the same dbt event again
RESPONSE=$(make_request "[$DBT_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "200" ]; then
    MESSAGE=$(echo "$BODY" | jq -r '.results[0].message')
    if [ "$MESSAGE" = "duplicate" ]; then
        print_test_result "Test 4" "PASS" "Status: $STATUS, Duplicate detected correctly"
    else
        print_test_result "Test 4" "FAIL" "Status: $STATUS, but message='$MESSAGE' (expected 'duplicate')"
    fi
else
    print_test_result "Test 4" "FAIL" "Expected status 200, got $STATUS"
fi
echo ""

#===============================================================================
# Test 5: Invalid event - missing eventTime
#===============================================================================
echo "Test 5: Invalid event - missing eventTime (422 Unprocessable Entity)"
INVALID_EVENT='{
  "eventType": "START",
  "producer": "https://github.com/dbt-labs/dbt-core/tree/1.5.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "invalid-run-id",
    "facets": {}
  },
  "job": {
    "namespace": "dbt://analytics",
    "name": "invalid_job",
    "facets": {}
  }
}'

RESPONSE=$(make_request "[$INVALID_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "422" ]; then
    # Check if error message mentions eventTime (in results[0].error field)
    ERROR_MSG=$(echo "$BODY" | jq -r '.results[0].error' 2>/dev/null || echo "")
    if echo "$ERROR_MSG" | grep -iq "eventTime"; then
        print_test_result "Test 5" "PASS" "Status: $STATUS, Validation error detected correctly"
    else
        print_test_result "Test 5" "FAIL" "Status: $STATUS, but error='$ERROR_MSG' (doesn't mention eventTime)"
    fi
else
    print_test_result "Test 5" "FAIL" "Expected status 422, got $STATUS"
fi
echo ""

#===============================================================================
# Test 6: Empty request body
#===============================================================================
echo "Test 6: Empty request body (400 Bad Request)"

RESPONSE=$(make_request "")
STATUS=$(echo "$RESPONSE" | jq -r '.status')

if [ "$STATUS" = "400" ]; then
    print_test_result "Test 6" "PASS" "Status: $STATUS, Empty body rejected correctly"
else
    print_test_result "Test 6" "FAIL" "Expected status 400, got $STATUS"
fi
echo ""

#===============================================================================
# Summary
#===============================================================================
echo "=================================================="
echo "Test Summary"
echo "=================================================="
echo "Total tests:  $TESTS_RUN"
echo -e "${GREEN}Passed:       $TESTS_PASSED${NC}"
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Failed:       $TESTS_FAILED${NC}"
    echo ""
    echo "❌ Some tests failed"
    exit 1
else
    echo "Failed:       $TESTS_FAILED"
    echo ""
    echo "✅ All tests passed!"
    exit 0
fi
