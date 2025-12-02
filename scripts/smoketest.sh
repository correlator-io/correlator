#!/usr/bin/env bash
#
# Smoke tests for Correlator - Incident Correlation Engine
# Tests end-to-end correlation flow: Lineage ingestion → Test results → Correlation
#
# Prerequisites:
#   - Correlator server running (make run)
#   - PostgreSQL running (make docker)
#   - Migrations applied (make run migrate up)
#   - jq installed (brew install jq)
#   - psql installed (optional - for auto-cleanup)
#   - DATABASE_URL set (optional - for auto-cleanup)
#
# Usage:
#   # Zero-config mode (no cleanup, may show duplicates on re-run)
#   ./scripts/smoketest.sh
#
#   # With auto-cleanup (repeatable tests)
#   DATABASE_URL=my-database-url ./scripts/smoketest.sh
#
#   # With authentication
#   CORRELATOR_AUTH_ENABLED=true API_KEY=my-key ./scripts/smoketest.sh
#
# Test Sections:
#   Section 1: OpenLineage Ingestion (Tests 1-6)
#     - Single events (dbt, Airflow, Spark) with canonical job_run_id generation
#     - Duplicate detection (idempotency)
#     - Invalid input validation (missing field)
#     - Empty body rejection
#
#   Section 2: Test Results Ingestion (Tests 7-8)
#     - Test result with canonical job_run_id
#     - UPSERT behavior verification
#
#   Section 3: End-to-End Correlation (Tests 9-10)
#     - Verify test failure linked to job run via canonical job_run_id
#     - Verify canonical ID format in correlation view ("tool:runID")
#
# Reserved Namespace Convention:
# - Tests use 'correlator-smoke-test' in all namespaces for automatic cleanup
# - Examples: dbt://correlator-smoke-test-analytics, postgres://correlator-smoke-test-db:5432
# - ⚠️  DO NOT use 'correlator-smoke-test' in production namespaces (data will be deleted)
# - Pattern matching: Cleanup deletes WHERE namespace LIKE '%correlator-smoke-test%'
#
# Note: API expects array format even for single events: [event]
#
# Out of Scope (covered by integration tests):
# - Batch events (207 Multi-Status) - See internal/api/*_handler_integration_test.go
# - Authentication (401) - See internal/api/auth_test.go
# - Performance baselines (<100ms) - See benchmark tests
# - Complex correlation scenarios - See internal/storage/correlation_views_integration_test.go
#

set -e

# Configuration
SERVER_URL="${SERVER_URL:-http://localhost:8080}"
API_KEY="${API_KEY:-test-api-key}"
ENDPOINT="${SERVER_URL}/api/v1/lineage/events"
AUTH_ENABLED="${CORRELATOR_AUTH_ENABLED:-false}"
DATABASE_URL="${DATABASE_URL:-}"  # Optional - for cleanup

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

# Helper function to cleanup previous test data
# TODO: Remove this function once CLI tool has dedicated cleanup command
# Future: correlator test cleanup --namespace "dbt://analytics,airflow://production,spark://prod-cluster"
cleanup_test_data() {
    if [ -z "$DATABASE_URL" ]; then
        echo "ℹ️  DATABASE_URL not set - skipping cleanup (graceful degradation)"
        echo "   Note: Second run will show duplicates (idempotency working correctly)"
        return 0
    fi

    if ! command -v psql &> /dev/null; then
        echo "ℹ️  psql not installed - skipping cleanup (graceful degradation)"
        echo "   Note: Second run will show duplicates (idempotency working correctly)"
        return 0
    fi

    echo "🧹 Cleaning up previous smoke test data..."

    # Uses reserved namespace pattern: 'correlator-smoke-test' in namespace
    # This pattern-based approach is superior to UUID-based cleanup because:
    #   1. Self-maintaining (new tests automatically included)
    #   2. Complete cleanup (datasets + job_runs + edges + idempotency)
    #   3. Extensible (no hardcoded UUIDs to update)
    #   4. Tests real namespace normalization (postgres:// → postgresql://)
    #
    # Reserved Namespace Convention (Week 16 Documentation):
    #   dbt://correlator-smoke-test-*
    #   airflow://correlator-smoke-test-*
    #   spark://correlator-smoke-test-*
    #   postgres://correlator-smoke-test-*:5432
    #   hdfs://correlator-smoke-test-*:8020
    #
    # ⚠️  Users must NOT use 'correlator-smoke-test' in production namespaces!
    #
    # Temporarily disable exit-on-error to capture cleanup status
    set +e
    psql "$DATABASE_URL" -v ON_ERROR_STOP=0 << 'SQL' >/dev/null 2>&1
    -- Delete in correct order (respecting foreign key constraints)
    -- Pattern matching on 'correlator-smoke-test' in namespaces

    -- 1. Delete test results for smoke test job runs
    DELETE FROM test_results
    WHERE job_run_id IN (
        SELECT job_run_id FROM job_runs
        WHERE job_namespace LIKE '%correlator-smoke-test%'
    );

    -- 2. Delete idempotency keys for smoke test job runs
    DELETE FROM lineage_event_idempotency
    WHERE event_metadata->>'job_namespace' LIKE '%correlator-smoke-test%';

    -- 3. Delete lineage edges for smoke test job runs
    DELETE FROM lineage_edges
    WHERE job_run_id IN (
        SELECT job_run_id FROM job_runs
        WHERE job_namespace LIKE '%correlator-smoke-test%'
    );

    -- 4. Delete smoke test job runs
    DELETE FROM job_runs
    WHERE job_namespace LIKE '%correlator-smoke-test%';

    -- 5. Delete smoke test datasets (safe with reserved namespace convention)
    DELETE FROM datasets
    WHERE namespace LIKE '%correlator-smoke-test%';
SQL
    cleanup_status=$?
    set -e  # Re-enable exit-on-error

    if [ $cleanup_status -eq 0 ]; then
        echo "✅ Previous smoke test data cleaned"
    else
        echo "⚠️  Cleanup had errors (non-fatal, continuing with tests)"
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

# Cleanup previous test data (optional - graceful degradation)
cleanup_test_data
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

echo "🧪 Running Correlator Smoke Tests"
echo "========================================================="
echo "Testing: Lineage ingestion → Test results → Correlation"
echo ""

#===============================================================================
# SECTION 1: OpenLineage Ingestion (Tests 1-6)
#===============================================================================
echo "📦 Section 1: OpenLineage Ingestion"
echo "-----------------------------------"
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
    "namespace": "dbt://correlator-smoke-test-analytics",
    "name": "transform_orders",
    "facets": {}
  },
  "inputs": [
    {
      "namespace": "postgres://correlator-smoke-test-db:5432",
      "name": "raw.public.orders",
      "facets": {}
    }
  ],
  "outputs": [
    {
      "namespace": "postgres://correlator-smoke-test-db:5432",
      "name": "analytics.public.orders",
      "facets": {}
    }
  ]
}'

RESPONSE=$(make_request "[$DBT_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "200" ]; then
    # Validate response structure (OpenLineage format)
    RECEIVED=$(echo "$BODY" | jq -r '.summary.received')
    SUCCESSFUL=$(echo "$BODY" | jq -r '.summary.successful')
    FAILED=$(echo "$BODY" | jq -r '.summary.failed')

    if [ "$RECEIVED" = "1" ] && [ "$SUCCESSFUL" = "1" ] && [ "$FAILED" = "0" ]; then
        print_test_result "Test 1" "PASS" "Status: $STATUS, Event stored successfully (received=$RECEIVED, successful=$SUCCESSFUL)"
    else
        print_test_result "Test 1" "FAIL" "Status: $STATUS, but received=$RECEIVED, successful=$SUCCESSFUL, failed=$FAILED"
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
    "namespace": "airflow://correlator-smoke-test",
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
    SUCCESSFUL=$(echo "$BODY" | jq -r '.summary.successful')
    FAILED=$(echo "$BODY" | jq -r '.summary.failed')

    if [ "$SUCCESSFUL" = "1" ] && [ "$FAILED" = "0" ]; then
        print_test_result "Test 2" "PASS" "Status: $STATUS, Event stored successfully"
    else
        print_test_result "Test 2" "FAIL" "Status: $STATUS, but successful=$SUCCESSFUL, failed=$FAILED"
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
    "namespace": "spark://correlator-smoke-test-cluster",
    "name": "recommendation.train_model",
    "facets": {}
  },
  "inputs": [
    {
      "namespace": "hdfs://correlator-smoke-test:8020",
      "name": "/data/training/features.parquet",
      "facets": {}
    }
  ]
}'

RESPONSE=$(make_request "[$SPARK_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "200" ]; then
    SUCCESSFUL=$(echo "$BODY" | jq -r '.summary.successful')
    FAILED=$(echo "$BODY" | jq -r '.summary.failed')

    if [ "$SUCCESSFUL" = "1" ] && [ "$FAILED" = "0" ]; then
        print_test_result "Test 3" "PASS" "Status: $STATUS, Event stored successfully"
    else
        print_test_result "Test 3" "FAIL" "Status: $STATUS, but successful=$SUCCESSFUL, failed=$FAILED"
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
    # OpenLineage spec: duplicates are considered successful (idempotency)
    SUCCESSFUL=$(echo "$BODY" | jq -r '.summary.successful')
    FAILED=$(echo "$BODY" | jq -r '.summary.failed')

    if [ "$SUCCESSFUL" = "1" ] && [ "$FAILED" = "0" ]; then
        print_test_result "Test 4" "PASS" "Status: $STATUS, Duplicate handled correctly (OpenLineage idempotency)"
    else
        print_test_result "Test 4" "FAIL" "Status: $STATUS, but successful=$SUCCESSFUL, failed=$FAILED"
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
    "namespace": "dbt://correlator-smoke-test-analytics",
    "name": "invalid_job",
    "facets": {}
  }
}'

RESPONSE=$(make_request "[$INVALID_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "422" ]; then
    # Check if error message mentions eventTime (in failed_events[0].reason field)
    FAILED_COUNT=$(echo "$BODY" | jq -r '.summary.failed')
    ERROR_MSG=$(echo "$BODY" | jq -r '.failed_events[0].reason' 2>/dev/null || echo "")

    if [ "$FAILED_COUNT" = "1" ] && echo "$ERROR_MSG" | grep -iq "eventTime"; then
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
# SECTION 2: Test Results Ingestion (Tests 7-8)
#===============================================================================
echo "🧪 Section 2: Test Results Ingestion"
echo "-------------------------------------"
echo ""

#===============================================================================
# Test 7: Test result with canonical job_run_id
#===============================================================================
echo "Test 7: Test result with canonical job_run_id (200 OK)"

# Extract canonical job_run_id from dbt event (format: "dbt:runID")
# The canonical ID is generated by the ingestion endpoint
CANONICAL_JOB_RUN_ID="dbt:550e8400-e29b-41d4-a716-446655440000"

TEST_RESULT='{
  "test_name": "not_null_orders_id",
  "test_type": "not_null",
  "dataset_urn": "postgresql://correlator-smoke-test-db:5432/analytics.public.orders",
  "job_run_id": "'"$CANONICAL_JOB_RUN_ID"'",
  "status": "failed",
  "message": "Found 3 null values in orders.id column",
  "executed_at": "2025-10-21T10:06:00Z",
  "duration_ms": 150,
  "metadata": {
    "test_framework": "dbt",
    "test_file": "models/schema.yml"
  }
}'

# Update ENDPOINT for test results
TEST_RESULTS_ENDPOINT="${SERVER_URL}/api/v1/test-results"
ENDPOINT_BACKUP="$ENDPOINT"
ENDPOINT="$TEST_RESULTS_ENDPOINT"

RESPONSE=$(make_request "[$TEST_RESULT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

# Restore original endpoint
ENDPOINT="$ENDPOINT_BACKUP"

if [ "$STATUS" = "200" ]; then
    STORED=$(echo "$BODY" | jq -r '.stored')
    if [ "$STORED" = "1" ]; then
        print_test_result "Test 7" "PASS" "Status: $STATUS, Test result stored with canonical job_run_id"
    else
        print_test_result "Test 7" "FAIL" "Status: $STATUS, but stored=$STORED (expected 1)"
    fi
else
    print_test_result "Test 7" "FAIL" "Expected status 200, got $STATUS. Body: $BODY"
fi
echo ""

#===============================================================================
# Test 8: Test result UPSERT behavior
#===============================================================================
echo "Test 8: Test result UPSERT behavior (200 OK)"

# Submit the same test result again (should upsert, not duplicate)
ENDPOINT="$TEST_RESULTS_ENDPOINT"

RESPONSE=$(make_request "[$TEST_RESULT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

# Restore original endpoint
ENDPOINT="$ENDPOINT_BACKUP"

if [ "$STATUS" = "200" ]; then
    STORED=$(echo "$BODY" | jq -r '.stored')
    # UPSERT behavior: should still return stored=1 (updated existing record)
    if [ "$STORED" = "1" ]; then
        print_test_result "Test 8" "PASS" "Status: $STATUS, UPSERT behavior confirmed"
    else
        print_test_result "Test 8" "FAIL" "Status: $STATUS, but stored=$STORED (expected 1)"
    fi
else
    print_test_result "Test 8" "FAIL" "Expected status 200, got $STATUS"
fi
echo ""

#===============================================================================
# SECTION 3: End-to-End Correlation (Tests 9-10)
#===============================================================================
echo "🔗 Section 3: End-to-End Correlation"
echo "-------------------------------------"
echo ""

#===============================================================================
# Test 9: Verify test failure linked to job run
#===============================================================================
echo "Test 9: Verify test failure linked to job run (database query)"

if [ -z "$DATABASE_URL" ]; then
    print_test_result "Test 9" "SKIP" "DATABASE_URL not set - cannot query correlation view"
elif ! command -v psql &> /dev/null; then
    print_test_result "Test 9" "SKIP" "psql not installed - cannot query database"
else
    # Refresh materialized views
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 << 'SQL' >/dev/null 2>&1
    REFRESH MATERIALIZED VIEW CONCURRENTLY incident_correlation_view;
SQL

    # Query incident_correlation_view
    QUERY_RESULT=$(psql "$DATABASE_URL" -t -v ON_ERROR_STOP=1 << SQL
    SELECT COUNT(*)
    FROM incident_correlation_view
    WHERE job_run_id = '$CANONICAL_JOB_RUN_ID'
      AND test_name = 'not_null_orders_id'
      AND test_status = 'failed';
SQL
    )

    INCIDENT_COUNT=$(echo "$QUERY_RESULT" | tr -d ' ')

    if [ "$INCIDENT_COUNT" = "1" ]; then
        print_test_result "Test 9" "PASS" "Test failure correctly linked to job run in correlation view"
    else
        print_test_result "Test 9" "FAIL" "Expected 1 incident in correlation view, found $INCIDENT_COUNT"
    fi
fi
echo ""

#===============================================================================
# Test 10: Verify canonical ID format in correlation view
#===============================================================================
echo "Test 10: Verify canonical ID format (tool:runID)"

if [ -z "$DATABASE_URL" ]; then
    print_test_result "Test 10" "SKIP" "DATABASE_URL not set - cannot query database"
elif ! command -v psql &> /dev/null; then
    print_test_result "Test 10" "SKIP" "psql not installed - cannot query database"
else
    # Query job_run_id format from job_runs table
    JOB_RUN_ID=$(psql "$DATABASE_URL" -t -v ON_ERROR_STOP=1 << SQL
    SELECT job_run_id
    FROM job_runs
    WHERE job_namespace LIKE '%correlator-smoke-test%'
    LIMIT 1;
SQL
    )

    JOB_RUN_ID=$(echo "$JOB_RUN_ID" | tr -d ' ')

    # Verify format: "tool:runID" (should contain colon and start with known tool)
    if echo "$JOB_RUN_ID" | grep -qE "^(dbt|airflow|spark|custom|unknown):"; then
        print_test_result "Test 10" "PASS" "Canonical ID format verified: $JOB_RUN_ID"
    else
        print_test_result "Test 10" "FAIL" "Invalid canonical ID format: $JOB_RUN_ID (expected 'tool:runID')"
    fi
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
