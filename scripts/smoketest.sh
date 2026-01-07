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
#   Section 2: End-to-End Correlation (Tests 7-10)
#     - Verify canonical ID format in job_runs table ("tool:runID")
#     - Event with dataQualityAssertions facet (realistic dbt-correlator format)
#     - Test results extracted and stored in test_results table
#     - Failed test status correctly identified
#     - Note: Incident correlation view queries deferred to Week 3 (UI sprint)
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

# Helper function to validate OpenLineage response format
# Enforces strict compliance with OpenLineage batch response specification
# Reference: https://openlineage.io/apidocs/openapi/#tag/OpenLineage/operation/postEventBatch
validate_openlineage_response() {
    local body="$1"
    local expected_status="$2"  # Expected status field value: "success" or "error"
    local expected_received="$3"
    local expected_successful="$4"
    local expected_failed="$5"

    # Extract all response fields
    local status_field=$(echo "$body" | jq -r '.status')
    local received=$(echo "$body" | jq -r '.summary.received')
    local successful=$(echo "$body" | jq -r '.summary.successful')
    local failed=$(echo "$body" | jq -r '.summary.failed')
    local retriable=$(echo "$body" | jq -r '.summary.retriable')
    local non_retriable=$(echo "$body" | jq -r '.summary.non_retriable')
    local failed_events_count=$(echo "$body" | jq '.failed_events | length')

    # Validate all fields match expected values
    if [ "$status_field" = "$expected_status" ] && \
       [ "$received" = "$expected_received" ] && \
       [ "$successful" = "$expected_successful" ] && \
       [ "$failed" = "$expected_failed" ]; then

        # Validate invariants (consistency checks)
        local sum=$((successful + failed))
        if [ "$sum" != "$received" ]; then
            echo "FAIL: Invariant violation - received ($received) != successful ($successful) + failed ($failed)"
            return 1
        fi

        # Validate failed_events array consistency
        if [ "$failed" != "$failed_events_count" ]; then
            echo "FAIL: failed_events count mismatch - failed=$failed but array length=$failed_events_count"
            return 1
        fi

        # Validate status field consistency
        if [ "$expected_status" = "success" ] && [ "$failed" != "0" ]; then
            echo "FAIL: Status 'success' but failed=$failed (should be 0)"
            return 1
        fi

        if [ "$expected_status" = "error" ] && [ "$successful" != "0" ]; then
            echo "FAIL: Status 'error' but successful=$successful (should be 0)"
            return 1
        fi

        echo "PASS: status=$status_field, received=$received, successful=$successful, failed=$failed, retriable=$retriable, non_retriable=$non_retriable"
        return 0
    else
        echo "FAIL: status=$status_field (expected $expected_status), received=$received (expected $expected_received), successful=$successful (expected $expected_successful), failed=$failed (expected $expected_failed)"
        return 1
    fi
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
    # Validate OpenLineage compliance (strict validation)
    VALIDATION_RESULT=$(validate_openlineage_response "$BODY" "success" 1 1 0)
    if [ $? -eq 0 ]; then
        print_test_result "Test 1" "PASS" "OpenLineage compliant - $VALIDATION_RESULT"
    else
        print_test_result "Test 1" "FAIL" "$VALIDATION_RESULT"
    fi
else
    print_test_result "Test 1" "FAIL" "Expected HTTP 200, got $STATUS"
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
    # Validate OpenLineage compliance (strict validation)
    VALIDATION_RESULT=$(validate_openlineage_response "$BODY" "success" 1 1 0)
    if [ $? -eq 0 ]; then
        print_test_result "Test 2" "PASS" "OpenLineage compliant - $VALIDATION_RESULT"
    else
        print_test_result "Test 2" "FAIL" "$VALIDATION_RESULT"
    fi
else
    print_test_result "Test 2" "FAIL" "Expected HTTP 200, got $STATUS"
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
    # Validate OpenLineage compliance (strict validation)
    VALIDATION_RESULT=$(validate_openlineage_response "$BODY" "success" 1 1 0)
    if [ $? -eq 0 ]; then
        print_test_result "Test 3" "PASS" "OpenLineage compliant - $VALIDATION_RESULT"
    else
        print_test_result "Test 3" "FAIL" "$VALIDATION_RESULT"
    fi
else
    print_test_result "Test 3" "FAIL" "Expected HTTP 200, got $STATUS"
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
    VALIDATION_RESULT=$(validate_openlineage_response "$BODY" "success" 1 1 0)
    if [ $? -eq 0 ]; then
        print_test_result "Test 4" "PASS" "OpenLineage idempotency - $VALIDATION_RESULT"
    else
        print_test_result "Test 4" "FAIL" "$VALIDATION_RESULT"
    fi
else
    print_test_result "Test 4" "FAIL" "Expected HTTP 200, got $STATUS"
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
    # Validate OpenLineage error response (status="error", all events failed)
    VALIDATION_RESULT=$(validate_openlineage_response "$BODY" "error" 1 0 1)
    if [ $? -eq 0 ]; then
        # Additionally check error message mentions eventTime
        ERROR_MSG=$(echo "$BODY" | jq -r '.failed_events[0].reason' 2>/dev/null || echo "")
        if echo "$ERROR_MSG" | grep -iq "eventTime"; then
            print_test_result "Test 5" "PASS" "OpenLineage compliant - $VALIDATION_RESULT, error mentions 'eventTime'"
        else
            print_test_result "Test 5" "FAIL" "OpenLineage format OK but error doesn't mention 'eventTime': $ERROR_MSG"
        fi
    else
        print_test_result "Test 5" "FAIL" "$VALIDATION_RESULT"
    fi
else
    print_test_result "Test 5" "FAIL" "Expected HTTP 422, got $STATUS"
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
# SECTION 2: End-to-End Correlation (Tests 7-8)
#===============================================================================
echo "🔗 Section 2: End-to-End Correlation"
echo "-------------------------------------"
echo ""

# Canonical job_run_id from dbt event (format: "dbt:runID")
CANONICAL_JOB_RUN_ID="dbt:550e8400-e29b-41d4-a716-446655440000"

#===============================================================================
# Test 7: Verify canonical ID format in correlation view
#===============================================================================
echo "Test 7: Verify canonical ID format (tool:runID)"

if [ -z "$DATABASE_URL" ]; then
    print_test_result "Test 7" "SKIP" "DATABASE_URL not set - cannot query database"
elif ! command -v psql &> /dev/null; then
    print_test_result "Test 7" "SKIP" "psql not installed - cannot query database"
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
        print_test_result "Test 7" "PASS" "Canonical ID format verified: $JOB_RUN_ID"
    else
        print_test_result "Test 7" "FAIL" "Invalid canonical ID format: $JOB_RUN_ID (expected 'tool:runID')"
    fi
fi
echo ""

#===============================================================================
# Test 8: E2E Correlation - dataQualityAssertions extraction
#===============================================================================
# This test validates the core correlation feature:
# - OpenLineage event with dataQualityAssertions facet
# - Test results extracted and stored in test_results table
# - Both passing and failing assertions included
#
# Note: Incident correlation view query tests deferred to Week 3 (UI sprint)
#===============================================================================
echo "Test 8: E2E event with dataQualityAssertions facet (200 OK)"

E2E_CORRELATION_EVENT='{
  "eventTime": "2025-10-21T10:30:00Z",
  "eventType": "COMPLETE",
  "producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440003"
  },
  "job": {
    "namespace": "dbt://correlator-smoke-test-analytics",
    "name": "jaffle_shop.test"
  },
  "inputs": [
    {
      "namespace": "postgres://correlator-smoke-test-db:5432",
      "name": "marts.orders",
      "inputFacets": {
        "dataQualityAssertions": {
          "_producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
          "assertions": [
            {
              "assertion": "not_null(order_id)",
              "success": true,
              "column": "order_id"
            },
            {
              "assertion": "unique(order_id)",
              "success": false,
              "column": "order_id"
            }
          ]
        }
      }
    }
  ],
  "outputs": []
}'

RESPONSE=$(make_request "[$E2E_CORRELATION_EVENT]")
STATUS=$(echo "$RESPONSE" | jq -r '.status')
BODY=$(echo "$RESPONSE" | jq -r '.body')

if [ "$STATUS" = "200" ]; then
    # Validate OpenLineage compliance
    VALIDATION_RESULT=$(validate_openlineage_response "$BODY" "success" 1 1 0)
    if [ $? -eq 0 ]; then
        print_test_result "Test 8" "PASS" "OpenLineage compliant with dataQualityAssertions - $VALIDATION_RESULT"
    else
        print_test_result "Test 8" "FAIL" "$VALIDATION_RESULT"
    fi
else
    print_test_result "Test 8" "FAIL" "Expected HTTP 200, got $STATUS"
fi
echo ""

#===============================================================================
# Test 9: Verify test_results table populated from dataQualityAssertions
#===============================================================================
echo "Test 9: Verify test results extracted from facet"

# Canonical job_run_id for E2E test event (format: "dbt:runID")
E2E_JOB_RUN_ID="dbt:550e8400-e29b-41d4-a716-446655440003"

if [ -z "$DATABASE_URL" ]; then
    print_test_result "Test 9" "SKIP" "DATABASE_URL not set - cannot query database"
elif ! command -v psql &> /dev/null; then
    print_test_result "Test 9" "SKIP" "psql not installed - cannot query database"
else
    TEST_COUNT=$(psql "$DATABASE_URL" -t -v ON_ERROR_STOP=1 << SQL
    SELECT COUNT(*) FROM test_results
    WHERE job_run_id = '$E2E_JOB_RUN_ID';
SQL
    )
    TEST_COUNT=$(echo "$TEST_COUNT" | tr -d ' ')

    if [ "$TEST_COUNT" -ge 2 ]; then
        print_test_result "Test 9" "PASS" "Test results extracted: $TEST_COUNT assertions stored in test_results table"
    else
        print_test_result "Test 9" "FAIL" "Expected ≥2 test results, got $TEST_COUNT (dataQualityAssertions extraction may have failed)"
    fi
fi
echo ""

#===============================================================================
# Test 10: Verify failed test status extracted correctly
#===============================================================================
echo "Test 10: Verify failed test detected in test_results"

if [ -z "$DATABASE_URL" ]; then
    print_test_result "Test 10" "SKIP" "DATABASE_URL not set - cannot query database"
elif ! command -v psql &> /dev/null; then
    print_test_result "Test 10" "SKIP" "psql not installed - cannot query database"
else
    FAILED_TEST=$(psql "$DATABASE_URL" -t -v ON_ERROR_STOP=1 << SQL
    SELECT test_name FROM test_results
    WHERE job_run_id = '$E2E_JOB_RUN_ID'
      AND status = 'failed'
    LIMIT 1;
SQL
    )
    FAILED_TEST=$(echo "$FAILED_TEST" | xargs)  # Trim whitespace

    if [ -n "$FAILED_TEST" ]; then
        print_test_result "Test 10" "PASS" "Failed test detected: '$FAILED_TEST' (correlation ready)"
    else
        print_test_result "Test 10" "FAIL" "No failed test found in test_results (status mapping may have failed)"
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
