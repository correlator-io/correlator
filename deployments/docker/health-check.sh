#!/bin/bash

# Correlator Health Check Script
# Validates that the development environment is working correctly

set -e

echo "🏥 Running Correlator health checks..."

# Function to check if service is running
check_service() {
    local service_name=$1
    local container_name=$2

    if docker compose ps --services --filter "status=running" | grep -q "^${service_name}$"; then
        echo "✅ $service_name service is running"
        return 0
    else
        echo "❌ $service_name service is not running"
        return 1
    fi
}

# Function to wait for service to be healthy
wait_for_health() {
    local service_name=$1
    local max_attempts=30
    local attempt=1

    echo "⏳ Waiting for $service_name to be healthy..."

    while [[ $attempt -le $max_attempts ]]; do
        local health_status=$(docker compose ps --format json | jq -r ".[] | select(.Service == \"$service_name\") | .Health")

        case $health_status in
            "healthy")
                echo "✅ $service_name is healthy"
                return 0
                ;;
            "unhealthy")
                echo "❌ $service_name is unhealthy"
                return 1
                ;;
            "starting"|"")
                echo "   Attempt $attempt/$max_attempts: $service_name health status: ${health_status:-starting}"
                sleep 2
                ;;
        esac

        ((attempt++))
    done

    echo "❌ $service_name failed to become healthy after $max_attempts attempts"
    return 1
}

# Function to test database connectivity and correlation setup
test_database() {
    echo "🔍 Testing database connectivity and correlation setup..."

    # Test basic connectivity
    if docker compose exec -T postgres pg_isready -U correlator >/dev/null 2>&1; then
        echo "✅ Database connectivity confirmed"
    else
        echo "❌ Database connectivity failed"
        return 1
    fi

    # Test correlation-specific extensions
    echo "🔍 Checking correlation-specific extensions..."

    local extensions_query="SELECT extname FROM pg_extension WHERE extname IN ('pg_trgm', 'pg_stat_statements');"
    local extensions=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "$extensions_query" 2>/dev/null | tr -d ' ')

    if echo "$extensions" | grep -q "pg_trgm"; then
        echo "✅ pg_trgm extension is installed (fuzzy text matching for canonical IDs)"
    else
        echo "❌ pg_trgm extension is missing"
        return 1
    fi

    if echo "$extensions" | grep -q "pg_stat_statements"; then
        echo "✅ pg_stat_statements extension is installed (query performance monitoring)"
    else
        echo "❌ pg_stat_statements extension is missing"
        return 1
    fi

    # Test correlation monitoring view
    echo "🔍 Testing correlation monitoring setup..."
    local view_exists=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'correlation_query_stats');" 2>/dev/null | tr -d ' ')

    if [[ "$view_exists" == "t" ]]; then
        echo "✅ correlation_query_stats monitoring view is available"
    else
        echo "⚠️  correlation_query_stats monitoring view is missing (non-critical)"
    fi

    return 0
}

# Function to display system information
show_system_info() {
    echo ""
    echo "📊 System Information:"
    echo "   Docker version: $(docker --version)"
    echo "   Docker Compose version: $(docker compose version --short)"

    if docker compose ps --services --filter "status=running" | grep -q postgres; then
        local pg_version=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "SELECT version();" 2>/dev/null | head -1 | sed 's/^ *//')
        echo "   PostgreSQL: ${pg_version}"

        # Show memory configuration
        echo ""
        echo "📈 PostgreSQL Configuration (Correlation-Optimized):"
        local configs=("shared_buffers" "effective_cache_size" "work_mem" "random_page_cost")
        for config in "${configs[@]}"; do
            local value=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "SHOW $config;" 2>/dev/null | tr -d ' ')
            echo "   $config: $value"
        done
    fi
}

# Main health check execution
main() {
    local exit_code=0

    # Check if docker-compose.yml exists
    if [[ ! -f "docker-compose.yml" ]]; then
        echo "❌ Error: Must run from deployments/docker directory"
        exit 1
    fi

    # Check if services are running
    if check_service "postgres" "correlator-postgres"; then
        wait_for_health "postgres" || exit_code=1
        test_database || exit_code=1
    else
        echo "ℹ️  PostgreSQL is not running. Start with: docker compose up -d postgres"
        exit_code=1
    fi

    # Check correlator service if running
    if docker compose ps --services --filter "status=running" | grep -q "correlator"; then
        echo "✅ Correlator service is running"
    else
        echo "ℹ️  Correlator service is not running (expected in Phase 2)"
    fi

    show_system_info

    echo ""
    if [[ $exit_code -eq 0 ]]; then
        echo "🎉 All health checks passed! Correlator development environment is ready."
    else
        echo "⚠️  Some health checks failed. Review the errors above."
    fi

    return $exit_code
}

# Run health checks
main "$@"
