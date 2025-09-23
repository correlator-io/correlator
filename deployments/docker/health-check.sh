#!/bin/bash

# Correlator Health Check Script
# Validates that the PostgreSQL database is ready for development

set -e

echo "🏥 Running Correlator database health checks..."

# Function to check if PostgreSQL service is running
check_postgres_running() {
    echo "🔍 Checking if PostgreSQL service is running..."

    if docker compose ps postgres --format "table {{.State}}" | grep -q "running"; then
        echo "✅ PostgreSQL container is running"
        return 0
    else
        echo "❌ PostgreSQL container is not running"
        echo "💡 Start with: docker compose up -d postgres"
        return 1
    fi
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    echo "⏳ Waiting for PostgreSQL to accept connections..."

    local max_attempts=30
    local attempt=1

    while [[ $attempt -le $max_attempts ]]; do
        if docker compose exec -T postgres pg_isready -U correlator >/dev/null 2>&1; then
            echo "✅ PostgreSQL is accepting connections"
            return 0
        else
            echo "   Attempt $attempt/$max_attempts: waiting for PostgreSQL..."
            sleep 2
            ((attempt++))
        fi
    done

    echo "❌ PostgreSQL failed to accept connections after $max_attempts attempts"
    return 1
}

# Function to test database connectivity and basic setup
test_database() {
    echo "🔍 Testing database connectivity and setup..."

    # Test database connection
    if docker compose exec -T postgres psql -U correlator -d correlator -c "SELECT 1;" >/dev/null 2>&1; then
        echo "✅ Database connection successful"
    else
        echo "❌ Database connection failed"
        return 1
    fi

    # Check if we can query basic information
    local db_exists=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "SELECT current_database();" 2>/dev/null | tr -d ' \n')
    if [[ "$db_exists" == "correlator" ]]; then
        echo "✅ Database 'correlator' is accessible"
    else
        echo "❌ Database 'correlator' is not accessible"
        return 1
    fi

    return 0
}

# Function to check PostgreSQL configuration
check_postgres_config() {
    echo "🔍 Checking PostgreSQL configuration..."

    # Check shared_buffers (important for performance)
    local shared_buffers=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "SHOW shared_buffers;" 2>/dev/null | tr -d ' \n')
    echo "   shared_buffers: $shared_buffers"

    # Check max_connections
    local max_connections=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "SHOW max_connections;" 2>/dev/null | tr -d ' \n')
    echo "   max_connections: $max_connections"

    echo "✅ PostgreSQL configuration checked"
    return 0
}

# Function to display system information
show_system_info() {
    echo ""
    echo "📊 System Information:"
    echo "   Docker version: $(docker --version 2>/dev/null || echo 'Docker not found')"

    # Try both Docker Compose variants for version info
    if docker compose version --short >/dev/null 2>&1; then
        echo "   Docker Compose version: $(docker compose version --short)"
    elif docker-compose --version >/dev/null 2>&1; then
        echo "   Docker Compose version: $(docker-compose --version)"
    else
        echo "   Docker Compose version: Not found"
    fi

    if docker compose ps postgres --format "table {{.State}}" | grep -q "running"; then
        local pg_version=$(docker compose exec -T postgres psql -U correlator -d correlator -t -c "SELECT version();" 2>/dev/null | head -1 | sed 's/^ *//' || echo "Unable to get PostgreSQL version")
        echo "   PostgreSQL: ${pg_version}"
    else
        echo "   PostgreSQL: Not running"
    fi
}

# Main health check execution
main() {
    local exit_code=0

    # Check if docker-compose.yml exists
    if [[ ! -f "docker-compose.yml" ]]; then
        echo "❌ Error: docker-compose.yml not found"
        echo "💡 Make sure to run this script from the deployments/docker directory"
        exit 1
    fi

    # Check Docker availability
    if ! command -v docker >/dev/null 2>&1; then
        echo "❌ Docker is not installed or not in PATH"
        exit 1
    fi

    # Check Docker Compose availability (try both new and old syntax)
    if docker compose version >/dev/null 2>&1; then
        echo "✅ Docker Compose (plugin) is available"
    elif docker-compose --version >/dev/null 2>&1; then
        echo "✅ Docker Compose (standalone) is available"
        echo "⚠️  Note: Using legacy docker-compose. Consider upgrading to Docker Compose plugin"
    else
        echo "❌ Docker Compose is not available"
        echo "💡 Install Docker Compose: https://docs.docker.com/compose/install/"
        exit 1
    fi

    echo ""

    # Run health checks
    if check_postgres_running; then
        if wait_for_postgres; then
            test_database || exit_code=1
            check_postgres_config || exit_code=1
        else
            exit_code=1
        fi
    else
        exit_code=1
    fi

    show_system_info

    echo ""
    if [[ $exit_code -eq 0 ]]; then
        echo "🎉 All health checks passed! PostgreSQL is ready for development."
        echo ""
        echo "🚀 Next steps:"
        echo "   make run migrate up    # Apply database migrations"
        echo "   make run               # Start development server"
    else
        echo "⚠️  Health checks failed. Review the errors above."
        echo ""
        echo "🔧 Troubleshooting:"
        echo "   docker compose logs postgres    # View PostgreSQL logs"
        echo "   docker compose down             # Stop all services"
        echo "   docker compose up -d postgres   # Restart PostgreSQL"
    fi

    return $exit_code
}

# Run health checks
main "$@"
