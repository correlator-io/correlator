#!/bin/bash

# Correlator Development Environment Diagnostics
# Use this to troubleshoot development environment issues

set -e

echo "🔍 Diagnosing Correlator development environment..."
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Track issues found
ISSUES_FOUND=0

# Helper function to report issues
report_issue() {
    echo -e "${RED}❌ $1${NC}"
    ((ISSUES_FOUND++))
}

report_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

report_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

report_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if we're in the right directory
echo "📂 Checking directory structure..."
if [[ ! -f "docker-compose.yml" ]]; then
    report_issue "Must run from deployments/docker directory"
    echo "   Current directory: $(pwd)"
    exit 1
else
    report_success "Running from correct directory"
fi

# Check .env file
echo ""
echo "📝 Checking environment configuration..."
if [[ ! -f ".env" ]]; then
    report_issue ".env file missing"
    echo "   Run: make start (to create from template)"
elif [[ ! -f ".env.example" ]]; then
    report_warning ".env.example template missing"
else
    report_success ".env file exists"

    # Check if .env has required variables
    if grep -q "POSTGRES_PASSWORD=" .env && grep -q "DATABASE_URL=" .env; then
        report_success "Required environment variables present"
    else
        report_warning "Some environment variables may be missing"
        echo "   Check: POSTGRES_PASSWORD, DATABASE_URL"
    fi
fi

# Docker system diagnostics
echo ""
echo "🐳 Checking Docker system..."
if ! command -v docker >/dev/null 2>&1; then
    report_issue "Docker not installed or not in PATH"
else
    report_success "Docker CLI available"

    if ! docker info >/dev/null 2>&1; then
        report_issue "Docker daemon not running"
        echo "   Solution: Start Docker Desktop or Docker service"
    else
        report_success "Docker daemon running"

        # Docker version info
        DOCKER_VERSION=$(docker --version | cut -d' ' -f3 | cut -d',' -f1)
        report_info "Docker version: $DOCKER_VERSION"
    fi
fi

# Docker Compose check
if docker compose version >/dev/null 2>&1; then
    report_success "Docker Compose available"
    COMPOSE_VERSION=$(docker compose version --short)
    report_info "Docker Compose version: $COMPOSE_VERSION"
elif docker-compose --version >/dev/null 2>&1; then
    report_warning "Using legacy docker-compose"
    echo "   Consider upgrading to Docker Compose V2"
else
    report_issue "Docker Compose not available"
fi

# Port availability check
echo ""
echo "🔌 Checking port availability..."
check_port() {
    local port=$1
    local service=$2

    if ss -tuln 2>/dev/null | grep -q ":$port " || netstat -tuln 2>/dev/null | grep -q ":$port "; then
        report_warning "Port $port is in use (needed for $service)"
        echo "   Check what's using it: lsof -i :$port (macOS/Linux)"
        return 1
    else
        report_success "Port $port available for $service"
        return 0
    fi
}

check_port 5432 "PostgreSQL"
check_port 8080 "Correlator API"
check_port 9090 "Correlator Metrics"

# Container status check
echo ""
echo "📦 Checking container status..."
if docker compose ps >/dev/null 2>&1; then
    RUNNING_CONTAINERS=$(docker compose ps --services --filter "status=running" | wc -l)
    TOTAL_CONTAINERS=$(docker compose ps --services | wc -l)

    if [[ $RUNNING_CONTAINERS -gt 0 ]]; then
        report_info "$RUNNING_CONTAINERS/$TOTAL_CONTAINERS containers running"
        docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    else
        report_info "No containers currently running"
    fi

    # Check for exited containers
    EXITED_CONTAINERS=$(docker compose ps --services --filter "status=exited" | wc -l)
    if [[ $EXITED_CONTAINERS -gt 0 ]]; then
        report_warning "$EXITED_CONTAINERS containers have exited"
        echo "   Check logs: make docker logs"
    fi
else
    report_info "No Docker Compose services defined in current context"
fi

# Migration files check
echo ""
echo "📋 Checking migration files..."
MIGRATIONS_DIR="../../migrations"
if [[ ! -d "$MIGRATIONS_DIR" ]]; then
    report_issue "Migrations directory not found at $MIGRATIONS_DIR"
else
    MIGRATION_COUNT=$(find "$MIGRATIONS_DIR" -name "*.sql" | wc -l)
    if [[ $MIGRATION_COUNT -eq 0 ]]; then
        report_warning "No migration files found"
        echo "   Database will start empty"
    else
        report_success "Found $MIGRATION_COUNT migration files"
    fi

    # Check for migration binary
    if [[ -f "../../bin/migrator" ]]; then
        report_success "Migrator binary exists"
    else
        report_info "Migrator binary not built (will be created on first use)"
    fi
fi

# Dev container diagnostics
echo ""
echo "🏗️  Checking dev container setup..."
if [[ -f "../../.devcontainer/devcontainer.json" ]]; then
    report_success "Dev container configuration found"

    # Check if devcontainer CLI is available
    if command -v devcontainer >/dev/null 2>&1; then
        report_success "devcontainer CLI available"

        # Check if dev container is running
        if docker ps --format "table {{.Names}}" | grep -q "correlator-dev-container"; then
            report_success "Dev container is running"
        else
            report_info "Dev container not currently running"
        fi
    else
        report_info "devcontainer CLI not installed (will be installed automatically)"
    fi
else
    report_warning "Dev container configuration missing"
fi

# Network connectivity (if containers are running)
echo ""
echo "🌐 Checking network connectivity..."
if docker compose ps postgres --format "table {{.State}}" | grep -q "running"; then
    if docker compose exec -T postgres pg_isready -U correlator >/dev/null 2>&1; then
        report_success "PostgreSQL is accepting connections"
    else
        report_warning "PostgreSQL not accepting connections yet"
        echo "   It may still be starting up"
    fi
else
    report_info "PostgreSQL not running - cannot test connectivity"
fi

# System resources check
echo ""
echo "💾 Checking system resources..."
if command -v df >/dev/null 2>&1; then
    DISK_USAGE=$(df -h . | awk 'NR==2 {print $5}' | sed 's/%//')
    if [[ $DISK_USAGE -gt 90 ]]; then
        report_warning "Disk usage is high: ${DISK_USAGE}%"
    else
        report_success "Disk space available: $((100-DISK_USAGE))% free"
    fi
fi

if command -v free >/dev/null 2>&1; then
    # Linux memory check
    MEM_USAGE=$(free | awk 'NR==2{printf "%.0f", $3*100/$2}')
    if [[ $MEM_USAGE -gt 85 ]]; then
        report_warning "Memory usage is high: ${MEM_USAGE}%"
    else
        report_success "Memory available: $((100-MEM_USAGE))% free"
    fi
elif command -v vm_stat >/dev/null 2>&1; then
    # macOS memory check
    report_info "Memory info available via: vm_stat"
fi

# Final summary
echo ""
echo "═══════════════════════════════════════════════════════════════"
if [[ $ISSUES_FOUND -eq 0 ]]; then
    echo -e "${GREEN}🎉 No critical issues found! Environment looks healthy.${NC}"
    echo ""
    echo "🚀 Ready to develop:"
    echo "   make start    # Start development environment"
    echo "   make run      # Start development server (from host)"
    echo "   make check    # Run code quality checks (from dev container)"
else
    echo -e "${RED}🚨 Found $ISSUES_FOUND issue(s) that need attention.${NC}"
    echo ""
    echo "🔧 Troubleshooting steps:"
    echo "   1. Address the issues listed above"
    echo "   2. Run this diagnostic again: make docker health"
    echo "   3. Try: make reset && make start"
fi

echo ""
echo "📚 Additional help:"
echo "   make help           # Show all available commands"
echo "   make docker logs    # View container logs"
echo "   make reset          # Nuclear reset (when things are broken)"

exit $ISSUES_FOUND
