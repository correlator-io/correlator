#!/bin/bash

# Correlator Development Environment Setup Script
# This script prepares the development environment for zero-config deployment

set -e  # Exit on any error

echo "🚀 Setting up Correlator development environment..."

# Check if we're in the right directory
if [[ ! -f "docker-compose.yml" ]]; then
    echo "❌ Error: Must run from deployments/docker directory"
    echo "   Current directory: $(pwd)"
    echo "   Expected files: docker-compose.yml"
    exit 1
fi

# Create .env file if it doesn't exist
if [[ ! -f ".env" ]]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ Created .env file with default development values"
    echo "   You can edit .env to customize database password"
else
    echo "✅ .env file already exists"
fi

# Validate Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    echo "   Please start Docker and try again"
    exit 1
fi

# Validate Docker Compose is available
if ! docker compose version >/dev/null 2>&1; then
    echo "❌ Error: docker compose is not available"
    echo "   Please install Docker Compose and try again"
    exit 1
fi

echo "🐳 Docker environment validated"

# Check if PostgreSQL port is available
if ss -tuln 2>/dev/null | grep -q ':5432 ' || netstat -tuln 2>/dev/null | grep -q ':5432 '; then
    echo "⚠️  Warning: Port 5432 is already in use"
    echo "   This might conflict with PostgreSQL container"
    echo "   You may need to stop other PostgreSQL instances"
fi

# Validate migrations directory exists
MIGRATIONS_DIR="../../migrations"
if [[ ! -d "$MIGRATIONS_DIR" ]]; then
    echo "❌ Error: Migrations directory not found at $MIGRATIONS_DIR"
    echo "   Please ensure you're in the correct project structure"
    exit 1
fi

# Count migration files
MIGRATION_COUNT=$(find "$MIGRATIONS_DIR" -name "*.sql" | wc -l)
echo "📋 Found $MIGRATION_COUNT migration files"

if [[ $MIGRATION_COUNT -eq 0 ]]; then
    echo "⚠️  Warning: No migration files found"
    echo "   Database will start empty"
fi

echo ""
echo "✅ Development environment setup complete!"
echo ""
echo "📚 Next steps:"
echo "   1. Start PostgreSQL only:    docker compose up postgres"
echo "   2. Start with logs:          docker compose up postgres --attach postgres"
echo "   3. Run in background:        docker compose up -d postgres"
echo "   4. Check health:             docker compose ps"
echo "   5. Connect to database:      docker compose exec postgres psql -U correlator -d correlator"
echo ""
echo "🔧 For development with correlator service:"
echo "   docker compose --profile full up"
echo ""
echo "🛑 To stop:"
echo "   docker compose down"
echo ""