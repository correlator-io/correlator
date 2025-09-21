.PHONY: build test test-unit test-integration test-race clean run dev docker-build docker-run setup migrate lint fmt vet build-all build-correlator build-ingester build-migrator build-migrator-prod deps tidy migrate-up migrate-down migrate-status migrate-version migrate-drop docker-build-migrator docker-migrate-up docker-migrate-down docker-migrate-status docker-migrate-version docker-migrate-drop docker-stop docker-logs help

# Variables
BINARY_NAME=correlator
DOCKER_TAG=correlator:latest
GO_VERSION=1.25

# Build commands
build:
	go build -o bin/$(BINARY_NAME) ./cmd/correlator

build-all: build-correlator build-ingester build-migrator

# Build individual components with enhanced versioning
build-correlator:
	go build -o bin/correlator ./cmd/correlator

build-ingester:
	go build -o bin/ingester ./cmd/ingester

build-migrator:
	@echo "Building migrator with enhanced versioning..."
	$(eval VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "1.0.0-dev"))
	$(eval COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown"))
	$(eval BUILD_TIME := $(shell date -u '+%Y-%m-%d %H:%M:%S UTC'))
	go build -ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(COMMIT) -X 'main.BuildTime=$(BUILD_TIME)'" -o bin/migrator ./migrations

# Development commands
dev:
	go run ./cmd/correlator

test:
	go test -short -v -cover ./... && go test -v -cover ./...

test-unit:
	go test -short -v -cover ./...

test-integration:
	go test -v -cover -timeout=10m ./...

test-race:
	go test -v -race -cover ./...

# Code quality
lint:
	golangci-lint run

fmt:
	golangci-lint fmt

vet:
	go vet ./...

# Dependencies
deps:
	go mod download
	go mod verify

tidy:
	go mod tidy

# Database Migration Commands (using embedded migrator)
migrate-up:
	go run ./migrations up

migrate-down:
	go run ./migrations down

migrate-status:
	go run ./migrations status

migrate-version:
	go run ./migrations version

migrate-drop:
	go run ./migrations drop

# Build migrator for production (optimized with enhanced versioning)
build-migrator-prod:
	@echo "Building production migrator with enhanced versioning..."
	$(eval VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "1.0.0-dev"))
	$(eval COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown"))
	$(eval BUILD_TIME := $(shell date -u '+%Y-%m-%d %H:%M:%S UTC'))
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(COMMIT) -X 'main.BuildTime=$(BUILD_TIME)'" -o build/migrator ./migrations

# Docker commands
docker-build:
	docker build -t $(DOCKER_TAG) .

# Build migrator Docker image
docker-build-migrator:
	cd deployments/docker && docker compose build migrator

# Docker migration commands (using containerized migrator)
docker-migrate-up:
	cd deployments/docker && docker compose --profile migration run --rm migrator ./migrator up

docker-migrate-down:
	cd deployments/docker && docker compose --profile migration run --rm migrator ./migrator down

docker-migrate-status:
	cd deployments/docker && docker compose --profile migration run --rm migrator ./migrator status

docker-migrate-version:
	cd deployments/docker && docker compose --profile migration run --rm migrator ./migrator version

docker-migrate-drop:
	cd deployments/docker && docker compose --profile migration run --rm migrator ./migrator drop

# Development environment setup
docker-dev-setup:
	cd deployments/docker && ./dev-setup.sh

# Start PostgreSQL only for development
docker-dev:
	cd deployments/docker && docker compose up postgres

# Start PostgreSQL in background
docker-dev-bg:
	cd deployments/docker && docker compose up -d postgres

# Start full stack (correlator + postgres)
docker-run:
	cd deployments/docker && docker compose --profile full up

docker-stop:
	cd deployments/docker && docker compose down

docker-logs:
	cd deployments/docker && docker compose logs -f

docker-logs-postgres:
	cd deployments/docker && docker compose logs -f postgres

# Health check for development environment
docker-health:
	cd deployments/docker && ./health-check.sh

# Setup commands
setup: deps
	cp .env.example .env
	@echo "Setup complete. Edit .env file with your configuration."

# Clean
clean:
	go clean
	rm -rf bin/

# Help
help:
	@echo "Available commands:"
	@echo ""
	@echo "Build & Development:"
	@echo "  build           - Build the correlator binary"
	@echo "  build-all       - Build all binaries"
	@echo "  dev             - Run in development mode"
	@echo "  test            - Run all tests (unit + integration)"
	@echo "  test-unit       - Run unit tests only (fast)"
	@echo "  test-integration - Run integration tests with real databases"
	@echo "  test-race       - Run tests with race detection"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint            - Run linter"
	@echo "  fmt             - Format code"
	@echo "  vet             - Vet code"
	@echo ""
	@echo "Dependencies:"
	@echo "  deps            - Download dependencies"
	@echo "  tidy            - Tidy dependencies"
	@echo ""
	@echo "Database Migration:"
	@echo "  migrate-up      - Apply all pending migrations"
	@echo "  migrate-down    - Rollback the last migration"
	@echo "  migrate-status  - Show migration status"
	@echo "  migrate-version - Show current migration version"
	@echo "  migrate-drop    - Drop all tables (requires confirmation)"
	@echo "  build-migrator  - Build migrator binary"
	@echo ""
	@echo "Docker Migration:"
	@echo "  docker-build-migrator - Build migrator Docker image"
	@echo "  docker-migrate-up     - Apply migrations using Docker"
	@echo "  docker-migrate-down   - Rollback migrations using Docker"
	@echo "  docker-migrate-status - Show migration status using Docker"
	@echo "  docker-migrate-version - Show migration version using Docker"
	@echo "  docker-migrate-drop   - Drop all tables using Docker"
	@echo ""
	@echo "Docker Development:"
	@echo "  docker-dev-setup - Setup development environment"
	@echo "  docker-dev       - Start PostgreSQL for development"
	@echo "  docker-dev-bg    - Start PostgreSQL in background"
	@echo "  docker-run       - Run full stack (correlator + postgres)"
	@echo "  docker-stop      - Stop all Docker services"
	@echo "  docker-logs      - View all service logs"
	@echo "  docker-logs-postgres - View PostgreSQL logs only"
	@echo "  docker-health    - Run development environment health checks"
	@echo ""
	@echo "Setup & Cleanup:"
	@echo "  setup           - Initial project setup"
	@echo "  clean           - Clean build artifacts"
