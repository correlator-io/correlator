.PHONY: build test clean run dev docker-build docker-run setup migrate lint fmt vet build-all test-race deps tidy migrate-up migrate-down migrate-create docker-stop docker-logs help

# Variables
BINARY_NAME=correlator
DOCKER_TAG=correlator:latest
GO_VERSION=1.23

# Build commands
build:
	go build -o bin/$(BINARY_NAME) ./cmd/correlator

build-all:
	go build -o bin/correlator ./cmd/correlator
	go build -o bin/ingester ./cmd/ingester
	go build -o bin/migrator ./cmd/migrator

# Development commands
dev:
	go run ./cmd/correlator

test:
	go test -v -cover ./...

test-race:
	go test -v -race -cover ./...

# Code quality
lint:
	golangci-lint run

fmt:
	go fmt ./...

vet:
	go vet ./...

# Dependencies  
deps:
	go mod download
	go mod verify

tidy:
	go mod tidy

# Database
migrate-up:
	go run ./cmd/migrator up

migrate-down:
	go run ./cmd/migrator down

migrate-create:
	@read -p "Enter migration name: " name; \
	go run ./cmd/migrator create $$name

# Docker commands
docker-build:
	docker build -t $(DOCKER_TAG) .

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
	@echo "  test            - Run tests"
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
	@echo "Database:"
	@echo "  migrate-up      - Run database migrations"
	@echo "  migrate-down    - Rollback database migrations"
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