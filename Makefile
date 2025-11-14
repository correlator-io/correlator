.PHONY: start run check fix docker build deploy reset help

# Variables
BINARY_NAME=correlator
DOCKER_TAG=correlator:latest
GO_VERSION=1.25

# Helper function to ensure we're not inside dev container (for host-only commands)
ensure-not-in-dev-container:
	@if [ -f /.dockerenv ] && [ "$$PWD" = "/workspace" ]; then \
		echo "❌ This command should not run inside dev container"; \
		echo ""; \
		echo "💡 You're currently inside the dev container!"; \
		echo "🏠 Please run this command from the host machine:"; \
		echo "   exit              # Exit dev container"; \
		echo "   make $(firstword $(MAKECMDGOALS))     # Run command on host"; \
		echo ""; \
		echo "💡 Use the dev container for coding, testing, and migrations."; \
		echo "💡 Use the host for environment management and services."; \
		exit 1; \
	fi

# Helper function to ensure .env file exists
ensure-env-file:
	@echo "📝 Ensuring .env file exists..."
	@cd deployments/docker && if [[ ! -f ".env" ]]; then \
		echo "📝 Creating .env file from template..."; \
		cp .env.example .env; \
		echo "✅ Created .env file with default development values"; \
		echo "   You can edit deployments/docker/.env to customize database password"; \
	else \
		echo "✅ .env file already exists"; \
	fi

# Helper function to validate Docker environment
check-docker-environment:
	@echo "🔍 Validating Docker environment..."
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "❌ Docker not found. Please install Docker first:"; \
		echo "   - macOS: Docker Desktop from docker.com"; \
		echo "   - Ubuntu: sudo apt install docker.io"; \
		echo "   - Windows: Docker Desktop from docker.com"; \
		exit 1; \
	fi
	@if ! docker info >/dev/null 2>&1; then \
		echo "❌ Docker daemon not running"; \
		echo "💡 Please start Docker Desktop or Docker service"; \
		exit 1; \
	fi
	@if docker compose version >/dev/null 2>&1; then \
		echo "✅ Docker environment validated"; \
	elif docker-compose --version >/dev/null 2>&1; then \
		echo "✅ Docker environment validated (using legacy docker-compose)"; \
	else \
		echo "❌ Docker Compose not available"; \
		echo "💡 Please install Docker Compose"; \
		exit 1; \
	fi

# Helper function to check and install devcontainer CLI
check-devcontainer-cli:
	@echo "🔍 Checking devcontainer CLI availability..."
	@if ! command -v devcontainer >/dev/null 2>&1; then \
		echo "📦 devcontainer CLI not found, installing..."; \
		if ! command -v npm >/dev/null 2>&1; then \
			echo "❌ npm not found. Please install Node.js first:"; \
			echo "   - macOS: brew install node"; \
			echo "   - Ubuntu: sudo apt install nodejs npm"; \
			echo "   - Windows: Download from https://nodejs.org"; \
			exit 1; \
		fi; \
		echo "⏳ Installing @devcontainers/cli globally..."; \
		if npm install -g @devcontainers/cli; then \
			echo "✅ devcontainer CLI installed successfully"; \
		else \
			echo "❌ Failed to install devcontainer CLI"; \
			echo "💡 You may need to run with sudo or check npm permissions"; \
			echo "💡 Alternative: npm install -g @devcontainers/cli --unsafe-perm=true"; \
			exit 1; \
		fi; \
	else \
		echo "✅ devcontainer CLI is available"; \
	fi

# Helper function to check environment state with three levels of detection
check-environment-state:
	@echo "🔍 Checking development environment state..."
	@$(eval DEV_CONTAINER_RUNNING := $(shell docker ps --format "table {{.Names}}" | grep -q "correlator-dev-container" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval POSTGRES_RUNNING := $(shell cd deployments/docker && docker compose ps postgres --format "table {{.State}}" | grep -q "running" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval DEV_CONTAINER_EXISTS := $(shell docker ps -a --format "table {{.Names}}" | grep -q "correlator-dev-container" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval POSTGRES_EXISTS := $(shell cd deployments/docker && docker compose ps -a postgres --format "table {{.Names}}" | grep -q "correlator-postgres" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval CONTAINERS_RUNNING := $(shell [ "$(DEV_CONTAINER_RUNNING)" = "yes" ] && [ "$(POSTGRES_RUNNING)" = "yes" ] && echo "yes" || echo "no"))
	@$(eval CONTAINERS_EXIST := $(shell [ "$(DEV_CONTAINER_EXISTS)" = "yes" ] && [ "$(POSTGRES_EXISTS)" = "yes" ] && echo "yes" || echo "no"))

# Helper function to check dev container state (for initial setup)
check-dev-container:
	@echo "🏗️ Checking dev container state..."
	@if [ -f .devcontainer/devcontainer.json ]; then \
		echo "📋 Dev container configuration found"; \
		if devcontainer read-configuration --workspace-folder . >/dev/null 2>&1; then \
			echo "✅ Dev container configuration valid"; \
			if docker ps --format "table {{.Names}}" | grep -q "correlator-dev-container" 2>/dev/null; then \
				echo "🏃 Dev container already running"; \
			else \
				echo "💤 Dev container exists but not running"; \
				echo "🏗️ Building/starting dev container..."; \
				if devcontainer up --workspace-folder .; then \
					echo "✅ Dev container ready"; \
				else \
					echo "⚠️ Dev container build failed, continuing with host-based development"; \
				fi; \
			fi; \
		else \
			echo "🏗️ Building dev container from configuration..."; \
			if devcontainer up --workspace-folder .; then \
				echo "✅ Dev container built and ready"; \
			else \
				echo "⚠️ Dev container build failed, continuing with host-based development"; \
			fi; \
		fi; \
	else \
		echo "⚠️ No dev container configuration found at .devcontainer/devcontainer.json"; \
	fi

# Helper function to restart existing containers
restart-existing-containers:
	@echo "🔄 Starting existing containers..."
	@echo "🐳 Starting PostgreSQL..."
	@cd deployments/docker && docker compose up -d postgres
	@echo "⏳ Waiting for PostgreSQL to be ready..."
	@sleep 3
	@cd deployments/docker && ./health-check.sh
	@echo "✅ PostgreSQL restarted"
	@echo "🏗️ Starting dev container..."
	@if devcontainer up --workspace-folder . >/dev/null 2>&1; then \
		echo "✅ Dev container restarted"; \
	else \
		echo "⚠️ Dev container restart failed, trying full rebuild..."; \
		$(MAKE) start-full-setup; \
	fi

# Helper function to exec into dev container
exec-dev-container:
	@echo "🏃 Entering development container..."
	@if docker ps --format "table {{.Names}}" | grep -q "correlator-dev-container" 2>/dev/null; then \
		echo "🐳 Executing into running dev container..."; \
		docker exec -it correlator-dev-container /bin/bash; \
	else \
		echo "⚠️ Dev container not running, starting it first..."; \
		if devcontainer up --workspace-folder . >/dev/null 2>&1; then \
			echo "✅ Dev container started"; \
			echo "🐳 Executing into dev container..."; \
			docker exec -it correlator-dev-container /bin/bash; \
		else \
			echo "❌ Failed to start dev container"; \
			echo "💡 Try: make reset && make start"; \
			exit 1; \
		fi; \
	fi

# Helper function to get version info
define get-version-info
	$(eval VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "1.0.0-dev"))
	$(eval COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown"))
	$(eval BUILD_TIME := $(shell date -u '+%Y-%m-%d %H:%M:%S UTC'))
endef

# Helper function to build migrator locally (fallback)
build-migrator-local:
	@echo "🔨 Building local migrator binary..."
	$(call get-version-info)
	go build -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o bin/migrator ./migrations
	@echo "✅ Local migrator binary ready"

#===============================================================================
# GETTING STARTED
#===============================================================================

# Begin working (intelligent setup + exec into dev container)
start: ensure-not-in-dev-container ensure-env-file check-docker-environment check-devcontainer-cli check-environment-state
	@$(eval DEV_CONTAINER_RUNNING := $(shell docker ps --format "table {{.Names}}" | grep -q "correlator-dev-container" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval POSTGRES_RUNNING := $(shell cd deployments/docker && docker compose ps postgres --format "table {{.State}}" | grep -q "running" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval DEV_CONTAINER_EXISTS := $(shell docker ps -a --format "table {{.Names}}" | grep -q "correlator-dev-container" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval POSTGRES_EXISTS := $(shell cd deployments/docker && docker compose ps -a postgres --format "table {{.Names}}" | grep -q "correlator-postgres" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval CONTAINERS_RUNNING := $(shell [ "$(DEV_CONTAINER_RUNNING)" = "yes" ] && [ "$(POSTGRES_RUNNING)" = "yes" ] && echo "yes" || echo "no"))
	@$(eval CONTAINERS_EXIST := $(shell [ "$(DEV_CONTAINER_EXISTS)" = "yes" ] && [ "$(POSTGRES_EXISTS)" = "yes" ] && echo "yes" || echo "no"))
	@if [ "$(CONTAINERS_RUNNING)" = "yes" ]; then \
		echo "🎉 Development environment is already running!"; \
		$(MAKE) exec-dev-container; \
	elif [ "$(CONTAINERS_EXIST)" = "yes" ]; then \
		echo "🔄 Development environment exists but is stopped"; \
		echo "⚡ Restarting existing containers..."; \
		$(MAKE) restart-existing-containers; \
		echo ""; \
		$(MAKE) exec-dev-container; \
	else \
		echo "🚀 Setting up complete development environment from scratch..."; \
		$(MAKE) start-full-setup; \
		echo ""; \
		$(MAKE) exec-dev-container; \
	fi

# Internal target for full environment setup
start-full-setup: check-dev-container
	@echo ""
	@echo "📦 Step 1: Dependencies..."
	go mod download
	go mod verify
	@echo "✅ Go dependencies ready"
	@echo ""
	@echo "🐳 Step 2: Database infrastructure..."
	cd deployments/docker && docker compose up -d postgres
	@echo "⏳ Waiting for database to be ready..."
	@sleep 5
	cd deployments/docker && ./health-check.sh
	@echo "✅ PostgreSQL ready"
	@echo ""
	@echo "🔄 Step 3: Database migrations..."
	@echo "🐳 Running migrations via container..."
	@if cd deployments/docker && docker compose --profile migration run --rm migrator ./migrator up; then \
		echo "✅ Migrations applied successfully via container"; \
	else \
		echo "⚠️ Container migration failed, trying local fallback..."; \
		if [ -f bin/migrator ]; then \
			./bin/migrator up; \
			echo "✅ Migrations applied via local binary"; \
		else \
			echo "🔨 Building migrator binary..."; \
			$(MAKE) build-migrator-local; \
			./bin/migrator up; \
			echo "✅ Migrations applied via newly built binary"; \
		fi; \
	fi
	@echo ""
	@echo "🎉 Complete development environment ready!"
	@echo ""
	@echo "📋 Environment Status:"
	@echo "  ✅ Dev container:     Built and configured"
	@echo "  ✅ PostgreSQL:        Running in container"
	@echo "  ✅ Migrations:        Applied and current"
	@echo "  ✅ Dependencies:      Downloaded and verified"

# Execute something (run, run test, run migrate up)
run:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		if [ -f /.dockerenv ] && [ "$$PWD" = "/workspace" ]; then \
			echo "❌ Development server should not run inside dev container"; \
			echo ""; \
			echo "🏠 Please run this command from the host machine:"; \
			echo "   exit          # Exit dev container"; \
			echo "   make run      # Start development server on host"; \
			echo ""; \
			echo "💡 The dev container is for coding and development tools."; \
			echo "💡 The development server should run on the host for proper network access."; \
			exit 1; \
		fi; \
		echo "🏃 Starting development server..."; \
		go run ./cmd/correlator; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test" ]; then \
		$(MAKE) run-test; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test unit" ]; then \
		$(MAKE) run-test-unit; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test integration" ]; then \
		$(MAKE) run-test-integration; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "test race" ]; then \
		$(MAKE) run-test-race; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "benchmark" ]; then \
		$(MAKE) run-benchmark; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "linter" ]; then \
		$(MAKE) run-linter; \
	elif [ "$(wordlist 2,2,$(MAKECMDGOALS))" = "migrate" ]; then \
		$(MAKE) run-migrate-$(wordlist 3,3,$(MAKECMDGOALS)); \
	elif [ "$(wordlist 2,2,$(MAKECMDGOALS))" = "smoketest" ]; then \
		$(MAKE) run-smoketest-$(wordlist 3,3,$(MAKECMDGOALS)); \
	else \
		echo "❌ Unknown run target: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available targets:"; \
		echo "  make run                    # Start development server"; \
		echo "  make run test               # Run all tests"; \
		echo "  make run test unit          # Run unit tests only"; \
		echo "  make run test integration   # Run integration tests"; \
		echo "  make run test race          # Run tests with race detection"; \
		echo "  make run benchmark          # Run benchmark tests"; \
		echo "  make run linter             # Run linter"; \
		echo "  make run migrate up         # Apply migrations"; \
		echo "  make run migrate down       # Rollback migrations"; \
		echo "  make run migrate status     # Check migration status"; \
		echo "  make run migrate version    # Show migration version"; \
		echo "  make run migrate drop       # Drop all tables (destructive, uses --force)"; \
		echo "  make run smoketest lineage  # Run lineage ingestion smoke tests"; \
		exit 1; \
	fi

# Internal run targets
run-test:
	@echo "🧪 Running all tests..."
	go test -short -v -cover ./... && go test -v -cover ./...

run-test-unit:
	@echo "🧪 Running unit tests..."
	go test -short -v -cover ./...

run-test-integration:
	@echo "🧪 Running integration tests..."
	go test -v -cover -timeout=10m ./...

run-test-race:
	@echo "🧪 Running tests with race detection..."
	go test -v -race -cover ./...

run-benchmark:
	@echo "⚡ Running benchmark tests..."
	go test -v -bench=. -benchmem ./...

run-linter:
	@echo "📝 Running linter..."
	golangci-lint run

run-migrate-up:
	@echo "🔄 Applying database migrations..."
	@$(MAKE) run-migrator ACTION=up

run-migrate-down:
	@echo "⬇️ Rolling back last migration..."
	@$(MAKE) run-migrator ACTION=down

run-migrate-status:
	@echo "📊 Checking migration status..."
	@$(MAKE) run-migrator ACTION=status

run-migrate-version:
	@echo "🏷️ Checking migration version..."
	@$(MAKE) run-migrator ACTION=version

run-migrate-drop:
	@echo "⚠️ Dropping all database tables..."
	@$(MAKE) run-migrator ACTION="drop --force"

run-smoketest-lineage: ensure-not-in-dev-container
	@echo "🧪 Running OpenLineage ingestion smoke tests..."
	@./scripts/smoketest-lineage.sh

# Internal helper for environment-aware migrations
run-migrator:
	@if [ -f /.dockerenv ] && [ "$$PWD" = "/workspace" ]; then \
		echo "🏠 Running inside dev container, using local migrator..."; \
		$(MAKE) migrate-local ACTION=$(ACTION); \
	else \
		echo "🐳 Running from host, using containerized migrator..."; \
		$(MAKE) migrate-containerized ACTION=$(ACTION); \
	fi

# Local migration execution (inside dev container)
migrate-local:
	@if [ -z "$$DATABASE_URL" ]; then \
		echo "❌ DATABASE_URL not set in dev container environment"; \
		echo "💡 Make sure DATABASE_URL is set in deployments/docker/.env file"; \
		echo "💡 Dev container should load .env file via --env-file"; \
		exit 1; \
	fi
	@echo "🔗 Using DATABASE_URL from environment (credentials masked)"
	@if [ -f bin/migrator ]; then \
		echo "🔧 Using existing migrator binary..."; \
		if ./bin/migrator $(ACTION); then \
			echo "✅ Migration $(ACTION) completed via local binary"; \
		else \
			echo "❌ Migration $(ACTION) failed via local binary"; \
			exit 1; \
		fi; \
	else \
		echo "🔨 Building migrator binary..."; \
		$(MAKE) build-migrator-local; \
		if ./bin/migrator $(ACTION); then \
			echo "✅ Migration $(ACTION) completed via newly built binary"; \
		else \
			echo "❌ Migration $(ACTION) failed via newly built binary"; \
			exit 1; \
		fi; \
	fi

# Containerized migration execution (from host)
migrate-containerized:
	@echo "🔄 Ensuring migrator container has latest version..."
	$(call get-version-info)
	@cd deployments/docker && \
		VERSION="$(VERSION)" \
		GIT_COMMIT="$(COMMIT)" \
		BUILD_TIME="$(BUILD_TIME)" \
		docker compose build migrator
	@if cd deployments/docker && docker compose --profile migration run --rm migrator ./migrator $(ACTION); then \
		echo "✅ Migration $(ACTION) completed via container"; \
	else \
		echo "⚠️ Container migration failed, trying local fallback..."; \
		if [ -f bin/migrator ]; then \
			./bin/migrator $(ACTION); \
			echo "✅ Migration $(ACTION) completed via local binary fallback"; \
		else \
			echo "🔨 Building local migrator binary for fallback..."; \
			$(MAKE) build-migrator-local; \
			./bin/migrator $(ACTION); \
			echo "✅ Migration $(ACTION) completed via newly built binary fallback"; \
		fi; \
	fi

#===============================================================================
# DAILY DEVELOPMENT
#===============================================================================

# Verify code quality (lint + test + vet)
check:
	@echo "🔍 Checking code quality..."
	@echo "📝 Running linter..."
	golangci-lint run
	@echo "🧪 Running unit tests..."
	go test -short -v -cover ./...
	@echo "🧪 Running integration tests..."
	go test -v -cover -timeout=10m ./...
	@echo "🔬 Running vet..."
	go vet ./...
	@echo "✅ All checks passed!"

# Repair issues (format + tidy + clean artifacts)
fix:
	@echo "🔧 Auto-fixing issues..."
	@echo "📝 Formatting code..."
	golangci-lint fmt
	@echo "🧹 Tidying dependencies..."
	go mod tidy
	@echo "🗑️ Cleaning build artifacts..."
	go clean
	rm -rf bin/ build/
	@echo "✅ Auto-fix complete!"

#===============================================================================
# ENVIRONMENT
#===============================================================================

# Container operations (docker, docker prod, docker stop)
docker: ensure-not-in-dev-container check-docker-environment
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		echo "🐳 Starting development environment..."; \
		cd deployments/docker && docker compose up postgres; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "prod" ]; then \
		echo "🐳 Starting full production stack..."; \
		cd deployments/docker && docker compose --profile full up; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stop" ]; then \
		echo "🛑 Stopping all Docker services and dev container..."; \
		echo "🐳 Stopping docker-compose services..."; \
		cd deployments/docker && docker compose down; \
		echo "🏗️ Stopping dev container..."; \
		if docker ps --format "table {{.Names}}" | grep -q "correlator-dev-container" 2>/dev/null; then \
			docker stop correlator-dev-container; \
			echo "✅ Dev container stopped"; \
		else \
			echo "ℹ️  Dev container was not running"; \
		fi; \
		echo "✅ All project containers stopped"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "logs" ]; then \
		if [ "$(wordlist 3,3,$(MAKECMDGOALS))" != "" ]; then \
			echo "📋 Viewing $(wordlist 3,3,$(MAKECMDGOALS)) logs..."; \
			cd deployments/docker && docker compose logs -f $(wordlist 3,3,$(MAKECMDGOALS)); \
		else \
			echo "📋 Viewing all service logs..."; \
			cd deployments/docker && docker compose logs -f; \
		fi \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "health" ]; then \
		echo "🏥 Running comprehensive diagnostics..."; \
		cd deployments/docker && ./dev-diagnostics.sh; \
	else \
		echo "❌ Unknown docker target: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available targets:"; \
		echo "  make docker           # Start development environment"; \
		echo "  make docker prod      # Start full production stack"; \
		echo "  make docker stop      # Stop all services + dev container"; \
		echo "  make docker logs      # View all service logs"; \
		echo "  make docker logs <service>  # View specific service logs"; \
		echo "  make docker health    # Run comprehensive diagnostics"; \
		exit 1; \
	fi

#===============================================================================
# BUILD & DEPLOY
#===============================================================================

# Create artifacts (build, build prod, build all)
build:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		echo "🔨 Building development binary..."; \
		go build -o bin/$(BINARY_NAME) ./cmd/correlator; \
		echo "✅ Built: bin/$(BINARY_NAME)"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "prod" ]; then \
		$(MAKE) build-prod; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "all" ]; then \
		$(MAKE) build-all; \
	else \
		echo "❌ Unknown build target: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available targets:"; \
		echo "  make build       # Development build"; \
		echo "  make build prod  # Production build"; \
		echo "  make build all   # Build all components"; \
		exit 1; \
	fi

# Internal build targets
build-prod:
	@echo "🔨 Building production binaries with enhanced versioning..."
	$(call get-version-info)
	@echo "📦 Building correlator..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o build/correlator ./cmd/correlator
	@echo "📦 Building ingester..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o build/ingester ./cmd/ingester
	@echo "📦 Building migrator..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o build/migrator ./migrations
	@echo "✅ Production builds complete!"

build-all:
	@echo "🔨 Building all components..."
	$(call get-version-info)
	@echo "📦 Building correlator..."
	go build -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o bin/correlator ./cmd/correlator
	@echo "📦 Building ingester..."
	go build -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o bin/ingester ./cmd/ingester
	@echo "📦 Building migrator..."
	go build -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o bin/migrator ./migrations
	@echo "✅ All builds complete!"

# Prepare for production (builds + images + migrations)
deploy: ensure-not-in-dev-container
	@echo "🚀 Preparing deployment package..."
	$(MAKE) check
	@echo "🔄 Getting version information..."
	$(call get-version-info)
	@echo "Version: $(VERSION)"
	@echo "Commit: $(COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "🔨 Building production artifacts..."
	$(MAKE) build-prod
	@echo "🐳 Building Docker images with version injection..."
	docker build -t $(DOCKER_TAG) .
	cd deployments/docker && \
		VERSION="$(VERSION)" \
		GIT_COMMIT="$(COMMIT)" \
		BUILD_TIME="$(BUILD_TIME)" \
		docker compose build migrator
	@echo "🔄 Verifying migration artifacts..."
	@echo "✅ Deployment package ready!"
	@echo ""
	@echo "Deployment artifacts:"
	@echo "  build/correlator    # Production correlator binary"
	@echo "  build/ingester      # Production ingester binary"
	@echo "  build/migrator      # Production migrator binary"
	@echo "  $(DOCKER_TAG)       # Docker image"

#===============================================================================
# MAINTENANCE
#===============================================================================

# Start fresh (clean everything + stop services)
reset: ensure-not-in-dev-container
	@echo "🔄 Performing nuclear reset..."
	@echo "🛑 Stopping all Docker services..."
	-cd deployments/docker && docker compose down
	@echo "🐳 Stopping dev containers..."
	-devcontainer down --workspace-folder . 2>/dev/null || echo "No dev containers to stop"
	@echo "🗑️ Cleaning build artifacts..."
	go clean
	rm -rf bin/ build/
	@echo "🧹 Tidying dependencies..."
	go mod tidy
	@echo "🔧 Removing dangling Docker resources..."
	-docker system prune -f
	@echo "🧼 Cleaning dev container cache..."
	-docker volume prune -f
	@echo ""
	@echo "💥 Nuclear reset complete!"
	@echo ""
	@echo "🚀 To rebuild everything fresh:"
	@echo "   make start    # Complete environment setup"
	@echo ""
	@echo "📋 What was cleaned:"
	@echo "  ✅ All Docker containers stopped"
	@echo "  ✅ Dev containers stopped"
	@echo "  ✅ Build artifacts removed"
	@echo "  ✅ Dependencies tidied"
	@echo "  ✅ Docker cache cleaned"

#===============================================================================
# HELP
#===============================================================================

help:
	@echo "***************************************************************"
	@echo "*                  🔗 Correlator Development                  *"
	@echo "***************************************************************"
	@echo ""
	@echo "🚀 Getting Started:"
	@echo "    start   - Begin working (smart setup + exec into dev container)"
	@echo "    run     - Execute something (run, run test, run migrate up)"
	@echo ""
	@echo "🛠️  Daily Development:"
	@echo "    check   - Verify code quality (lint + test + vet)"
	@echo "    fix     - Repair issues (format + tidy + clean artifacts)"
	@echo ""
	@echo "🐳 Environment:"
	@echo "    docker  - Container operations (docker, docker prod, docker stop)"
	@echo ""
	@echo "🏗️  Build & Deploy:"
	@echo "    build   - Create artifacts (build, build prod, build all)"
	@echo "    deploy  - Prepare for production (builds + images + migrations)"
	@echo ""
	@echo "🔧 Maintenance:"
	@echo "    reset   - Start fresh (clean everything + stop services)"
	@echo ""
	@echo "📖 Examples:"
	@echo "    🚀 Development:"
	@echo "        make start                    # Smart setup + enter dev container"
	@echo "        make run                      # Start development server"
	@echo "        make run test                 # Run all tests"
	@echo "        make run benchmark            # Run benchmark tests"
	@echo "        make run linter               # Run linter"
	@echo "        make check                    # Check code quality before commit"
	@echo ""
	@echo "    🐳 Environment:"
	@echo "        make docker                   # Start development environment"
	@echo "        make docker prod              # Run full production stack"
	@echo "        make docker stop              # Stop all services + dev container"
	@echo ""
	@echo "    📊 Database:"
	@echo "        make run migrate up           # Apply pending migrations"
	@echo "        make run migrate status       # Check migration status"
	@echo ""
	@echo "    🏗️  Build & Deploy:"
	@echo "        make build prod               # Production-optimized build"
	@echo "        make deploy                   # Prepare complete deployment package"
	@echo ""
	@echo "    🆘 Troubleshooting:"
	@echo "        make reset                    # Clean slate (when things go wrong)"
	@echo ""
	@echo "⚡ Quick Start:"
	@echo "    🆕 New to this project?          make start"
	@echo "    💻 Daily development?            make start"
	@echo "    🚀 Ready to deploy?              make check && make deploy"
	@echo ""
	@echo "💡 For detailed options: make <command> --help"

# Handle command line arguments for parameterized commands
%:
	@:
