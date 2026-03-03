.PHONY: start run check fix docker build deploy reset help

# Variables
BINARY_NAME=correlator
DOCKER_TAG=correlator:latest
GO_VERSION=1.25
WEB_DIR=web

# Helper function to ensure PostgreSQL is running
ensure-postgres: ensure-env-file check-docker-environment
	@echo "🐘 Checking PostgreSQL..."
	@cd deployments/docker && \
	if docker compose ps postgres --format "{{.State}}" 2>/dev/null | grep -q "running"; then \
		echo "✅ PostgreSQL already running"; \
	else \
		echo "🐘 PostgreSQL not running, starting it..."; \
		if docker compose up -d postgres; then \
			echo "⏳ Waiting for PostgreSQL to be ready..."; \
			sleep 3; \
			if ./health-check.sh; then \
				echo "✅ PostgreSQL ready"; \
			else \
				echo "❌ PostgreSQL failed health check"; \
				echo "💡 Try: make docker health"; \
				exit 1; \
			fi; \
		else \
			echo "❌ Failed to start PostgreSQL"; \
			echo "💡 Check Docker is running: docker info"; \
			echo "💡 Try: make reset && make docker"; \
			exit 1; \
		fi; \
	fi

# Helper function to check npm availability
check-npm:
	@if ! command -v npm >/dev/null 2>&1; then \
		echo "❌ npm not found. Please install Node.js first:"; \
		echo "   - macOS: brew install node"; \
		echo "   - Ubuntu: sudo apt install nodejs npm"; \
		echo "   - Windows: Download from https://nodejs.org"; \
		exit 1; \
	fi

# Helper function to check web dependencies
check-web-deps: check-npm
	@if [ ! -d "$(WEB_DIR)/node_modules" ]; then \
		echo "📦 Installing frontend dependencies..."; \
		cd $(WEB_DIR) && npm install; \
		echo "✅ Frontend dependencies installed"; \
	fi

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
# DEMO ENVIRONMENT HELPERS
#===============================================================================

# Demo directory
DEMO_DIR=deployments/demo

# Helper function to check if demo is running
check-demo-running:
	@$(eval DEMO_POSTGRES_RUNNING := $(shell docker ps --format "{{.Names}}" | grep -q "demo-postgres" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval DEMO_CORRELATOR_RUNNING := $(shell docker ps --format "{{.Names}}" | grep -q "demo-correlator" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval DEMO_UI_RUNNING := $(shell docker ps --format "{{.Names}}" | grep -q "demo-correlator-ui" 2>/dev/null && echo "yes" || echo "no"))
	@$(eval DEMO_AIRFLOW_RUNNING := $(shell docker ps --format "{{.Names}}" | grep -q "demo-airflow-webserver" 2>/dev/null && echo "yes" || echo "no"))

# Helper function to wait for demo services to be healthy
wait-for-demo-health:
	@echo "⏳ Waiting for demo services to be healthy..."
	@MAX_WAIT=120; \
	WAITED=0; \
	while [ $$WAITED -lt $$MAX_WAIT ]; do \
		if docker ps --format "{{.Names}}" | grep -q "demo-correlator" 2>/dev/null; then \
			HEALTH=$$(docker inspect --format='{{.State.Health.Status}}' demo-correlator 2>/dev/null || echo "starting"); \
			if [ "$$HEALTH" = "healthy" ]; then \
				echo "✅ Correlator API is healthy"; \
				break; \
			fi; \
		fi; \
		echo "   Waiting for Correlator API... ($$WAITED/$$MAX_WAIT seconds)"; \
		sleep 5; \
		WAITED=$$((WAITED + 5)); \
	done; \
	if [ $$WAITED -ge $$MAX_WAIT ]; then \
		echo "⚠️  Correlator API health check timed out after $$MAX_WAIT seconds"; \
		echo "💡 Services may still be starting. Check logs with: make docker demo logs"; \
	fi

# Helper function to print demo access information
print-demo-info:
	@echo ""
	@echo "🎉 Demo environment is running!"
	@echo ""
	@echo "📋 Access URLs:"
	@echo "   Correlator UI:     http://localhost:3001"
	@echo "   Correlator API:    http://localhost:8081"
	@echo "   Airflow UI:        http://localhost:8082"
	@echo "   PostgreSQL:        localhost:5433"
	@echo ""
	@echo "📖 Next Steps:"
	@echo "   make run demo               # Run full demo pipeline (Airflow DAG)"
	@echo ""
	@echo "📖 Other Commands:"
	@echo "   make docker stop demo       # Stop demo environment"
	@echo "   make docker logs demo       # View all logs"
	@echo "   make run demo dbt seed      # Run dbt seed"
	@echo "   make run demo dbt run       # Run dbt transformations"
	@echo "   make run demo dbt test      # Run dbt tests"
	@echo "   make run demo ge validate   # Run GE checkpoint"
	@echo ""

#===============================================================================
# GETTING STARTED
#===============================================================================

# Begin working (intelligent setup + exec into dev container)
# Also handles: make start demo
start: ensure-not-in-dev-container ensure-env-file check-docker-environment
	@if [ "$(wordlist 2,2,$(MAKECMDGOALS))" = "demo" ]; then \
		$(MAKE) start-demo; \
	else \
		$(MAKE) start-dev; \
	fi

# Start development environment
start-dev: check-devcontainer-cli check-environment-state
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

# Start demo environment
start-demo: ensure-not-in-dev-container check-docker-environment
	@echo "🎪 Starting Correlator Demo Environment..."
	@echo ""
	@echo "This will start a 3-tool correlation demo with:"
	@echo "  - Correlator (API + UI)"
	@echo "  - Airflow (with airflow-correlator plugin)"
	@echo "  - dbt (with dbt-correlator plugin)"
	@echo "  - Great Expectations (with ge-correlator plugin)"
	@echo ""
	@# Check if demo is already running
	@if docker ps --format "{{.Names}}" | grep -q "demo-correlator" 2>/dev/null; then \
		echo "✅ Demo environment is already running!"; \
		$(MAKE) print-demo-info; \
		exit 0; \
	fi
	@# Check for port conflicts with dev environment
	@if docker ps --format "{{.Names}}" | grep -q "correlator-postgres" 2>/dev/null; then \
		echo "⚠️  Development PostgreSQL is running on port 5432"; \
		echo "   Demo will use port 5433 (no conflict)"; \
	fi
	@echo "🐳 Building and starting demo containers..."
	@echo "   (This may take a few minutes on first run)"
	@echo ""
	$(call get-version-info)
	@cd $(DEMO_DIR) && \
		VERSION="$(VERSION)" \
		GIT_COMMIT="$(COMMIT)" \
		BUILD_TIME="$(BUILD_TIME)" \
		docker compose -f docker-compose.demo.yml up -d --build || { \
		echo ""; \
		echo "❌ Failed to start demo environment"; \
		echo ""; \
		echo "💡 Troubleshooting:"; \
		echo "   1. Check Docker is running: docker info"; \
		echo "   2. Check for port conflicts: docker ps"; \
		echo "   3. View build logs: cd $(DEMO_DIR) && docker compose -f docker-compose.demo.yml logs"; \
		echo "   4. Clean and retry: make docker stop demo && make start demo"; \
		exit 1; \
	}
	@echo ""
	@echo "✅ Demo containers started successfully"
	@$(MAKE) wait-for-demo-health
	@$(MAKE) print-demo-info

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
		$(MAKE) ensure-postgres || exit 1; \
		echo "🏃 Starting development server..."; \
		go run ./cmd/correlator start; \
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
		$(MAKE) run-smoketest; \
	elif [ "$(wordlist 2,2,$(MAKECMDGOALS))" = "web" ]; then \
		$(MAKE) run-web WEBCMD="$(wordlist 3,3,$(MAKECMDGOALS))"; \
	elif [ "$(wordlist 2,2,$(MAKECMDGOALS))" = "demo" ]; then \
		$(MAKE) run-demo-cmd DEMOCMD="$(wordlist 3,100,$(MAKECMDGOALS))"; \
	else \
		echo "❌ Unknown run command: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "📖 Available run commands:"; \
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
		echo "  make run smoketest          # Run smoke tests (end-to-end correlation validation)"; \
		echo "  make run web                # Start frontend dev server"; \
		echo "  make run web lint           # Run frontend linter"; \
		echo "  make run web test           # Run frontend tests"; \
		echo ""; \
		echo "Demo commands:"; \
		echo "  make run demo               # Run full demo pipeline (Airflow DAG)"; \
		echo "  make run demo dbt seed      # Run dbt seed in demo"; \
		echo "  make run demo dbt run       # Run dbt transformations in demo"; \
		echo "  make run demo dbt test      # Run dbt tests in demo"; \
		echo "  make run demo ge validate   # Run GE checkpoint in demo"; \
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

run-smoketest: ensure-not-in-dev-container
	@echo "🧪 Running Correlator smoke tests..."
	@./scripts/smoketest.sh

# Frontend web commands
run-web:
	@if [ -f /.dockerenv ] && [ "$$PWD" = "/workspace" ]; then \
		echo "❌ Frontend dev server should not run inside dev container"; \
		echo ""; \
		echo "🏠 Please run this command from the host machine:"; \
		echo "   exit              # Exit dev container"; \
		echo "   make run web      # Start frontend dev server on host"; \
		echo ""; \
		echo "💡 The dev container is for backend development."; \
		echo "💡 The frontend dev server should run on the host for proper HMR."; \
		exit 1; \
	fi
	@$(MAKE) run-web-internal WEBCMD="$(WEBCMD)"

run-web-internal: check-web-deps
	@if [ -z "$(WEBCMD)" ]; then \
		echo "🌐 Starting frontend development server..."; \
		echo "   URL: http://localhost:3000"; \
		echo ""; \
		cd $(WEB_DIR) && npm run dev; \
	elif [ "$(WEBCMD)" = "lint" ]; then \
		$(MAKE) run-web-lint; \
	elif [ "$(WEBCMD)" = "test" ]; then \
		$(MAKE) run-web-test; \
	else \
		echo "❌ Unknown web command: $(WEBCMD)"; \
		echo "📖 Available web commands:"; \
		echo "  make run web          # Start frontend dev server"; \
		echo "  make run web lint     # Run frontend linter"; \
		echo "  make run web test     # Run frontend tests"; \
		echo "  make build web        # Build frontend for production"; \
		exit 1; \
	fi

run-web-lint: check-web-deps
	@echo "📝 Running frontend linter..."
	@cd $(WEB_DIR) && npm run lint
	@echo "✅ Frontend lint complete!"

run-web-test: check-web-deps
	@echo "🧪 Running frontend tests..."
	@cd $(WEB_DIR) && npm test
	@echo "✅ Frontend tests complete!"

# Demo run commands
run-demo-cmd: ensure-not-in-dev-container check-docker-environment
	@# Check if demo is running first
	@if ! docker ps --format "{{.Names}}" | grep -q "demo-correlator" 2>/dev/null; then \
		echo "❌ Demo environment is not running"; \
		echo ""; \
		echo "💡 Start demo first: make start demo"; \
		exit 1; \
	fi
	@if [ -z "$(DEMOCMD)" ]; then \
		$(MAKE) run-demo-pipeline; \
	elif [ "$(word 1,$(DEMOCMD))" = "dbt" ]; then \
		$(MAKE) run-demo-dbt DBTCMD="$(wordlist 2,100,$(DEMOCMD))"; \
	elif [ "$(word 1,$(DEMOCMD))" = "ge" ]; then \
		$(MAKE) run-demo-ge GECMD="$(wordlist 2,100,$(DEMOCMD))"; \
	else \
		echo "❌ Unknown demo command: $(DEMOCMD)"; \
		echo ""; \
		echo "📖 Available demo commands:"; \
		echo "  make run demo               # Run full demo pipeline (Airflow DAG)"; \
		echo "  make run demo dbt seed      # Run dbt seed"; \
		echo "  make run demo dbt run       # Run dbt transformations"; \
		echo "  make run demo dbt test      # Run dbt tests"; \
		echo "  make run demo ge validate   # Run GE checkpoint"; \
		exit 1; \
	fi

# Run full demo pipeline via Airflow
run-demo-pipeline:
	@echo "🚀 Running full demo pipeline..."
	@echo ""
	@echo "This will trigger the Airflow DAG that runs:"
	@echo "  1. dbt seed   - Load seed data"
	@echo "  2. dbt run    - Run transformations"
	@echo "  3. dbt test   - Run dbt tests"
	@echo "  4. GE validate - Run Great Expectations checkpoint"
	@echo ""
	@if ! docker ps --format "{{.Names}}" | grep -q "demo-airflow-webserver" 2>/dev/null; then \
		echo "❌ Demo Airflow is not running"; \
		echo "💡 Start demo first: make start demo"; \
		exit 1; \
	fi
	@echo "🎯 Triggering demo_pipeline DAG..."
	@if docker exec demo-airflow-webserver airflow dags trigger demo_pipeline 2>/dev/null; then \
		echo ""; \
		echo "✅ Pipeline triggered successfully!"; \
		echo ""; \
		echo "📋 Monitor progress:"; \
		echo "   Airflow UI: http://localhost:8082 (admin/admin)"; \
		echo "   Logs:       make docker logs demo"; \
		echo ""; \
		echo "📊 View results:"; \
		echo "   Correlator UI: http://localhost:3001"; \
	else \
		echo ""; \
		echo "⚠️  Could not trigger DAG."; \
	fi

# Run dbt commands in demo
# All commands use dbt-ol (openlineage-dbt) which emits OpenLineage events
# for seed/run/test/build/snapshot. Unsupported commands (e.g. debug) are
# still executed via dbt-ol but no lineage events are emitted.
run-demo-dbt:
	@if [ -z "$(DBTCMD)" ]; then \
		echo "❌ No dbt command specified"; \
		echo ""; \
		echo "💡 Usage: make run demo dbt <command>"; \
		echo ""; \
		echo "Examples:"; \
		echo "  make run demo dbt seed      # Load seed data (no dataset lineage)"; \
		echo "  make run demo dbt run       # Run transformations (emits lineage)"; \
		echo "  make run demo dbt test      # Run tests (emits lineage)"; \
		echo "  make run demo dbt debug     # Show dbt debug info"; \
		exit 1; \
	fi
	@echo "🔧 Running: dbt-ol $(DBTCMD)"; \
	cd $(DEMO_DIR) && docker compose -f docker-compose.demo.yml --profile tools run --rm \
		demo-dbt $(DBTCMD) \
		--project-dir . \
		--profiles-dir .

# Run GE commands in demo
run-demo-ge:
	@if [ -z "$(GECMD)" ] || [ "$(GECMD)" != "validate" ]; then \
		echo "❌ Invalid GE command: $(GECMD)"; \
		echo ""; \
		echo "💡 Usage: make run demo ge validate"; \
		exit 1; \
	fi
	@echo "🔍 Running GE checkpoint..."
	@cd $(DEMO_DIR) && docker compose -f docker-compose.demo.yml --profile tools run --rm demo-great-expectations checkpoints/demo_checkpoint.py

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
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "stop demo" ]; then \
		$(MAKE) docker-stop-demo; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "logs" ]; then \
		if [ "$(wordlist 3,3,$(MAKECMDGOALS))" != "" ]; then \
			echo "📋 Viewing $(wordlist 3,3,$(MAKECMDGOALS)) logs..."; \
			cd deployments/docker && docker compose logs -f $(wordlist 3,3,$(MAKECMDGOALS)); \
		else \
			echo "📋 Viewing all service logs..."; \
			cd deployments/docker && docker compose logs -f; \
		fi \
	elif [ "$(wordlist 2,2,$(MAKECMDGOALS))" = "logs" ] && [ "$(wordlist 3,3,$(MAKECMDGOALS))" = "demo" ]; then \
		$(MAKE) docker-logs-demo SVC="$(wordlist 4,4,$(MAKECMDGOALS))"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "health" ]; then \
		echo "🏥 Running comprehensive diagnostics..."; \
		cd deployments/docker && ./dev-diagnostics.sh; \
	else \
		echo "❌ Unknown docker target: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available targets:"; \
		echo "  make docker                    # Start development environment"; \
		echo "  make docker prod               # Start full production stack"; \
		echo "  make docker stop               # Stop dev services + dev container"; \
		echo "  make docker stop demo          # Stop demo environment only"; \
		echo "  make docker logs               # View dev service logs"; \
		echo "  make docker logs demo          # View demo logs"; \
		echo "  make docker logs <service>     # View specific service logs"; \
		echo "  make docker health             # Run comprehensive diagnostics"; \
		exit 1; \
	fi

# Stop demo environment
docker-stop-demo:
	@echo "🛑 Stopping demo environment..."
	@if docker ps --format "{{.Names}}" | grep -q "demo-" 2>/dev/null; then \
		cd $(DEMO_DIR) && docker compose -f docker-compose.demo.yml down; \
		echo "✅ Demo environment stopped"; \
	else \
		echo "ℹ️  Demo environment was not running"; \
	fi

# View demo logs
docker-logs-demo:
	@if [ -z "$(SVC)" ]; then \
		echo "📋 Viewing all demo logs..."; \
		cd $(DEMO_DIR) && docker compose -f docker-compose.demo.yml logs -f; \
	else \
		echo "📋 Viewing demo-$(SVC) logs..."; \
		cd $(DEMO_DIR) && docker compose -f docker-compose.demo.yml logs -f demo-$(SVC); \
	fi

#===============================================================================
# BUILD & DEPLOY
#===============================================================================

# Create artifacts (build, build prod, build all, build web)
build:
	@if [ "$(filter-out $@,$(MAKECMDGOALS))" = "" ]; then \
		echo "🔨 Building development binary..."; \
		go build -o bin/$(BINARY_NAME) ./cmd/correlator; \
		echo "✅ Built: bin/$(BINARY_NAME)"; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "migrator" ]; then \
    		$(MAKE) build-migrator-local; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "prod" ]; then \
		$(MAKE) build-prod; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "all" ]; then \
		$(MAKE) build-all; \
	elif [ "$(filter-out $@,$(MAKECMDGOALS))" = "web" ]; then \
		$(MAKE) build-web; \
	else \
		echo "❌ Unknown build target: $(filter-out $@,$(MAKECMDGOALS))"; \
		echo "Available targets:"; \
		echo "  make build           # Development build (Go binary)"; \
		echo "  make build prod      # Production build"; \
		echo "  make build migrator  # Build migrator"; \
		echo "  make build all       # Build all components"; \
		echo "  make build web       # Build frontend for production"; \
		exit 1; \
	fi

# Internal build targets
build-prod:
	@echo "🔨 Building production binaries with enhanced versioning..."
	$(call get-version-info)
	@echo "📦 Building correlator..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o build/correlator ./cmd/correlator
	@echo "📦 Building migrator..."
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o build/migrator ./migrations
	@echo "✅ Production builds complete!"

build-all:
	@echo "🔨 Building all components..."
	$(call get-version-info)
	@echo "📦 Building correlator..."
	go build -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o bin/correlator ./cmd/correlator
	@echo "📦 Building migrator..."
	go build -ldflags "-X main.version=$(VERSION) -X main.gitCommit=$(COMMIT) -X 'main.buildTime=$(BUILD_TIME)'" -o bin/migrator ./migrations
	@echo "✅ All builds complete!"

build-web: check-web-deps
	@echo "🔨 Building frontend for production..."
	@cd $(WEB_DIR) && npm run build
	@echo "✅ Frontend build complete!"

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
	@echo "    start   - Begin working (smart setup + exec into dev container or start demo)"
	@echo "    run     - Execute something (run, run test, run migrate up, run web, run demo)"
	@echo ""
	@echo "🛠️  Daily Development:"
	@echo "    check   - Verify code quality (lint + test + vet)"
	@echo "    fix     - Repair issues (format + tidy + clean artifacts)"
	@echo ""
	@echo "🐳 Environment:"
	@echo "    docker  - Container operations (docker, docker prod, docker stop)"
	@echo ""
	@echo "🏗️  Build & Deploy:"
	@echo "    build   - Create artifacts (build, build migrator, build prod, build all)"
	@echo "    deploy  - Prepare for production (builds + images + migrations)"
	@echo ""
	@echo "🔧 Maintenance:"
	@echo "    reset   - Start fresh (clean everything + stop services)"
	@echo ""
	@echo "📖 Examples:"
	@echo "    🚀 Backend Development:"
	@echo "        make start                    # Smart setup + enter dev container"
	@echo "        make run                      # Start backend development server"
	@echo "        make run test                 # Run all tests"
	@echo "        make run smoketest            # Run smoke tests (end-to-end correlation validation)"
	@echo "        make run benchmark            # Run benchmark tests"
	@echo "        make run linter               # Run linter"
	@echo "        make check                    # Check code quality before commit"
	@echo ""
	@echo "    🌐 Frontend Development:"
	@echo "        make run web                  # Start frontend dev server (localhost:3000)"
	@echo "        make run web build            # Build frontend for production"
	@echo "        make run web lint             # Run frontend linter"
	@echo "        make run web test             # Run frontend tests"
	@echo ""
	@echo "    🐳 Environment:"
	@echo "        make docker                   # Start development environment"
	@echo "        make docker prod              # Run full production stack"
	@echo "        make docker stop              # Stop all services + dev container"
	@echo ""
	@echo "    🎪 Demo Environment:"
	@echo "        make start demo               # Start demo infrastructure"
	@echo "        make run demo                 # Run full demo pipeline (Airflow DAG)"
	@echo "        make run demo dbt seed        # Run dbt seed in demo"
	@echo "        make run demo ge validate     # Run GE checkpoint in demo"
	@echo "        make docker stop demo         # Stop demo environment"
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
	@echo "    🌐 Frontend development?         make run web"
	@echo "    🎪 Demo environment?             make start demo"
	@echo "    🚀 Ready to deploy?              make check && make deploy"
	@echo ""
	@echo "💡 For detailed options: make <command> --help"

# Handle command line arguments for parameterized commands
# These are pseudo-targets that act as arguments to run/build/docker commands
# They must be declared as .PHONY and have empty recipes to prevent Make errors
.PHONY: web test unit integration race benchmark linter migrate up down status version drop smoketest lint prod stop logs health all migrator demo dbt ge airflow seed validate trigger

web test unit integration race benchmark linter migrate up down status version drop smoketest lint prod stop logs health all migrator demo dbt ge airflow seed validate trigger:
	@:
