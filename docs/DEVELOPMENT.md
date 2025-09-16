# Development Guide

Welcome to the Correlator development environment! This guide provides everything you need to start contributing to the project.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Linting & Formatting](#linting--formatting)
- [Integration / Integration Tests](#integration--integration-tests)
- [Hot Reload / Development Server](#hot-reload--development-server)
- [Environment Variables & Configuration](#environment-variables--configuration)
- [Makefile / Common Commands](#makefile--common-commands)
- [Code Style & Best Practices](#code-style--best-practices)
- [Versioning & Releases](#versioning--releases)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before contributing, ensure you have the following installed and configured:

### Required Software

- **Go 1.25.0** or later
- **Docker & Docker Compose** for database development
- **Make** for build automation
- **Git** with pre-commit hooks

### Recommended Tools

- **golangci-lint v2.4.0** for linting (installed automatically)
- **pre-commit** for code quality gates
- **detect-secrets** for security scanning

---

## Setup

Steps to get a local development environment up and running:

1. Clone the repository
   ```bash
   git clone https://github.com/correlator-io/correlator.git
   cd correlator
   ```

2. Ensure Go modules are enabled and download dependencies
   ```bash
   go mod download
   ```

3. (Optional) Bring up supporting services (databases, caches, etc.)  
   If you use Docker/TestContainers or docker-compose, you might run:
   ```bash
   make dev-services
   ```

4. Set up your environment variables  
   Copy a sample env file, if it exists:
   ```bash
   cp .env.example .env
   ```
   Populate required vars.

---

## Development Workflow

A typical dev iteration looks like this:

- Pull latest changes from `main` / default branch
- Create a feature / bugfix branch (`git checkout -b feat/xyz`)
- Write or update code
- Run tests (unit + integration) locally
- Run lint & formatting checks
- Commit & push
- Open PR / Merge Request
- Ensure CI passes before merging

---

## Testing

- **Unit Tests**  
  Use `make test-unit` to run all unit tests. Prefer table‑driven tests and keep tests fast.

- **Integration Tests**  
  These tests depend on external services (databases, message queues, etc.). We use TestContainers (or similar) for integration tests. Run:
  ```bash
  make test-integration
  ```
---

## Linting & Formatting

- Always run formatting tools before commits:
  ```bash
  make fmt
  ```

- Use linter tools (e.g., `golangci-lint`)
  ```bash
  make lint
  ```

- CI or pre-commit hooks should catch formatting / lint failures.

---

## Integration / Integration Tests

- Integration tests use test containers

---

## Environment Variables & Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your local settings (optional - has sensible defaults)
```

---

## Makefile / Common Commands

Complete reference of all available make targets in the Correlator project:

### Build & Development

| Target | Description |
|--------|-------------|
| `make build` | Build the correlator binary |
| `make build-all` | Build all binaries (correlator, ingester, migrator) |
| `make dev` | Run correlator in development mode |
| `make clean` | Clean build artifacts |

### Testing

| Target | Description |
|--------|-------------|
| `make test` | Run all tests (unit + integration) with coverage |
| `make test-unit` | Run unit tests only (fast, -short flag) |
| `make test-integration` | Run integration tests (real databases, 10min timeout) |
| `make test-race` | Run tests with race detection |

### Code Quality

| Target | Description |
|--------|-------------|
| `make lint` | Run golangci-lint v2 comprehensive analysis |
| `make fmt` | Format code with golangci-lint fmt |
| `make vet` | Run go vet static analysis |

### Dependencies

| Target | Description |
|--------|-------------|
| `make deps` | Download and verify Go module dependencies |
| `make tidy` | Tidy go modules (remove unused dependencies) |

### Database Migration (Local Development)

| Target | Description |
|--------|-------------|
| `make migrate-up` | Apply all pending migrations (local) |
| `make migrate-down` | Rollback the last migration (local) |
| `make migrate-status` | Show current migration status (local) |
| `make migrate-version` | Show current migration version (local) |
| `make migrate-drop` | Drop all tables - destructive! (local) |

### Build Migration Tool

| Target | Description |
|--------|-------------|
| `make build-migrator` | Build migrator binary for development |
| `make build-migrator-prod` | Build optimized migrator for production (Linux) |

### Docker Development Environment

| Target | Description |
|--------|-------------|
| `make docker-dev` | Start PostgreSQL for development (foreground) |
| `make docker-dev-bg` | Start PostgreSQL in background |
| `make docker-run` | Run full stack (correlator + postgres) |
| `make docker-stop` | Stop all Docker services |
| `make docker-logs` | View all service logs |
| `make docker-logs-postgres` | View PostgreSQL logs only |

### Docker Migration Commands

| Target | Description |
|--------|-------------|
| `make docker-build-migrator` | Build migrator Docker image |
| `make docker-migrate-up` | Apply migrations using Docker |
| `make docker-migrate-down` | Rollback migrations using Docker |
| `make docker-migrate-status` | Show migration status using Docker |
| `make docker-migrate-version` | Show migration version using Docker |
| `make docker-migrate-drop` | Drop all tables using Docker |

### Development Environment Setup

| Target | Description |
|--------|-------------|
| `make setup` | Initial project setup (copy .env.example to .env) |
| `make docker-dev-setup` | Setup development environment with Docker |
| `make docker-health` | Run comprehensive health checks |

### Help & Information

| Target | Description |
|--------|-------------|
| `make help` | Show all available commands with descriptions |

### Common Development Workflows

**Daily Development:**
```bash
# Start development
make docker-dev-bg && make docker-migrate-up

# Code quality check
make fmt && make vet && make lint && make test-unit

# Full validation
make test-integration
```

**Database Development:**
```bash
# Check migration status
make docker-migrate-status

# Apply new migrations
make docker-migrate-up

# Rollback if needed
make docker-migrate-down
```

**CI/CD Pipeline Simulation:**
```bash
# Same commands as GitHub Actions
make deps && make fmt && make vet && make lint && make test-unit && make test-integration
```

---

## Code Style & Best Practices

- Follow idiomatic Go: naming, error handling, zero values, etc.
- Keep functions small and focused.
- Use interfaces for abstractions; avoid over‑engineering.
- Avoid global state where possible.
- Write comments for exported functions/types. Keep code self‑documenting.
- Handle errors explicitly; prefer clarity over cleverness.

---

## Versioning & Releases

- Use Semantic Versioning (SemVer): `vMAJOR.MINOR.PATCH`.
- Tag releases in Git with annotated tags.
- Maintain a `CHANGELOG.md` for user‑facing changes and migrations.

---

## Troubleshooting
### Common Issues

#### golangci-lint Version Mismatch
**Problem**: Configuration errors with golangci-lint
**Solution**: Ensure you're using v2.4.0:
```bash
golangci-lint version  # Should show v2.4.0
brew install golangci-lint  # If using different version
```

#### Go Version Mismatch
**Problem**: CI failures with different Go version
**Solution**: Ensure Go 1.25 everywhere:
```bash
go version  # Should show go1.25
# Update go.mod, .golangci.yml, and .github/workflows/pr.yml if needed
```

#### Docker Database Issues
**Problem**: Migration or connection failures
**Solution**: Reset Docker environment:
```bash
make docker-stop
docker compose down -v  # Remove volumes
make docker-dev-bg
make docker-migrate-up
```

#### Pre-commit Hook Failures
**Problem**: Hooks failing or not running
**Solution**: Reinstall hooks:
```bash
pre-commit uninstall
pre-commit install
pre-commit run --all-files
```

#### Integration Test Timeouts
**Problem**: testcontainers-go tests timing out
**Solution**: Increase Docker resources or skip integration tests:
```bash
make test-unit  # Skip integration tests for faster feedback
# Or increase Docker Desktop memory allocation
```

---

## References

- [CONTRIBUTING](CONTRIBUTING.md)

