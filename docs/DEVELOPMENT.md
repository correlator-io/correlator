# Development Guide

Welcome to the Correlator development environment! This guide provides everything you need to start contributing to the
project.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Dev Container Setup (Recommended)](#dev-container-setup-recommended)
- [Testing](#testing)
- [Linting & Formatting](#linting--formatting)
- [Integration / Integration Tests](#integration--integration-tests)
- [Environment Variables & Configuration](#environment-variables--configuration)
- [Makefile / Common Commands](#makefile--common-commands)
- [GitHub Workflows](#github-workflows)
- [Code Style & Best Practices](#code-style--best-practices)
- [Versioning & Releases](#versioning--releases)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

You can skip installing the prerequisite tools by using our [development container](https://containers.dev/). This is the recommended [setup](#dev-container-setup-recommended).

If you prefer to set up your own local development environment, ensure you have the following installed and configured:

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

2. Connect to the project [dev container](#dev-container-setup-recommended)
---

## Dev Container Setup (Recommended)

**🚀 Quick Start**: Use our pre-configured development container for instant setup with exact CI environment match.

### Why Use Dev Containers?

- **Identical Environment**: Same Go 1.25.0 + golangci-lint v2.4.0 as CI
- **Zero Config**: All tools pre-installed and configured
- **testcontainers Ready**: Docker socket access for integration tests
- **Cross-Platform**: Works on macOS, Linux, Windows
- **No "Works on My Machine"**: Consistent across all contributors

### Prerequisites

- **Docker Desktop** MUST be running on your machine
- **IntelliJ IDEA** with dev container support, **VS Code**, or any dev container compatible IDE

### Setup Steps

1. **Open in Dev Container**
   - **IntelliJ**: Use "Remote Development" → "Dev Containers" → "Open in Container"
   - **VS Code**: Command Palette → "Dev Containers: Reopen in Container"

2. **First Build** (~3-5 minutes)
   - Container downloads Go 1.25.0, installs golangci-lint v2.4.0, PostgreSQL client
   - Runs `make deps` automatically

3. **Verify Setup**
   ```bash
   go version                 # Should show: go version go1.25.0
   golangci-lint version     # Should show: v2.4.0
   docker ps                 # Should work (testcontainers access)
   make test-integration     # Should spawn PostgreSQL containers
   ```

### Dev Container Features

**Environment Match**:
- ✅ Go 1.25.0 (MUST match CI)
- ✅ golangci-lint v2.4.0 (MUST match CI)
- ✅ PostgreSQL client for debugging
- ✅ All Makefile commands work identically

**testcontainers Integration**:
- ✅ Docker socket mounted with proper permissions
- ✅ Integration tests spawn real PostgreSQL containers
- ✅ Same behavior as local development

### Development Workflow in Container
The dev container has all the required tools for development such as git and pre-commit. You must use it as your development.

```bash
# All standard commands work
make build-all           # Build all binaries
make test-unit           # Fast unit tests
make test-integration    # Integration tests with testcontainers
make lint                # golangci-lint v2.4.0
make fmt                 # Format code

# Database development
make docker-dev-bg       # Start PostgreSQL in background
make docker-migrate-up   # Apply migrations
```

Please make sure to read [CONTRIBUTING](CONTRIBUTING.md#contributing-guidelines-for-correlator) for more guidelines.
 
---

## Testing

### Test Categories

We maintain strict separation between unit and integration tests using Go's `testing.Short()` flag:

- **Unit Tests**: Fast, isolated, no external dependencies
- **Integration Tests**: Use real external services (databases, containers, etc.)

### Test Conventions

**Unit Tests**:
- Add this condition at the start of every unit test function:
  ```go
  if !testing.Short() {
      t.Skip("skipping unit test in non-short mode")
  }
  ```
- Keep tests fast (< 100ms each)
- Use mocks/stubs for external dependencies
- Test individual functions/methods in isolation

**Integration Tests**:
- Add this condition at the start of every integration test function:
  ```go
  if testing.Short() {
      t.Skip("skipping integration test in short mode")
  }
  ```
- Use real external services (databases, testcontainers, etc.)
- Test end-to-end workflows
- Allow longer timeouts (up to 10 minutes)

### Running Tests

- **Unit Tests Only**:
  ```bash
  make test-unit
  # Runs with -short flag, only executes unit tests
  ```

- **Integration Tests Only**:
  ```bash
  make test-integration
  # Runs without -short flag, only executes integration tests
  ```

- **All Tests**:
  ```bash
  make test
  # Runs both unit and integration tests with coverage
  ```

### Adding New Tests

When creating new test functions:

1. **For unit tests**: Test individual functions, use mocks, keep fast
2. **For integration tests**: Test full workflows, use real services, allow time
3. **Always add the appropriate skip condition** based on test type
4. **Use descriptive test names** that clearly indicate the test scope

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

| Target           | Description                                         |
|------------------|-----------------------------------------------------|
| `make build`     | Build the correlator binary                         |
| `make build-all` | Build all binaries (correlator, ingester, migrator) |
| `make dev`       | Run correlator in development mode                  |
| `make clean`     | Clean build artifacts                               |

### Testing

| Target                  | Description                                           |
|-------------------------|-------------------------------------------------------|
| `make test`             | Run all tests (unit + integration) with coverage      |
| `make test-unit`        | Run unit tests only (fast, -short flag)               |
| `make test-integration` | Run integration tests (real databases, 10min timeout) |
| `make test-race`        | Run tests with race detection                         |

### Code Quality

| Target      | Description                                 |
|-------------|---------------------------------------------|
| `make lint` | Run golangci-lint v2 comprehensive analysis |
| `make fmt`  | Format code with golangci-lint fmt          |
| `make vet`  | Run go vet static analysis                  |

### Dependencies

| Target      | Description                                  |
|-------------|----------------------------------------------|
| `make deps` | Download and verify Go module dependencies   |
| `make tidy` | Tidy go modules (remove unused dependencies) |

### Database Migration (Local Development)

| Target                 | Description                            |
|------------------------|----------------------------------------|
| `make migrate-up`      | Apply all pending migrations (local)   |
| `make migrate-down`    | Rollback the last migration (local)    |
| `make migrate-status`  | Show current migration status (local)  |
| `make migrate-version` | Show current migration version (local) |
| `make migrate-drop`    | Drop all tables - destructive! (local) |

### Build Migration Tool

| Target                     | Description                                     |
|----------------------------|-------------------------------------------------|
| `make build-migrator`      | Build migrator binary for development           |
| `make build-migrator-prod` | Build optimized migrator for production (Linux) |

### Docker Development Environment

| Target                      | Description                                   |
|-----------------------------|-----------------------------------------------|
| `make docker-dev`           | Start PostgreSQL for development (foreground) |
| `make docker-dev-bg`        | Start PostgreSQL in background                |
| `make docker-run`           | Run full stack (correlator + postgres)        |
| `make docker-stop`          | Stop all Docker services                      |
| `make docker-logs`          | View all service logs                         |
| `make docker-logs-postgres` | View PostgreSQL logs only                     |

### Docker Migration Commands

| Target                        | Description                         |
|-------------------------------|-------------------------------------|
| `make docker-build-migrator`  | Build migrator Docker image         |
| `make docker-migrate-up`      | Apply migrations using Docker       |
| `make docker-migrate-down`    | Rollback migrations using Docker    |
| `make docker-migrate-status`  | Show migration status using Docker  |
| `make docker-migrate-version` | Show migration version using Docker |
| `make docker-migrate-drop`    | Drop all tables using Docker        |

### Development Environment Setup

| Target                  | Description                                       |
|-------------------------|---------------------------------------------------|
| `make setup`            | Initial project setup (copy .env.example to .env) |
| `make docker-dev-setup` | Setup development environment with Docker         |
| `make docker-health`    | Run comprehensive health checks                   |

### Help & Information

| Target      | Description                                   |
|-------------|-----------------------------------------------|
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
---
## GitHub Workflows

This directory contains CI/CD workflows for the Correlator project.

### Pull Request Workflow (`pr.yml`)

Runs on all pull requests and pushes to `main` branches.

#### Jobs

1. **Lint** - Code Quality Checks
    - Check formatting (`gofmt`)
    - Static analysis (`go vet`)
    - Comprehensive linting (`golangci-lint`)

2. **Test** - Comprehensive testing
    - **Unit Tests**: Fast tests with mocked dependencies (`-short` flag)
    - **Integration Tests**: Real PostgreSQL database tests using testcontainers
    - **Database**: PostgreSQL 15-alpine service container
    - **Coverage**: Test coverage reporting

#### Configuration

- **Go Version**: 1.25.0 (set in `env.GO_VERSION`)
- **golangci-lint Version**: v2.4.0
- **Test Timeout**: 10 minutes for integration tests
- **Security**: PostgreSQL test password stored in GitHub Secrets (`POSTGRES_TEST_PASSWORD`) - no plaintext credentials in workflow files

**Near-Perfect CI/Local Consistency**: The PR workflow uses the same `make` commands you run locally, ensuring identical behavior between development and CI environments.

**Note on Linting**: CI uses the official v2 of `golangci-lint`. This must be the same version as the local development environment. Both use `make lint` and the same `.golangci.yml` configuration for identical linting rules.

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
brew install golangci-lint  # If using different version on mac (see instruction for other OS)
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

- see [CONTRIBUTING](CONTRIBUTING.md) for more guidelines.

