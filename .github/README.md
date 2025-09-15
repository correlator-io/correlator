# GitHub Workflows

This directory contains CI/CD workflows for the Correlator project.

## PR Workflow (`pr.yml`)

Runs on all pull requests and pushes to `main`/`master` branches.

### Jobs

1. **Lint** - Code quality checks
   - Go formatting verification (`gofmt`)
   - Static analysis (`go vet`)
   - Comprehensive linting (`golangci-lint-action@v8` with PR annotations)
   - Dependency verification

2. **Test** - Comprehensive testing
   - **Unit Tests**: Fast tests with mocked dependencies (`-short` flag)
   - **Integration Tests**: Real PostgreSQL database tests using testcontainers
   - **Database**: PostgreSQL 15-alpine service container
   - **Coverage**: Test coverage reporting

### Configuration

- **Go Version**: 1.25.0 (set in `env.GO_VERSION`)
- **golangci-lint Version**: v1.59.1 (via official GitHub Action v8.0.0)
- **Test Timeout**: 10 minutes for integration tests
- **Security**: PostgreSQL test password stored in GitHub Secrets (`POSTGRES_TEST_PASSWORD`) - no plaintext credentials in workflow files
- **Cache**: Go modules, build cache, and golangci-lint cache for faster runs

### Development

To run the same checks locally (matching CI pipeline):

```bash
# Download and verify dependencies
make deps

# Format code
make fmt

# Run static analysis
make vet

# Run linting (requires golangci-lint installation)
make lint

# Run unit tests (fast)
make test-unit

# Run all tests including integration
make test-integration

# Build all binaries
make build-all
```

**Near-Perfect CI/Local Consistency**: The PR workflow uses the same `make` commands you run locally, ensuring identical behavior between development and CI environments.

**Note on Linting**: CI uses the official `golangci-lint-action@v8` for better performance and PR annotations, while local development uses `make lint`. Both use the same `.golangci.yml` configuration for identical linting rules.

### Required GitHub Secrets

The workflow requires the following secrets to be configured in the repository settings:

- `POSTGRES_TEST_PASSWORD`: PostgreSQL password for integration tests (e.g., `correlator_test_password`)

### Benefits of GitHub Action Integration

- **Enhanced PR Experience**: Lint issues appear as inline PR comments
- **Optimized Performance**: Built-in caching for faster CI runs
- **Automatic Updates**: Action handles golangci-lint version compatibility
- **Better Error Reporting**: Rich formatting and context in CI logs

### Phase 3 Status ✅

Current implementation includes:
- Production-ready migration system with comprehensive testing
- Real database integration testing with testcontainers-go
- Code quality validation with comprehensive PR workflow
- GitHub Actions workflow with official golangci-lint integration

### Build & Deployment Workflows

Build artifacts and Docker images are generated at milestone gates (Alpha, Beta, Release) rather than on every PR to optimize CI performance and focus PR validation on code quality.

### Phase 4 Planned

- Enhanced versioning with build-time injection
- Embedded migration system testing
- Performance validation with real correlation workloads
- Release workflow for milestone builds