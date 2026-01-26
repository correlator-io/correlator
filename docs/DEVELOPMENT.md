# Correlator Development Guide

Welcome to Correlator! This guide gets you from clone to coding in minutes with our streamlined 8-command development system.

---

## Quick Start

**New to this project?** One command gets you started:

```bash
git clone https://github.com/correlator-io/correlator.git
cd correlator
make start
```

That's it! The command will:
- ✅ Install dependencies automatically
- ✅ Create your dev container  
- ✅ Start the database
- ✅ Apply migrations
- ✅ Put you inside the dev container ready to code

---

## The 8 Essential Commands

Our development workflow is built around **8 intent-based commands** that handle everything:

### 🚀 Getting Started
- **`make start`** - Begin working (smart setup + enter dev container)
- **`make run`** - Execute something (run, run test, run migrate up)

### 🛠️ Daily Development  
- **`make check`** - Verify code quality (lint + test + vet)
- **`make fix`** - Repair issues (format + tidy + clean artifacts)

### 🐳 Environment
- **`make docker`** - Container operations (docker, docker prod, docker stop)

### 🏗️ Build & Deploy
- **`make build`** - Create artifacts (build, build migrator, build prod, build all)
- **`make deploy`** - Prepare for production (builds + images + migrations)

### 🔧 Maintenance
- **`make reset`** - Start fresh (clean everything + stop services)

---

## Development Workflow

### Backend Development (Inside Dev Container)

#### First Time Setup

```bash
# Clone and start (zero configuration)
git clone https://github.com/correlator-io/correlator.git
cd correlator
make start                    # Complete environment setup + enter dev container

# You're now inside the dev container, ready to code!
```

```bash
# Morning startup
make start                    # Quick entry into dev container

# Inside dev container - your daily commands:
make run test                 # Run all tests
make run test unit            # Quick unit tests
make run benchmark            # Performance benchmarks
make check                    # Code quality before commit

# Database operations:
make run migrate up           # Apply new migrations
make run migrate status       # Check migration status
```

### Frontend Development (On Host Machine)

Frontend development runs on the host machine, not inside the dev container. This provides better Hot Module Replacement (HMR) and file watching performance.

```bash
# Open a new terminal (don't enter dev container)
cd correlator
make run web                  # Start Next.js dev server at localhost:3000

# Other frontend commands (from host):
make run web build            # Build for production
make run web lint             # Run ESLint
make run web test             # Run frontend tests
```

**Tip:** Run backend and frontend in separate terminals:
- Terminal 1: `make start` → backend dev container
- Terminal 2: `make run web` → frontend dev server

### Service Management (From Host)

```bash
# Run from host (outside dev container):
make run                      # Start backend development server
make docker prod              # Run full production stack
make docker stop              # Stop all services
make deploy                   # Prepare for production
```

---

## Architecture: Container-First Development

### Two Development Contexts

**🏗️ Dev Container** (for backend development):
- All Go development tools (Go, linters, etc.)
- Direct database access via environment variables  
- Code editing, testing, migrations
- **Access**: `make start` enters automatically

**🏠 Host Machine** (for frontend and services):
- Frontend development server (Next.js)
- Backend development server execution
- Docker orchestration
- Production builds and deployments
- **Usage**: Exit dev container or open new terminal
- Production builds and deployments
- **Usage**: Exit dev container, then run commands

### Smart Command Routing

Our Makefile automatically routes commands to the right context:

- **Inside dev container**: `make run migrate up` uses local binary
- **From host**: `make run migrate up` uses containerized migrator
- **Guard clauses** prevent running wrong commands in wrong places

---

## Command Reference

### 🚀 `make start` - Smart Environment Setup

**Context**: Host machine only

**Behavior**:
- **First time**: Full setup (dependencies + dev container + database + migrations) + enter container
- **Daily use**: Quick entry into existing dev container
- **After restart**: Smart container restart + enter container

### 🏃 `make run` - Execute Operations

**Examples**:
```bash
# Development server (host only)
make run                      

# Testing (works everywhere)
make run test                 # All tests
make run test unit            # Unit tests only
make run test integration     # Integration tests only
make run test race            # Race detection
make run benchmark            # Benchmark tests

# Database migrations (works everywhere) 
make run migrate up           # Apply migrations
make run migrate down         # Rollback migration
make run migrate status       # Migration status

# Frontend development (host only)
make run web                  # Start frontend dev server (localhost:3000)
make run web build            # Build frontend for production
make run web lint             # Run frontend linter
make run web test             # Run frontend tests
```

### 🔍 `make check` - Code Quality

**Context**: Dev container (where tools live)

```bash
make check                    # Full quality suite:
                              # - golangci-lint 
                              # - unit tests
                              # - integration tests  
                              # - go vet
```

### 🔧 `make fix` - Auto-Repair

**Context**: Dev container

```bash
make fix                      # Auto-fix what we can:
                              # - Format code
                              # - Tidy dependencies
                              # - Clean build artifacts
```

### 🐳 `make docker` - Container Operations

**Context**: Host machine only

```bash
make docker                   # Start dev environment
make docker prod              # Full production stack
make docker stop              # Stop all services + dev container
make docker logs              # View service logs
make docker health            # Comprehensive diagnostics
```

### 🏗️ `make build` - Create Artifacts

**Context**: Both (optimized per context)

```bash
make build                    # Development build
make build prod               # Production-optimized 
make build all                # All components
```

### 🚀 `make deploy` - Production Preparation

**Context**: Host machine only

```bash
make deploy                   # Complete deployment package:
                              # - Code quality check
                              # - Production builds  
                              # - Docker images
                              # - Migration readiness
```

### 💥 `make reset` - Nuclear Reset

**Context**: Host machine only

```bash
make reset                    # When things go wrong:
                              # - Stop all containers
                              # - Clean build artifacts
                              # - Remove Docker cache
                              # - Fresh state
```

---

## Development Environment Details

### Prerequisites

**Required**:
- **Docker Desktop** (must be running)
- **Git**
- **npm**

**Optional** (auto-installed):
- devcontainer CLI
- Go toolchain (inside dev container)

### Tech Stack

**Dev Container**:
- **Go 1.25.0** - Latest stable
- **golangci-lint** - Code quality
- **PostgreSQL client** - Database access
- **All development tools** - Pre-configured

**Database**:
- **PostgreSQL 15** - Production-matched version
- **Extensions**: pg_trgm, pg_stat_statements
- **Optimized configuration** - For correlation workloads

---

## Testing

### Test Categories

- **Unit Tests**: Fast, isolated, mocked dependencies
- **Integration Tests**: Real database, full workflows

### Running Tests

```bash
# Inside dev container:
make run test                 # All tests (unit + integration)
make run test unit            # Fast unit tests only
make run test integration     # Integration tests only
make run test race            # Race condition detection
make run benchmark            # Performance benchmarks

# Quality check (includes tests):
make check                    # Full suite before commit
```

### Test Conventions
We use `testing.Short()` flag to separate unit tests from integration tests. You must add the `testing.Short()` flag 
at the beginning of the unit test suit and the negated flag `!testing.Short()` at the beginning of the integration test suite.

**Unit Tests**:
```go
if !testing.Short() {
    t.Skip("skipping unit test in non-short mode")
}
```

**Integration Tests**:
```go
if testing.Short() {
    t.Skip("skipping integration test in short mode")
}
```

**Benchmark Tests**:
```go
func BenchmarkMyFunction(b *testing.B) {
    for i := 0; i < b.N; i++ {
        // Code to benchmark
        result := MyFunction()
        _ = result // Prevent optimization
    }
}
```

---

## Database Development

### Migration Workflow

```bash
# Inside dev container:
make run migrate status       # Check current state
make run migrate up           # Apply new migrations
make run migrate down         # Rollback if needed

# View logs (from host):
make docker logs postgres     # Database logs
```

### Environment-Aware Migrations

- **Dev container**: Uses local migrator binary + DATABASE_URL
- **Host machine**: Uses containerized migrator + Docker networking
- **Same interface**: `make run migrate up` works everywhere

---

## Troubleshooting

### Common Issues

**🔧 Environment Problems**
```bash
make docker health            # Comprehensive diagnostics
make reset && make start      # Nuclear reset + fresh start
```

**🐛 Container Issues**
```bash
# Exit dev container, then:
make docker stop              # Stop everything
make start                    # Restart fresh
```

**⚡ Performance Issues**
```bash
make fix                      # Clean up artifacts
make run test unit            # Skip integration tests
```

### Diagnostic Tools

**Comprehensive Health Check**:
```bash
make docker health            # Checks:
                              # - Docker system
                              # - Port availability  
                              # - Container status
                              # - Migration files
                              # - Network connectivity
                              # - System resources
```

**Quick Status**:
```bash
make docker logs              # Service logs
docker ps                     # Container status
```

---

## Getting Help

### Command Help

```bash
make help                     # Show all commands with examples
make run                      # Shows available run targets
make docker                   # Shows available docker operations
```

### Further Reading

- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Contribution guidelines
- **[Migrator](../docs/MIGRATOR.md)** - Database migrator

---

## Pro Tips 💡

### Efficient Workflows

**Morning Startup**:
```bash
make start                    # One command, ready to code
```

**Frontend Development**:
```bash
# From host machine (not inside dev container)
make run web                  # Start Next.js dev server at localhost:3000
```

**Pre-Commit Check**:
```bash
make check                    # Backend quality gate
make run web lint             # Frontend quality gate
```

**Performance Analysis**:
```bash
make run benchmark            # Profile performance bottlenecks
make docker logs postgres     # View database logs
make run migrate status       # Check migration state
```

**Clean Slate**:
```bash
make reset && make start      # Nuclear reset when stuck
```

### IDE Integration

**VS Code**: 
- Dev container integration works automatically
- All extensions pre-configured in container

**IntelliJ**: 
- Use "Remote Development" → "Dev Containers"
- Full debugging support in container

### Performance Optimization

- **Use `make run test unit`** for fast feedback during development
- **Use `make check`** before committing (runs full suite)  
- **Keep dev container running** - restart is faster than rebuild

### Git Configuration

#### **First-Time Setup (Required)**

Configure your identity for proper commit attribution and signing:

```bash
# Set your name and email (required for signed commits)
git config --global user.name "Your Full Name"
git config --global user.email "your.email@example.com"

# Verify configuration
git config --global --list | grep user
```

**⚠️ Important**: These settings are **required** for `git commit -s` (signed-off commits) to work properly. Without them, your commits cannot be attributed to you.

#### **Editor Configuration**

The dev container comes pre-configured with:
- **nano** (default editor for Git commits)
- **vim** (alternative editor for advanced users)

```bash
git commit -s                         # Opens nano for commit message
git config --global core.editor vim   # Switch to vim if preferred
git config --global core.editor nano  # Switch back to nano
```

#### **Common Git Workflows**

```bash
# Standard workflow with signed commits
git add .
git commit -s -m "feat: add new feature"

# Interactive commit with editor
git add .
git commit -s                         # Opens nano for detailed message

# Check your configuration
git config --global --list | grep -E "(user|core.editor)"
```

#### **Authentication Setup**

**Problem**: Asked for username/password repeatedly when pushing from dev container.

**🚀 Quick Fix (Credential Cache)**:
```bash
# Cache credentials for 1 hour (3600 seconds)
git config --global credential.helper 'cache --timeout=3600'

# Or cache for 8 hours (full workday)
git config --global credential.helper 'cache --timeout=28800'

# First push will ask for credentials, then cached for the timeout period
git push origin main
```

**🔐 Best Practice (SSH Keys)**:
```bash
# 1. Generate SSH key inside dev container (if you don't have one)
ssh-keygen -t ed25519 -C "your.email@example.com"

# 2. Display public key to copy to GitHub/GitLab
cat ~/.ssh/id_ed25519.pub

# 3. Add to your Git provider (GitHub: Settings → SSH and GPG keys)

# 4. Test SSH connection
ssh -T git@github.com

# 5. Update remote URL to use SSH (if currently using HTTPS)
git remote set-url origin git@github.com:username/repository.git
```

**💡 Personal Access Token (Alternative)**:
```bash
# If you prefer HTTPS, use Personal Access Token instead of password
# GitHub: Settings → Developer settings → Personal access tokens
# Use token as password when prompted

# Cache the token
git config --global credential.helper 'cache --timeout=86400'  # 24 hours
```
---

*Happy coding! 🚀*