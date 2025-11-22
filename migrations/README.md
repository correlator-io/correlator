# Correlator Database Migrator

The Correlator database migrator is a **zero-config deployment** migration tool that uses embedded SQL migrations for seamless database schema management. It ensures reliable, production-ready database migrations with no external file dependencies.

---

## What It Does

Manages PostgreSQL schema for the Correlator incident correlation engine:
- Applies database migrations automatically
- Handles version tracking and rollbacks
- Validates schema integrity
- Supports zero-downtime deployments

---

## Zero-Config Deployment Architecture

The migrator is designed around Correlator's core principle of **zero-config deployment**:

- ✅ **No file system dependencies**: Migrations are embedded at compile time using Go's `go:embed`
- ✅ **Single binary deployment**: All migrations bundled into the executable
- ✅ **Production-ready out-of-the-box**: No configuration files, directories, or external dependencies
- ✅ **Docker-friendly**: Supports `docker-compose up` for <30 minute production deployment

This eliminates common deployment issues like missing migration files, incorrect permissions, or path dependencies.

**How it works:**
```
SQL files → go:embed → Compiled into binary → Deployed as single executable
```

No need to copy migration files to servers. No need to mount volumes. Just run the binary.

---

## Why Migrator is Co-located with Migration Files

**Design Decision**: The migrator code is co-located in `/migrations` instead of the conventional `/cmd` structure.

### Benefits of Co-location

1. **Cohesion**: Migration logic and migration files are maintained together as a single unit
2. **Dependency Injection**: Easy access to embedded migrations for testing and validation
3. **Test Architecture**: Integration tests can use the same embedded migrations as production
4. **Development Workflow**: Changes to migrations and migrator logic are versioned together
5. **Zero-Config Principle**: Self-contained migration module with no external dependencies

### Trade-offs Accepted

- **Convention**: Deviates from typical Go project structure (`cmd/` for executables)
- **Package Organization**: Migrator becomes a specialized module rather than a general-purpose command

The benefits outweigh the trade-offs for Correlator's **zero-config** architecture.

---

## Documentation

**For detailed usage, commands, and workflows:**
- **Operating the tool**: [docs/MIGRATOR.md](../docs/MIGRATOR.md) - CLI commands, Make targets, troubleshooting
- **Writing migration files**: [docs/MIGRATIONS.md](../docs/MIGRATIONS.md) - SQL patterns, best practices, adding migrations
- **Development setup**: [docs/DEVELOPMENT.md](../docs/DEVELOPMENT.md) - Full development environment
