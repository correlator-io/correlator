# Correlator Demo Environment

Correlation demo: **dbt + Airflow + Great Expectations**

## Quick Start

```bash
# 1. Start the demo infrastructure
make start demo

# 2. Run the full demo pipeline
make run demo
```

That's it! The demo will:

1. Start all containers (PostgreSQL, Correlator, Airflow)
2. Trigger the Airflow DAG that runs dbt and GE
3. Generate lineage events that appear in Correlator UI

## Access URLs

| Service        | URL                   | Credentials                          |
|----------------|-----------------------|--------------------------------------|
| Correlator UI  | http://localhost:3001 | -                                    |
| Correlator API | http://localhost:8081 | -                                    |
| Airflow UI     | http://localhost:8082 | admin / admin                        |
| PostgreSQL     | localhost:5433        | correlator / correlator_dev_password |

## Port Mapping

| Service           | Dev Port | Demo Port | Mental Model             |
|-------------------|----------|-----------|--------------------------|
| PostgreSQL        | 5432     | 5433      | +1                       |
| Correlator API    | 8080     | 8081      | +1                       |
| Correlator UI     | 3000     | 3001      | +1                       |
| Airflow Webserver | 8080     | 8082      | +2 (avoids API conflict) |

**Mental model:** Demo ports are Dev ports + 1 (Airflow is +2 to avoid API conflict)

## Commands

### Starting and Stopping

```bash
make start demo            # Start demo infrastructure
make run demo              # Run full demo pipeline (Airflow DAG)
make docker stop demo      # Stop demo environment
make docker logs demo      # View all logs
```

### Running Data Tools Manually

```bash
# dbt commands
make run demo dbt seed      # Load seed data
make run demo dbt run       # Run transformations
make run demo dbt test      # Run dbt tests
make run demo dbt debug     # Show dbt debug info

# Great Expectations
make run demo ge validate   # Run GE checkpoint
```

## PostgreSQL Schemas

| Schema       | Purpose                 |
|--------------|-------------------------|
| `public`     | Demo data (Jaffle Shop) |
| `airflow`    | Airflow metadata        |
| `correlator` | Correlator lineage data |

## Job Namespace Convention

Each tool emits OpenLineage events with a **job namespace** that identifies the tool. Correlator uses these namespaces
to categorize and correlate events.

| Tool               | Job Namespace               | Canonical ID Format |
|--------------------|-----------------------------|---------------------|
| dbt                | `dbt://demo`                | `dbt:run-123`       |
| Great Expectations | `great_expectations://demo` | `ge:validation-456` |
| Airflow            | `airflow://demo`            | `airflow:dag-789`   |

**Why this matters:**

- The namespace prefix (before `://`) identifies the tool type
- Correlator's canonicalizer extracts this prefix to generate canonical job run IDs
- This enables filtering by tool (e.g., "show all dbt runs") and cross-tool correlation

**Configuration:**

- dbt: Set via `OPENLINEAGE_NAMESPACE` environment variable (or `--openlineage-namespace` CLI flag)
- GE: Set via `GE_JOB_NAMESPACE` environment variable or `job_namespace` in checkpoint
- Airflow: Automatically set by Airflow's OpenLineage provider

**Namespace Aliasing:**

If tools emit different namespace formats, configure aliases in `config/.correlator.yaml` to map them to canonical
forms. See the file for examples.

## Demo Scenarios

### Scenario 1: Success Path

1. Start demo: `make start demo`
2. Run pipeline: `make run demo`
3. **Expected:** No incidents in Correlator UI

### Scenario 2: Failure Path

1. Inject bad data (see `scripts/seed-failure.sh`)
2. Run pipeline: `make run demo`
3. **Expected:** Incidents appear with cross-tool correlation

### Scenario 3: Orphan Namespace Detection

1. Run all tools with default namespaces
2. Check Health page - orphan namespaces visible
3. Configure `config/.correlator.yaml` with aliases
4. Restart: `make docker stop demo && make start demo`
5. **Expected:** Orphan count drops to 0

## Directory Structure

```
deployments/demo/
├── docker-compose.demo.yml      # Main compose file
├── dockerfiles/
│   ├── dbt.Dockerfile           # dbt + dbt-correlator
│   ├── airflow.Dockerfile       # Airflow + all plugins (dbt, GE, correlator)
│   └── ge.Dockerfile            # GE + ge-correlator
├── postgres-init/
│   └── 01-init-schemas.sql      # Schema initialization
├── config/
│   └── .correlator.yaml         # Namespace aliases
├── dbt/                         # dbt project (Jaffle Shop)
│   ├── macros/                  # Custom dbt macros
│   │   └── generate_schema_name.sql  # Schema naming override
│   ├── models/                  # SQL transformations
│   ├── seeds/                   # Raw CSV data
│   └── profiles.yml             # Database connection
├── airflow/
│   ├── dags/                    # Airflow DAGs
│   │   └── demo_pipeline.py     # Main demo DAG
│   └── openlineage.yml          # airflow-correlator config
├── great-expectations/          # GE project
│   └── checkpoints/
│       └── demo_checkpoint.py   # Validation checkpoint
└── scripts/                     # Helper scripts
    ├── seed-success.sh          # Use good data
    ├── seed-failure.sh          # Use bad data
    └── restore-good-data.sh     # Restore after failure
```

## dbt Schema Naming

The dbt project includes a custom `generate_schema_name` macro that overrides dbt's default schema naming behavior.

**Why this is needed:**

By default, dbt creates schemas using the pattern `{target_schema}_{custom_schema}`. With our `profiles.yml` setting
`schema: public`, this would create:

- `public_staging` instead of `staging`
- `public_marts` instead of `marts`

The custom macro ensures schemas are created with their intended names (`staging`, `marts`), which is required for:

- Great Expectations checkpoint to find tables in the expected schema
- Cleaner schema organization in the demo database

The macro is located at `dbt/macros/generate_schema_name.sql`.

## Troubleshooting

### Services not starting

```bash
# Check logs
make docker logs demo

# Check specific service
make docker logs demo correlator
make docker logs demo airflow-webserver
```

### Database connection issues

```bash
# Connect to PostgreSQL using a visual DB client or psql directly:
docker exec -it demo-postgres psql -U correlator -d demo

# Check schemas
\dn

# Check tables in correlator schema
SET search_path TO correlator;
\dt
```

### Port conflicts

If you see port conflicts, the development environment may be running:

```bash
# Stop development environment first
make docker stop

# Then start demo
make start demo
```

### Clean restart

```bash
# Stop and remove demo containers
make docker stop demo

# Remove demo volumes (fresh start)
cd deployments/demo && docker compose -f docker-compose.demo.yml down -v

# Start fresh
make start demo
```

## Plugin Versions

All plugins are installed from **TestPyPI** for development workflow:

| Package              | Source   |
|----------------------|----------|
| `dbt-correlator`     | TestPyPI |
| `airflow-correlator` | TestPyPI |
| `ge-correlator`      | TestPyPI |

## Credentials

The demo uses the same credentials as the development environment for simplicity:

- **Database User:** `correlator`
- **Database Password:** `correlator_dev_password`
- **Airflow Admin:** `admin` / `admin`
