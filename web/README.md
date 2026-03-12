# 🔗 Correlator Web UI

**Incident correlation user interface for data engineers**

---

## What It Does

Provides a visual interface for incident correlation:

- View test failures with their correlation status
- Navigate from failure → producing job → downstream impact
- Identify namespace mismatches breaking correlation
- Monitor correlation health across your data stack

---

## Quick Start

```bash
# From the repository root
make run web                  # Start dev server at localhost:3000

# Or using npm directly
cd web
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to view the dashboard.

---

## Pages

| Page               | URL               | Description                                             |
|--------------------|-------------------|---------------------------------------------------------|
| Incidents          | `/incidents`      | List of test failures with status and downstream impact |
| Incident Detail    | `/incidents/[id]` | Full correlation view: test → job → lineage graph       |
| Correlation Health | `/health`         | Namespace issues, correlation rate, suggested fixes     |

---

## Development

Frontend development runs on the host machine (not inside the dev container):

```bash
# Start development server
make run web

# Build for production
make run web build

# Run linter
make run web lint

# Run tests
make run web test
```

See [docs/DEVELOPMENT.md](../docs/DEVELOPMENT.md) for the full development workflow.

---

## Tech Stack

- **Framework**: Next.js 14+ (App Router)
- **UI Components**: shadcn/ui (Radix primitives)
- **Styling**: Tailwind CSS
- **Lineage Visualization**: React Flow
- **Icons**: Lucide React

---

## Project Structure

```
web/
├── app/                      # Next.js App Router pages
│   ├── incidents/            # Incident list and detail pages
│   └── health/               # Correlation health dashboard
├── components/
│   ├── incidents/            # Incident-specific components
│   ├── health/               # Health dashboard components
│   ├── lineage/              # React Flow lineage graph
│   ├── layout/               # App shell, sidebar, header
│   ├── icons/                # Producer icons (dbt, Airflow, GE)
│   └── ui/                   # shadcn/ui components
└── lib/
    ├── types.ts              # TypeScript interfaces
    ├── utils.ts              # Utility functions
    └── mock-data.ts          # Mock data (until API integration)
```

---

## Links

- **Correlator**: https://github.com/correlator-io/correlator
- **shadcn/ui**: https://ui.shadcn.com/
- **React Flow**: https://reactflow.dev/

---

## License

Apache 2.0 - See [LICENSE](../LICENSE) for details.
