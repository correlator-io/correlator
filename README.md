# Correlator
**Incident Correlation Engine for Data Teams**

> "We don't monitor. We correlate." — The missing link between your existing data tools.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.25+-blue.svg)](https://golang.org)
[![Status](https://img.shields.io/badge/Status-MVP%20Development-orange.svg)]()

---

## What does this do?

Correlator automatically connects test failures → job runs → downstream impact across your data stack. Instead of manually hunting through dbt logs, Airflow traces, and test results during incidents, Correlator correlates everything for you and shows the root cause in 2 clicks.

**The problem:** Data teams spend 40% of their time firefighting incidents because observability tools exist in silos. Average incident resolution takes 15 hours with constant context switching between 5-8 different tools.

**The solution:** An intelligent correlation engine that links your existing tools via canonical identifiers (`datasetURN`, `jobRunId`, `traceId`) and turns investigation into navigation.

---

## Why should I care?

- **Reduce MTTR by 75%** - From 15-hour investigations to 2-click root cause analysis
- **Stop context switching** - Single dashboard instead of juggling multiple tools
- **Works with your existing stack** - Correlates dbt, Airflow, Great Expectations, OpenTelemetry
- **Standards-first** - Built on OpenLineage and OpenTelemetry, no vendor lock-in

---

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
