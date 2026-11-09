---
name: derive-databricks-metrics
description: Derive the A-J Spark benchmark metrics for a single Databricks job run using Databricks-native evidence only — the Jobs API run object, the Spark event log the run wrote, and system tables (lakeflow job timeline, compute node timeline + node types, billing usage, query history). Given a run_id it emits one JSON document with job identity and every metric derivable from Databricks alone, each tagged with evidence category, availability, and source. Use for Databricks-only performance/efficiency/cost measurement, Spark event-log parsing, and system-table metric collection.
---

# Derive Databricks Metrics

Given one Databricks **run_id**, derive the benchmark metrics (identity, time,
efficiency, work-done, engine acceleration, cost) using **only** evidence that
Databricks itself exposes — no second platform, no cloud billing export, no
orchestrator logs. Each metric carries its evidence category, availability, and
source, so a reviewer sees exactly what was measured versus what Databricks
cannot provide for that run.

This is the Databricks-only sibling of `databricks-migration-metrics`: it
vendors that project's platform-neutral Spark event engine and metric envelope,
then layers Databricks-native collectors on top.

## Databricks-native sources

| Source | Metrics |
|---|---|
| Jobs API `get-run` | A identity, B time (proxies), capacity, engine flag, result state |
| Spark event log (from `spark.eventLog.dir`) | B compute (measured), C3/C4/C5/C7/C10, all of D |
| `system.compute.node_timeline` + `node_types` | C1/C2 measured vCPU/memory-hours, C6 VM CPU% |
| `system.lakeflow.job_run_timeline` | run/queue timeline |
| `system.billing.usage` | I5 SKU/tier, I6 DBUs |
| `system.query.history` | read rows/bytes for serverless/SQL runs (classic jobs use the event log) |

## Usage

```bash
cd scripts
python3 derive_job_metrics.py \
  --run-id <run_id> \
  --profile <databricks-cli-profile> \
  --warehouse-id <sql-warehouse-id> \
  [--event-log ./eventlog]   # optional: skip auto-download, use a local copy
  [--output metrics.json]
```

The driver pulls the run object, auto-discovers and downloads the event log from
`spark.eventLog.dir` (or uses `--event-log`), queries the system tables, and
prints one JSON document: `job` (identity), `metrics` (every A-J envelope), and
`coverage` (available vs unavailable metric ids). Omit `--warehouse-id` (or pass
`--no-system-tables`) to derive only the Jobs-API + event-log metrics.

## Rules that keep it honest

- **Databricks-only.** Metrics that fundamentally need a second source — B2
  (orchestrator trigger), I1/I2/I10 (cloud billing export), I8 (contract rate),
  G parity jobs, Git/run-register items — are *not* emitted as available. They
  are either omitted or returned `unavailable` with a reason.
- **Measured over proxy.** C1/C2 use real per-node uptime from
  `node_timeline × node_types`, not a configuration upper-bound. B compute uses
  first→last Spark task from the event log, falling back to the Jobs API
  execution-duration proxy only when the log is absent (labeled as such).
- **`null`, never `0`, for absent evidence.** A metric the event log reports as
  genuinely 0 (e.g. input metrics a reader never populated) is reported as 0 but
  flagged in `source`; a metric with no evidence is `unavailable`.
- **Engine acceleration (F1).** `STANDARD` = exactly 0%. An accelerated engine
  (Photon/native) needs operator/query-profile timing to prove its time share;
  an engine label alone (`runtime_engine=PHOTON`) is not sufficient and yields
  `unavailable` with that reason.

## Layout

- `scripts/databricks_sources.py` — the only Databricks-specific I/O (CLI + SQL).
- `scripts/metrics_databricks.py` — A-J assembly; measured C1/C2/C6/C12 + A/I.
- `scripts/derive_job_metrics.py` — driver: run_id → full metric document.
- `scripts/{spark_event_metrics,canonical,metrics,cost}.py` — vendored,
  platform-neutral engine (do not edit per Databricks specifics).
- `references/metric-coverage.md` — per-metric source and derivability table.
