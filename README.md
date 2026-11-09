# databricks-job-metrics

Derive the A–J Spark benchmark metrics for a single **Databricks job run** using
**Databricks-native evidence only** — the Jobs API run object, the Spark event
log the run wrote, and system tables. No second platform, no cloud billing
export, no orchestrator logs.

Give it a `run_id`; it returns one JSON document with the job identity and every
metric it could derive from Databricks alone, each tagged with evidence
category, availability, and source.

It ships as a skill under
[`skill/derive-databricks-metrics/`](skill/derive-databricks-metrics/) and
vendors the platform-neutral Spark event engine from
[`databricks-migration-metrics`](https://github.com/geethudb/databricks-migration-metrics),
then layers Databricks-native collectors on top.

## Databricks-native sources

| Source | Metrics |
|---|---|
| Jobs API `get-run` | A identity, B time, capacity, engine, result state |
| Spark event log (`spark.eventLog.dir`) | B compute (measured), C3/C4/C5/C7/C10, all of D |
| `system.compute.node_timeline` + `node_types` | C1/C2 measured vCPU/memory-hours, C6 VM CPU% |
| `system.lakeflow.job_run_timeline` | run / queue timeline |
| `system.billing.usage` | I5 SKU, I6 DBUs |
| `system.query.history` | read rows/bytes for serverless/SQL runs |

## Usage

```bash
cd skill/derive-databricks-metrics/scripts
python3 derive_job_metrics.py \
  --run-id 89830100569270 \
  --profile <databricks-cli-profile> \
  --warehouse-id <sql-warehouse-id> \
  [--event-log ./eventlog] [--output metrics.json]
```

Requires the `databricks` CLI authenticated to the target workspace (the only
runtime dependency — the code itself is stdlib-only Python 3). Omit
`--warehouse-id` to derive just the Jobs-API + event-log metrics.

## Example output

A real run of `benchmark-event-log-word-count-long-photon` (Photon, classic,
1×i3.xlarge worker) derived **32 of 36** metrics live, including measured
`C1 = 2.53 vCPU-hours`, `C2 = 19.3 GiB-hours`, `B4 compute = 688.6 s`, and
`I6 = 1.52 DBU` (ENTERPRISE_JOBS_COMPUTE_PHOTON). See
[`skill/derive-databricks-metrics/examples/metrics.example.json`](skill/derive-databricks-metrics/examples/metrics.example.json).
The four not derived — B2, C8, C9, F1 — each need evidence Databricks cannot
supply for that run (orchestrator timestamp, executor config, Photon operator
profiling), and are returned `unavailable` with a reason rather than guessed.

## Tests

```bash
cd skill/derive-databricks-metrics
python3 tests/run_tests.py      # no pytest dependency; also compatible with: pytest tests/
```
