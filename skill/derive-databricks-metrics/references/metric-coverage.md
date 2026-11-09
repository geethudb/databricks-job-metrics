# Metric coverage (Databricks-only)

Which A-J metrics this toolkit derives from Databricks-native evidence, and the
source each uses. "Derivable" reflects the benchmark sheet's
*Databricks-only derivable?* column. Metrics that need a second platform are
listed as out of scope so the gap is explicit, not silent.

## Derived (available when the evidence is present)

| ID | Metric | Databricks source | Notes |
|---|---|---|---|
| A4 | Run ID | Jobs API `run_id` | |
| A5 | Spark application ID(s) | `cluster_instance.spark_context_id` / event log | |
| A6 | Cluster ID | Jobs API `cluster_instance.cluster_id` | |
| B1 | Submit/start time | Jobs API `start_time` | platform-start proxy for external submit |
| B3 | Startup | event log (submit→first task), else `setup_duration` | proxy labeled in `source` |
| B4 | Compute | event log (first→last task), else `execution_duration` | measured preferred |
| B5 | Commit tail | Jobs API end − last Spark task | |
| B6 | End-to-end | Jobs API end − start | |
| B7 | Attempts | Jobs API task attempts | |
| B8 | Result / failure | Jobs API terminal state | |
| C1 | Allocated vCPU-hours | `node_timeline` minutes × `node_types` cores | **measured**, not a proxy |
| C2 | Allocated memory GiB-hours | `node_timeline` minutes × `node_types` memory | measured |
| C3 | Task-time core-hours | event log Σ executor run time | |
| C4 | Task CPU-hours | event log Σ executor CPU time | |
| C5 | CPU/runtime ratio | event log | |
| C6 | VM CPU % (avg) | `node_timeline` cpu_user+cpu_system | |
| C7 | GC time % | event log | |
| C10 | Peak concurrent executors | event log executor add/remove | |
| C12 | Idle capacity vCPU-hours | C1 − C3 | derived |
| D1-D14 | Work done (input/shuffle/spill/output/stages/tasks) | event log task metrics | |
| I5 | DBU SKU / tier | `system.billing.usage` | |
| I6 | DBUs | `system.billing.usage` by `job_run_id` | |

## Conditionally available

| ID | Metric | Needs |
|---|---|---|
| C8 | Task slots | `spark.executor.cores` in the cluster spec |
| C9 | Memory per slot | `spark.executor.memory` + off-heap in the cluster spec |
| F1 | Accelerated-engine time share | operator/query-profile Photon timing (0 for STANDARD; `unavailable` for an accelerated engine without profile timing) |

## Out of scope (needs a second source — not Databricks-only)

| ID | Metric | Why |
|---|---|---|
| B2 | Scheduler delay | external orchestrator (Airflow/Composer) trigger timestamp |
| I1/I2/I10 | VM / disk / storage+network cost | cloud billing export |
| I8/I11-I13 | Contract-rate DBU cost and roll-ups | Finance contract rate (external) |
| G1-G5 | Correctness / parity | a neutral parity job must be created and run |
| A1-A3, A8 | Workload / leg / run # / class | experiment run register |
| J1-J5 | Effort & operations | Git, run register, work logs |
