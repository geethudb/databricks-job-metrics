#!/usr/bin/env python3
"""Derive the A-J benchmark metrics for one Databricks job run, Databricks-only.

    python3 derive_job_metrics.py \
        --run-id 89830100569270 \
        --profile fe-vm-classic-stable-vx5v2m \
        --warehouse-id e8772103c419610c \
        [--event-log ./eventlog] [--output metrics.json]

Given a run_id it pulls the Jobs API run object, the Spark event log the run
wrote (auto-discovered from spark.eventLog.dir, or supply a local copy with
--event-log), and the relevant system tables, then emits one JSON document with
the job identity and every metric it could derive from Databricks alone.
"""
import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import databricks_sources as dbx  # noqa: E402
import metrics_databricks as mdb  # noqa: E402
from canonical import Capacity, RunDescriptor, normalize_result_state  # noqa: E402
from spark_event_metrics import read_events, summarize_events  # noqa: E402


def _run_to_descriptor(run):
    """Jobs API get-run JSON -> canonical RunDescriptor (control-plane + capacity)."""
    tasks = run.get("tasks") or []
    state = run.get("state") or {}
    clusters, app_ids = [], []
    cap = Capacity()
    engine = "STANDARD"
    spark_version = None
    for item in [run, *tasks]:
        ci = item.get("cluster_instance") or {}
        if ci.get("cluster_id"):
            clusters.append(ci["cluster_id"])
        if ci.get("spark_context_id"):
            app_ids.append(str(ci["spark_context_id"]))
        nc = item.get("new_cluster") or {}
        if nc:
            engine = (nc.get("runtime_engine") or engine)
            spark_version = nc.get("spark_version") or spark_version
            if nc.get("num_workers") is not None:
                cap.worker_count = int(nc["num_workers"])
    attempts = max([int(t.get("attempt_number", 0)) + 1 for t in tasks] or [1])
    return RunDescriptor(
        platform="databricks",
        run_id=str(run.get("run_id") or ""),
        job_id=str(run["job_id"]) if run.get("job_id") is not None else None,
        cluster_id=clusters[0] if clusters else None,
        workspace_or_project=str(run["workspace_id"]) if run.get("workspace_id") is not None else None,
        application_ids=app_ids,
        start_time_ms=run.get("start_time"),
        end_time_ms=run.get("end_time"),
        setup_duration_ms=run.get("setup_duration"),
        execution_duration_ms=run.get("execution_duration"),
        cleanup_duration_ms=run.get("cleanup_duration"),
        result_state=normalize_result_state(state.get("result_state")),
        failure_reason=state.get("state_message") or None,
        attempts=attempts,
        capacity=cap,
        runtime_engine=(engine or "STANDARD").upper(),
        spark_version=spark_version,
    )


def parse_args():
    p = argparse.ArgumentParser(description="Derive A-J benchmark metrics for one Databricks run")
    p.add_argument("--run-id", required=True)
    p.add_argument("--profile", required=True)
    p.add_argument("--warehouse-id", help="SQL warehouse for system-table queries")
    p.add_argument("--event-log", type=Path, help="Local event-log dir (else auto-download)")
    p.add_argument("--no-system-tables", action="store_true", help="Skip system-table queries")
    p.add_argument("--output", type=Path)
    return p.parse_args()


def main():
    args = parse_args()
    run = dbx.get_run(args.run_id, args.profile)
    descriptor = _run_to_descriptor(run)
    job_name = run.get("run_name") or (run.get("job_parameters") or {}).get("name") or descriptor.job_id

    # Event log: use the supplied copy, else auto-download from spark.eventLog.dir.
    event_log_path = args.event_log
    log_dir = dbx.event_log_dir(run)
    if event_log_path is None and log_dir:
        event_log_path = dbx.download_event_log(log_dir, args.profile, Path(tempfile.mkdtemp()) / "eventlog")
    events = summarize_events(read_events(event_log_path)) if event_log_path else {}

    # Node types from the Jobs API cluster spec (for measured C1/C2 capacity).
    driver_type = worker_type = None
    for item in [run, *(run.get("tasks") or [])]:
        nc = item.get("new_cluster") or {}
        if nc.get("node_type_id"):
            worker_type = nc["node_type_id"]
            driver_type = nc.get("driver_node_type_id") or nc["node_type_id"]
            break

    node_rows = billing_rows = query_rows = []
    warnings = []

    def _try(label, fn):
        try:
            return fn()
        except dbx.DatabricksError as exc:
            warnings.append(f"{label}: {exc}")
            return []

    if args.warehouse_id and not args.no_system_tables:
        if descriptor.cluster_id:
            node_rows = _try("node_timeline", lambda: dbx.node_timeline(
                descriptor.cluster_id, args.warehouse_id, args.profile,
                driver_node_type=driver_type, worker_node_type=worker_type))
        billing_rows = _try("billing_usage", lambda: dbx.billing_usage(args.run_id, args.warehouse_id, args.profile))
        query_rows = _try("query_history", lambda: dbx.query_history(args.run_id, args.warehouse_id, args.profile))

    metrics = mdb.derive_all(descriptor, events, node_rows, billing_rows, query_rows)

    doc = {
        "job": {
            "job_name": job_name, "run_id": descriptor.run_id, "job_id": descriptor.job_id,
            "cluster_id": descriptor.cluster_id, "runtime_engine": descriptor.runtime_engine,
            "spark_version": descriptor.spark_version, "result_state": descriptor.result_state,
            "application_ids": descriptor.application_ids,
            "event_log_dir": log_dir, "profile": args.profile,
        },
        "metrics": metrics,
        "coverage": {
            "available": sorted(m for m, e in metrics.items() if e["availability"] == "available"),
            "unavailable": sorted(m for m, e in metrics.items() if e["availability"] == "unavailable"),
        },
        "warnings": warnings,
    }
    text = json.dumps(doc, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    sys.stdout.write(text)


if __name__ == "__main__":
    main()
