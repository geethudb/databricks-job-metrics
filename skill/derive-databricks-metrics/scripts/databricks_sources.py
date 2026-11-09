"""Databricks-only live source layer.

Given a job *run_id* and a CLI profile, this module pulls everything the
A-J metrics need from Databricks itself — no second platform, no cloud billing
export, no orchestrator logs. Every call shells out to the `databricks` CLI so
the toolkit has zero Python dependencies (stdlib only), matching the event
engine it sits on top of.

Sources used (all Databricks-native):
  * Jobs API  `databricks jobs get-run`      -> identity, wall-clock, capacity, engine
  * DBFS/Volume `databricks fs cp`            -> the Spark event log the run wrote
  * SQL Statement Execution API over system tables:
        system.lakeflow.job_run_timeline     -> queue / run-timeline (B)
        system.compute.node_timeline + node_types -> measured vCPU/mem hours (C1/C2), CPU% (C6)
        system.billing.usage                 -> DBUs by SKU (I5/I6)
        system.query.history                 -> read rows/bytes, Photon time share (D/F, serverless/SQL)
"""
import json
import subprocess
import time
from pathlib import Path


class DatabricksError(RuntimeError):
    pass


def _cli(args, profile, parse_json=True, json_body=None):
    cmd = ["databricks", *args, "--profile", profile]
    if json_body is not None:
        cmd += ["--json", json.dumps(json_body)]
    if parse_json:
        cmd += ["--output", "json"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise DatabricksError(f"{' '.join(cmd[:3])} ...\n{proc.stderr.strip()}")
    out = proc.stdout.strip()
    if not parse_json:
        return out
    # The CLI may emit a harmless setup banner on stdout's first line; find JSON.
    start = min((i for i in (out.find("{"), out.find("[")) if i != -1), default=-1)
    if start == -1:
        return {}
    return json.loads(out[start:])


# ----------------------------------------------------------------- Jobs API ---
def get_run(run_id, profile):
    """Raw Jobs API get-run JSON for one run."""
    return _cli(["jobs", "get-run", str(run_id)], profile)


def event_log_dir(run):
    """Read spark.eventLog.dir from the run's task cluster spec, if the job set it."""
    for task in [run, *(run.get("tasks") or [])]:
        conf = ((task.get("new_cluster") or {}).get("spark_conf") or {})
        if conf.get("spark.eventLog.enabled") in (True, "true") and conf.get("spark.eventLog.dir"):
            return conf["spark.eventLog.dir"]
    return None


def download_event_log(remote_dir, profile, dest):
    """Copy a DBFS/Volume event-log directory to local `dest`; return the path."""
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    _cli(["fs", "cp", "-r", "--overwrite", remote_dir, str(dest)], profile, parse_json=False)
    return dest


# --------------------------------------------------- SQL Statement Execution ---
def run_sql(statement, warehouse_id, profile, wait="50s", poll_timeout=300):
    """Execute one SQL statement on a warehouse; return list[dict] rows.

    Auto-starts a stopped warehouse. Polls if the statement outlives `wait`.
    """
    body = {"warehouse_id": warehouse_id, "wait_timeout": wait,
            "on_wait_timeout": "CONTINUE", "statement": statement}
    resp = _cli(["api", "post", "/api/2.0/sql/statements"], profile, json_body=body)
    statement_id = resp.get("statement_id")
    deadline = time.time() + poll_timeout
    while resp.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() > deadline:
            raise DatabricksError(f"SQL statement {statement_id} timed out")
        time.sleep(3)
        resp = _cli(["api", "get", f"/api/2.0/sql/statements/{statement_id}"], profile)
    state = resp.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise DatabricksError(f"SQL failed ({state}): {resp.get('status', {}).get('error')}")
    cols = [c["name"] for c in resp.get("manifest", {}).get("schema", {}).get("columns", [])]
    data = (resp.get("result") or {}).get("data_array") or []
    return [dict(zip(cols, row)) for row in data]


# ------------------------------------------------- system-table collectors ---
def job_run_timeline(run_id, warehouse_id, profile):
    return run_sql(
        "SELECT period_start_time, period_end_time, result_state, termination_code "
        f"FROM system.lakeflow.job_run_timeline WHERE run_id = {int(run_id)} "
        "ORDER BY period_start_time", warehouse_id, profile)


def node_timeline(cluster_id, warehouse_id, profile, driver_node_type=None, worker_node_type=None):
    """Per-instance uptime samples, with node-type vCPU/memory capacity resolved
    from the Jobs API cluster spec (driver/worker node types passed in).

    Returns rows shaped for measured C1/C2/C6:
    [{instance_id, is_driver, node_type, start_time, end_time, minutes,
      core_count, memory_mb, avg_cpu_percent}].
    Each node_timeline sample is a 1-minute bucket, so minute-count = uptime.
    """
    rows = run_sql(
        "SELECT instance_id, MIN(driver) AS is_driver, COUNT(*) AS minutes, "
        "MIN(start_time) AS t0, MAX(end_time) AS t1, "
        "ROUND(AVG(cpu_user_percent + cpu_system_percent), 2) AS avg_cpu "
        "FROM system.compute.node_timeline "
        f"WHERE cluster_id = '{cluster_id}' GROUP BY instance_id", warehouse_id, profile)
    if not rows:
        return []
    types = {r["node_type"]: r for r in node_types(warehouse_id, profile)}
    out = []
    for r in rows:
        is_driver = str(r.get("is_driver")).lower() == "true"
        ntype = (driver_node_type if is_driver else worker_node_type) or worker_node_type or driver_node_type
        cap = types.get(ntype, {})
        out.append({
            "instance_id": r["instance_id"], "is_driver": is_driver, "node_type": ntype,
            "start_time": r["t0"], "end_time": r["t1"], "minutes": int(r.get("minutes") or 0),
            "core_count": float(cap["core_count"]) if cap.get("core_count") else None,
            "memory_mb": float(cap["memory_mb"]) if cap.get("memory_mb") else None,
            "avg_cpu_percent": r.get("avg_cpu"),
        })
    return out


def node_types(warehouse_id, profile):
    return run_sql("SELECT node_type, core_count, memory_mb FROM system.compute.node_types",
                   warehouse_id, profile)


def billing_usage(run_id, warehouse_id, profile):
    return run_sql(
        "SELECT sku_name, usage_unit, ROUND(SUM(usage_quantity), 6) AS usage_quantity "
        f"FROM system.billing.usage WHERE usage_metadata.job_run_id = '{run_id}' "
        "GROUP BY sku_name, usage_unit", warehouse_id, profile)


def query_history(run_id, warehouse_id, profile):
    """Query-level read metrics for serverless/SQL runs. Empty for classic Spark
    jobs, whose equivalent numbers come from the event log. Best-effort: returns
    [] if the run has no query-history rows or the column set differs."""
    try:
        return run_sql(
            "SELECT statement_id, read_rows, read_bytes, total_task_duration_ms "
            "FROM system.query.history "
            f"WHERE query_source.job_info.job_run_id = '{run_id}'", warehouse_id, profile)
    except DatabricksError:
        return []
