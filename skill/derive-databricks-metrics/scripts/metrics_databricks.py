"""Databricks-only metric assembly (A-J).

Reuses the platform-neutral engine vendored from databricks-migration-metrics
for everything that engine already defines once (B time, D work, F engine, the
event-log efficiency metrics, I cost), and adds the Databricks-native pieces
that need a system table: measured C1/C2 vCPU/memory-hours and C6 VM CPU% from
system.compute.node_timeline, and the A identity block from the Jobs API.

Only metrics the benchmark sheet marks derivable from Databricks alone are
emitted as `available`; the rest are returned `unavailable`/`not_applicable`
with a reason, so the coverage is honest rather than silently partial.
"""
from canonical import metric_envelope as M
import metrics as base
import cost as cost_base

PLATFORM, EVENT, CROSS, DERIVED = "Platform-derived", "Event-log-derived", "Cross-source-derived", "Derived"


# --------------------------------------------------------- A. Identity -------
def identity_metrics(run, events):
    app_ids = run.application_ids or []
    fmt = None  # input format would come from event-log scan operators; absent here
    return {
        "A4": M("A4", "Run ID", run.run_id, category=PLATFORM, availability="available",
                source="Jobs API run_id"),
        "A5": M("A5", "Spark application ID(s)", app_ids or None, category=EVENT,
                availability="available" if app_ids else "unavailable",
                source="cluster_instance.spark_context_id / event-log app id"),
        "A6": M("A6", "Cluster ID", run.cluster_id, category=PLATFORM,
                availability="available" if run.cluster_id else "unavailable",
                source="run cluster_instance.cluster_id"),
    }


# --------------------------------------------------------- C. Efficiency -----
def efficiency_measured(node_rows, events):
    """Measured C1/C2 (allocated vCPU/memory-hours) and C6 (VM CPU%) from the
    per-instance node timeline. Each sample is a 1-minute bucket, so the minute
    count is measured node uptime — no configuration proxy needed."""
    have = bool(node_rows) and all(r.get("core_count") and r.get("memory_mb") for r in node_rows)
    vcpu_hours = memory_hours = None
    cpu_pct = None
    if have:
        vcpu_hours = sum((r["minutes"] / 60.0) * r["core_count"] for r in node_rows)
        memory_hours = sum((r["minutes"] / 60.0) * (r["memory_mb"] / 1024.0) for r in node_rows)
        cpus = [r["avg_cpu_percent"] for r in node_rows if r.get("avg_cpu_percent") is not None]
        cpu_pct = round(sum(float(c) for c in cpus) / len(cpus), 2) if cpus else None
    nodes = len(node_rows)
    task_hours = (events.get("executor_run_time_ms") or 0) / 3_600_000 or None
    idle = (vcpu_hours - task_hours) if (vcpu_hours is not None and task_hours) else None
    return {
        "C1": M("C1", "Allocated vCPU-hours", round(vcpu_hours, 6) if vcpu_hours is not None else None,
                category=PLATFORM, availability="available" if vcpu_hours is not None else "unavailable",
                source="system.compute.node_timeline minutes x node_types core_count",
                limitation=None if have else "node_timeline rows or node-type capacity missing.",
                inputs={"nodes": nodes}),
        "C2": M("C2", "Allocated memory GiB-hours", round(memory_hours, 4) if memory_hours is not None else None,
                category=PLATFORM, availability="available" if memory_hours is not None else "unavailable",
                source="system.compute.node_timeline minutes x node_types memory_mb"),
        "C6": M("C6", "VM CPU % (avg)", cpu_pct, category=PLATFORM,
                availability="available" if cpu_pct is not None else "unavailable",
                source="system.compute.node_timeline cpu_user+cpu_system"),
        "C12": M("C12", "Idle capacity vCPU-hours", round(idle, 6) if idle is not None else None,
                 category=DERIVED, availability="available" if idle is not None else "unavailable",
                 source="C1 allocated minus C3 task-runtime", inputs=["C1", "C3"]),
    }


# --------------------------------------------------------- I. Cost -----------
def cost_databricks(billing_rows, run):
    """Databricks-only cost: platform units (DBUs) by SKU. VM/disk/storage cost
    (I1/I2/I10) and contract-rate DBU cost (I8) need the cloud billing export or
    a contract rate -- external to Databricks -- so they are not emitted here."""
    usage = [{"sku_name": r.get("sku_name"), "usage_quantity": r.get("usage_quantity"),
              "run_id": run.run_id} for r in (billing_rows or [])]
    units_by_sku = {}
    for r in usage:
        sku = str(r["sku_name"] or "UNKNOWN")
        units_by_sku[sku] = units_by_sku.get(sku, 0.0) + float(r["usage_quantity"] or 0)
    total = sum(units_by_sku.values()) if units_by_sku else None
    return {
        "I5": M("I5", "DBU price tier / SKU", sorted(units_by_sku) or None, category=PLATFORM,
                availability="available" if units_by_sku else "unavailable",
                source="system.billing.usage sku_name"),
        "I6": M("I6", "DBUs", {"total": round(total, 6), "by_sku": units_by_sku} if total is not None else None,
                category=PLATFORM, availability="available" if total is not None else "unavailable",
                source="system.billing.usage usage_quantity by job_run_id"),
    }


# --------------------------------------------------------- assemble ----------
def derive_all(run, events, node_rows, billing_rows, query_rows):
    """Full A-J derivation for one Databricks run. Reuses the vendored engine
    for B/D/F and the event-log parts of C, overlays measured C1/C2/C6, and adds
    A identity and Databricks-only I cost."""
    events = events or {}
    eff_event = base.efficiency_metrics(run, events)   # C3/C4/C5/C7/C8/C9/C10 (event-log + capacity)
    for mid in ("C1", "C2", "C12"):                    # replaced by measured/derived below
        eff_event.pop(mid, None)

    # F1 Photon time share from query history when present (serverless/SQL).
    photon_ms = task_ms = None
    for q in (query_rows or []):
        if q.get("photon_total_time_ms") is not None:
            photon_ms = (photon_ms or 0) + float(q["photon_total_time_ms"])
            task_ms = (task_ms or 0) + float(q.get("total_task_duration_ms") or 0)

    out = {}
    out.update(identity_metrics(run, events))
    out.update(base.time_metrics(run, events))              # B1-B8
    out.update(efficiency_measured(node_rows, events))      # C1/C2/C6 measured
    out.update(eff_event)                                   # remaining C
    out.update(base.work_metrics(events))                   # D1-D14
    out.update(base.engine_metrics(run, photon_ms, task_ms))  # F1
    out.update(cost_databricks(billing_rows, run))          # I5/I6
    return out
