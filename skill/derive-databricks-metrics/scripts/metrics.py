"""Platform-neutral metric calculations.

Every function takes a canonical `RunDescriptor` and/or a Spark event summary
and returns metric envelopes. There is exactly one definition per metric, so a
Dataproc run and a Databricks run are measured by the same code — which is the
entire point of a migration benchmark.

Prefer measured evidence over proxies, and emit `null` (never 0) when the
required evidence is absent.
"""
from canonical import metric_envelope as M

EVENT = "Event-log-derived"
PLATFORM = "Platform-derived"
DERIVED = "Derived"
CROSS = "Cross-source-derived"
EXTERNAL = "External evidence"


def _secs(end_ms, start_ms):
    if end_ms is None or start_ms is None:
        return None
    return max(0, end_ms - start_ms) / 1000


# ---------------------------------------------------------------- B. Time ----
def time_metrics(run, events=None):
    events = events or {}
    first = events.get("first_task_launch_ms")
    last = events.get("last_task_finish_ms")
    submit = run.submit_time_ms if run.submit_time_ms is not None else run.start_time_ms
    exact = first is not None and last is not None
    startup = (
        _secs(first, submit) if first is not None
        else (run.setup_duration_ms or 0) / 1000 if run.setup_duration_ms is not None else None
    )
    compute = (
        _secs(last, first) if exact
        else (run.execution_duration_ms or 0) / 1000 if run.execution_duration_ms is not None else None
    )
    return {
        "B1": M("B1", "Submit/start time (ms)", submit, category=PLATFORM,
                availability="available" if submit is not None else "unavailable",
                source="external submit" if run.submit_time_ms is not None else "platform start-time proxy"),
        "B2": M("B2", "Scheduler delay (s)", _secs(run.start_time_ms, run.submit_time_ms),
                category=CROSS, availability="available" if run.submit_time_ms is not None else "unavailable",
                source="orchestrator submit vs platform start",
                limitation=None if run.submit_time_ms is not None else "Requires an external orchestrator submit timestamp."),
        "B3": M("B3", "Startup (s)", startup, category=CROSS if first is not None else PLATFORM,
                availability="available" if startup is not None else "unavailable",
                source="submit to first Spark task" if first is not None else "platform setup-duration proxy"),
        "B4": M("B4", "Compute (s)", compute, category=EVENT if exact else PLATFORM,
                availability="available" if compute is not None else "unavailable",
                source="first task to last task" if exact else "platform execution-duration proxy"),
        "B5": M("B5", "Commit tail (s)", _secs(run.end_time_ms, last), category=CROSS,
                availability="available" if (run.end_time_ms and last) else "unavailable",
                source="platform end minus last Spark task"),
        "B6": M("B6", "End-to-end (s)", _secs(run.end_time_ms, submit), category=PLATFORM,
                availability="available" if (run.end_time_ms and submit) else "unavailable",
                source="platform end minus submit/start"),
        "B7": M("B7", "Attempts", run.attempts, category=PLATFORM, availability="available",
                source="control-plane task attempts"),
        "B8": M("B8", "Result / failure", {"state": run.result_state, "reason": run.failure_reason},
                category=PLATFORM, availability="available", source="control-plane terminal state"),
    }


# ---------------------------------------------------- C. Efficiency ----------
def efficiency_metrics(run, events=None):
    events = events or {}
    cap = run.capacity
    cluster_seconds = _secs(run.end_time_ms, run.start_time_ms)

    # C1/C2: prefer measured node timeline; else configuration upper-bound proxy.
    if run.node_timeline:
        import datetime as dt

        def ts(v):
            if isinstance(v, (int, float)):
                return dt.datetime.fromtimestamp(v / 1000, dt.timezone.utc)
            return dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))

        start = ts(run.start_time_ms)
        end = ts(run.end_time_ms)
        vcpu_hours = memory_hours = 0.0
        for row in run.node_timeline:
            node_start = max(start, ts(row["start_time"]))
            node_end = min(end, ts(row.get("end_time") or run.end_time_ms))
            node_s = max(0.0, (node_end - node_start).total_seconds())
            vcpu_hours += (node_s / 3600) * float(row["core_count"])
            memory_hours += (node_s / 3600) * (float(row["memory_mb"]) / 1024)
        c1_method, c1_limit = "node_timeline", None
    elif cluster_seconds is not None and None not in (cap.driver_vcpus, cap.worker_count, cap.worker_vcpus):
        hours = cluster_seconds / 3600
        vcpu_hours = hours * (cap.driver_vcpus or 0) + hours * (cap.worker_count or 0) * (cap.worker_vcpus or 0)
        memory_hours = hours * (cap.driver_memory_gib or 0) + hours * (cap.worker_count or 0) * (cap.worker_memory_gib or 0)
        c1_method = "configuration_upper_bound_proxy"
        c1_limit = "No node timeline supplied; configuration-based upper-bound proxy used."
        if cluster_seconds < 600:
            c1_limit += " Run under 600s: short-lived nodes may be omitted from measured timelines."
    else:
        vcpu_hours = memory_hours = None
        c1_method, c1_limit = "unavailable", "Supply node timeline or full capacity to compute C1/C2."

    run_ms = events.get("executor_run_time_ms", 0)
    cpu_ns = events.get("executor_cpu_time_ns", 0)
    gc_ms = events.get("jvm_gc_time_ms", 0)
    peak_exec = events.get("peak_concurrent_executors")
    task_hours = run_ms / 3_600_000 if run_ms else None

    return {
        "C1": M("C1", "Allocated vCPU-hours", vcpu_hours, category=PLATFORM if c1_method == "node_timeline" else CROSS,
                availability="available" if vcpu_hours is not None else "unavailable",
                source=c1_method, limitation=c1_limit),
        "C2": M("C2", "Allocated memory GiB-hours", memory_hours, category=PLATFORM if c1_method == "node_timeline" else CROSS,
                availability="available" if memory_hours is not None else "unavailable", source=c1_method, limitation=c1_limit),
        "C3": M("C3", "Task-time core-hours", task_hours, category=EVENT,
                availability="available" if task_hours is not None else "unavailable", source="sum executor run time"),
        "C4": M("C4", "Task CPU-hours", cpu_ns / 3_600_000_000_000 if cpu_ns else None, category=EVENT,
                availability="available" if cpu_ns else "unavailable", source="sum executor CPU time"),
        "C5": M("C5", "CPU/runtime ratio", cpu_ns / (run_ms * 1_000_000) if run_ms and cpu_ns else None,
                category=EVENT, availability="available" if run_ms and cpu_ns else "unavailable", source="event log"),
        "C7": M("C7", "GC time %", 100 * gc_ms / run_ms if run_ms else None, category=EVENT,
                availability="available" if run_ms else "unavailable", source="event log"),
        "C8": M("C8", "Task slots", cap.executor_cores * peak_exec if cap.executor_cores and peak_exec else None,
                category=EVENT, availability="available" if cap.executor_cores and peak_exec else "unavailable",
                source="executor cores × peak concurrent executors"),
        "C9": M("C9", "Memory per slot (GiB)",
                (cap.executor_memory_gib + cap.executor_off_heap_gib) / cap.executor_cores
                if cap.executor_memory_gib is not None and cap.executor_cores else None,
                category=EXTERNAL, availability="available" if cap.executor_memory_gib and cap.executor_cores else "unavailable",
                source="(executor memory + off-heap) / executor cores"),
        "C10": M("C10", "Peak concurrent executors", peak_exec, category=EVENT,
                 availability="available" if peak_exec is not None else "unavailable", source="executor add/remove events"),
        "C12": M("C12", "Idle capacity vCPU-hours", vcpu_hours - task_hours if vcpu_hours is not None and task_hours is not None else None,
                 category=DERIVED, availability="available" if vcpu_hours is not None and task_hours is not None else "unavailable",
                 source="C1 allocated minus C3 task-runtime", inputs=["C1", "C3"]),
    }


# ---------------------------------------------------- D. Work done -----------
def work_metrics(events):
    events = events or {}
    spill_disk = events.get("disk_bytes_spilled")
    spill_mem = events.get("memory_bytes_spilled")
    return {
        "D1": M("D1", "Input records", events.get("input_records"), category=EVENT, availability="available", source="task input metrics"),
        "D2": M("D2", "Input bytes", events.get("input_bytes"), category=EVENT, availability="available", source="task input metrics"),
        "D4": M("D4", "Shuffle write bytes", events.get("shuffle_write_bytes"), category=EVENT, availability="available", source="task shuffle metrics"),
        "D5": M("D5", "Shuffle read bytes", events.get("shuffle_read_bytes"), category=EVENT, availability="available", source="task shuffle metrics"),
        "D6": M("D6", "Shuffle fetch wait (ms)", events.get("shuffle_fetch_wait_ms"), category=EVENT, availability="available", source="task shuffle metrics"),
        "D7": M("D7", "Disk spill bytes", spill_disk, category=EVENT, availability="available", source="task metrics"),
        "D8": M("D8", "Memory spill bytes", spill_mem, category=EVENT, availability="available", source="task metrics"),
        "D9": M("D9", "Peak task execution memory", events.get("peak_execution_memory_bytes"), category=EVENT, availability="available", source="task metrics"),
        "D12": M("D12", "Stage count", events.get("stage_count"), category=EVENT, availability="available", source="distinct stage ids"),
        "D13": M("D13", "Failed/speculative task attempts",
                 {"failed": events.get("failed_task_attempts"), "speculative": events.get("speculative_task_attempts")},
                 category=EVENT, availability="available", source="task end events"),
        "D14": M("D14", "Output records / bytes",
                 {"records": events.get("output_records"), "bytes": events.get("output_bytes")},
                 category=EVENT, availability="available", source="task output metrics"),
    }


# ---------------------------------------------------- F. Engine accel --------
def engine_metrics(run, photon_total_time_ms=None, task_total_time_ms=None):
    """Generalized target-engine acceleration (Photon on Databricks, native
    engines elsewhere). A STANDARD engine is exactly 0% accelerated time."""
    engine = (run.runtime_engine or "STANDARD").upper()
    if engine == "STANDARD":
        share = 0.0
        avail, limit = "available", None
    elif photon_total_time_ms is not None and task_total_time_ms:
        share = photon_total_time_ms / task_total_time_ms
        avail, limit = "available", None
    else:
        share = None
        avail, limit = "unavailable", "Accelerated engine needs operator/query-profile timing to prove time share."
    return {
        "F1": M("F1", "Accelerated-engine time share", share, category=PLATFORM, availability=avail,
                source=f"engine={engine}", limitation=limit),
    }
