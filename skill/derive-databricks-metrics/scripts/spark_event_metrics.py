"""Platform-agnostic Spark event-log engine.

Dataproc, EMR, HDInsight, Databricks, and on-prem Spark all emit identical
`SparkListener*` events. Every event-log-derived metric in this skill is
computed here, once, so the numbers are defined the same way for the source
platform and the target platform. Nothing in this module is Databricks- or
Dataproc-specific; it only understands the open Spark event-log format.
"""
import bz2
import gzip
import json
import lzma
from pathlib import Path


def open_event_file(path):
    suffix = path.suffix.lower()
    if suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    if suffix == ".bz2":
        return bz2.open(path, "rt", encoding="utf-8", errors="replace")
    if suffix in {".xz", ".lzma"}:
        return lzma.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("rt", encoding="utf-8", errors="replace")


def event_files(path):
    path = Path(path).expanduser()
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(f"Event log path does not exist: {path}")
    return sorted(
        candidate
        for candidate in path.rglob("*")
        if candidate.is_file() and candidate.stat().st_size
    )


def read_events(path):
    for source_file in event_files(path):
        with open_event_file(source_file) as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"Invalid JSON event in {source_file}:{line_number}"
                    ) from error


def nested(mapping, *keys, default=0):
    value = mapping
    for key in keys:
        if not isinstance(value, dict):
            return default
        value = value.get(key)
    return default if value is None else value


def summarize_events(events):
    """Fold a stream of Spark events into one portable summary.

    Every field below is defined identically regardless of which engine
    produced the log, which is what makes source/target numbers comparable.
    """
    summary = {
        "application_start_ms": None,
        "application_end_ms": None,
        "first_task_launch_ms": None,
        "last_task_finish_ms": None,
        "task_attempts": 0,
        "failed_task_attempts": 0,
        "speculative_task_attempts": 0,
        "stage_ids": set(),
        "executor_run_time_ms": 0,
        "executor_cpu_time_ns": 0,
        "jvm_gc_time_ms": 0,
        "input_records": 0,
        "input_bytes": 0,
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
        "shuffle_fetch_wait_ms": 0,
        "disk_bytes_spilled": 0,
        "memory_bytes_spilled": 0,
        "peak_execution_memory_bytes": 0,
        "output_records": 0,
        "output_bytes": 0,
        "executor_events": [],
        "sql_execution_starts": [],
        "sql_execution_ends": [],
    }
    for event in events:
        event_type = str(event.get("Event", ""))
        if event_type.endswith("SparkListenerApplicationStart"):
            summary["application_start_ms"] = event.get("Timestamp")
        elif event_type.endswith("SparkListenerApplicationEnd"):
            summary["application_end_ms"] = event.get("Timestamp")
        elif event_type.endswith("SparkListenerTaskEnd"):
            info = event.get("Task Info") or {}
            metrics = event.get("Task Metrics") or {}
            launch = info.get("Launch Time")
            finish = info.get("Finish Time")
            if launch is not None:
                current = summary["first_task_launch_ms"]
                summary["first_task_launch_ms"] = launch if current is None else min(current, launch)
            if finish is not None:
                current = summary["last_task_finish_ms"]
                summary["last_task_finish_ms"] = finish if current is None else max(current, finish)
            summary["task_attempts"] += 1
            summary["failed_task_attempts"] += int(bool(info.get("Failed")))
            summary["speculative_task_attempts"] += int(bool(info.get("Speculative")))
            summary["stage_ids"].add(event.get("Stage ID"))
            summary["executor_run_time_ms"] += nested(metrics, "Executor Run Time")
            summary["executor_cpu_time_ns"] += nested(metrics, "Executor CPU Time")
            summary["jvm_gc_time_ms"] += nested(metrics, "JVM GC Time")
            summary["input_records"] += nested(metrics, "Input Metrics", "Records Read")
            summary["input_bytes"] += nested(metrics, "Input Metrics", "Bytes Read")
            summary["shuffle_read_bytes"] += nested(metrics, "Shuffle Read Metrics", "Remote Bytes Read")
            summary["shuffle_read_bytes"] += nested(metrics, "Shuffle Read Metrics", "Local Bytes Read")
            summary["shuffle_write_bytes"] += nested(metrics, "Shuffle Write Metrics", "Shuffle Bytes Written")
            summary["shuffle_fetch_wait_ms"] += nested(metrics, "Shuffle Read Metrics", "Fetch Wait Time")
            summary["disk_bytes_spilled"] += nested(metrics, "Disk Bytes Spilled")
            summary["memory_bytes_spilled"] += nested(metrics, "Memory Bytes Spilled")
            summary["peak_execution_memory_bytes"] = max(
                summary["peak_execution_memory_bytes"], nested(metrics, "Peak Execution Memory")
            )
            summary["output_records"] += nested(metrics, "Output Metrics", "Records Written")
            summary["output_bytes"] += nested(metrics, "Output Metrics", "Bytes Written")
        elif event_type.endswith("SparkListenerExecutorAdded"):
            summary["executor_events"].append((event.get("Timestamp"), "added", str(event.get("Executor ID"))))
        elif event_type.endswith("SparkListenerExecutorRemoved"):
            summary["executor_events"].append((event.get("Timestamp"), "removed", str(event.get("Executor ID"))))
        elif event_type.endswith("SparkListenerSQLExecutionStart"):
            summary["sql_execution_starts"].append(event)
        elif event_type.endswith("SparkListenerSQLExecutionEnd"):
            summary["sql_execution_ends"].append(event)
    summary["stage_ids"].discard(None)
    summary["stage_count"] = len(summary.pop("stage_ids"))
    summary["peak_concurrent_executors"] = peak_concurrent_executors(summary["executor_events"])
    return summary


def peak_concurrent_executors(events):
    active = set()
    peak = 0
    for _, action, executor_id in sorted(events, key=lambda item: (item[0] or 0, item[1])):
        if executor_id == "driver":
            continue
        if action == "added":
            active.add(executor_id)
            peak = max(peak, len(active))
        else:
            active.discard(executor_id)
    return peak
