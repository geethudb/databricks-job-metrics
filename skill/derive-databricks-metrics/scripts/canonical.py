"""The canonical run descriptor and metric envelope.

A *run descriptor* is the platform-neutral shape that every platform adapter
(Databricks, Dataproc, EMR, ...) must produce from its own control-plane JSON.
Collectors only ever read this shape, so one collector body serves every
platform and source/target numbers are directly comparable.

The event-log-derived numbers do NOT live here; they come from
`spark_event_metrics.summarize_events`. This descriptor carries only what the
*control plane* knows: identity, wall-clock boundaries, and provisioned
capacity.
"""
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class Capacity:
    # Provisioned (not necessarily used) compute. Fill what the platform exposes.
    driver_vcpus: Optional[float] = None
    driver_memory_gib: Optional[float] = None
    worker_count: Optional[int] = None
    worker_vcpus: Optional[float] = None
    worker_memory_gib: Optional[float] = None
    # Per-executor settings, used for task-slot and memory-per-slot math.
    executor_cores: Optional[float] = None
    executor_memory_gib: Optional[float] = None
    executor_off_heap_gib: float = 0.0


@dataclass
class RunDescriptor:
    platform: str                       # "databricks", "dataproc", "emr", ...
    run_id: str
    job_id: Optional[str] = None
    cluster_id: Optional[str] = None
    workspace_or_project: Optional[str] = None
    application_ids: list = field(default_factory=list)

    # Control-plane wall-clock boundaries, epoch milliseconds UTC.
    submit_time_ms: Optional[int] = None     # external orchestrator submit, if known
    start_time_ms: Optional[int] = None      # platform-recorded start
    end_time_ms: Optional[int] = None
    setup_duration_ms: Optional[int] = None   # proxy for startup, if the platform reports it
    execution_duration_ms: Optional[int] = None
    cleanup_duration_ms: Optional[int] = None

    # Lifecycle.
    result_state: Optional[str] = None       # normalized: SUCCESS | FAILED | CANCELLED | UNKNOWN
    failure_reason: Optional[str] = None
    attempts: int = 1

    # Compute shape and engine.
    capacity: Capacity = field(default_factory=Capacity)
    runtime_engine: str = "STANDARD"         # STANDARD | PHOTON | NATIVE | ...
    runtime_version: Optional[str] = None
    spark_version: Optional[str] = None

    # Evidence pointers (not materialized here).
    event_log_uris: list = field(default_factory=list)
    node_timeline: Optional[list] = None     # measured [{start_time,end_time,core_count,memory_mb}]
    evidence_link: Optional[str] = None

    def to_dict(self):
        data = asdict(self)
        return data

    @classmethod
    def from_dict(cls, data):
        data = dict(data)
        cap = data.pop("capacity", {}) or {}
        descriptor = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        descriptor.capacity = Capacity(**{k: v for k, v in cap.items() if k in Capacity.__dataclass_fields__})
        return descriptor


RESULT_STATES = {"SUCCESS", "FAILED", "CANCELLED", "UNKNOWN"}


def normalize_result_state(raw):
    """Map a platform-specific terminal state to the canonical vocabulary."""
    if raw is None:
        return "UNKNOWN"
    text = str(raw).strip().upper()
    success = {"SUCCESS", "SUCCEEDED", "DONE", "COMPLETED", "FINISHED"}
    failed = {"FAILED", "ERROR", "INTERNAL_ERROR"}
    cancelled = {"CANCELLED", "CANCELED", "KILLED", "TIMEDOUT", "ABORTED"}
    if text in success:
        return "SUCCESS"
    if text in failed:
        return "FAILED"
    if text in cancelled:
        return "CANCELLED"
    return "UNKNOWN"


def metric_envelope(metric_id, name, value, *, category, availability,
                    source, evidence_reason=None, limitation=None, inputs=None,
                    cross_sources=None):
    """Uniform output shape for every metric, source or target.

    Carrying provenance (category/source/limitation) on every metric is what
    lets a reviewer trust a cross-platform comparison instead of a bare number.
    """
    return {
        "metric_id": metric_id,
        "metric": name,
        "value": value,
        "evidence_category": category,
        "evidence_reason": evidence_reason,
        "availability": availability,     # available | unavailable | not_applicable
        "source": source,
        "inputs": inputs,
        "cross_sources": cross_sources,
        "limitation": limitation,
    }
