import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

import metrics_databricks as mdb  # noqa: E402
from canonical import Capacity, RunDescriptor  # noqa: E402
from spark_event_metrics import summarize_events  # noqa: E402


def _events():
    return [
        {"Event": "SparkListenerApplicationStart", "Timestamp": 1000},
        {"Event": "SparkListenerExecutorAdded", "Timestamp": 1100, "Executor ID": "1"},
        {"Event": "SparkListenerTaskEnd", "Stage ID": 0,
         "Task Info": {"Launch Time": 1200, "Finish Time": 1700},
         "Task Metrics": {"Executor Run Time": 3_600_000, "Executor CPU Time": 1_800_000_000_000,
                          "Input Metrics": {"Records Read": 100, "Bytes Read": 2048}}},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 2500},
    ]


def _node_rows():
    # 2 nodes, 60 one-minute samples each, 4 cores / 31232 MB -> 8 vCPU-hours.
    return [
        {"instance_id": "i-d", "is_driver": True, "node_type": "i3.xlarge", "minutes": 60,
         "core_count": 4.0, "memory_mb": 31232.0, "avg_cpu_percent": 10.0,
         "start_time": "t0", "end_time": "t1"},
        {"instance_id": "i-w", "is_driver": False, "node_type": "i3.xlarge", "minutes": 60,
         "core_count": 4.0, "memory_mb": 31232.0, "avg_cpu_percent": 20.0,
         "start_time": "t0", "end_time": "t1"},
    ]


def test_measured_c1_c2_c6_c12():
    events = summarize_events(_events())
    c = mdb.efficiency_measured(_node_rows(), events)
    assert c["C1"]["value"] == 8.0                 # 2 nodes × 1h × 4 cores
    assert round(c["C2"]["value"], 1) == 61.0      # 2 × 1h × 30.5 GiB
    assert c["C6"]["value"] == 15.0                # mean of 10 and 20
    assert c["C1"]["evidence_category"] == "Platform-derived"
    # C12 = C1 (8.0) − C3 task-hours (3_600_000ms = 1.0h) = 7.0
    assert c["C12"]["value"] == 7.0
    assert c["C12"]["evidence_category"] == "Derived"


def test_c1_unavailable_without_capacity():
    rows = [{"instance_id": "x", "is_driver": True, "node_type": None, "minutes": 10,
             "core_count": None, "memory_mb": None, "avg_cpu_percent": None,
             "start_time": "t0", "end_time": "t1"}]
    c = mdb.efficiency_measured(rows, {})
    assert c["C1"]["availability"] == "unavailable"


def test_cost_databricks_dbus_only():
    run = RunDescriptor(platform="databricks", run_id="1")
    billing = [{"sku_name": "ENTERPRISE_JOBS_COMPUTE_(PHOTON)", "usage_quantity": 1.5}]
    i = mdb.cost_databricks(billing, run)
    assert i["I6"]["value"]["total"] == 1.5
    assert i["I5"]["value"] == ["ENTERPRISE_JOBS_COMPUTE_(PHOTON)"]


def test_derive_all_photon_engine_without_profile_is_unavailable():
    run = RunDescriptor(platform="databricks", run_id="1", start_time_ms=1000, end_time_ms=2500,
                        runtime_engine="PHOTON", capacity=Capacity())
    out = mdb.derive_all(run, summarize_events(_events()), _node_rows(), [], [])
    assert out["F1"]["availability"] == "unavailable"           # engine label alone is not proof
    assert "PHOTON" in out["F1"]["source"]
    assert out["A4"]["value"] == "1"
    assert out["D1"]["value"] == 100
