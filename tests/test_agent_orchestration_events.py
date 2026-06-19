from scripts.generate_mock_orchestration_logs import write_jsonl
from scripts.spark_build_dws_agent_orchestration_daily_metrics import (
    build_agent_orchestration_daily_metrics,
)
from scripts.spark_transform_agent_orchestration_events import (
    transform_agent_orchestration_events,
)


def make_raw_orchestration_events(spark):
    base = {
        "trace_id": "trace_001",
        "parent_run_id": "run_parent",
        "child_run_id": "run_child",
        "parent_agent_id": "planner",
        "child_agent_id": "researcher",
        "handoff_type": "delegate",
        "payload_size": 256,
        "created_at": "2026-06-01T00:00:00+00:00",
        "date": "2026-06-01",
    }
    return spark.createDataFrame(
        [
            {**base, "orchestration_id": "orch_001", "handoff_latency_ms": 100, "status": "success"},
            {**base, "orchestration_id": "orch_002", "handoff_latency_ms": 200, "status": "error"},
            {**base, "orchestration_id": "orch_003", "handoff_latency_ms": 300, "status": "timeout"},
        ]
    )


def test_transform_agent_orchestration_events_casts_expected_types(spark):
    events = transform_agent_orchestration_events(make_raw_orchestration_events(spark))
    schema = dict(events.dtypes)

    assert schema["payload_size"] == "int"
    assert schema["handoff_latency_ms"] == "int"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"


def test_build_agent_orchestration_daily_metrics_aggregates_statuses(spark):
    events = transform_agent_orchestration_events(make_raw_orchestration_events(spark))
    row = build_agent_orchestration_daily_metrics(events).collect()[0]

    assert row["handoff_cnt_1d"] == 3
    assert row["success_cnt_1d"] == 1
    assert row["error_cnt_1d"] == 1
    assert row["timeout_cnt_1d"] == 1
    assert row["avg_handoff_latency_ms"] == 200.0
    assert row["p95_handoff_latency_ms"] == 300


def test_mock_orchestration_jsonl_writes_expected_rows(tmp_path):
    output_path = tmp_path / "orchestration.jsonl"
    write_jsonl(4, output_path, seed=42)

    rows = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) == 4
    assert "orchestration_id" in rows[0]
