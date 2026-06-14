from datetime import date, datetime

import pytest

from scripts.spark_build_dws_agent_daily_metrics import build_agent_daily_metrics


def test_build_agent_daily_metrics_aggregates_runs_and_spans(spark):
    runs = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "run_id": "run_001",
                "app_name": "ai_agent_platform",
                "agent_id": "agent_support",
                "agent_name": "customer_support_agent",
                "task_type": "customer_support",
                "status": "success",
                "turn_count": 1,
                "llm_call_count": 1,
                "tool_call_count": 1,
                "retrieval_count": 1,
                "total_tokens": 100,
                "estimated_cost_usd": 0.0001,
                "duration_ms": 100,
            },
            {
                "date": date(2026, 1, 1),
                "run_id": "run_002",
                "app_name": "ai_agent_platform",
                "agent_id": "agent_support",
                "agent_name": "customer_support_agent",
                "task_type": "customer_support",
                "status": "error",
                "turn_count": 2,
                "llm_call_count": 1,
                "tool_call_count": 0,
                "retrieval_count": 1,
                "total_tokens": 200,
                "estimated_cost_usd": 0.0002,
                "duration_ms": 300,
            },
        ]
    )
    spans = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "run_id": "run_001",
                "agent_id": "agent_support",
                "span_type": "llm_call",
                "status": "success",
            },
            {
                "date": date(2026, 1, 1),
                "run_id": "run_002",
                "agent_id": "agent_support",
                "span_type": "tool_call",
                "status": "error",
            },
        ]
    )

    row = build_agent_daily_metrics(runs, spans).collect()[0]

    assert row["run_count"] == 2
    assert row["success_count"] == 1
    assert row["error_count"] == 1
    assert row["turn_count"] == 3
    assert row["total_tokens"] == 300
    assert row["estimated_cost_usd"] == pytest.approx(0.0003)
    assert row["avg_duration_ms"] == 200.0
    assert row["p95_duration_ms"] == 300
    assert row["span_count"] == 2
    assert row["failed_span_count"] == 1
    assert "success_rate" not in row.asDict()
    assert "error_rate" not in row.asDict()
    assert "span_failure_rate" not in row.asDict()


def test_build_agent_daily_metrics_keeps_span_metrics_at_run_group_grain(spark):
    runs = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "run_id": "run_support",
                "app_name": "ai_agent_platform",
                "agent_id": "agent_shared",
                "agent_name": "shared_agent",
                "task_type": "customer_support",
                "status": "success",
                "turn_count": 1,
                "llm_call_count": 1,
                "tool_call_count": 1,
                "retrieval_count": 0,
                "total_tokens": 100,
                "estimated_cost_usd": 0.0001,
                "duration_ms": 100,
            },
            {
                "date": date(2026, 1, 1),
                "run_id": "run_research",
                "app_name": "ai_agent_platform",
                "agent_id": "agent_shared",
                "agent_name": "shared_agent",
                "task_type": "research",
                "status": "success",
                "turn_count": 1,
                "llm_call_count": 1,
                "tool_call_count": 0,
                "retrieval_count": 1,
                "total_tokens": 200,
                "estimated_cost_usd": 0.0002,
                "duration_ms": 200,
            },
        ]
    )
    spans = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "run_id": "run_support",
                "agent_id": "agent_shared",
                "span_type": "tool_call",
                "status": "error",
            },
            {
                "date": date(2026, 1, 1),
                "run_id": "run_research",
                "agent_id": "agent_shared",
                "span_type": "llm_call",
                "status": "success",
            },
        ]
    )

    rows = {
        row["task_type"]: row.asDict()
        for row in build_agent_daily_metrics(runs, spans).collect()
    }

    assert rows["customer_support"]["span_count"] == 1
    assert rows["customer_support"]["failed_span_count"] == 1
    assert rows["research"]["span_count"] == 1
    assert rows["research"]["failed_span_count"] == 0
