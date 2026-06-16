from datetime import date

import pytest

from scripts.spark_build_dws_agent_team_daily_metrics import build_agent_team_daily_metrics


def test_build_agent_team_daily_metrics_joins_user_team_and_spans(spark):
    runs = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "run_id": "run_001",
                "user_id": "user_001",
                "app_name": "ai_agent_platform",
                "agent_id": "agent_support",
                "agent_name": "support_agent",
                "task_type": "customer_support",
                "status": "success",
                "turn_count": 1,
                "llm_call_count": 1,
                "tool_call_count": 1,
                "retrieval_count": 0,
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
                "duration_ms": 100,
            },
            {
                "date": date(2026, 1, 1),
                "run_id": "run_002",
                "user_id": "user_002",
                "app_name": "ai_agent_platform",
                "agent_id": "agent_support",
                "agent_name": "support_agent",
                "task_type": "customer_support",
                "status": "error",
                "turn_count": 2,
                "llm_call_count": 1,
                "tool_call_count": 0,
                "retrieval_count": 1,
                "total_tokens": 200,
                "estimated_cost_usd": 0.20,
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
    users = spark.createDataFrame(
        [
            {"user_id": "user_001", "team_id": "team_support"},
            {"user_id": "user_002", "team_id": "team_support"},
        ]
    )

    row = build_agent_team_daily_metrics(runs, spans, users).collect()[0]

    assert row["team_id"] == "team_support"
    assert row["run_cnt_1d"] == 2
    assert row["success_cnt_1d"] == 1
    assert row["error_cnt_1d"] == 1
    assert row["turn_cnt_1d"] == 3
    assert row["retrieval_cnt_1d"] == 1
    assert row["total_token_cnt_1d"] == 300
    assert row["estimated_cost_amt_1d"] == pytest.approx(0.30)
    assert row["avg_duration_ms"] == 200.0
    assert row["p95_duration_ms"] == 300
    assert row["span_cnt_1d"] == 2
    assert row["failed_span_cnt_1d"] == 1


def test_build_agent_team_daily_metrics_uses_unknown_team_for_missing_user(spark):
    runs = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "run_id": "run_001",
                "user_id": "missing_user",
                "app_name": "ai_agent_platform",
                "agent_id": "agent_support",
                "agent_name": "support_agent",
                "task_type": "customer_support",
                "status": "success",
                "turn_count": 1,
                "llm_call_count": 1,
                "tool_call_count": 0,
                "retrieval_count": 0,
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
                "duration_ms": 100,
            }
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
            }
        ]
    )
    users = spark.createDataFrame([{"user_id": "user_001", "team_id": "team_support"}])

    row = build_agent_team_daily_metrics(runs, spans, users).collect()[0]

    assert row["team_id"] == "unknown"
    assert row["run_cnt_1d"] == 1
