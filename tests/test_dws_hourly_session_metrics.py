from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from scripts.spark_build_dws_llm_feature_hourly_metrics import build_feature_hourly_metrics
from scripts.spark_build_dws_llm_session_daily_metrics import build_session_daily_metrics


def test_build_feature_hourly_metrics_groups_by_event_hour(spark):
    events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 5), "created_at": "2026-01-05 09:05:00",
                "app_name": "support", "feature_name": "chat", "model_name": "model-a",
                "status": "success", "prompt_tokens": 10, "completion_tokens": 5,
                "total_tokens": 15, "estimated_cost_usd": 0.1, "latency_ms": 100,
            },
            {
                "date": date(2026, 1, 5), "created_at": "2026-01-05 09:45:00",
                "app_name": "support", "feature_name": "chat", "model_name": "model-a",
                "status": "error", "prompt_tokens": 20, "completion_tokens": 0,
                "total_tokens": 20, "estimated_cost_usd": 0.2, "latency_ms": 300,
            },
            {
                "date": date(2026, 1, 5), "created_at": "2026-01-05 10:01:00",
                "app_name": "support", "feature_name": "chat", "model_name": "model-a",
                "status": "success", "prompt_tokens": 30, "completion_tokens": 10,
                "total_tokens": 40, "estimated_cost_usd": 0.3, "latency_ms": 500,
            },
        ]
    )

    timestamped_events = events.withColumn("created_at", F.to_timestamp("created_at"))
    rows = {row["hour"]: row for row in build_feature_hourly_metrics(timestamped_events).collect()}

    assert set(rows) == {9, 10}
    assert rows[9]["request_cnt_1h"] == 2
    assert rows[9]["success_cnt_1h"] == 1
    assert rows[9]["error_cnt_1h"] == 1
    assert rows[9]["total_token_cnt_1h"] == 35
    assert rows[9]["estimated_cost_amt_1h"] == pytest.approx(0.3)
    assert rows[9]["avg_latency_ms"] == 200.0
    assert rows[9]["p95_latency_ms"] == 300


def test_build_session_daily_metrics_aggregates_sessions_and_resolution(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 5), "app_name": "support", "feature_name": "chat",
                "session_id": "s1", "created_at": datetime(2026, 1, 5, 9, 0),
                "latency_ms": 100, "total_tokens": 100,
            },
            {
                "date": date(2026, 1, 5), "app_name": "support", "feature_name": "chat",
                "session_id": "s1", "created_at": datetime(2026, 1, 5, 9, 1),
                "latency_ms": 200, "total_tokens": 200,
            },
            {
                "date": date(2026, 1, 5), "app_name": "support", "feature_name": "chat",
                "session_id": "s2", "created_at": datetime(2026, 1, 5, 10, 0),
                "latency_ms": 300, "total_tokens": 100,
            },
        ]
    )
    feedback_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 5), "app_name": "support", "feature_name": "chat",
                "session_id": "s1", "feedback_type": "thumbs_up", "rating_value": None,
            },
            {
                "date": date(2026, 1, 5), "app_name": "support", "feature_name": "chat",
                "session_id": "s2", "feedback_type": "thumbs_down", "rating_value": None,
            },
        ],
        "date date, app_name string, feature_name string, session_id string, feedback_type string, rating_value int",
    )

    row = build_session_daily_metrics(llm_events, feedback_events).collect()[0]

    assert row["session_cnt_1d"] == 2
    assert row["avg_turns_per_session"] == 1.5
    assert row["avg_tokens_per_session"] == 200.0
    assert row["avg_duration_per_session_ms"] == 30250.0
    assert row["resolved_session_cnt_1d"] == 1
