from datetime import date

import pytest

from scripts.spark_build_ads_satisfaction import build_satisfaction_daily
from scripts.spark_build_dws_feedback_daily_metrics import build_feedback_daily_metrics
from scripts.spark_transform_feedback_events import transform_feedback_events


def make_raw_feedback_events(spark):
    return spark.createDataFrame(
        [
            {
                "feedback_id": "fb_001",
                "trace_id": "trace_001",
                "request_id": "req_001",
                "run_id": "run_001",
                "session_id": "session_001",
                "conversation_id": "conv_001",
                "user_id": "user_001",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "agent_id": "agent_001",
                "feedback_type": "thumbs_up",
                "rating_value": None,
                "feedback_text_hash": "",
                "feedback_text_length": 0,
                "response_latency_ms": 800,
                "model_name": "deepseek-chat",
                "prompt_version": "v1",
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            },
            {
                "feedback_id": "fb_002",
                "trace_id": "trace_002",
                "request_id": "req_002",
                "run_id": "run_002",
                "session_id": "session_002",
                "conversation_id": "conv_002",
                "user_id": "user_002",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "agent_id": "agent_001",
                "feedback_type": "thumbs_down",
                "rating_value": None,
                "feedback_text_hash": "",
                "feedback_text_length": 0,
                "response_latency_ms": 1200,
                "model_name": "deepseek-chat",
                "prompt_version": "v1",
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:01:00+00:00",
                "date": "2026-01-01",
            },
            {
                "feedback_id": "fb_003",
                "trace_id": "trace_003",
                "request_id": "req_002",
                "run_id": "run_002",
                "session_id": "session_002",
                "conversation_id": "conv_002",
                "user_id": "user_002",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "agent_id": "agent_001",
                "feedback_type": "rating",
                "rating_value": 4,
                "feedback_text_hash": "",
                "feedback_text_length": 0,
                "response_latency_ms": 1200,
                "model_name": "deepseek-chat",
                "prompt_version": "v1",
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:02:00+00:00",
                "date": "2026-01-01",
            },
        ]
    )


def test_transform_feedback_events_casts_expected_types(spark):
    events = transform_feedback_events(make_raw_feedback_events(spark))
    schema = dict(events.dtypes)

    assert schema["rating_value"] == "int"
    assert schema["feedback_text_length"] == "int"
    assert schema["response_latency_ms"] == "int"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"


def test_build_feedback_daily_metrics_aggregates_actions(spark):
    row = build_feedback_daily_metrics(transform_feedback_events(make_raw_feedback_events(spark))).collect()[0]

    assert row["feedback_cnt_1d"] == 3
    assert row["thumbs_up_cnt_1d"] == 1
    assert row["thumbs_down_cnt_1d"] == 1
    assert row["avg_rating"] == 4.0
    assert row["rated_request_cnt_1d"] == 2


def test_build_satisfaction_daily_joins_request_count_and_flags(spark):
    feedback = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "agent_id": "agent_001",
                "feedback_cnt_1d": 4,
                "thumbs_up_cnt_1d": 1,
                "thumbs_down_cnt_1d": 1,
                "regenerate_cnt_1d": 2,
                "report_cnt_1d": 0,
                "avg_rating": 4.0,
                "rated_request_cnt_1d": 3,
            }
        ]
    )
    llm = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "model_name": "deepseek-chat",
                "request_count": 10,
            }
        ]
    )

    row = build_satisfaction_daily(feedback, llm).collect()[0]

    assert row["request_cnt_1d"] == 10
    assert row["satisfaction_rate_1d"] == 0.5
    assert row["regeneration_rate_1d"] == 0.2
    assert row["is_satisfaction_breach"] is True
    assert row["is_regeneration_breach"] is True
