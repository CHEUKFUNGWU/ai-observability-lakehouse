from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from scripts.spark_build_ads_llm_feature_daily_metrics import build_feature_daily_metrics


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-ads-feature-daily-metrics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session

    session.stop()


def make_dwd_events(spark):
    return spark.createDataFrame(
        [
            {
                "request_id": "req_001",
                "user_id": "user_001",
                "session_id": "session_001",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "prompt_category": "support",
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "provider": "deepseek",
                "prompt_text": "hello",
                "response_text": "hi",
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150,
                "latency_ms": 100,
                "status": "success",
                "error_type": "",
                "http_status": 200,
                "estimated_cost_usd": 0.0001,
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": datetime(2026, 1, 1, 0, 0, 0),
                "date": date(2026, 1, 1),
            },
            {
                "request_id": "req_002",
                "user_id": "user_002",
                "session_id": "session_002",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "prompt_category": "support",
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "provider": "deepseek",
                "prompt_text": "hello",
                "response_text": "",
                "prompt_tokens": 200,
                "completion_tokens": 0,
                "total_tokens": 200,
                "latency_ms": 300,
                "status": "error",
                "error_type": "timeout",
                "http_status": 500,
                "estimated_cost_usd": 0.0002,
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": datetime(2026, 1, 1, 0, 1, 0),
                "date": date(2026, 1, 1),
            },
            {
                "request_id": "req_003",
                "user_id": "user_003",
                "session_id": "session_003",
                "app_name": "ai_support_bot",
                "feature_name": "summary",
                "prompt_category": "support",
                "prompt_id": "prompt_002",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "provider": "deepseek",
                "prompt_text": "summarize",
                "response_text": "summary",
                "prompt_tokens": 300,
                "completion_tokens": 100,
                "total_tokens": 400,
                "latency_ms": 900,
                "status": "success",
                "error_type": "",
                "http_status": 200,
                "estimated_cost_usd": 0.0004,
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": datetime(2026, 1, 1, 0, 2, 0),
                "date": date(2026, 1, 1),
            },
        ]
    )


def test_build_feature_daily_metrics_groups_by_daily_feature_model(spark):
    events = make_dwd_events(spark)

    metrics = build_feature_daily_metrics(events)

    assert metrics.count() == 2


def test_build_feature_daily_metrics_aggregates_counts_and_rates(spark):
    events = make_dwd_events(spark)

    rows = {
        row["feature_name"]: row
        for row in build_feature_daily_metrics(events).collect()
    }

    chat = rows["chat"]

    assert chat["request_count"] == 2
    assert chat["success_count"] == 1
    assert chat["error_count"] == 1
    assert chat["success_rate"] == 0.5
    assert chat["error_rate"] == 0.5


def test_build_feature_daily_metrics_aggregates_tokens_and_cost(spark):
    events = make_dwd_events(spark)

    rows = {
        row["feature_name"]: row
        for row in build_feature_daily_metrics(events).collect()
    }

    chat = rows["chat"]

    assert chat["prompt_tokens"] == 300
    assert chat["completion_tokens"] == 50
    assert chat["total_tokens"] == 350
    assert chat["estimated_cost_usd"] == pytest.approx(0.0003)


def test_build_feature_daily_metrics_aggregates_latency(spark):
    events = make_dwd_events(spark)

    rows = {
        row["feature_name"]: row
        for row in build_feature_daily_metrics(events).collect()
    }

    chat = rows["chat"]

    assert chat["avg_latency_ms"] == 200.0
    assert chat["p95_latency_ms"] == 300