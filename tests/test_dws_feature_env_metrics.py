from datetime import date, datetime

import pytest

from scripts.spark_build_dws_llm_feature_env_daily_metrics import build_feature_env_daily_metrics


def test_build_feature_env_daily_metrics_groups_by_environment(spark):
    events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "model_name": "deepseek-chat",
                "environment": "prod",
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150,
                "latency_ms": 100,
                "status": "success",
                "estimated_cost_usd": 0.10,
                "created_at": datetime(2026, 1, 1, 0, 0, 0),
            },
            {
                "date": date(2026, 1, 1),
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "model_name": "deepseek-chat",
                "environment": "prod",
                "prompt_tokens": 200,
                "completion_tokens": 0,
                "total_tokens": 200,
                "latency_ms": 300,
                "status": "error",
                "estimated_cost_usd": 0.20,
                "created_at": datetime(2026, 1, 1, 0, 1, 0),
            },
            {
                "date": date(2026, 1, 1),
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "model_name": "deepseek-chat",
                "environment": "staging",
                "prompt_tokens": 50,
                "completion_tokens": 25,
                "total_tokens": 75,
                "latency_ms": 900,
                "status": "success",
                "estimated_cost_usd": 0.05,
                "created_at": datetime(2026, 1, 1, 0, 2, 0),
            },
        ]
    )

    rows = {
        row["environment"]: row
        for row in build_feature_env_daily_metrics(events).collect()
    }

    assert set(rows) == {"prod", "staging"}
    assert rows["prod"]["request_cnt_1d"] == 2
    assert rows["prod"]["success_cnt_1d"] == 1
    assert rows["prod"]["error_cnt_1d"] == 1
    assert rows["prod"]["total_token_cnt_1d"] == 350
    assert rows["prod"]["estimated_cost_amt_1d"] == pytest.approx(0.30)
    assert rows["prod"]["avg_latency_ms"] == 200.0
    assert rows["prod"]["p95_latency_ms"] == 300
    assert rows["staging"]["request_cnt_1d"] == 1
