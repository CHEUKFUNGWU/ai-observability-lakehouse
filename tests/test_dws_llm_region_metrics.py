from datetime import date

import pytest

from scripts.spark_build_dws_llm_region_daily_metrics import build_llm_region_daily_metrics


def test_build_llm_region_daily_metrics_groups_by_region_and_environment(spark):
    events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "region": "us",
                "environment": "prod",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150,
                "latency_ms": 100,
                "status": "success",
                "estimated_cost_usd": 0.10,
            },
            {
                "date": date(2026, 1, 1),
                "region": "us",
                "environment": "prod",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "prompt_tokens": 200,
                "completion_tokens": 0,
                "total_tokens": 200,
                "latency_ms": 300,
                "status": "error",
                "estimated_cost_usd": 0.20,
            },
            {
                "date": date(2026, 1, 1),
                "region": "eu",
                "environment": "prod",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "prompt_tokens": 50,
                "completion_tokens": 25,
                "total_tokens": 75,
                "latency_ms": 500,
                "status": "success",
                "estimated_cost_usd": 0.05,
            },
        ]
    )

    rows = {
        row["region"]: row
        for row in build_llm_region_daily_metrics(events).collect()
    }

    assert set(rows) == {"us", "eu"}
    assert rows["us"]["request_cnt_1d"] == 2
    assert rows["us"]["success_cnt_1d"] == 1
    assert rows["us"]["error_cnt_1d"] == 1
    assert rows["us"]["total_token_cnt_1d"] == 350
    assert rows["us"]["estimated_cost_amt_1d"] == pytest.approx(0.30)
    assert rows["us"]["avg_latency_ms"] == 200.0
    assert rows["us"]["p95_latency_ms"] == 300
    assert rows["eu"]["request_cnt_1d"] == 1
