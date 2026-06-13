from datetime import date

from scripts.spark_build_ads_cost_anomaly import build_cost_anomaly_metrics
from scripts.spark_build_ads_prompt_version_metrics import build_prompt_version_metrics
from scripts.spark_build_ads_sla_daily import build_sla_daily_report


def test_build_cost_anomaly_metrics_flags_large_jumps(spark):
    metrics = spark.createDataFrame(
        [
            {"date": date(2026, 1, 1), "app_name": "app", "feature_name": "chat", "model_name": "deepseek-chat", "estimated_cost_usd": 1.0},
            {"date": date(2026, 1, 2), "app_name": "app", "feature_name": "chat", "model_name": "deepseek-chat", "estimated_cost_usd": 4.5},
        ]
    )

    rows = build_cost_anomaly_metrics(metrics).orderBy("date").collect()

    assert rows[0]["is_anomaly"] is False
    assert rows[1]["is_anomaly"] is True


def test_build_sla_daily_report_marks_breaches(spark):
    metrics = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "app_name": "app",
                "feature_name": "chat",
                "model_name": "deepseek-chat",
                "request_count": 10,
                "error_count": 1,
                "p95_latency_ms": 4000,
            }
        ]
    )

    report = build_sla_daily_report(
        metrics,
        [{"feature_name": "chat", "p95_latency_ms_max": 3000, "error_rate_max": 0.05}],
    ).collect()[0]

    assert report["is_latency_breach"] is True
    assert report["is_error_breach"] is True


def test_build_prompt_version_metrics_groups_by_prompt_version(spark):
    events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "latency_ms": 100,
                "status": "success",
                "estimated_cost_usd": 0.1,
            },
            {
                "date": date(2026, 1, 1),
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "latency_ms": 200,
                "status": "error",
                "estimated_cost_usd": 0.2,
            },
        ]
    )

    row = build_prompt_version_metrics(events).collect()[0]

    assert row["request_count"] == 2
    assert row["error_count"] == 1
    assert row["avg_latency_ms"] == 150.0
