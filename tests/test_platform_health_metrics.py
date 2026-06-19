import json

from scripts.generate_mock_platform_health_logs import load_thresholds, write_jsonl
from scripts.spark_build_dws_platform_health_daily_metrics import (
    build_platform_health_daily_metrics,
    transform_platform_health_metrics,
)


def test_platform_health_thresholds_cover_all_components():
    thresholds = load_thresholds()

    assert set(thresholds) == {"kafka", "flink", "paimon", "doris"}
    assert thresholds["kafka"]["consumer_lag"] == 10000
    assert thresholds["flink"]["checkpoint_duration_ms"] == 30000


def test_build_platform_health_daily_metrics_uses_daily_max(spark):
    raw = spark.createDataFrame(
        [
            {
                "metric_event_id": "health_001",
                "component": "kafka",
                "metric_name": "consumer_lag",
                "metric_value": 9000.0,
                "threshold": 10000.0,
                "created_at": "2026-06-01T00:00:00+00:00",
                "date": "2026-06-01",
            },
            {
                "metric_event_id": "health_002",
                "component": "kafka",
                "metric_name": "consumer_lag",
                "metric_value": 12000.0,
                "threshold": 10000.0,
                "created_at": "2026-06-01T00:05:00+00:00",
                "date": "2026-06-01",
            },
        ]
    )

    row = build_platform_health_daily_metrics(transform_platform_health_metrics(raw)).collect()[0]
    assert row["metric_value"] == 12000.0
    assert row["threshold"] == 10000.0
    assert row["is_breach"] is True


def test_mock_platform_health_jsonl_covers_every_configured_metric(tmp_path):
    output_path = tmp_path / "health.jsonl"
    write_jsonl(2, output_path, seed=42)

    rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    configured_metric_count = sum(len(metrics) for metrics in load_thresholds().values())
    assert len(rows) == 2 * configured_metric_count
    assert {row["component"] for row in rows} == {"kafka", "flink", "paimon", "doris"}
