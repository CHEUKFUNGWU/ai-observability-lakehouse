from datetime import date

from scripts.spark_build_ads_cost_anomaly import build_cost_anomaly_metrics
from scripts.spark_build_ads_prompt_version_metrics import build_prompt_version_comparison, build_prompt_version_metrics
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


def test_build_prompt_version_metrics_projects_dws_prompt_version_metrics(spark):
    metrics = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "request_cnt_1d": 2,
                "success_cnt_1d": 1,
                "error_cnt_1d": 1,
                "total_token_cnt_1d": 300,
                "estimated_cost_amt_1d": 0.3,
                "avg_latency_ms": 150.0,
                "p95_latency_ms": 200,
                "evaluation_cnt_1d": 2,
                "pass_cnt_1d": 1,
                "fail_cnt_1d": 1,
                "evaluation_score_num_1d": 1.4,
                "evaluation_score_den_1d": 2,
                "avg_evaluation_score": 0.7,
                "metadata_conflict_cnt_1d": 0,
            },
        ]
    )

    row = build_prompt_version_metrics(metrics).collect()[0]

    assert row["request_count"] == 2
    assert row["success_count"] == 1
    assert row["error_count"] == 1
    assert row["total_tokens"] == 300
    assert row["avg_latency_ms"] == 150.0
    assert row["evaluation_count"] == 2
    assert row["pass_count"] == 1
    assert row["evaluation_score_numerator"] == 1.4
    assert row["evaluation_score_denominator"] == 2


def test_build_prompt_version_comparison_derives_rates_from_summed_numerators(spark):
    metrics = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "request_cnt_1d": 1,
                "success_cnt_1d": 1,
                "error_cnt_1d": 0,
                "total_token_cnt_1d": 100,
                "estimated_cost_amt_1d": 0.1,
                "avg_latency_ms": 100.0,
                "p95_latency_ms": 100,
                "evaluation_cnt_1d": 1,
                "pass_cnt_1d": 1,
                "fail_cnt_1d": 0,
                "evaluation_score_num_1d": 1.0,
                "evaluation_score_den_1d": 1,
                "avg_evaluation_score": 1.0,
                "metadata_conflict_cnt_1d": 0,
            },
            {
                "date": date(2026, 1, 2),
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "request_cnt_1d": 9,
                "success_cnt_1d": 0,
                "error_cnt_1d": 9,
                "total_token_cnt_1d": 900,
                "estimated_cost_amt_1d": 0.9,
                "avg_latency_ms": 300.0,
                "p95_latency_ms": 300,
                "evaluation_cnt_1d": 9,
                "pass_cnt_1d": 0,
                "fail_cnt_1d": 9,
                "evaluation_score_num_1d": 0.0,
                "evaluation_score_den_1d": 9,
                "avg_evaluation_score": 0.0,
                "metadata_conflict_cnt_1d": 0,
            },
        ]
    )

    row = build_prompt_version_comparison(metrics).collect()[0]

    assert row["request_count"] == 10
    assert row["success_rate"] == 0.1
    assert row["error_rate"] == 0.9
    assert row["pass_rate"] == 0.1
    assert row["avg_evaluation_score"] == 0.1
