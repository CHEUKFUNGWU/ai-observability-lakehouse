from datetime import date

from scripts.spark_build_dws_prompt_version_daily_metrics import build_prompt_version_daily_metrics


def test_build_prompt_version_daily_metrics_aggregates_requests_and_scores(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "prompt_id": "prompt_001",
                "prompt_version": "v2",
                "model_name": "deepseek-chat",
                "latency_ms": 100,
                "status": "success",
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
            },
            {
                "date": date(2026, 1, 1),
                "prompt_id": "prompt_001",
                "prompt_version": "v2",
                "model_name": "deepseek-chat",
                "latency_ms": 300,
                "status": "error",
                "total_tokens": 200,
                "estimated_cost_usd": 0.20,
            },
        ]
    )
    evaluation_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "evaluated_prompt_version": "v2",
                "evaluated_model_name": "deepseek-chat",
                "score": 0.80,
            },
            {
                "date": date(2026, 1, 1),
                "evaluated_prompt_version": "v2",
                "evaluated_model_name": "deepseek-chat",
                "score": 0.60,
            },
        ]
    )

    row = build_prompt_version_daily_metrics(llm_events, evaluation_events).collect()[0]

    assert row["request_cnt_1d"] == 2
    assert row["success_cnt_1d"] == 1
    assert row["error_cnt_1d"] == 1
    assert row["avg_latency_ms"] == 200.0
    assert row["p95_latency_ms"] == 300
    assert row["total_token_cnt_1d"] == 300
    assert row["estimated_cost_amt_1d"] == 0.30000000000000004
    assert row["avg_evaluation_score"] == 0.7


def test_build_prompt_version_daily_metrics_allows_missing_evaluations(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "prompt_id": "prompt_001",
                "prompt_version": "v2",
                "model_name": "deepseek-chat",
                "latency_ms": 100,
                "status": "success",
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
            }
        ]
    )

    row = build_prompt_version_daily_metrics(llm_events).collect()[0]

    assert row["request_cnt_1d"] == 1
    assert row["avg_evaluation_score"] is None
