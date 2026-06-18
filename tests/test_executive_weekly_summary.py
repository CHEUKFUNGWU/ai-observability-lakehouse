from datetime import date

import pytest

from scripts.load_dws_metrics_to_doris import EXECUTIVE_WEEKLY_COLUMNS
from scripts.spark_build_ads_executive_weekly_summary import build_executive_weekly_summary


def test_build_executive_weekly_summary_combines_domains_with_weighted_metrics(spark):
    llm = spark.createDataFrame(
        [
            {"date": date(2026, 1, 5), "app_name": "support", "request_count": 10, "success_count": 9, "error_count": 1, "total_tokens": 100, "estimated_cost_usd": 1.0, "avg_latency_ms": 100.0, "p95_latency_ms": 200},
            {"date": date(2026, 1, 6), "app_name": "support", "request_count": 30, "success_count": 27, "error_count": 3, "total_tokens": 300, "estimated_cost_usd": 3.0, "avg_latency_ms": 300.0, "p95_latency_ms": 500},
        ]
    )
    agent = spark.createDataFrame(
        [{"date": date(2026, 1, 5), "app_name": "support", "run_count": 5, "success_count": 4, "error_count": 1, "estimated_cost_usd": 2.0}]
    )
    retrieval = spark.createDataFrame(
        [{"date": date(2026, 1, 5), "app_name": "support", "retrieval_cnt_1d": 4, "returned_cnt_1d": 10, "hit_cnt_1d": 8}]
    )
    feedback = spark.createDataFrame(
        [{"date": date(2026, 1, 5), "app_name": "support", "feedback_cnt_1d": 12, "thumbs_up_cnt_1d": 8, "thumbs_down_cnt_1d": 2}]
    )
    guardrail = spark.createDataFrame(
        [{"date": date(2026, 1, 5), "app_name": "support", "check_cnt_1d": 20, "triggered_cnt_1d": 3, "block_cnt_1d": 1}]
    )
    evaluation = spark.createDataFrame(
        [
            {"date": date(2026, 1, 5), "app_name": "support", "evaluation_cnt_1d": 2, "pass_cnt_1d": 1, "fail_cnt_1d": 1, "avg_score": 0.5},
            {"date": date(2026, 1, 6), "app_name": "support", "evaluation_cnt_1d": 8, "pass_cnt_1d": 8, "fail_cnt_1d": 0, "avg_score": 0.9},
        ]
    )

    row = build_executive_weekly_summary(llm, agent, retrieval, feedback, guardrail, evaluation).collect()[0]

    assert set(row.asDict()) == set(EXECUTIVE_WEEKLY_COLUMNS)
    assert row["week_start_date"] == date(2026, 1, 5)
    assert row["request_cnt_1w"] == 40
    assert row["avg_latency_ms"] == 250.0
    assert row["p95_latency_ms_max"] == 500
    assert row["total_ai_cost_amt_1w"] == pytest.approx(6.0)
    assert row["retrieval_hit_rate_1w"] == 0.8
    assert row["satisfaction_rate_1w"] == 0.8
    assert row["evaluation_pass_rate_1w"] == 0.9
    assert row["avg_evaluation_score"] == 0.82
