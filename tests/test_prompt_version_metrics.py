from datetime import date

from scripts.spark_build_dws_prompt_version_daily_metrics import build_prompt_version_daily_metrics


def test_build_prompt_version_daily_metrics_compares_multiple_versions_and_scores(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "request_id": "req_v2_success",
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
                "request_id": "req_v2_error",
                "prompt_id": "prompt_001",
                "prompt_version": "v2",
                "model_name": "deepseek-chat",
                "latency_ms": 300,
                "status": "error",
                "total_tokens": 200,
                "estimated_cost_usd": 0.20,
            },
            {
                "date": date(2026, 1, 1),
                "request_id": "req_v3_success",
                "prompt_id": "prompt_001",
                "prompt_version": "v3",
                "model_name": "deepseek-chat",
                "latency_ms": 80,
                "status": "success",
                "total_tokens": 50,
                "estimated_cost_usd": 0.05,
            },
        ]
    )
    evaluation_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "request_id": "req_v2_success",
                "evaluated_prompt_version": "v2",
                "evaluated_model_name": "deepseek-chat",
                "score": 0.80,
                "passed": True,
            },
            {
                "date": date(2026, 1, 1),
                "request_id": "req_v2_error",
                "evaluated_prompt_version": "v2",
                "evaluated_model_name": "deepseek-chat",
                "score": 0.60,
                "passed": False,
            },
            {
                "date": date(2026, 1, 1),
                "request_id": "req_v3_success",
                "evaluated_prompt_version": "v3",
                "evaluated_model_name": "deepseek-chat",
                "score": 0.95,
                "passed": True,
            },
        ]
    )

    rows = {
        row["prompt_version"]: row
        for row in build_prompt_version_daily_metrics(llm_events, evaluation_events).collect()
    }
    row = rows["v2"]

    assert row["request_cnt_1d"] == 2
    assert row["success_cnt_1d"] == 1
    assert row["error_cnt_1d"] == 1
    assert row["avg_latency_ms"] == 200.0
    assert row["p95_latency_ms"] == 300
    assert row["total_token_cnt_1d"] == 300
    assert row["estimated_cost_amt_1d"] == 0.30000000000000004
    assert row["evaluation_cnt_1d"] == 2
    assert row["pass_cnt_1d"] == 1
    assert row["fail_cnt_1d"] == 1
    assert row["evaluation_score_num_1d"] == 1.4
    assert row["evaluation_score_den_1d"] == 2
    assert row["avg_evaluation_score"] == 0.7
    assert rows["v3"]["request_cnt_1d"] == 1
    assert rows["v3"]["avg_evaluation_score"] == 0.95


def test_build_prompt_version_daily_metrics_allows_missing_evaluations(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "request_id": "req_001",
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
    assert row["evaluation_cnt_1d"] == 0
    assert row["pass_cnt_1d"] == 0
    assert row["evaluation_score_num_1d"] == 0.0
    assert row["evaluation_score_den_1d"] == 0
    assert row["avg_evaluation_score"] is None


def test_build_prompt_version_daily_metrics_routes_missing_prompt_metadata_to_unknown(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "request_id": "req_missing_prompt",
                "prompt_id": "",
                "prompt_version": "",
                "model_name": "deepseek-chat",
                "latency_ms": 100,
                "status": "success",
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
            }
        ]
    )

    row = build_prompt_version_daily_metrics(llm_events).collect()[0]

    assert row["prompt_id"] == "unknown"
    assert row["prompt_version"] == "unknown"
    assert row["model_name"] == "deepseek-chat"


def test_build_prompt_version_daily_metrics_joins_scores_by_request_id_not_version_only(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "request_id": "req_score_join",
                "prompt_id": "prompt_001",
                "prompt_version": "v3",
                "model_name": "deepseek-chat",
                "latency_ms": 100,
                "status": "success",
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
            }
        ]
    )
    evaluation_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "request_id": "req_score_join",
                "evaluated_prompt_version": "stale_metadata",
                "evaluated_model_name": "deepseek-chat",
                "score": 0.9,
                "passed": True,
            }
        ]
    )

    row = build_prompt_version_daily_metrics(llm_events, evaluation_events).collect()[0]

    assert row["prompt_id"] == "prompt_001"
    assert row["prompt_version"] == "v3"
    assert row["evaluation_cnt_1d"] == 1
    assert row["avg_evaluation_score"] == 0.9


def test_build_prompt_version_daily_metrics_counts_conflicting_request_metadata(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "request_id": "req_conflict",
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "latency_ms": 100,
                "status": "success",
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
            },
            {
                "date": date(2026, 1, 1),
                "request_id": "req_conflict",
                "prompt_id": "prompt_001",
                "prompt_version": "v2",
                "model_name": "deepseek-chat",
                "latency_ms": 120,
                "status": "success",
                "total_tokens": 120,
                "estimated_cost_usd": 0.12,
            },
        ]
    )

    rows = build_prompt_version_daily_metrics(llm_events).orderBy("prompt_version").collect()

    assert [row["metadata_conflict_cnt_1d"] for row in rows] == [1, 1]
