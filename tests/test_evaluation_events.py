from scripts.generate_mock_evaluation_logs import write_jsonl
from scripts.spark_build_dws_evaluation_daily_metrics import build_evaluation_daily_metrics
from scripts.spark_transform_evaluation_events import transform_evaluation_events


def make_raw_evaluation_events(spark):
    return spark.createDataFrame(
        [
            {
                "evaluation_id": "eval_001",
                "trace_id": "trace_001",
                "request_id": "req_001",
                "run_id": "run_001",
                "app_name": "ai_support_bot",
                "feature_name": "rag_answer",
                "evaluator_type": "llm_judge",
                "evaluator_model": "deepseek-chat",
                "evaluation_dimension": "faithfulness",
                "score": 0.90,
                "raw_score": "0.90",
                "pass_threshold": 0.80,
                "passed": True,
                "evaluated_model_name": "deepseek-chat",
                "evaluated_prompt_version": "v1",
                "evaluation_latency_ms": 1200,
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            },
            {
                "evaluation_id": "eval_002",
                "trace_id": "trace_002",
                "request_id": "req_002",
                "run_id": "run_002",
                "app_name": "ai_support_bot",
                "feature_name": "rag_answer",
                "evaluator_type": "llm_judge",
                "evaluator_model": "deepseek-chat",
                "evaluation_dimension": "faithfulness",
                "score": 0.40,
                "raw_score": "0.40",
                "pass_threshold": 0.80,
                "passed": False,
                "evaluated_model_name": "deepseek-chat",
                "evaluated_prompt_version": "v1",
                "evaluation_latency_ms": 800,
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:01:00+00:00",
                "date": "2026-01-01",
            },
        ]
    )


def test_transform_evaluation_events_casts_expected_types(spark):
    events = transform_evaluation_events(make_raw_evaluation_events(spark))
    schema = dict(events.dtypes)

    assert schema["score"] == "double"
    assert schema["pass_threshold"] == "double"
    assert schema["passed"] == "boolean"
    assert schema["evaluation_latency_ms"] == "int"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"


def test_build_evaluation_daily_metrics_aggregates_scores_and_passes(spark):
    row = build_evaluation_daily_metrics(
        transform_evaluation_events(make_raw_evaluation_events(spark))
    ).collect()[0]

    assert row["evaluation_cnt_1d"] == 2
    assert row["pass_cnt_1d"] == 1
    assert row["fail_cnt_1d"] == 1
    assert row["avg_score"] == 0.65
    assert row["p10_score"] == 0.4
    assert row["avg_evaluation_latency_ms"] == 1000.0


def test_mock_evaluation_jsonl_writes_expected_rows(tmp_path):
    output_path = tmp_path / "evaluation.jsonl"

    write_jsonl(count=3, output_path=output_path, seed=42)

    rows = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) == 3
    assert "evaluation_id" in rows[0]
