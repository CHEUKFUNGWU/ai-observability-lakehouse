from pyspark.sql import functions as F

from app.data_quality import split_valid_quarantine, validate_llm_events


def make_row(**overrides):
    row = {
        "request_id": "req_001",
        "created_at": "2026-01-01T00:00:00+00:00",
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "total_tokens": 15,
        "latency_ms": 100,
        "status": "success",
        "estimated_cost_usd": 0.0001,
        "mode": "mock",
        "user_id": "user_001",
        "session_id": "session_001",
        "app_name": "app",
        "feature_name": "chat",
        "prompt_category": "support",
        "prompt_id": "prompt_001",
        "prompt_version": "v1",
        "model_name": "deepseek-chat",
        "provider": "deepseek",
        "error_type": "",
        "http_status": 200,
        "region": "us",
        "environment": "dev",
        "date": "2026-01-01",
    }
    row.update(overrides)
    return row


def test_validate_llm_events_passes_valid_row(spark):
    events = spark.createDataFrame([make_row()])
    validated = validate_llm_events(events)
    valid, quarantine = split_valid_quarantine(validated)

    assert valid.count() == 1
    assert quarantine.count() == 0


def test_validate_llm_events_collects_multiple_rule_failures(spark):
    events = spark.createDataFrame([make_row()]).withColumn(
        "request_id",
        F.lit(None).cast("string"),
    ).withColumn(
        "total_tokens",
        F.lit(999),
    ).withColumn(
        "latency_ms",
        F.lit(0),
    ).withColumn(
        "status",
        F.lit("unknown"),
    ).withColumn(
        "estimated_cost_usd",
        F.lit(-1.0),
    ).withColumn(
        "mode",
        F.lit("bad"),
    )
    validated = validate_llm_events(events)
    valid, quarantine = split_valid_quarantine(validated)

    assert valid.count() == 0
    row = quarantine.collect()[0]
    assert "completeness:missing_request_id" in row["_dq_errors"]
    assert "consistency:token_total_mismatch" in row["_dq_errors"]
    assert "validity:non_positive_latency" in row["_dq_errors"]
    assert "validity:invalid_status" in row["_dq_errors"]
    assert "validity:negative_cost" in row["_dq_errors"]
    assert "validity:invalid_mode" in row["_dq_errors"]
