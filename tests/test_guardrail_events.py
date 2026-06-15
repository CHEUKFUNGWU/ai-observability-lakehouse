from datetime import date

from scripts.spark_build_ads_guardrail_violation import build_guardrail_violation
from scripts.spark_build_dws_guardrail_daily_metrics import build_guardrail_daily_metrics
from scripts.spark_transform_guardrail_events import transform_guardrail_events


def make_raw_guardrail_events(spark):
    return spark.createDataFrame(
        [
            {
                "guardrail_event_id": "gr_001",
                "trace_id": "trace_001",
                "request_id": "req_001",
                "run_id": "run_001",
                "user_id": "user_001",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "guardrail_stage": "pre_request",
                "rule_name": "pii_email",
                "rule_category": "pii_detection",
                "triggered": True,
                "action_taken": "block",
                "severity": "high",
                "matched_pattern_hash": "hash",
                "input_text_length": 100,
                "guardrail_latency_ms": 80,
                "model_name": "deepseek-chat",
                "prompt_version": "v1",
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            },
            {
                "guardrail_event_id": "gr_002",
                "trace_id": "trace_002",
                "request_id": "req_002",
                "run_id": "run_002",
                "user_id": "user_002",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "guardrail_stage": "pre_request",
                "rule_name": "pii_email",
                "rule_category": "pii_detection",
                "triggered": False,
                "action_taken": "pass",
                "severity": "high",
                "matched_pattern_hash": "",
                "input_text_length": 100,
                "guardrail_latency_ms": 40,
                "model_name": "deepseek-chat",
                "prompt_version": "v1",
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:01:00+00:00",
                "date": "2026-01-01",
            },
        ]
    )


def test_transform_guardrail_events_casts_expected_types(spark):
    events = transform_guardrail_events(make_raw_guardrail_events(spark))
    schema = dict(events.dtypes)

    assert schema["triggered"] == "boolean"
    assert schema["input_text_length"] == "int"
    assert schema["guardrail_latency_ms"] == "int"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"


def test_build_guardrail_daily_metrics_aggregates_rule_actions(spark):
    rows = {
        row["action_taken"]: row
        for row in build_guardrail_daily_metrics(
            transform_guardrail_events(make_raw_guardrail_events(spark))
        ).collect()
    }

    assert rows["block"]["check_cnt_1d"] == 1
    assert rows["block"]["triggered_cnt_1d"] == 1
    assert rows["block"]["block_cnt_1d"] == 1
    assert rows["pass"]["triggered_cnt_1d"] == 0
    assert rows["pass"]["distinct_user_cnt_1d"] == 1


def test_build_guardrail_violation_derives_rates_and_flags(spark):
    metrics = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "app_name": "ai_support_bot",
                "rule_category": "pii_detection",
                "action_taken": "block",
                "check_cnt_1d": 10,
                "triggered_cnt_1d": 3,
                "block_cnt_1d": 2,
                "redact_cnt_1d": 0,
                "warn_cnt_1d": 1,
                "avg_guardrail_latency_ms": 350.0,
                "distinct_user_cnt_1d": 8,
            }
        ]
    )

    row = build_guardrail_violation(metrics).collect()[0]

    assert row["trigger_rate_1d"] == 0.3
    assert row["block_rate_1d"] == 0.2
    assert row["is_trigger_rate_breach"] is True
    assert row["is_latency_breach"] is True
