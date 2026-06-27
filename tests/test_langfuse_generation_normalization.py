import json
from pathlib import Path

from app.langfuse_generation import normalize_generation_records
from app.llm_event import text_sha256
from app.data_quality import split_valid_quarantine, validate_llm_events
from scripts.normalize_langfuse_generations import normalize_jsonl
from scripts.spark_transform_llm_events import transform_llm_events


FIXTURE_PATH = Path("tests/fixtures/langfuse/generations.jsonl")
RAW_TEXT_FIELDS = {"input", "output", "prompt", "completion", "response", "prompt_text", "response_text"}


def load_fixture() -> list[dict]:
    return [json.loads(line) for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()]


def test_normalize_langfuse_generations_maps_fixture_without_raw_text():
    events, quarantine = normalize_generation_records(load_fixture())

    assert len(events) == 2
    assert len(quarantine) == 1

    row = events[0]
    assert row["request_id"] == "gen_success_001"
    assert row["trace_id"] == "trace_langfuse_001"
    assert row["run_id"] == ""
    assert row["span_id"] == "span_parent_001"
    assert row["app_name"] == "ai_support_bot"
    assert row["feature_name"] == "chat"
    assert row["model_name"] == "gpt-4o-mini"
    assert row["provider"] == "openai"
    assert row["prompt_tokens"] == 42
    assert row["completion_tokens"] == 18
    assert row["total_tokens"] == 60
    assert row["latency_ms"] == 1500
    assert row["status"] == "success"
    assert row["estimated_cost_usd"] == 0.00021
    assert row["mode"] == "replay"
    assert row["date"] == "2026-06-26"

    source_input = load_fixture()[0]["input"]
    canonical_input = json.dumps(source_input, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert row["prompt_hash"] == text_sha256(canonical_input)
    assert row["input_chars"] == len(canonical_input)
    assert RAW_TEXT_FIELDS.isdisjoint(row)


def test_normalize_langfuse_generations_deduplicates_replayed_generation():
    record = load_fixture()[0]

    events, quarantine = normalize_generation_records([record, record])

    assert [row["request_id"] for row in events] == ["gen_success_001"]
    assert quarantine == []


def test_normalize_langfuse_generations_marks_estimated_cost_source():
    langfuse_record = load_fixture()[0]
    metadata_record = json.loads(json.dumps(langfuse_record))
    metadata_record["id"] = "gen_metadata_cost_001"
    metadata_record.pop("costDetails")
    metadata_record["metadata"]["estimated_cost_usd"] = 0.00031

    events, quarantine = normalize_generation_records([langfuse_record, metadata_record])

    assert quarantine == []
    assert events[0]["estimated_cost_source"] == "langfuse_cost_details"
    assert events[1]["estimated_cost_source"] == "metadata_estimate"


def test_langfuse_error_generation_maps_to_error_llm_request():
    events, _ = normalize_generation_records(load_fixture())
    row = events[1]

    assert row["request_id"] == "gen_error_001"
    assert row["run_id"] == "run_sales_001"
    assert row["status"] == "error"
    assert row["error_type"] == "rate_limit"
    assert row["http_status"] == 429
    assert row["completion_tokens"] == 0
    assert row["total_tokens"] == 28


def test_invalid_langfuse_generation_enters_sanitized_quarantine():
    _, quarantine = normalize_generation_records(load_fixture())
    row = quarantine[0]

    assert row["_dq_status"] == "quarantine"
    assert "completeness:missing_feature_name" in row["_dq_errors"]
    assert "validity:non_positive_latency" in row["_dq_errors"]
    assert "consistency:token_total_mismatch" in row["_dq_errors"]
    assert "_source_payload_hash" in row
    assert RAW_TEXT_FIELDS.isdisjoint(row)


def test_langfuse_generation_events_are_spark_llm_request_compatible(spark):
    events, quarantine = normalize_generation_records(load_fixture())
    assert not quarantine[0].get("prompt_text")

    raw_events = spark.createDataFrame(events)
    projected = transform_llm_events(raw_events)
    validated = validate_llm_events(projected)
    valid_events, quarantine_events = split_valid_quarantine(validated)

    assert valid_events.count() == 2
    assert quarantine_events.count() == 0
    assert "prompt_text" not in valid_events.columns
    assert "response_text" not in valid_events.columns


def test_normalize_jsonl_writes_events_and_quarantine(tmp_path):
    output_path = tmp_path / "events.jsonl"
    quarantine_path = tmp_path / "quarantine.jsonl"

    result = normalize_jsonl(FIXTURE_PATH, output_path, quarantine_path)

    assert result == {"input_rows": 3, "output_rows": 2, "quarantine_rows": 1}
    events = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    quarantine = [json.loads(line) for line in quarantine_path.read_text(encoding="utf-8").splitlines()]
    assert len(events) == 2
    assert len(quarantine) == 1
    assert RAW_TEXT_FIELDS.isdisjoint(events[0])
    assert RAW_TEXT_FIELDS.isdisjoint(quarantine[0])
