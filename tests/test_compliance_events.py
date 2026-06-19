import json

from app.compliance_event import hash_ip_address
from scripts.generate_mock_compliance_logs import write_jsonl
from scripts.spark_transform_compliance_events import (
    transform_access_audit_events,
    transform_data_retention_events,
)


def test_hash_ip_address_is_stable_and_non_reversible():
    hashed = hash_ip_address("192.0.2.10", salt="test")

    assert hashed == hash_ip_address("192.0.2.10", salt="test")
    assert hashed != hash_ip_address("192.0.2.11", salt="test")
    assert len(hashed) == 64
    assert "192.0.2.10" not in hashed


def test_transform_compliance_events_casts_expected_types(spark):
    access_raw = spark.createDataFrame(
        [
            {
                "audit_event_id": "audit_001",
                "user_id": "user_001",
                "action_type": "view_prompt",
                "resource_type": "prompt_text",
                "resource_id": "prompt_001",
                "ip_address": hash_ip_address("192.0.2.10"),
                "access_granted": False,
                "denial_reason": "restricted_data",
                "data_classification": "restricted",
                "created_at": "2026-06-01T00:00:00+00:00",
                "date": "2026-06-01",
            }
        ]
    )
    retention_raw = spark.createDataFrame(
        [
            {
                "retention_event_id": "retention_001",
                "table_name": "dwd_ai_llm_request_di",
                "partition_date": "2025-01-01",
                "action_type": "anonymize",
                "rows_affected": 100,
                "policy_name": "restricted_90d",
                "created_at": "2026-06-01T00:00:00+00:00",
                "date": "2026-06-01",
            }
        ]
    )

    access = transform_access_audit_events(access_raw).collect()[0]
    retention = transform_data_retention_events(retention_raw).collect()[0]

    assert access["access_granted"] is False
    assert len(access["ip_address"]) == 64
    assert retention["rows_affected"] == 100
    assert str(retention["partition_date"]) == "2025-01-01"


def test_mock_compliance_jsonl_writes_hashed_access_and_retention_rows(tmp_path):
    access_path = tmp_path / "access.jsonl"
    retention_path = tmp_path / "retention.jsonl"

    write_jsonl(3, access_path, retention_path, seed=42)

    access_rows = [json.loads(line) for line in access_path.read_text(encoding="utf-8").splitlines()]
    retention_rows = [json.loads(line) for line in retention_path.read_text(encoding="utf-8").splitlines()]
    assert len(access_rows) == 3
    assert len(retention_rows) == 3
    assert all(len(row["ip_address"]) == 64 for row in access_rows)
    assert all(row["action_type"] in {"archive", "anonymize", "delete"} for row in retention_rows)
