import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.compliance_event import (
    ComplianceAccessAuditEvent,
    ComplianceDataRetentionEvent,
    hash_ip_address,
)


ACCESS_OUTPUT_PATH = Path("data/raw/mock_compliance_access_audit/events.jsonl")
RETENTION_OUTPUT_PATH = Path("data/raw/mock_compliance_data_retention/events.jsonl")
ACTION_TYPES = ["query", "export", "view_prompt", "view_response", "delete", "admin_override"]
RESOURCE_TYPES = ["dashboard", "dwd_table", "raw_log", "prompt_text", "response_text"]
CLASSIFICATIONS = ["public", "internal", "confidential", "restricted"]
RETENTION_ACTION_TYPES = ["archive", "anonymize", "delete"]
TABLE_NAMES = [
    "dwd_ai_llm_request_di",
    "dwd_ai_agent_run_di",
    "dwd_ai_feedback_action_di",
    "dwd_ai_compliance_access_audit_di",
]


def build_mock_access_event(created_at: datetime) -> ComplianceAccessAuditEvent:
    access_granted = random.random() >= 0.12
    octet = random.randint(1, 254)
    return ComplianceAccessAuditEvent(
        audit_event_id=f"audit_{random.getrandbits(128):032x}",
        user_id=f"user_{random.randint(1, 30):04d}",
        action_type=random.choice(ACTION_TYPES),
        resource_type=random.choice(RESOURCE_TYPES),
        resource_id=f"resource_{random.randint(1, 100):04d}",
        ip_address=hash_ip_address(f"192.0.2.{octet}", salt="mock-compliance"),
        access_granted=access_granted,
        denial_reason=None if access_granted else random.choice(["insufficient_role", "restricted_data"]),
        data_classification=random.choice(CLASSIFICATIONS),
        created_at=created_at,
    )


def build_mock_retention_event(created_at: datetime) -> ComplianceDataRetentionEvent:
    return ComplianceDataRetentionEvent(
        retention_event_id=f"retention_{random.getrandbits(128):032x}",
        table_name=random.choice(TABLE_NAMES),
        partition_date=(created_at - timedelta(days=random.randint(30, 730))).date(),
        action_type=random.choice(RETENTION_ACTION_TYPES),
        rows_affected=random.randint(1, 100000),
        policy_name=random.choice(["restricted_90d", "operational_365d", "audit_730d"]),
        created_at=created_at,
    )


def write_jsonl(
    count: int,
    access_output_path: Path = ACCESS_OUTPUT_PATH,
    retention_output_path: Path = RETENTION_OUTPUT_PATH,
    seed: int | None = None,
    start_time: datetime | None = None,
) -> None:
    if seed is not None:
        random.seed(seed)
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    access_output_path.parent.mkdir(parents=True, exist_ok=True)
    retention_output_path.parent.mkdir(parents=True, exist_ok=True)
    with access_output_path.open("w", encoding="utf-8") as access_file, retention_output_path.open(
        "w", encoding="utf-8"
    ) as retention_file:
        for index in range(count):
            created_at = start_time + timedelta(seconds=index)
            access_file.write(json.dumps(build_mock_access_event(created_at).to_dict()) + "\n")
            retention_file.write(json.dumps(build_mock_retention_event(created_at).to_dict()) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--access-output", type=Path, default=ACCESS_OUTPUT_PATH)
    parser.add_argument("--retention-output", type=Path, default=RETENTION_OUTPUT_PATH)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    write_jsonl(args.count, args.access_output, args.retention_output, args.seed)


if __name__ == "__main__":
    main()
