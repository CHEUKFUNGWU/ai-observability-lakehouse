import hashlib
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Optional


def hash_ip_address(ip_address: str, salt: str = "") -> str:
    """Return a stable, non-reversible identifier for an IP address."""
    return hashlib.sha256(f"{salt}:{ip_address}".encode("utf-8")).hexdigest()


@dataclass
class ComplianceAccessAuditEvent:
    audit_event_id: str
    user_id: str
    action_type: str
    resource_type: str
    resource_id: str
    ip_address: str
    access_granted: bool
    denial_reason: Optional[str]
    data_classification: str
    created_at: datetime

    @property
    def date(self) -> str:
        return self.created_at.date().isoformat()

    def to_dict(self) -> dict:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        data["date"] = self.date
        return data


@dataclass
class ComplianceDataRetentionEvent:
    retention_event_id: str
    table_name: str
    partition_date: date
    action_type: str
    rows_affected: int
    policy_name: str
    created_at: datetime

    @property
    def date(self) -> str:
        return self.created_at.date().isoformat()

    def to_dict(self) -> dict:
        data = asdict(self)
        data["partition_date"] = self.partition_date.isoformat()
        data["created_at"] = self.created_at.isoformat()
        data["date"] = self.date
        return data
