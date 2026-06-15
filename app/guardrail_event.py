from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional


@dataclass
class GuardrailEvent:
    guardrail_event_id: str
    trace_id: str
    request_id: str
    run_id: str
    user_id: str
    app_name: str
    feature_name: str
    guardrail_stage: str
    rule_name: str
    rule_category: str
    triggered: bool
    action_taken: str
    severity: str
    matched_pattern_hash: str
    input_text_length: int
    guardrail_latency_ms: int
    model_name: str
    prompt_version: str
    mode: str
    environment: str
    created_at: datetime

    @property
    def date(self) -> str:
        return self.created_at.date().isoformat()

    def to_dict(self) -> dict:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        data["date"] = self.date
        return data
