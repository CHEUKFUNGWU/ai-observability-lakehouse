from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional


@dataclass
class FeedbackEvent:
    feedback_id: str
    trace_id: str
    request_id: str
    run_id: str
    session_id: str
    conversation_id: str
    user_id: str
    app_name: str
    feature_name: str
    agent_id: str
    feedback_type: str
    rating_value: Optional[int]
    feedback_text_hash: str
    feedback_text_length: int
    response_latency_ms: int
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
