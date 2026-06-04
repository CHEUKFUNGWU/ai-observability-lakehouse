import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional


def text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class LLMRequestEvent:
    request_id: str
    trace_id: str
    run_id: str
    span_id: str
    agent_id: str
    agent_name: str
    channel: str
    user_id: str
    session_id: str
    conversation_id: str
    app_name: str
    feature_name: str
    prompt_category: str
    prompt_id: str
    prompt_version: str
    model_name: str
    provider: str

    prompt_text: str
    response_text: str
    prompt_hash: str
    response_hash: str
    input_chars: int
    output_chars: int

    prompt_tokens: int
    completion_tokens: int
    total_tokens: int

    request_type: str
    is_streaming: bool
    temperature: float
    max_tokens: int
    finish_reason: Optional[str]
    retry_count: int

    latency_ms: int
    status: str
    error_type: Optional[str]
    http_status: int

    estimated_cost_usd: float

    mode: str
    region: str
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
