from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional


@dataclass
class AgentRunEvent:
    run_id: str
    trace_id: str
    agent_id: str
    agent_name: str
    agent_version: str
    app_name: str
    user_id: str
    session_id: str
    conversation_id: str
    task_type: str
    channel: str
    toolsets_used: str
    input_text_hash: str
    output_text_hash: str
    start_time: datetime
    end_time: datetime
    duration_ms: int
    status: str
    error_type: Optional[str]
    turn_count: int
    llm_call_count: int
    tool_call_count: int
    retrieval_count: int
    total_tokens: int
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
        for field in ("start_time", "end_time", "created_at"):
            data[field] = data[field].isoformat()
        data["date"] = self.date
        return data


@dataclass
class AgentSpanEvent:
    span_id: str
    parent_span_id: Optional[str]
    run_id: str
    trace_id: str
    agent_id: str
    span_name: str
    span_type: str
    span_order: int
    start_time: datetime
    end_time: datetime
    duration_ms: int
    status: str
    error_type: Optional[str]
    retry_count: int
    input_size: int
    output_size: int
    model_name: Optional[str]
    tool_name: Optional[str]
    mode: str
    region: str
    environment: str
    created_at: datetime

    @property
    def date(self) -> str:
        return self.created_at.date().isoformat()

    def to_dict(self) -> dict:
        data = asdict(self)
        for field in ("start_time", "end_time", "created_at"):
            data[field] = data[field].isoformat()
        data["date"] = self.date
        return data


@dataclass
class AgentToolCallEvent:
    tool_call_id: str
    span_id: str
    run_id: str
    trace_id: str
    agent_id: str
    tool_name: str
    tool_type: str
    arguments_json: str
    result_text: str
    result_size: int
    duration_ms: int
    status: str
    error_type: Optional[str]
    retry_count: int
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
