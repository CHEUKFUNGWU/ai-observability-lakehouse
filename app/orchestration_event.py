from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class AgentOrchestrationEvent:
    orchestration_id: str
    trace_id: str
    parent_run_id: str
    child_run_id: str
    parent_agent_id: str
    child_agent_id: str
    handoff_type: str
    payload_size: int
    handoff_latency_ms: int
    status: str
    created_at: datetime

    @property
    def date(self) -> str:
        return self.created_at.date().isoformat()

    def to_dict(self) -> dict:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        data["date"] = self.date
        return data
