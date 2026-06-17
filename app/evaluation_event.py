from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class EvaluationEvent:
    evaluation_id: str
    trace_id: str
    request_id: str
    run_id: str
    app_name: str
    feature_name: str
    evaluator_type: str
    evaluator_model: str
    evaluation_dimension: str
    score: float
    raw_score: str
    pass_threshold: float
    passed: bool
    evaluated_model_name: str
    evaluated_prompt_version: str
    evaluation_latency_ms: int
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
