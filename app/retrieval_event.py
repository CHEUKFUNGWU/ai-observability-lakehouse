from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RetrievalEvent:
    retrieval_id: str
    trace_id: str
    run_id: str
    span_id: str
    request_id: str
    agent_id: str
    app_name: str
    feature_name: str
    user_id: str
    knowledge_base_id: str
    knowledge_base_name: str
    embedding_model: str
    retrieval_strategy: str
    query_text_hash: str
    query_length: int
    top_k: int
    returned_count: int
    hit_count: int
    max_similarity_score: float
    min_similarity_score: float
    avg_similarity_score: float
    embedding_latency_ms: int
    search_latency_ms: int
    total_latency_ms: int
    status: str
    error_type: Optional[str]
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
