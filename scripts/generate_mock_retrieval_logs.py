import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.llm_event import text_sha256
from app.logging_utils import get_logger, log_info
from app.retrieval_event import RetrievalEvent

OUTPUT_PATH = Path("data/raw/mock_retrieval_requests/events.jsonl")

APPS = ["ai_support_bot", "sales_assistant", "internal_copilot"]
FEATURES = ["rag_answer", "chat", "summary"]
KNOWLEDGE_BASES = [
    ("kb_product_docs", "product_docs"),
    ("kb_support_playbook", "support_playbook"),
    ("kb_sales_enablement", "sales_enablement"),
]
EMBEDDING_MODELS = ["bge-large-en", "text-embedding-3-large", "e5-mistral"]
RETRIEVAL_STRATEGIES = ["vector", "hybrid", "keyword", "graph"]
LOGGER = get_logger(__name__)


def build_mock_event(created_at: datetime | None = None) -> RetrievalEvent:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    is_success = random.random() > 0.06
    top_k = random.choice([3, 5, 8, 10])
    returned_count = random.randint(0, top_k) if is_success else 0
    hit_count = random.randint(0, returned_count) if returned_count else 0
    max_similarity_score = round(random.uniform(0.65, 0.98), 4) if returned_count else 0.0
    min_similarity_score = round(random.uniform(0.20, max_similarity_score), 4) if returned_count else 0.0
    avg_similarity_score = round((max_similarity_score + min_similarity_score) / 2, 4)
    embedding_latency_ms = random.randint(25, 400)
    search_latency_ms = random.randint(40, 1500)
    knowledge_base_id, knowledge_base_name = random.choice(KNOWLEDGE_BASES)
    query_text = f"mock retrieval query {random.randint(1, 10000)}"

    return RetrievalEvent(
        retrieval_id=f"ret_{random.getrandbits(128):032x}",
        trace_id=f"trace_{random.getrandbits(128):032x}",
        run_id=f"run_{random.getrandbits(128):032x}",
        span_id=f"span_{random.getrandbits(128):032x}",
        request_id=f"req_{random.getrandbits(128):032x}",
        agent_id=f"agent_{random.randint(1, 20):03d}",
        app_name=random.choice(APPS),
        feature_name=random.choice(FEATURES),
        user_id=f"user_{random.randint(1, 500):04d}",
        knowledge_base_id=knowledge_base_id,
        knowledge_base_name=knowledge_base_name,
        embedding_model=random.choice(EMBEDDING_MODELS),
        retrieval_strategy=random.choice(RETRIEVAL_STRATEGIES),
        query_text_hash=text_sha256(query_text),
        query_length=len(query_text),
        top_k=top_k,
        returned_count=returned_count,
        hit_count=hit_count,
        max_similarity_score=max_similarity_score,
        min_similarity_score=min_similarity_score,
        avg_similarity_score=avg_similarity_score,
        embedding_latency_ms=embedding_latency_ms,
        search_latency_ms=search_latency_ms,
        total_latency_ms=embedding_latency_ms + search_latency_ms,
        status="success" if is_success else "error",
        error_type=None if is_success else random.choice(["vector_store_timeout", "embedding_error"]),
        mode="mock",
        environment="dev",
        created_at=created_at,
    )


def write_jsonl(
    count: int,
    output_path: Path,
    seed: int | None = None,
    start_time: datetime | None = None,
) -> None:
    if seed is not None:
        random.seed(seed)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    with output_path.open("w", encoding="utf-8") as file:
        for index in range(count):
            event = build_mock_event(created_at=start_time + timedelta(seconds=index))
            file.write(json.dumps(event.to_dict()) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--start-time", type=str, default=None)
    args = parser.parse_args()
    start_time = datetime.fromisoformat(args.start_time) if args.start_time is not None else None

    write_jsonl(args.count, args.output, seed=args.seed, start_time=start_time)
    log_info(LOGGER, "mock_retrieval_events_written", count=args.count, output=str(args.output))


if __name__ == "__main__":
    main()
