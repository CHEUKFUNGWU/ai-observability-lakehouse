import argparse
import json
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path


from app.llm_event import LLMRequestEvent, text_sha256

OUTPUT_PATH = Path("data/raw/mock_llm_requests/events.jsonl")

APPS = ["ai_support_bot", "sales_assistant", "internal_copilot"]
FEATURES = ["chat", "summary", "rewrite", "rag_answer"]
PROMPT_CATEGORIES = ["support", "sales", "productivity", "knowledge_base"]
REGIONS = ["us", "eu", "apac"]

def estimate_cost_usd(prompt_tokens: int, completion_tokens: int) -> float:
    input_price_per_1m = 0.14
    output_price_per_1m = 0.28

    input_cost = prompt_tokens / 1_000_000 * input_price_per_1m
    output_cost = completion_tokens / 1_000_000 * output_price_per_1m
    return round(input_cost + output_cost, 8)

def build_mock_event(created_at: datetime | None = None) -> LLMRequestEvent:
    prompt_tokens = random.randint(20, 800)
    completion_tokens = random.randint(20, 1200)
    total_tokens = prompt_tokens + completion_tokens

    is_success = random.random() > 0.08
    status = "success" if is_success else "error"
    error_type = None if is_success else random.choice(["timeout", "rate_limit", "server_error"])
    http_status = 200 if is_success else random.choice([429, 500, 503])
    prompt_text = "mock user prompt"
    response_text = "mock model response" if is_success else ""

    if created_at is None:
        created_at = datetime.now(timezone.utc)

    return LLMRequestEvent(
        request_id=f"req_{random.getrandbits(128):032x}",
        trace_id=f"trace_{random.getrandbits(128):032x}",
        run_id=f"run_{random.getrandbits(128):032x}",
        span_id=f"span_{random.getrandbits(128):032x}",
        agent_id=f"agent_{random.randint(1, 20):03d}",
        agent_name=random.choice(["support_agent", "sales_agent", "ops_copilot"]),
        channel=random.choice(["web", "slack", "api"]),
        user_id=f"user_{random.randint(1, 500):04d}",
        session_id=f"session_{random.randint(1, 2000):05d}",
        conversation_id=f"conv_{random.randint(1, 3000):05d}",
        app_name=random.choice(APPS),
        feature_name=random.choice(FEATURES),
        prompt_category=random.choice(PROMPT_CATEGORIES),
        prompt_id=f"prompt_{random.randint(1, 50):03d}",
        prompt_version=random.choice(["v1", "v2", "v3"]),
        model_name="deepseek-chat",
        provider="deepseek",
        prompt_text=prompt_text,
        response_text=response_text,
        prompt_hash=text_sha256(prompt_text),
        response_hash=text_sha256(response_text),
        input_chars=len(prompt_text),
        output_chars=len(response_text),
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens if is_success else 0,
        total_tokens=total_tokens if is_success else prompt_tokens,
        request_type="chat",
        is_streaming=False,
        temperature=random.choice([0.2, 0.7, 1.0]),
        max_tokens=random.choice([256, 512, 1024]),
        finish_reason="stop" if is_success else "error",
        retry_count=random.randint(0, 2) if not is_success else 0,
        latency_ms=random.randint(200, 5000),
        status=status,
        error_type=error_type,
        http_status=http_status,
        estimated_cost_usd=estimate_cost_usd(prompt_tokens, completion_tokens if is_success else 0),
        mode="mock",
        region=random.choice(REGIONS),
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
    start_time = (
    datetime.fromisoformat(args.start_time)
    if args.start_time is not None
    else None
    )

    write_jsonl(args.count, args.output, seed=args.seed, start_time=start_time)
    print(f"Wrote {args.count} events to {args.output}")

if __name__ == "__main__":
    main()
