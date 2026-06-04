import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.deepseek_client import (
    DEFAULT_DEEPSEEK_MODEL,
    DeepSeekCallResult,
    call_deepseek_chat,
)
from app.llm_event import LLMRequestEvent, text_sha256


DEFAULT_OUTPUT_PATH = Path("data/raw/live_llm_requests/deepseek_events.jsonl")
DEFAULT_APP_NAME = "ai_observability_demo"
DEFAULT_FEATURE_NAME = "live_chat"
DEFAULT_PROMPT_CATEGORY = "demo"
DEFAULT_REGION = "unknown"
DEFAULT_ENVIRONMENT = "dev"

def estimate_cost_usd(prompt_tokens: int, completion_tokens: int) -> float:
    input_price_per_1m = 0.14
    output_price_per_1m = 0.28

    input_cost = prompt_tokens / 1_000_000 * input_price_per_1m
    output_cost = completion_tokens / 1_000_000 * output_price_per_1m
    return round(input_cost + output_cost, 8)


def build_live_event(
    result: DeepSeekCallResult,
    prompt_id: str,
    prompt_version: str,
    max_tokens: int = 0,
    temperature: float = 0.0,
    is_streaming: bool = False,
    created_at: datetime | None = None,
) -> LLMRequestEvent:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    return LLMRequestEvent(
        request_id=f"req_{uuid4().hex}",
        trace_id=f"trace_{uuid4().hex}",
        run_id=f"run_{uuid4().hex}",
        span_id=f"span_{uuid4().hex}",
        agent_id="agent_live_demo",
        agent_name="live_demo_agent",
        channel="api",
        user_id="live_user",
        session_id=f"session_{uuid4().hex}",
        conversation_id=f"conv_{uuid4().hex}",
        app_name=DEFAULT_APP_NAME,
        feature_name=DEFAULT_FEATURE_NAME,
        prompt_category=DEFAULT_PROMPT_CATEGORY,
        prompt_id=prompt_id,
        prompt_version=prompt_version,
        model_name=result.model_name,
        provider="deepseek",
        prompt_text=result.prompt_text,
        response_text=result.response_text,
        prompt_hash=text_sha256(result.prompt_text),
        response_hash=text_sha256(result.response_text),
        input_chars=len(result.prompt_text),
        output_chars=len(result.response_text),
        prompt_tokens=result.prompt_tokens,
        completion_tokens=result.completion_tokens,
        total_tokens=result.total_tokens,
        request_type="chat",
        is_streaming=is_streaming,
        temperature=temperature,
        max_tokens=max_tokens,
        finish_reason=result.finish_reason,
        retry_count=0,
        latency_ms=result.latency_ms,
        status="success",
        error_type=None,
        http_status=result.http_status,
        estimated_cost_usd=estimate_cost_usd(
            result.prompt_tokens,
            result.completion_tokens,
        ),
        mode="live",
        region=DEFAULT_REGION,
        environment=DEFAULT_ENVIRONMENT,
        created_at=created_at,
    )


def get_exception_http_status(error: Exception) -> int:
    status_code = getattr(error, "status_code", None)
    if isinstance(status_code, int):
        return status_code

    response = getattr(error, "response", None)
    response_status_code = getattr(response, "status_code", None)
    if isinstance(response_status_code, int):
        return response_status_code

    return 0


def build_error_event(
    prompt: str,
    prompt_id: str,
    prompt_version: str,
    model_name: str,
    error_type: str,
    http_status: int,
    latency_ms: int,
    max_tokens: int = 0,
    temperature: float = 0.0,
    is_streaming: bool = False,
    retry_count: int = 0,
    created_at: datetime | None = None,
) -> LLMRequestEvent:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    return LLMRequestEvent(
        request_id=f"req_{uuid4().hex}",
        trace_id=f"trace_{uuid4().hex}",
        run_id=f"run_{uuid4().hex}",
        span_id=f"span_{uuid4().hex}",
        agent_id="agent_live_demo",
        agent_name="live_demo_agent",
        channel="api",
        user_id="live_user",
        session_id=f"session_{uuid4().hex}",
        conversation_id=f"conv_{uuid4().hex}",
        app_name=DEFAULT_APP_NAME,
        feature_name=DEFAULT_FEATURE_NAME,
        prompt_category=DEFAULT_PROMPT_CATEGORY,
        prompt_id=prompt_id,
        prompt_version=prompt_version,
        model_name=model_name,
        provider="deepseek",
        prompt_text=prompt,
        response_text="",
        prompt_hash=text_sha256(prompt),
        response_hash=text_sha256(""),
        input_chars=len(prompt),
        output_chars=0,
        prompt_tokens=0,
        completion_tokens=0,
        total_tokens=0,
        request_type="chat",
        is_streaming=is_streaming,
        temperature=temperature,
        max_tokens=max_tokens,
        finish_reason="error",
        retry_count=retry_count,
        latency_ms=latency_ms,
        status="error",
        error_type=error_type,
        http_status=http_status,
        estimated_cost_usd=0.0,
        mode="live",
        region=DEFAULT_REGION,
        environment=DEFAULT_ENVIRONMENT,
        created_at=created_at,
    )


def write_events(events: list[LLMRequestEvent], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event.to_dict()) + "\n")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--model", type=str, default=None)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--prompt-version", type=str, default="v1")
    parser.add_argument(
        "--prompt",
        action="append",
        required=True,
        help="Prompt to send to DeepSeek. Can be provided multiple times.",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    events = []

    for index, prompt in enumerate(args.prompt, start=1):
        prompt_id = f"live_prompt_{index:03d}"
        started_at = time.perf_counter()

        try:
            result = call_deepseek_chat(
                prompt=prompt,
                model=args.model,
                max_tokens=args.max_tokens,
                temperature=args.temperature,
            )

            event = build_live_event(
                result=result,
                prompt_id=prompt_id,
                prompt_version=args.prompt_version,
                max_tokens=args.max_tokens,
                temperature=args.temperature,
            )
        except Exception as error:
            latency_ms = int((time.perf_counter() - started_at) * 1000)
            event = build_error_event(
                prompt=prompt,
                prompt_id=prompt_id,
                prompt_version=args.prompt_version,
                model_name=args.model or os.getenv("DEEPSEEK_MODEL", DEFAULT_DEEPSEEK_MODEL),
                error_type=type(error).__name__,
                http_status=get_exception_http_status(error),
                latency_ms=latency_ms,
                max_tokens=args.max_tokens,
                temperature=args.temperature,
            )
            print(f"Captured error event for {prompt_id}: {event.error_type}")

        events.append(event)

    write_events(events, args.output)
    print(f"Wrote {len(events)} live events to {args.output}")


if __name__ == "__main__":
    main()
