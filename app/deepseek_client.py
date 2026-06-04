import os
import time
from dataclasses import dataclass
from typing import Optional

from openai import OpenAI


DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEFAULT_DEEPSEEK_MODEL = "deepseek-v4-flash"


@dataclass
class DeepSeekCallResult:
    prompt_text: str
    response_text: str
    model_name: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_ms: int
    http_status: int
    finish_reason: Optional[str] = None


def build_deepseek_client(
    api_key: str | None = None,
    base_url: str = DEFAULT_DEEPSEEK_BASE_URL,
) -> OpenAI:
    resolved_api_key = api_key or os.getenv("DEEPSEEK_API_KEY")

    if not resolved_api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is required")

    return OpenAI(api_key=resolved_api_key, base_url=base_url)


def call_deepseek_chat(
    prompt: str,
    client: OpenAI | None = None,
    model: str | None = None,
    max_tokens: int = 256,
    temperature: float = 0.0,
    disable_thinking: bool = True,
) -> DeepSeekCallResult:
    resolved_model = model or os.getenv("DEEPSEEK_MODEL", DEFAULT_DEEPSEEK_MODEL)
    resolved_client = client or build_deepseek_client()

    started_at = time.perf_counter()

    request_kwargs = {
        "model": resolved_model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    if disable_thinking:
        request_kwargs["extra_body"] = {"thinking": {"type": "disabled"}}

    response = resolved_client.chat.completions.create(**request_kwargs)

    latency_ms = int((time.perf_counter() - started_at) * 1000)

    message = response.choices[0].message
    finish_reason = response.choices[0].finish_reason
    usage = response.usage

    return DeepSeekCallResult(
        prompt_text=prompt,
        response_text=message.content or "",
        model_name=response.model or resolved_model,
        prompt_tokens=usage.prompt_tokens if usage else 0,
        completion_tokens=usage.completion_tokens if usage else 0,
        total_tokens=usage.total_tokens if usage else 0,
        latency_ms=latency_ms,
        http_status=200,
        finish_reason=finish_reason,
    )
