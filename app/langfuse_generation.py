from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

from app.llm_event import text_sha256


ALLOWED_LLM_REQUEST_MODES = {"mock", "live", "replay", "hermes"}
REQUIRED_TEXT_FIELDS = (
    "request_id",
    "trace_id",
    "user_id",
    "session_id",
    "app_name",
    "feature_name",
    "prompt_category",
    "prompt_id",
    "prompt_version",
    "model_name",
    "provider",
    "region",
    "environment",
    "created_at",
    "date",
)


def normalize_generation_records(records: Iterable[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    valid: list[dict] = []
    quarantine: list[dict] = []
    seen_source_payloads: set[str] = set()

    for record in records:
        source_payload_hash = text_sha256(_canonical_json(record))
        if source_payload_hash in seen_source_payloads:
            continue
        seen_source_payloads.add(source_payload_hash)

        normalized, errors = normalize_generation_record(record)
        if errors:
            quarantine.append(build_quarantine_record(record, normalized, errors))
        else:
            valid.append(normalized)

    return valid, quarantine


def normalize_generation_record(record: dict[str, Any]) -> tuple[dict, list[str]]:
    normalized = _build_llm_request_event(record)
    errors = _validate_normalized_event(normalized)

    observation_type = str(
        _first_present(record, ("type",), ("observationType",), ("observation_type",), default="generation")
    ).lower()
    if observation_type != "generation":
        errors.append("validity:unsupported_observation_type")

    return normalized, errors


def build_quarantine_record(record: dict[str, Any], normalized: dict, errors: list[str]) -> dict:
    return {
        **normalized,
        "_dq_status": "quarantine",
        "_dq_errors": errors,
        "_source_system": "langfuse",
        "_source_entity": "generation",
        "_source_id": str(_first_present(record, ("id",), ("generationId",), ("observationId",), default="")),
        "_source_payload_hash": text_sha256(_canonical_json(record)),
    }


def _build_llm_request_event(record: dict[str, Any]) -> dict:
    metadata = _merged_metadata(record)
    trace = _first_present(record, ("trace",), default={})
    if not isinstance(trace, dict):
        trace = {}

    started_at = _parse_time(
        _first_present(record, ("startTime",), ("start_time",), ("timestamp",), ("createdAt",), ("created_at",))
    )
    ended_at = _parse_time(_first_present(record, ("endTime",), ("end_time",)))
    created_at = started_at
    latency_ms = _latency_ms(started_at, ended_at)
    if latency_ms is None:
        latency_ms = _to_int(_metadata_value(metadata, "latency_ms", "duration_ms"), default=0)

    request_id = str(_first_present(record, ("id",), ("generationId",), ("observationId",), default=""))
    trace_id = str(_first_present(record, ("traceId",), ("trace_id",), ("trace", "id"), default=""))
    input_text = _canonical_text(_first_present(record, ("input",), ("prompt",), ("messages",), default=""))
    output_text = _canonical_text(_first_present(record, ("output",), ("completion",), ("response",), default=""))

    prompt_tokens = _extract_token_count(record, metadata, "prompt")
    completion_tokens = _extract_token_count(record, metadata, "completion")
    total_tokens = _extract_token_count(record, metadata, "total")
    if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
        total_tokens = prompt_tokens + completion_tokens

    status = _normalize_status(record, metadata)
    error_type = _normalize_error_type(record, metadata, status)
    http_status = _to_int(_metadata_value(metadata, "http_status", "status_code"), default=500 if status == "error" else 200)
    cost, cost_source = _extract_cost_with_source(record, metadata)

    event = {
        "request_id": request_id,
        "trace_id": trace_id,
        "run_id": str(_metadata_value(metadata, "run_id", default="")),
        "span_id": str(_first_present(record, ("parentObservationId",), ("parent_observation_id",), default=request_id)),
        "agent_id": str(_metadata_value(metadata, "agent_id", default="")),
        "agent_name": str(_metadata_value(metadata, "agent_name", default="")),
        "channel": str(_metadata_value(metadata, "channel", default="")),
        "user_id": str(
            _first_present(record, ("userId",), ("user_id",), ("trace", "userId"), default=_metadata_value(metadata, "user_id", default=""))
        ),
        "session_id": str(
            _first_present(
                record,
                ("sessionId",),
                ("session_id",),
                ("trace", "sessionId"),
                default=_metadata_value(metadata, "session_id", default=""),
            )
        ),
        "conversation_id": str(_metadata_value(metadata, "conversation_id", default="")),
        "app_name": str(_metadata_value(metadata, "app_name", default=_first_present(record, ("projectName",), default=""))),
        "feature_name": str(_metadata_value(metadata, "feature_name", default=_first_present(trace, ("name",), default=""))),
        "prompt_category": str(_metadata_value(metadata, "prompt_category", default="")),
        "prompt_id": str(_metadata_value(metadata, "prompt_id", "prompt_name", default="")),
        "prompt_version": str(_metadata_value(metadata, "prompt_version", default="")),
        "model_name": str(_first_present(record, ("model",), ("modelName",), default=_metadata_value(metadata, "model_name", default=""))),
        "provider": str(_metadata_value(metadata, "provider", "model_provider", default="")),
        "prompt_hash": text_sha256(input_text) if input_text else "",
        "response_hash": text_sha256(output_text) if output_text else "",
        "input_chars": len(input_text),
        "output_chars": len(output_text),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "request_type": str(_metadata_value(metadata, "request_type", default="chat")),
        "is_streaming": _to_bool(_metadata_value(metadata, "is_streaming", "streaming", default=False)),
        "temperature": _to_float(
            _first_present(record, ("modelParameters", "temperature"), ("model_parameters", "temperature"), default=0.0),
            default=0.0,
        ),
        "max_tokens": _to_int(
            _first_present(record, ("modelParameters", "max_tokens"), ("modelParameters", "maxTokens"), default=0),
            default=0,
        ),
        "finish_reason": str(_metadata_value(metadata, "finish_reason", default="error" if status == "error" else "stop")),
        "retry_count": _to_int(_metadata_value(metadata, "retry_count", default=0), default=0),
        "latency_ms": latency_ms,
        "status": status,
        "error_type": error_type,
        "http_status": http_status,
        "estimated_cost_usd": cost,
        "estimated_cost_source": cost_source,
        "mode": str(_metadata_value(metadata, "mode", default="replay")),
        "region": str(_metadata_value(metadata, "region", default="")),
        "environment": str(_metadata_value(metadata, "environment", "env", default="")),
        "created_at": created_at.isoformat() if created_at else "",
        "date": created_at.date().isoformat() if created_at else "",
    }
    return event


def _validate_normalized_event(event: dict) -> list[str]:
    errors: list[str] = []
    for field in REQUIRED_TEXT_FIELDS:
        if event.get(field) in (None, ""):
            errors.append(f"completeness:missing_{field}")

    prompt_tokens = event.get("prompt_tokens")
    completion_tokens = event.get("completion_tokens")
    total_tokens = event.get("total_tokens")
    if prompt_tokens is None:
        errors.append("completeness:missing_prompt_tokens")
    elif prompt_tokens < 0:
        errors.append("validity:negative_prompt_tokens")
    if completion_tokens is None:
        errors.append("completeness:missing_completion_tokens")
    elif completion_tokens < 0:
        errors.append("validity:negative_completion_tokens")
    if total_tokens is None:
        errors.append("completeness:missing_total_tokens")
    elif prompt_tokens is not None and completion_tokens is not None and total_tokens != prompt_tokens + completion_tokens:
        errors.append("consistency:token_total_mismatch")

    if event.get("latency_ms", 0) <= 0:
        errors.append("validity:non_positive_latency")
    if event.get("status") not in {"success", "error"}:
        errors.append("validity:invalid_status")
    if event.get("estimated_cost_usd", 0.0) < 0:
        errors.append("validity:negative_cost")
    if event.get("mode") not in ALLOWED_LLM_REQUEST_MODES:
        errors.append("validity:invalid_mode")
    if not 100 <= event.get("http_status", 0) <= 599:
        errors.append("validity:invalid_http_status")

    return errors


def _merged_metadata(record: dict[str, Any]) -> dict[str, Any]:
    trace_metadata = _first_present(record, ("trace", "metadata"), default={})
    observation_metadata = _first_present(record, ("metadata",), default={})
    merged: dict[str, Any] = {}
    if isinstance(trace_metadata, dict):
        merged.update(trace_metadata)
    if isinstance(observation_metadata, dict):
        merged.update(observation_metadata)
    return merged


def _extract_token_count(record: dict[str, Any], metadata: dict[str, Any], kind: str) -> int | None:
    aliases = {
        "prompt": ("prompt_tokens", "input_tokens", "promptTokens", "input"),
        "completion": ("completion_tokens", "output_tokens", "completionTokens", "output"),
        "total": ("total_tokens", "totalTokens", "total"),
    }[kind]
    value = _first_present(
        record,
        *[("usageDetails", alias) for alias in aliases],
        *[("usage", alias) for alias in aliases],
        *[("usage_details", alias) for alias in aliases],
        default=_metadata_value(metadata, *aliases, default=None),
    )
    return _to_int(value, default=None)


def _extract_cost(record: dict[str, Any], metadata: dict[str, Any]) -> float:
    return _extract_cost_with_source(record, metadata)[0]


def _extract_cost_with_source(record: dict[str, Any], metadata: dict[str, Any]) -> tuple[float, str]:
    langfuse_cost = _first_present(
        record,
        ("costDetails", "total"),
        ("costDetails", "totalCost"),
        ("cost_details", "total"),
        ("totalCost",),
        default=None,
    )
    if langfuse_cost is not None:
        return _to_float(langfuse_cost, default=0.0), "langfuse_cost_details"

    metadata_cost = _metadata_value(metadata, "estimated_cost_usd", "total_cost_usd", default=None)
    if metadata_cost is not None:
        return _to_float(metadata_cost, default=0.0), "metadata_estimate"

    return 0.0, "default_zero"


def _normalize_status(record: dict[str, Any], metadata: dict[str, Any]) -> str:
    status = str(_metadata_value(metadata, "status", default="")).lower()
    if status in {"success", "error"}:
        return status
    level = str(_first_present(record, ("level",), default="")).lower()
    if level in {"error", "fatal"}:
        return "error"
    return "success"


def _normalize_error_type(record: dict[str, Any], metadata: dict[str, Any], status: str) -> str | None:
    if status != "error":
        return None
    value = _metadata_value(metadata, "error_type", "error", default="")
    if not value:
        value = _first_present(record, ("statusMessage",), ("status_message",), default="generation_error")
    return str(value)


def _latency_ms(started_at: datetime | None, ended_at: datetime | None) -> int | None:
    if started_at is None or ended_at is None:
        return None
    return int((ended_at - started_at).total_seconds() * 1000)


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _canonical_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value
    return _canonical_json(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _first_present(mapping: dict[str, Any], *paths: tuple[str, ...], default: Any = None) -> Any:
    for path in paths:
        value: Any = mapping
        found = True
        for key in path:
            if not isinstance(value, dict) or key not in value:
                found = False
                break
            value = value[key]
        if found and value is not None:
            return value
    return default


def _metadata_value(metadata: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in metadata and metadata[key] is not None:
            return metadata[key]
    return default


def _to_int(value: Any, default: int | None) -> int | None:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "y"}
    return bool(value)
