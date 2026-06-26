from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Iterable

from app.langfuse_generation import (
    _canonical_json,
    _canonical_text,
    _extract_cost,
    _extract_token_count,
    _first_present,
    _latency_ms,
    _merged_metadata,
    _metadata_value,
    _normalize_error_type,
    _normalize_status,
    _parse_time,
    _to_float,
    _to_int,
)
from app.langfuse_score import normalize_score_record
from app.llm_event import text_sha256


SPAN_OBSERVATION_TYPES = {"span", "chain"}
TOOL_OBSERVATION_TYPES = {"tool"}
RETRIEVER_OBSERVATION_TYPES = {"retriever", "retrieval"}
SUPPORTED_OBSERVATION_TYPES = SPAN_OBSERVATION_TYPES | TOOL_OBSERVATION_TYPES | RETRIEVER_OBSERVATION_TYPES | {
    "generation"
}

RETRIEVAL_REQUIRED_FIELDS = (
    "retrieval_id",
    "trace_id",
    "app_name",
    "feature_name",
    "user_id",
    "knowledge_base_id",
    "embedding_model",
    "retrieval_strategy",
    "top_k",
    "returned_count",
    "hit_count",
    "total_latency_ms",
    "status",
    "environment",
    "created_at",
    "date",
)


@dataclass
class LangfuseTraceNormalizationResult:
    trace_envelopes: list[dict] = field(default_factory=list)
    agent_runs: list[dict] = field(default_factory=list)
    agent_spans: list[dict] = field(default_factory=list)
    agent_tool_calls: list[dict] = field(default_factory=list)
    retrieval_requests: list[dict] = field(default_factory=list)
    feedback_actions: list[dict] = field(default_factory=list)
    evaluation_judgments: list[dict] = field(default_factory=list)
    quarantine: list[dict] = field(default_factory=list)


def normalize_trace_records(records: Iterable[dict[str, Any]]) -> LangfuseTraceNormalizationResult:
    result = LangfuseTraceNormalizationResult()
    for record in records:
        normalized = normalize_trace_record(record)
        result.trace_envelopes.extend(normalized.trace_envelopes)
        result.agent_runs.extend(normalized.agent_runs)
        result.agent_spans.extend(normalized.agent_spans)
        result.agent_tool_calls.extend(normalized.agent_tool_calls)
        result.retrieval_requests.extend(normalized.retrieval_requests)
        result.feedback_actions.extend(normalized.feedback_actions)
        result.evaluation_judgments.extend(normalized.evaluation_judgments)
        result.quarantine.extend(normalized.quarantine)
    return result


def normalize_trace_record(record: dict[str, Any]) -> LangfuseTraceNormalizationResult:
    trace = _trace_payload(record)
    trace_metadata = _trace_metadata(trace)
    observations = _trace_observations(record)
    trace_id = str(_first_present(trace, ("id",), ("traceId",), ("trace_id",), default=""))

    result = LangfuseTraceNormalizationResult()
    envelope, envelope_errors = _build_trace_envelope(trace, trace_metadata, observations)
    if envelope_errors:
        result.quarantine.append(_build_quarantine_record("trace", record, trace_id, envelope_errors, envelope))
        return result
    result.trace_envelopes.append(envelope)

    run_context = _run_context(trace, trace_metadata)
    if run_context:
        run_event, run_errors = _build_agent_run(trace, trace_metadata, observations, run_context)
        if run_errors:
            result.quarantine.append(_build_quarantine_record("trace", record, trace_id, run_errors, run_event))
        else:
            result.agent_runs.append(run_event)

    span_order = 0
    for observation in observations:
        observation_type = _observation_type(observation)
        observation_metadata = _observation_metadata(trace, observation)
        if observation_type in SPAN_OBSERVATION_TYPES:
            span_order += 1
            span_event, errors = _build_agent_span(observation, observation_metadata, trace, run_context, span_order)
            if errors:
                result.quarantine.append(
                    _build_quarantine_record("observation", observation, trace_id, errors, span_event)
                )
            else:
                result.agent_spans.append(span_event)
        elif observation_type in TOOL_OBSERVATION_TYPES:
            tool_event, errors = _build_agent_tool_call(observation, observation_metadata, trace, run_context)
            if errors:
                result.quarantine.append(
                    _build_quarantine_record("observation", observation, trace_id, errors, tool_event)
                )
            else:
                result.agent_tool_calls.append(tool_event)
        elif observation_type in RETRIEVER_OBSERVATION_TYPES:
            retrieval_event, errors = _build_retrieval_request(observation, observation_metadata, trace, run_context)
            if errors:
                result.quarantine.append(
                    _build_quarantine_record("observation", observation, trace_id, errors, retrieval_event)
                )
            else:
                result.retrieval_requests.append(retrieval_event)
        elif observation_type not in SUPPORTED_OBSERVATION_TYPES:
            result.quarantine.append(
                _build_quarantine_record(
                    "observation",
                    observation,
                    trace_id,
                    [f"validity:unsupported_observation_type:{observation_type or 'missing'}"],
                    {"trace_id": trace_id, "_source_id": _observation_id(observation)},
                )
            )

    for score in _trace_scores(record, trace):
        normalized_score = normalize_score_record(score, trace_context=trace)
        result.feedback_actions.extend(normalized_score.feedback_actions)
        result.evaluation_judgments.extend(normalized_score.evaluation_judgments)
        result.quarantine.extend(normalized_score.quarantine)

    return result


def _trace_payload(record: dict[str, Any]) -> dict[str, Any]:
    trace = _first_present(record, ("trace",), default=record)
    return trace if isinstance(trace, dict) else record


def _trace_metadata(trace: dict[str, Any]) -> dict[str, Any]:
    metadata = _first_present(trace, ("metadata",), default={})
    return metadata if isinstance(metadata, dict) else {}


def _trace_observations(record: dict[str, Any]) -> list[dict[str, Any]]:
    observations = _first_present(record, ("observations",), ("trace", "observations"), default=[])
    if not isinstance(observations, list):
        return []
    return [observation for observation in observations if isinstance(observation, dict)]


def _trace_scores(record: dict[str, Any], trace: dict[str, Any]) -> list[dict[str, Any]]:
    scores = _first_present(record, ("scores",), ("scoreEvents",), ("score_events",), default=None)
    if scores is None:
        scores = _first_present(trace, ("scores",), ("scoreEvents",), ("score_events",), default=[])
    if not isinstance(scores, list):
        return []
    return [score for score in scores if isinstance(score, dict)]


def _build_trace_envelope(
    trace: dict[str, Any],
    metadata: dict[str, Any],
    observations: list[dict[str, Any]],
) -> tuple[dict, list[str]]:
    started_at, ended_at = _trace_time_bounds(trace, observations)
    trace_id = str(_first_present(trace, ("id",), ("traceId",), ("trace_id",), default=""))
    run_context = _run_context(trace, metadata)
    envelope = {
        "trace_id": trace_id,
        "source_system": "langfuse",
        "source_entity": "trace",
        "trace_name": str(_first_present(trace, ("name",), default="")),
        "run_id": run_context["run_id"] if run_context else "",
        "has_run_metadata": run_context is not None,
        "app_name": str(_metadata_value(metadata, "app_name", default="")),
        "feature_name": str(_metadata_value(metadata, "feature_name", default=_first_present(trace, ("name",), default=""))),
        "user_id": str(_first_present(trace, ("userId",), ("user_id",), default=_metadata_value(metadata, "user_id", default=""))),
        "session_id": str(
            _first_present(trace, ("sessionId",), ("session_id",), default=_metadata_value(metadata, "session_id", default=""))
        ),
        "environment": str(_metadata_value(metadata, "environment", "env", default="")),
        "created_at": started_at.isoformat() if started_at else "",
        "date": started_at.date().isoformat() if started_at else "",
        "observation_count": len(observations),
    }
    return envelope, _missing_errors(envelope, ("trace_id", "created_at", "date"), "trace_envelope")


def _run_context(trace: dict[str, Any], metadata: dict[str, Any]) -> dict[str, str] | None:
    run_id = str(_metadata_value(metadata, "run_id", "agent_run_id", default=""))
    task_type = str(_metadata_value(metadata, "task_type", "agent_task_type", default=""))
    if not run_id or not task_type:
        return None
    return {"run_id": run_id, "task_type": task_type}


def _build_agent_run(
    trace: dict[str, Any],
    metadata: dict[str, Any],
    observations: list[dict[str, Any]],
    run_context: dict[str, str],
) -> tuple[dict, list[str]]:
    started_at, ended_at = _trace_time_bounds(trace, observations)
    duration_ms = _latency_ms(started_at, ended_at)
    input_text = _canonical_text(_first_present(trace, ("input",), default=_metadata_value(metadata, "input", default="")))
    output_text = _canonical_text(_first_present(trace, ("output",), default=_metadata_value(metadata, "output", default="")))
    status = _trace_status(trace, observations, metadata)
    tool_names = sorted(
        {
            str(_metadata_value(_observation_metadata(trace, observation), "tool_name", default=_first_present(observation, ("name",), default="")))
            for observation in observations
            if _observation_type(observation) in TOOL_OBSERVATION_TYPES
        }
        - {""}
    )
    event = {
        "run_id": run_context["run_id"],
        "trace_id": str(_first_present(trace, ("id",), ("traceId",), ("trace_id",), default="")),
        "agent_id": str(_metadata_value(metadata, "agent_id", default="")),
        "agent_name": str(_metadata_value(metadata, "agent_name", default="")),
        "agent_version": str(_metadata_value(metadata, "agent_version", default="")),
        "app_name": str(_metadata_value(metadata, "app_name", default="")),
        "user_id": str(_first_present(trace, ("userId",), ("user_id",), default=_metadata_value(metadata, "user_id", default=""))),
        "session_id": str(
            _first_present(trace, ("sessionId",), ("session_id",), default=_metadata_value(metadata, "session_id", default=""))
        ),
        "conversation_id": str(_metadata_value(metadata, "conversation_id", default="")),
        "task_type": run_context["task_type"],
        "channel": str(_metadata_value(metadata, "channel", default="")),
        "toolsets_used": json.dumps(tool_names, ensure_ascii=False),
        "input_text_hash": text_sha256(input_text) if input_text else "",
        "output_text_hash": text_sha256(output_text) if output_text else "",
        "start_time": started_at.isoformat() if started_at else "",
        "end_time": ended_at.isoformat() if ended_at else "",
        "duration_ms": duration_ms if duration_ms is not None else 0,
        "status": status,
        "error_type": _trace_error_type(trace, observations, metadata, status) or "",
        "turn_count": _to_int(_metadata_value(metadata, "turn_count", default=0), default=0),
        "llm_call_count": sum(1 for observation in observations if _observation_type(observation) == "generation"),
        "tool_call_count": sum(1 for observation in observations if _observation_type(observation) in TOOL_OBSERVATION_TYPES),
        "retrieval_count": sum(1 for observation in observations if _observation_type(observation) in RETRIEVER_OBSERVATION_TYPES),
        "total_tokens": sum(_observation_total_tokens(observation) for observation in observations),
        "estimated_cost_usd": sum(_observation_cost(observation) for observation in observations),
        "mode": str(_metadata_value(metadata, "mode", default="replay")),
        "region": str(_metadata_value(metadata, "region", default="")),
        "environment": str(_metadata_value(metadata, "environment", "env", default="")),
        "created_at": started_at.isoformat() if started_at else "",
        "date": started_at.date().isoformat() if started_at else "",
    }
    required = (
        "run_id",
        "trace_id",
        "agent_id",
        "agent_name",
        "app_name",
        "user_id",
        "session_id",
        "task_type",
        "start_time",
        "end_time",
        "created_at",
        "date",
        "environment",
    )
    errors = _missing_errors(event, required, "agent_run")
    if event["duration_ms"] <= 0:
        errors.append("validity:non_positive_duration")
    if event["status"] not in {"success", "error"}:
        errors.append("validity:invalid_status")
    return event, errors


def _build_agent_span(
    observation: dict[str, Any],
    metadata: dict[str, Any],
    trace: dict[str, Any],
    run_context: dict[str, str] | None,
    span_order: int,
) -> tuple[dict, list[str]]:
    started_at, ended_at, duration_ms = _observation_time_bounds(observation, metadata)
    status = _normalize_status(observation, metadata)
    event = {
        "span_id": _observation_id(observation),
        "parent_span_id": _parent_observation_id(observation) or None,
        "run_id": run_context["run_id"] if run_context else str(_metadata_value(metadata, "run_id", default="")),
        "trace_id": _trace_id(trace, observation),
        "agent_id": str(_metadata_value(metadata, "agent_id", default="")),
        "span_name": str(_first_present(observation, ("name",), default="")),
        "span_type": _observation_type(observation),
        "span_order": span_order,
        "start_time": started_at.isoformat() if started_at else "",
        "end_time": ended_at.isoformat() if ended_at else "",
        "duration_ms": duration_ms if duration_ms is not None else 0,
        "status": status,
        "error_type": _normalize_error_type(observation, metadata, status) or "",
        "retry_count": _to_int(_metadata_value(metadata, "retry_count", default=0), default=0),
        "input_size": len(_canonical_text(_first_present(observation, ("input",), default=""))),
        "output_size": len(_canonical_text(_first_present(observation, ("output",), default=""))),
        "model_name": _optional_string(_first_present(observation, ("model",), default=_metadata_value(metadata, "model_name", default=""))),
        "tool_name": _optional_string(_metadata_value(metadata, "tool_name", default="")),
        "mode": str(_metadata_value(metadata, "mode", default="replay")),
        "region": str(_metadata_value(metadata, "region", default="")),
        "environment": str(_metadata_value(metadata, "environment", "env", default="")),
        "created_at": started_at.isoformat() if started_at else "",
        "date": started_at.date().isoformat() if started_at else "",
    }
    required = ("span_id", "trace_id", "span_name", "span_type", "start_time", "end_time", "created_at", "date")
    errors = _missing_errors(event, required, "agent_span")
    if event["duration_ms"] <= 0:
        errors.append("validity:non_positive_duration")
    return event, errors


def _build_agent_tool_call(
    observation: dict[str, Any],
    metadata: dict[str, Any],
    trace: dict[str, Any],
    run_context: dict[str, str] | None,
) -> tuple[dict, list[str]]:
    started_at, _, duration_ms = _observation_time_bounds(observation, metadata)
    status = _normalize_status(observation, metadata)
    tool_name = str(_metadata_value(metadata, "tool_name", default=_first_present(observation, ("name",), default="")))
    arguments = _first_present(observation, ("input",), default=_metadata_value(metadata, "arguments", "args", default={}))
    result_text = _canonical_text(_first_present(observation, ("output",), default=""))
    event = {
        "tool_call_id": _observation_id(observation),
        "span_id": _parent_observation_id(observation) or _observation_id(observation),
        "run_id": run_context["run_id"] if run_context else str(_metadata_value(metadata, "run_id", default="")),
        "trace_id": _trace_id(trace, observation),
        "agent_id": str(_metadata_value(metadata, "agent_id", default="")),
        "tool_name": tool_name,
        "tool_type": str(_metadata_value(metadata, "tool_type", default="function")),
        "arguments_json": _canonical_json(arguments),
        "result_text": result_text,
        "result_size": len(result_text),
        "duration_ms": duration_ms if duration_ms is not None else 0,
        "status": status,
        "error_type": _normalize_error_type(observation, metadata, status) or "",
        "retry_count": _to_int(_metadata_value(metadata, "retry_count", default=0), default=0),
        "mode": str(_metadata_value(metadata, "mode", default="replay")),
        "region": str(_metadata_value(metadata, "region", default="")),
        "environment": str(_metadata_value(metadata, "environment", "env", default="")),
        "created_at": started_at.isoformat() if started_at else "",
        "date": started_at.date().isoformat() if started_at else "",
    }
    required = (
        "tool_call_id",
        "span_id",
        "trace_id",
        "tool_name",
        "tool_type",
        "duration_ms",
        "status",
        "environment",
        "created_at",
        "date",
    )
    errors = _missing_errors(event, required, "agent_tool_call")
    if event["duration_ms"] <= 0:
        errors.append("validity:non_positive_duration")
    return event, errors


def _build_retrieval_request(
    observation: dict[str, Any],
    metadata: dict[str, Any],
    trace: dict[str, Any],
    run_context: dict[str, str] | None,
) -> tuple[dict, list[str]]:
    started_at, _, duration_ms = _observation_time_bounds(observation, metadata)
    query_text = _canonical_text(_first_present(observation, ("input",), default=_metadata_value(metadata, "query", "query_text", default="")))
    scores = _similarity_scores(observation, metadata)
    returned_count = _to_int(
        _metadata_value(metadata, "returned_count", "returned_cnt", default=_first_present(observation, ("output", "count"), default=None)),
        default=None,
    )
    if returned_count is None:
        output = _first_present(observation, ("output",), default=None)
        returned_count = len(output) if isinstance(output, list) else 0
    hit_count = _to_int(_metadata_value(metadata, "hit_count", "hit_cnt", default=returned_count), default=0)
    event = {
        "retrieval_id": _observation_id(observation),
        "trace_id": _trace_id(trace, observation),
        "run_id": run_context["run_id"] if run_context else str(_metadata_value(metadata, "run_id", default="")),
        "span_id": _parent_observation_id(observation) or _observation_id(observation),
        "request_id": str(_metadata_value(metadata, "request_id", default="")),
        "agent_id": str(_metadata_value(metadata, "agent_id", default="")),
        "app_name": str(_metadata_value(metadata, "app_name", default="")),
        "feature_name": str(_metadata_value(metadata, "feature_name", default=_first_present(trace, ("name",), default=""))),
        "user_id": str(_first_present(trace, ("userId",), ("user_id",), default=_metadata_value(metadata, "user_id", default=""))),
        "knowledge_base_id": str(_metadata_value(metadata, "knowledge_base_id", "kb_id", default="")),
        "knowledge_base_name": str(_metadata_value(metadata, "knowledge_base_name", "kb_name", default="")),
        "embedding_model": str(_metadata_value(metadata, "embedding_model", default="")),
        "retrieval_strategy": str(_metadata_value(metadata, "retrieval_strategy", "strategy", default="")),
        "query_text_hash": text_sha256(query_text) if query_text else "",
        "query_length": len(query_text),
        "top_k": _to_int(_metadata_value(metadata, "top_k", default=None), default=0),
        "returned_count": returned_count,
        "hit_count": hit_count,
        "max_similarity_score": max(scores) if scores else _to_float(_metadata_value(metadata, "max_similarity_score", default=0.0), default=0.0),
        "min_similarity_score": min(scores) if scores else _to_float(_metadata_value(metadata, "min_similarity_score", default=0.0), default=0.0),
        "avg_similarity_score": (sum(scores) / len(scores))
        if scores
        else _to_float(_metadata_value(metadata, "avg_similarity_score", default=0.0), default=0.0),
        "embedding_latency_ms": _to_int(_metadata_value(metadata, "embedding_latency_ms", default=0), default=0),
        "search_latency_ms": _to_int(_metadata_value(metadata, "search_latency_ms", default=duration_ms or 0), default=0),
        "total_latency_ms": _to_int(_metadata_value(metadata, "total_latency_ms", "latency_ms", default=duration_ms or 0), default=0),
        "status": _normalize_status(observation, metadata),
        "error_type": None,
        "mode": str(_metadata_value(metadata, "mode", default="replay")),
        "environment": str(_metadata_value(metadata, "environment", "env", default="")),
        "created_at": started_at.isoformat() if started_at else "",
        "date": started_at.date().isoformat() if started_at else "",
    }
    event["error_type"] = _normalize_error_type(observation, metadata, event["status"]) or ""
    errors = _missing_errors(event, RETRIEVAL_REQUIRED_FIELDS, "retrieval_request")
    if not event["query_text_hash"]:
        errors.append("completeness:missing_query_text")
    if event["top_k"] <= 0:
        errors.append("validity:non_positive_top_k")
    if event["returned_count"] < 0:
        errors.append("validity:negative_returned_count")
    if event["hit_count"] < 0:
        errors.append("validity:negative_hit_count")
    if event["hit_count"] > event["returned_count"]:
        errors.append("consistency:hit_count_exceeds_returned_count")
    if event["total_latency_ms"] <= 0:
        errors.append("validity:non_positive_total_latency")
    return event, errors


def _observation_metadata(trace: dict[str, Any], observation: dict[str, Any]) -> dict[str, Any]:
    merged = dict(_trace_metadata(trace))
    observation_metadata = _merged_metadata(observation)
    merged.update(observation_metadata)
    return merged


def _observation_type(observation: dict[str, Any]) -> str:
    return str(_first_present(observation, ("type",), ("observationType",), ("observation_type",), default="")).lower()


def _observation_id(observation: dict[str, Any]) -> str:
    return str(_first_present(observation, ("id",), ("observationId",), ("observation_id",), default=""))


def _parent_observation_id(observation: dict[str, Any]) -> str:
    return str(_first_present(observation, ("parentObservationId",), ("parent_observation_id",), default=""))


def _trace_id(trace: dict[str, Any], observation: dict[str, Any]) -> str:
    return str(_first_present(observation, ("traceId",), ("trace_id",), default=_first_present(trace, ("id",), ("traceId",), ("trace_id",), default="")))


def _trace_time_bounds(
    trace: dict[str, Any],
    observations: list[dict[str, Any]],
) -> tuple[datetime | None, datetime | None]:
    started_at = _parse_time(
        _first_present(trace, ("startTime",), ("start_time",), ("timestamp",), ("createdAt",), ("created_at",))
    )
    ended_at = _parse_time(_first_present(trace, ("endTime",), ("end_time",)))
    observation_bounds = [_observation_time_bounds(observation, _observation_metadata(trace, observation)) for observation in observations]
    starts = [bounds[0] for bounds in observation_bounds if bounds[0] is not None]
    ends = [bounds[1] for bounds in observation_bounds if bounds[1] is not None]
    if started_at is None and starts:
        started_at = min(starts)
    if ended_at is None and ends:
        ended_at = max(ends)
    return started_at, ended_at


def _observation_time_bounds(
    observation: dict[str, Any],
    metadata: dict[str, Any],
) -> tuple[datetime | None, datetime | None, int | None]:
    started_at = _parse_time(
        _first_present(observation, ("startTime",), ("start_time",), ("timestamp",), ("createdAt",), ("created_at",))
    )
    ended_at = _parse_time(_first_present(observation, ("endTime",), ("end_time",)))
    duration_ms = _latency_ms(started_at, ended_at)
    if duration_ms is None:
        duration_ms = _to_int(_metadata_value(metadata, "duration_ms", "latency_ms", default=None), default=None)
    if ended_at is None and started_at is not None and duration_ms is not None:
        ended_at = started_at + timedelta(milliseconds=duration_ms)
    return started_at, ended_at, duration_ms


def _trace_status(trace: dict[str, Any], observations: list[dict[str, Any]], metadata: dict[str, Any]) -> str:
    status = str(_metadata_value(metadata, "status", default="")).lower()
    if status in {"success", "error"}:
        return status
    level = str(_first_present(trace, ("level",), default="")).lower()
    if level in {"error", "fatal"}:
        return "error"
    if any(_normalize_status(observation, _observation_metadata(trace, observation)) == "error" for observation in observations):
        return "error"
    return "success"


def _trace_error_type(
    trace: dict[str, Any],
    observations: list[dict[str, Any]],
    metadata: dict[str, Any],
    status: str,
) -> str | None:
    if status != "error":
        return None
    value = _metadata_value(metadata, "error_type", "error", default="")
    if value:
        return str(value)
    for observation in observations:
        error_type = _normalize_error_type(observation, _observation_metadata(trace, observation), _normalize_status(observation, _observation_metadata(trace, observation)))
        if error_type:
            return error_type
    return str(_first_present(trace, ("statusMessage",), ("status_message",), default="trace_error"))


def _observation_total_tokens(observation: dict[str, Any]) -> int:
    metadata = _merged_metadata(observation)
    return _extract_token_count(observation, metadata, "total") or 0


def _observation_cost(observation: dict[str, Any]) -> float:
    return _extract_cost(observation, _merged_metadata(observation))


def _similarity_scores(observation: dict[str, Any], metadata: dict[str, Any]) -> list[float]:
    values = _metadata_value(metadata, "similarity_scores", "scores", default=None)
    if values is None:
        output = _first_present(observation, ("output",), default=None)
        if isinstance(output, list):
            values = [
                _first_present(item, ("score",), ("similarity_score",), default=None)
                for item in output
                if isinstance(item, dict)
            ]
    if not isinstance(values, list):
        return []
    scores: list[float] = []
    for value in values:
        score = _to_float(value, default=-1.0)
        if score >= 0:
            scores.append(score)
    return scores


def _missing_errors(event: dict, fields: tuple[str, ...], prefix: str) -> list[str]:
    errors: list[str] = []
    for field_name in fields:
        if event.get(field_name) in (None, ""):
            errors.append(f"completeness:missing_{prefix}_{field_name}")
    return errors


def _build_quarantine_record(
    source_entity: str,
    source: dict[str, Any],
    trace_id: str,
    errors: list[str],
    normalized: dict,
) -> dict:
    return {
        **normalized,
        "_dq_status": "quarantine",
        "_dq_errors": errors,
        "_source_system": "langfuse",
        "_source_entity": source_entity,
        "_source_id": str(_first_present(source, ("id",), ("observationId",), ("traceId",), default="")),
        "_source_trace_id": trace_id,
        "_source_payload_hash": text_sha256(_canonical_json(source)),
    }


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return ""
    return str(value)
