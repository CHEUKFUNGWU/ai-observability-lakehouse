from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from app.langfuse_generation import (
    _canonical_json,
    _canonical_text,
    _first_present,
    _metadata_value,
    _parse_time,
    _to_bool,
    _to_float,
    _to_int,
)
from app.llm_event import text_sha256


FEEDBACK_ACTION_TYPES = {"thumbs_up", "thumbs_down", "rating", "regenerate", "edit", "report"}
EVALUATOR_TYPES = {"llm_judge", "human", "ground_truth", "regex", "classifier"}
EVALUATION_DIMENSIONS = {"relevance", "faithfulness", "coherence", "toxicity", "hallucination"}
FEEDBACK_MODES = {"mock", "live"}
EVALUATION_MODES = {"mock", "live", "offline"}

FEEDBACK_SOURCE_HINTS = {"user", "manual", "feedback", "user_feedback", "manual_feedback"}
EVALUATION_SOURCE_HINTS = {
    "automated",
    "auto",
    "dataset-run",
    "dataset_run",
    "eval",
    "evaluation",
    "evaluator",
    "judge",
    "test",
}
FEEDBACK_SIGNAL_HINTS = FEEDBACK_SOURCE_HINTS | FEEDBACK_ACTION_TYPES | {"thumbs", "thumbs-up", "thumbs-down"}
EVALUATION_SIGNAL_HINTS = EVALUATION_SOURCE_HINTS | EVALUATOR_TYPES | EVALUATION_DIMENSIONS | {
    "dataset",
    "groundtruth",
    "ground-truth",
    "pass_threshold",
}


@dataclass
class LangfuseScoreNormalizationResult:
    feedback_actions: list[dict] = field(default_factory=list)
    evaluation_judgments: list[dict] = field(default_factory=list)
    quarantine: list[dict] = field(default_factory=list)


def normalize_score_records(records: Iterable[dict[str, Any]]) -> LangfuseScoreNormalizationResult:
    result = LangfuseScoreNormalizationResult()
    for record in records:
        normalized = normalize_score_record(record)
        result.feedback_actions.extend(normalized.feedback_actions)
        result.evaluation_judgments.extend(normalized.evaluation_judgments)
        result.quarantine.extend(normalized.quarantine)
    return result


def normalize_score_record(
    record: dict[str, Any],
    trace_context: dict[str, Any] | None = None,
) -> LangfuseScoreNormalizationResult:
    result = LangfuseScoreNormalizationResult()
    metadata = _score_metadata(record, trace_context)
    score_class, classification_errors = _classify_score(record, metadata)

    if score_class == "feedback":
        event, errors = _build_feedback_action(record, metadata, trace_context)
        errors = [*classification_errors, *errors]
        if errors:
            result.quarantine.append(_build_quarantine_record(record, trace_context, errors, event))
        else:
            result.feedback_actions.append(event)
    elif score_class == "evaluation":
        event, errors = _build_evaluation_judgment(record, metadata, trace_context)
        errors = [*classification_errors, *errors]
        if errors:
            result.quarantine.append(_build_quarantine_record(record, trace_context, errors, event))
        else:
            result.evaluation_judgments.append(event)
    else:
        normalized = _base_score_context(record, metadata, trace_context)
        result.quarantine.append(
            _build_quarantine_record(record, trace_context, classification_errors or ["validity:unknown_score_classification"], normalized)
        )

    return result


def _score_metadata(record: dict[str, Any], trace_context: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if trace_context:
        trace_metadata = _first_present(trace_context, ("metadata",), default={})
        if isinstance(trace_metadata, dict):
            merged.update(trace_metadata)
    for path in (("config",), ("scoreConfig",), ("score_config",), ("metadata",)):
        value = _first_present(record, path, default={})
        if isinstance(value, dict):
            merged.update(value)
    return merged


def _classify_score(record: dict[str, Any], metadata: dict[str, Any]) -> tuple[str | None, list[str]]:
    source = _normalize_signal(_first_present(record, ("source",), default=_metadata_value(metadata, "source", default="")))
    name = _normalize_signal(_first_present(record, ("name",), ("scoreName",), ("score_name",), default=""))
    signals = _score_signals(record, metadata)

    feedback = source in FEEDBACK_SOURCE_HINTS or any(signal in FEEDBACK_SIGNAL_HINTS for signal in signals)
    evaluation = source in EVALUATION_SOURCE_HINTS or any(signal in EVALUATION_SIGNAL_HINTS for signal in signals)

    if _metadata_value(metadata, "feedback_type", "feedbackType", default=""):
        feedback = True
    if _metadata_value(
        metadata,
        "evaluator_type",
        "evaluatorType",
        "evaluation_dimension",
        "dimension",
        "pass_threshold",
        "passThreshold",
        default="",
    ):
        evaluation = True
    if _metadata_value(metadata, "dataset_run_id", "datasetRunId", "dataset_item_id", "datasetItemId", default=""):
        evaluation = True

    if feedback and evaluation:
        return None, ["consistency:conflicting_score_classification"]
    if feedback:
        return "feedback", []
    if evaluation:
        return "evaluation", []
    if source or name:
        return None, ["validity:unknown_score_classification"]
    return None, ["completeness:missing_score_classification"]


def _score_signals(record: dict[str, Any], metadata: dict[str, Any]) -> set[str]:
    values: list[Any] = [
        _first_present(record, ("source",), default=""),
        _first_present(record, ("name",), ("scoreName",), ("score_name",), default=""),
        _first_present(record, ("dataType",), ("data_type",), default=""),
    ]
    values.extend(metadata.values())
    return {_normalize_signal(value) for value in values if _normalize_signal(value)}


def _build_feedback_action(
    record: dict[str, Any],
    metadata: dict[str, Any],
    trace_context: dict[str, Any] | None,
) -> tuple[dict, list[str]]:
    created_at = _score_time(record)
    feedback_text = _canonical_text(
        _first_present(record, ("comment",), ("reason",), ("text",), default=_metadata_value(metadata, "comment", "reason", default=""))
    )
    score_value = _score_value(record)
    feedback_type = _feedback_type(record, metadata, score_value)
    rating_value = _rating_value(record, metadata, feedback_type, score_value)
    event = {
        **_base_score_context(record, metadata, trace_context),
        "feedback_id": _score_id(record),
        "session_id": _context_value(record, metadata, trace_context, ("sessionId",), ("session_id",), default=""),
        "conversation_id": str(_metadata_value(metadata, "conversation_id", default="")),
        "user_id": _context_value(record, metadata, trace_context, ("userId",), ("user_id",), default=""),
        "agent_id": str(_metadata_value(metadata, "agent_id", default="")),
        "feedback_type": feedback_type,
        "rating_value": rating_value,
        "feedback_text_hash": text_sha256(feedback_text) if feedback_text else "",
        "feedback_text_length": len(feedback_text),
        "response_latency_ms": _to_int(_metadata_value(metadata, "response_latency_ms", "latency_ms", default=1), default=1),
        "model_name": str(_metadata_value(metadata, "model_name", "evaluated_model_name", default="")),
        "prompt_version": str(_metadata_value(metadata, "prompt_version", "evaluated_prompt_version", default="")),
        "mode": _feedback_mode(metadata),
        "created_at": created_at.isoformat() if created_at else "",
        "date": created_at.date().isoformat() if created_at else "",
    }
    errors = _target_errors(event)
    errors.extend(_missing_errors(event, ("feedback_id", "created_at", "date"), "feedback_action"))
    if event["feedback_type"] not in FEEDBACK_ACTION_TYPES:
        errors.append("validity:invalid_feedback_type")
    if event["rating_value"] is not None and not 1 <= event["rating_value"] <= 5:
        errors.append("validity:invalid_rating_value")
    if event["response_latency_ms"] <= 0:
        errors.append("validity:non_positive_response_latency")
    if event["mode"] not in FEEDBACK_MODES:
        errors.append("validity:invalid_mode")
    return event, errors


def _build_evaluation_judgment(
    record: dict[str, Any],
    metadata: dict[str, Any],
    trace_context: dict[str, Any] | None,
) -> tuple[dict, list[str]]:
    created_at = _score_time(record)
    score = _score_value(record)
    pass_threshold = _to_float(_metadata_value(metadata, "pass_threshold", "passThreshold", default=0.5), default=0.5)
    event = {
        **_base_score_context(record, metadata, trace_context),
        "evaluation_id": _score_id(record),
        "evaluator_type": _evaluator_type(record, metadata),
        "evaluator_model": str(_metadata_value(metadata, "evaluator_model", "evaluatorModel", "judge_model", default="")),
        "evaluation_dimension": _evaluation_dimension(record, metadata),
        "score": score,
        "raw_score": str(_first_present(record, ("value",), ("score",), ("numericValue",), ("numeric_value",), default="")),
        "pass_threshold": pass_threshold,
        "passed": _passed(record, metadata, score, pass_threshold),
        "evaluated_model_name": str(_metadata_value(metadata, "evaluated_model_name", "model_name", default="")),
        "evaluated_prompt_version": str(_metadata_value(metadata, "evaluated_prompt_version", "prompt_version", default="")),
        "evaluation_latency_ms": _to_int(_metadata_value(metadata, "evaluation_latency_ms", "latency_ms", default=1), default=1),
        "mode": _evaluation_mode(metadata),
        "created_at": created_at.isoformat() if created_at else "",
        "date": created_at.date().isoformat() if created_at else "",
    }
    errors = _target_errors(event)
    errors.extend(_missing_errors(event, ("evaluation_id", "created_at", "date"), "evaluation_judgment"))
    if score is None:
        errors.append("completeness:missing_score")
    elif not 0.0 <= score <= 1.0:
        errors.append("validity:invalid_score_range")
    if not 0.0 <= pass_threshold <= 1.0:
        errors.append("validity:invalid_pass_threshold")
    if event["evaluator_type"] not in EVALUATOR_TYPES:
        errors.append("validity:invalid_evaluator_type")
    if event["evaluation_dimension"] not in EVALUATION_DIMENSIONS:
        errors.append("validity:invalid_evaluation_dimension")
    if event["evaluation_latency_ms"] <= 0:
        errors.append("validity:non_positive_evaluation_latency")
    if event["mode"] not in EVALUATION_MODES:
        errors.append("validity:invalid_mode")
    return event, errors


def _base_score_context(
    record: dict[str, Any],
    metadata: dict[str, Any],
    trace_context: dict[str, Any] | None,
) -> dict:
    observation_id = str(_first_present(record, ("observationId",), ("observation_id",), default=""))
    request_id = _context_value(record, metadata, trace_context, ("requestId",), ("request_id",), default=observation_id)
    return {
        "trace_id": _context_value(record, metadata, trace_context, ("traceId",), ("trace_id",), default=""),
        "request_id": request_id,
        "run_id": _context_value(record, metadata, trace_context, ("runId",), ("run_id",), default=""),
        "app_name": _context_value(record, metadata, trace_context, ("projectName",), ("app_name",), default=""),
        "feature_name": _context_value(record, metadata, trace_context, ("feature_name",), default=_trace_name(trace_context)),
        "environment": _context_value(record, metadata, trace_context, ("environment",), ("env",), default=""),
    }


def _context_value(
    record: dict[str, Any],
    metadata: dict[str, Any],
    trace_context: dict[str, Any] | None,
    *paths: tuple[str, ...],
    default: Any = "",
) -> str:
    trace = trace_context or {}
    trace_metadata = _first_present(trace, ("metadata",), default={})
    value = _first_present(record, *paths, default=None)
    if value is None:
        value = _metadata_value(metadata, *(path[-1] for path in paths), default=None)
    if value is None and isinstance(trace_metadata, dict):
        value = _metadata_value(trace_metadata, *(path[-1] for path in paths), default=None)
    if value is None and any(path[-1] in {"traceId", "trace_id"} for path in paths):
        value = _first_present(trace, ("id",), ("traceId",), ("trace_id",), default=None)
    if value is None:
        value = _first_present(trace, *paths, default=default)
    return str(value if value is not None else default)


def _feedback_type(record: dict[str, Any], metadata: dict[str, Any], score_value: float | None) -> str:
    explicit = _normalize_signal(_metadata_value(metadata, "feedback_type", "feedbackType", default=""))
    if explicit in FEEDBACK_ACTION_TYPES:
        return explicit
    name = _normalize_signal(_first_present(record, ("name",), ("scoreName",), ("score_name",), default=""))
    if name in FEEDBACK_ACTION_TYPES:
        return name
    if name in {"thumbs-up", "thumbsup", "like"}:
        return "thumbs_up"
    if name in {"thumbs-down", "thumbsdown", "dislike"}:
        return "thumbs_down"
    if name in {"regeneration", "retry"}:
        return "regenerate"
    if name in {"flag", "flagged"}:
        return "report"
    if name == "rating":
        return "rating"
    if score_value is not None:
        return "thumbs_up" if score_value >= 0.5 else "thumbs_down"
    return ""


def _rating_value(
    record: dict[str, Any],
    metadata: dict[str, Any],
    feedback_type: str,
    score_value: float | None,
) -> int | None:
    explicit = _to_int(_metadata_value(metadata, "rating_value", "ratingValue", default=None), default=None)
    if explicit is not None:
        return explicit
    if feedback_type != "rating" or score_value is None:
        return None
    if 1 <= score_value <= 5 and float(score_value).is_integer():
        return int(score_value)
    if 0 <= score_value <= 1:
        return min(5, max(1, round(score_value * 4) + 1))
    return int(score_value)


def _evaluator_type(record: dict[str, Any], metadata: dict[str, Any]) -> str:
    value = _normalize_signal(
        _metadata_value(metadata, "evaluator_type", "evaluatorType", default=_first_present(record, ("source",), default=""))
    )
    if value in EVALUATOR_TYPES:
        return value
    if value in {"judge", "evaluator", "llm", "llm-judge", "llmjudge"}:
        return "llm_judge"
    if value in {"dataset-run", "dataset_run", "test", "ground-truth", "groundtruth"}:
        return "ground_truth"
    if value in {"automated", "auto", "classifier"}:
        return "classifier"
    if value in {"rule", "rules"}:
        return "regex"
    return value


def _evaluation_dimension(record: dict[str, Any], metadata: dict[str, Any]) -> str:
    explicit = _normalize_signal(_metadata_value(metadata, "evaluation_dimension", "dimension", default=""))
    if explicit in EVALUATION_DIMENSIONS:
        return explicit
    name = _normalize_signal(_first_present(record, ("name",), ("scoreName",), ("score_name",), default=""))
    for dimension in EVALUATION_DIMENSIONS:
        if dimension in name:
            return dimension
    return explicit or name


def _passed(record: dict[str, Any], metadata: dict[str, Any], score: float | None, pass_threshold: float) -> bool:
    explicit = _first_present(record, ("passed",), default=_metadata_value(metadata, "passed", default=None))
    if explicit is not None:
        return _to_bool(explicit)
    return bool(score is not None and score >= pass_threshold)


def _score_id(record: dict[str, Any]) -> str:
    value = str(_first_present(record, ("id",), ("scoreId",), ("score_id",), default=""))
    if value:
        return value
    return f"score_{text_sha256(_canonical_json(record))[:16]}"


def _score_value(record: dict[str, Any]) -> float | None:
    value = _first_present(record, ("value",), ("score",), ("numericValue",), ("numeric_value",), default=None)
    if value in (None, ""):
        return None
    return _to_float(value, default=float("nan"))


def _score_time(record: dict[str, Any]):
    return _parse_time(
        _first_present(record, ("timestamp",), ("createdAt",), ("created_at",), ("eventTime",), ("event_time",), default=None)
    )


def _feedback_mode(metadata: dict[str, Any]) -> str:
    value = str(_metadata_value(metadata, "mode", default="live")).lower()
    return value if value in FEEDBACK_MODES else "live"


def _evaluation_mode(metadata: dict[str, Any]) -> str:
    value = str(_metadata_value(metadata, "mode", default="offline")).lower()
    return value if value in EVALUATION_MODES else "offline"


def _target_errors(event: dict) -> list[str]:
    if event.get("trace_id") or event.get("request_id") or event.get("run_id"):
        return []
    return ["completeness:missing_score_target"]


def _missing_errors(event: dict, fields: tuple[str, ...], prefix: str) -> list[str]:
    return [f"completeness:missing_{prefix}_{field}" for field in fields if event.get(field) in (None, "")]


def _build_quarantine_record(
    record: dict[str, Any],
    trace_context: dict[str, Any] | None,
    errors: list[str],
    normalized: dict,
) -> dict:
    return {
        **normalized,
        "_dq_status": "quarantine",
        "_dq_errors": errors,
        "_source_system": "langfuse",
        "_source_entity": "score_event",
        "_source_id": str(_first_present(record, ("id",), ("scoreId",), ("score_id",), default="")),
        "_source_trace_id": _context_value(record, {}, trace_context, ("traceId",), ("trace_id",), default=""),
        "_source_payload_hash": text_sha256(_canonical_json(record)),
    }


def _trace_name(trace_context: dict[str, Any] | None) -> str:
    if not trace_context:
        return ""
    return str(_first_present(trace_context, ("name",), default=""))


def _normalize_signal(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip().lower().replace(" ", "_")
