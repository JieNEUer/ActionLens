from __future__ import annotations

from typing import Any

from .evals import EvalCaseCandidate
from .models import StructuredToolOutput, ToolSpec, TrajectoryEvent

SCHEMA_COMPATIBILITY = {
    "actionlens.tool.v1": {"unknown_fields": "ignore", "enum_extensions": "reader_must_reject_unknown_required_behavior"},
    "actionlens.output.v1": {"unknown_fields": "ignore", "enum_extensions": "additive"},
    "actionlens.event.v1": {"unknown_fields": "ignore", "enum_extensions": "event_type_is_open"},
    "actionlens.inspect-transcript.v1": {"unknown_fields": "ignore", "enum_extensions": "additive"},
    "actionlens.eval-case-candidate.v1": {"unknown_fields": "ignore", "enum_extensions": "additive"},
    "actionlens.inspect-sample.v1": {"unknown_fields": "ignore", "enum_extensions": "additive"},
    "actionlens.evidence-bundle-manifest.v1": {"unknown_fields": "ignore", "enum_extensions": "additive"},
    "actionlens.sft.v1": {"unknown_fields": "ignore", "enum_extensions": "additive"},
}


def read_event(payload: dict[str, Any]) -> TrajectoryEvent:
    _require_schema(payload, "actionlens.event.v1")
    return TrajectoryEvent.model_validate(payload)


def read_output(payload: dict[str, Any]) -> StructuredToolOutput:
    _require_schema(payload, "actionlens.output.v1")
    return StructuredToolOutput.model_validate(payload)


def read_tool_spec(payload: dict[str, Any]) -> ToolSpec:
    _require_schema(payload, "actionlens.tool.v1")
    return ToolSpec.model_validate(payload)


def read_eval_case_candidate(payload: dict[str, Any]) -> EvalCaseCandidate:
    _require_schema(payload, "actionlens.eval-case-candidate.v1")
    return EvalCaseCandidate.model_validate(payload)


def _require_schema(payload: dict[str, Any], expected: str) -> None:
    version = payload.get("schema_version", expected)
    if version != expected:
        raise ValueError(f"unsupported schema_version {version!r}; expected {expected!r}")
