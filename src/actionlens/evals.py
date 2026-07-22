from __future__ import annotations

import hashlib
import json
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator


EVAL_CASE_SCHEMA = "actionlens.eval-case-candidate.v1"
EVAL_CONTEXTS_SCHEMA = "actionlens.eval-case-contexts.v1"
EVAL_MAPPER_VERSION = "actionlens.mapper.trajectory-to-eval-candidate.v1"
INSPECT_MAPPER_VERSION = "actionlens.mapper.eval-candidate-to-inspect-sample.v1"


class EvalEnvironmentSpec(BaseModel):
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    spec_ref: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)


class OutcomeEvidence(BaseModel):
    kind: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    ref: str | None = None
    observed_state: Any | None = None
    verified_by: str | None = None

    model_config = ConfigDict(frozen=True)


class EvalCaseContext(BaseModel):
    """Host-supplied facts that cannot be reconstructed from a tool trajectory."""

    case_id: str | None = Field(default=None, min_length=1)
    task_input: Any | None = None
    environment_spec: EvalEnvironmentSpec | None = None
    target: Any | None = None
    rubric: dict[str, Any] | None = None
    outcome_evidence: list[OutcomeEvidence] = Field(default_factory=list)
    scorer_version: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class EvalCaseProvenance(BaseModel):
    project: str
    session_id: str
    run_id: str
    source_event_ids: list[str]
    source_files: list[dict[str, str]]
    mapper_version: str = EVAL_MAPPER_VERSION
    redaction_policy_id: str = "actionlens.export.default.v1"
    artifact_bodies_read: bool = False

    model_config = ConfigDict(frozen=True)


class EvalCaseCandidate(BaseModel):
    schema_version: Literal["actionlens.eval-case-candidate.v1"] = EVAL_CASE_SCHEMA
    case_id: str
    state: Literal["READY", "INCOMPLETE"]
    task_input: Any | None = None
    environment_spec: EvalEnvironmentSpec | None = None
    target: Any | None = None
    rubric: dict[str, Any] | None = None
    outcome_evidence: list[OutcomeEvidence] = Field(default_factory=list)
    scorer_version: str | None = None
    trajectory: list[dict[str, Any]] = Field(default_factory=list)
    provenance: EvalCaseProvenance
    missing_fields: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_readiness(self) -> EvalCaseCandidate:
        expected: list[str] = []
        if self.task_input is None:
            expected.append("task_input")
        if self.environment_spec is None:
            expected.append("environment_spec")
        if self.target is None and self.rubric is None:
            expected.append("target_or_rubric")
        if not self.outcome_evidence:
            expected.append("outcome_evidence")
        if not self.scorer_version:
            expected.append("scorer_version")
        expected_state = "INCOMPLETE" if expected else "READY"
        if self.missing_fields != expected or self.state != expected_state:
            raise ValueError(
                f"candidate readiness is inconsistent; expected state={expected_state} "
                f"and missing_fields={expected}"
            )
        return self


def stable_case_id(project: str, session_id: str, run_id: str) -> str:
    payload = json.dumps(
        {"project": project, "run_id": run_id, "session_id": session_id},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"alcase_{hashlib.sha256(payload).hexdigest()[:24]}"


def resolve_case_context(
    contexts: Mapping[str, EvalCaseContext], *, project: str, session_id: str, run_id: str
) -> EvalCaseContext:
    for key in (f"{project}/{session_id}/{run_id}", f"{session_id}/{run_id}", run_id):
        if key in contexts:
            return contexts[key]
    return EvalCaseContext()


def candidate_missing_fields(context: EvalCaseContext) -> list[str]:
    missing: list[str] = []
    if context.task_input is None:
        missing.append("task_input")
    if context.environment_spec is None:
        missing.append("environment_spec")
    if context.target is None and context.rubric is None:
        missing.append("target_or_rubric")
    if not context.outcome_evidence:
        missing.append("outcome_evidence")
    if not context.scorer_version:
        missing.append("scorer_version")
    return missing
