from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RiskLevel(str, Enum):
    READ = "READ"
    EXTERNAL_IO = "EXTERNAL_IO"
    MUTATION = "MUTATION"
    DESTRUCTIVE = "DESTRUCTIVE"


class IdempotencyPolicy(str, Enum):
    OFF = "OFF"
    AUTO_HASH = "AUTO_HASH"
    REQUIRED = "REQUIRED"
    CACHE_READ = "CACHE_READ"


class ConcurrencyPolicy(str, Enum):
    UNKNOWN = "UNKNOWN"
    SAFE = "SAFE"
    UNSAFE = "UNSAFE"


class ArtifactSourceRef(BaseModel):
    """Stable identity of an artifact used as a derived artifact's source.

    A compact identity intentionally replaces a nested ``ArtifactRef`` here.
    Recursive full references duplicate arbitrary lineage at every hop and can
    become unbounded in events, sidecars, and model-visible output.
    """

    uri: str = Field(min_length=1)
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(min_length=1)

    model_config = ConfigDict(frozen=True)

    @field_validator("uri")
    @classmethod
    def _remove_embedded_reference_credentials(cls, value: str) -> str:
        # Provenance must not create another durable copy of a signed URL or
        # embedded credential. Windows drive paths are not URI schemes here.
        if len(value) >= 3 and value[1] == ":":
            return value
        parts = urlsplit(value)
        if not parts.scheme:
            return value
        hostname = parts.hostname or ""
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        try:
            port = parts.port
        except ValueError:
            port = None
        netloc = hostname if port is None else f"{hostname}:{port}"
        return urlunsplit((parts.scheme, netloc, parts.path, "", ""))

    @classmethod
    def from_artifact(cls, artifact: "ArtifactRef") -> "ArtifactSourceRef":
        return cls(
            uri=artifact.uri,
            media_type=artifact.media_type,
            size_bytes=artifact.size_bytes,
            sha256=artifact.sha256,
        )


class ArtifactProvenance(BaseModel):
    """Auditable transformation record for a derived artifact.

    The host owns the actual media processor. ActionLens records the source
    identity and transformation facts without importing FFmpeg, OCR, ASR, or
    model SDKs into the core package.
    """

    source_ref: ArtifactSourceRef
    operation: str = Field(min_length=1)
    operation_version: str = Field(min_length=1)
    parameters: dict[str, Any] = Field(default_factory=dict)
    created_by_tool: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("parameters")
    @classmethod
    def _parameters_must_be_json_safe(cls, value: dict[str, Any]) -> dict[str, Any]:
        try:
            json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("provenance parameters must be JSON-serializable") from exc
        return value

    @classmethod
    def from_source(
        cls,
        source: "ArtifactRef",
        *,
        operation: str,
        operation_version: str,
        parameters: dict[str, Any] | None = None,
        created_by_tool: str | None = None,
    ) -> "ArtifactProvenance":
        return cls(
            source_ref=ArtifactSourceRef.from_artifact(source),
            operation=operation,
            operation_version=operation_version,
            parameters=parameters or {},
            created_by_tool=created_by_tool,
        )


class MediaMetadata(BaseModel):
    """Optional media facts for safe, metadata-first artifact selection."""

    width: int | None = Field(default=None, ge=0)
    height: int | None = Field(default=None, ge=0)
    duration_sec: float | None = Field(default=None, ge=0)
    codec: str | None = Field(default=None, min_length=1)
    fps: float | None = Field(default=None, ge=0)
    sample_rate: int | None = Field(default=None, ge=0)
    channels: int | None = Field(default=None, ge=0)

    model_config = ConfigDict(allow_inf_nan=False)


class ArtifactRef(BaseModel):
    uri: str
    media_type: str
    size_bytes: int
    sha256: str
    preview: str | None = None
    redacted: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime | None = None
    confidentiality: dict[str, Any] = Field(default_factory=dict)
    media_metadata: MediaMetadata | None = None
    provenance: ArtifactProvenance | None = None


class ArtifactPolicy(BaseModel):
    policy_id: str = "default"
    raw_mode: Literal["store", "redact_then_store", "reference_only", "deny"] = "store"
    encryption: Literal["none", "provider"] = "none"
    max_bytes_per_run: int | None = None
    allowed_media_types: list[str] | None = None
    retention_days: int | None = None
    reference_schemes: list[str] = Field(default_factory=lambda: ["https", "s3", "gs", "az"])

    model_config = ConfigDict(frozen=True)


class ErrorRecord(BaseModel):
    taxonomy: str
    message: str
    type_name: str | None = None
    retryable: bool = False


class ApprovalTicket(BaseModel):
    ticket_id: str
    idempotency_key: str | None = None
    call_id: str
    tool_name: str
    safe_args: dict[str, Any]
    risk: RiskLevel
    reason: str
    status: Literal["PENDING", "APPROVED", "DENIED", "EXPIRED"] = "PENDING"
    approved_by: str | None = None
    decision_note: str | None = None
    modified_args: dict[str, Any] | None = None
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    approved_at: datetime | None = None
    expires_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PolicyDecision(BaseModel):
    action: Literal["ALLOW", "DENY", "MODIFY_ARGS", "PENDING_APPROVAL"]
    reason: str
    modified_args: dict[str, Any] | None = None
    approval_ticket: ApprovalTicket | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class OutputPolicy(BaseModel):
    max_inline_bytes: int = 4096
    max_inline_items: int = 50
    artifact_threshold_bytes: int = 8192
    redact_keys: list[str] = Field(
        default_factory=lambda: ["password", "token", "secret", "authorization"]
    )
    redact_patterns: list[str] = Field(default_factory=list)
    include_raw_in_trajectory: bool = False
    summary_fields: list[str] | None = None
    streaming_tail_lines: int = 80


class ToolCallContext(BaseModel):
    project: str
    session_id: str
    run_id: str
    call_id: str
    tool_name: str
    environment: str = "default"
    tenant_id: str | None = None
    parent_call_id: str | None = None
    actor_id: str | None = None
    framework: str | None = None
    attempt: int = 0
    context_source: Literal["explicit", "contextvar", "generated"] = "generated"
    metadata: dict[str, Any] = Field(default_factory=dict)


class StructuredToolOutput(BaseModel):
    schema_version: str = "actionlens.output.v1"
    status: Literal[
        "SUCCESS",
        "FAILED",
        "SKIPPED",
        "PENDING_APPROVAL",
        "DENIED",
        "TIMEOUT",
        "UNCERTAIN",
    ]
    result_summary: str
    result: Any | None = None
    artifact_refs: list[ArtifactRef] = Field(default_factory=list)
    error_taxonomy: str | None = None
    recovery_hint: str | None = None
    governance: dict[str, Any] = Field(default_factory=dict)


class TrajectoryEvent(BaseModel):
    schema_version: str = "actionlens.event.v1"
    event_id: str
    timestamp: datetime
    project: str
    session_id: str
    run_id: str
    sequence: int
    event_type: str
    phase: Literal["INSTRUMENT", "PRE_FLIGHT", "EXECUTION", "POST_FLIGHT", "EXPORT"]
    call_id: str | None = None
    tool_name: str | None = None
    input_ref: ArtifactRef | None = None
    output_ref: ArtifactRef | None = None
    error: ErrorRecord | None = None
    decision: PolicyDecision | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OutboxRecord(BaseModel):
    delivery_id: str
    event: TrajectoryEvent
    attempt: int = 0
    next_retry_at: datetime
    last_error: str | None = None
    delivered_at: datetime | None = None
    claimed_by: str | None = None
    claim_expires_at: datetime | None = None
    dead_letter_at: datetime | None = None
    terminated_at: datetime | None = None


class ToolSpec(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    description: str | None = None
    risk: RiskLevel = RiskLevel.READ
    idempotency: IdempotencyPolicy = IdempotencyPolicy.OFF
    idempotency_key_param: str = "idempotency_key"
    hash_ignore_keys: list[str] = Field(default_factory=list)
    idempotency_key_fn: Callable[[dict[str, Any], ToolCallContext], str] | None = Field(
        default=None, exclude=True
    )
    timeout_sec: float | None = None
    run_sync_in_thread: bool = False
    concurrency: ConcurrencyPolicy = ConcurrencyPolicy.UNKNOWN
    approval_required: bool = False
    approval_ttl_sec: float | None = 86400.0
    lease_seconds: float = 30.0
    fencing_supported: bool = False
    output: OutputPolicy = Field(default_factory=OutputPolicy)
    tags: dict[str, str] = Field(default_factory=dict)
    schema_version: str = "actionlens.tool.v1"
