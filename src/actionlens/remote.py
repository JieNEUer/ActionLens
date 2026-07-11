from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from .models import StructuredToolOutput, ToolCallContext


class RemoteToolRequest(BaseModel):
    call_id: str
    idempotency_key: str
    args: dict[str, Any]
    args_hash: str
    tool_schema_hash: str
    context: ToolCallContext
    deadline: datetime | None = None
    capability: str | None = Field(default=None, repr=False)


class RemoteJobRef(BaseModel):
    job_id: str
    submitted_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class RemoteJobStatus(BaseModel):
    status: Literal["SUBMITTED", "ACCEPTED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"]
    progress: float | None = None
    message: str | None = None


class CancelResult(BaseModel):
    accepted: bool
    status: RemoteJobStatus


@runtime_checkable
class RemoteToolRunner(Protocol):
    def submit(self, request: RemoteToolRequest) -> RemoteJobRef: ...
    def status(self, job: RemoteJobRef) -> RemoteJobStatus: ...
    def result(self, job: RemoteJobRef) -> StructuredToolOutput: ...
    def cancel(self, job: RemoteJobRef) -> CancelResult: ...
