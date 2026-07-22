"""Thin DBOS bridge for ActionLens-governed workflow Steps.

DBOS owns workflow checkpointing, durable waits, and queues. ActionLens remains
the authority for tool idempotency, approvals, artifacts, and trajectory facts.
No DBOS SDK import is required for this context mapper or runner.
"""
from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Mapping
from typing import Any

from actionlens.models import ToolCallContext


def context_from_dbos_workflow(
    workflow_id: str,
    *,
    tool_name: str,
    step_id: str | None = None,
    call_id: str | None = None,
    run_id: str | None = None,
    attempt: int = 0,
    project: str = "default",
    environment: str = "default",
    tenant_id: str | None = None,
    actor_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ToolCallContext:
    """Create a replay-safe context from DBOS workflow and Step identity.

    DBOS workflow IDs are the stable ActionLens session anchors. A retry may
    change ``attempt`` for observability, but ActionLens idempotency identity is
    still defined by business parameters or a caller-provided idempotency key.
    """

    workflow_id = _required_identifier("workflow_id", workflow_id)
    tool_name = _required_identifier("tool_name", tool_name)
    if step_id is not None and call_id is not None:
        raise ValueError("pass either step_id or call_id, not both")
    if step_id is not None:
        step_id = _required_identifier("step_id", step_id)
    if call_id is not None:
        call_id = _required_identifier("call_id", call_id)
    if attempt < 0:
        raise ValueError("attempt must not be negative")
    resolved_run_id = _required_identifier("run_id", run_id or workflow_id)
    stable_call_id = call_id or step_id or f"dbos:{workflow_id}:{resolved_run_id}:{tool_name}"
    durable_metadata = _json_safe_mapping(metadata)
    durable_metadata.update(
        {
            "dbos_workflow_id": workflow_id,
            "dbos_step_id": step_id,
            "dbos_attempt": attempt,
        }
    )
    return ToolCallContext(
        project=project,
        environment=environment,
        tenant_id=tenant_id,
        session_id=workflow_id,
        run_id=resolved_run_id,
        call_id=stable_call_id,
        tool_name=tool_name,
        actor_id=actor_id,
        framework="dbos",
        attempt=attempt,
        context_source="explicit",
        metadata=durable_metadata,
    )


class DBOSStepRunner:
    """Invoke one ActionLens-governed callable from a DBOS Step."""

    def __init__(self, tool: Callable[..., Any]) -> None:
        if not hasattr(tool, "actionlens_spec"):
            raise TypeError("tool must be wrapped by ActionLens before use in a DBOS Step")
        self.tool = tool

    def run(self, context: ToolCallContext, /, *args: Any, **kwargs: Any) -> Any:
        result = self.tool(*args, **kwargs, __al_ctx=context)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("async ActionLens tools require await DBOSStepRunner.arun(...)")
        return result

    async def arun(self, context: ToolCallContext, /, *args: Any, **kwargs: Any) -> Any:
        result = self.tool(*args, **kwargs, __al_ctx=context)
        return await result if inspect.isawaitable(result) else result


def _required_identifier(name: str, value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _json_safe_mapping(values: Mapping[str, Any] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in (values or {}).items():
        try:
            json.dumps(value)
            result[str(key)] = value
        except (TypeError, ValueError):
            result[str(key)] = str(value)
    return result
