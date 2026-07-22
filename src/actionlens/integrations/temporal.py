"""Thin Temporal bridge for ActionLens-governed Activities.

Temporal owns workflow replay, scheduling, and signals. ActionLens owns the
governance of the callable executed inside an Activity. This module imports no
Temporal SDK symbols, so the core package remains usable without the optional
runtime installed.
"""
from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Mapping
from typing import Any

from actionlens.models import ToolCallContext


def context_from_temporal_workflow(
    workflow_id: str,
    run_id: str,
    *,
    tool_name: str,
    activity_id: str | None = None,
    call_id: str | None = None,
    attempt: int = 0,
    project: str = "default",
    environment: str = "default",
    tenant_id: str | None = None,
    actor_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ToolCallContext:
    """Create a replay-safe context from Temporal Activity metadata.

    ``workflow_id`` is the stable business-session anchor. ``run_id`` and
    ``attempt`` are retained only for correlation; neither participates in
    ActionLens auto-hash idempotency. Pass Temporal's stable ``activity_id``
    whenever the workflow can call one tool more than once.
    """

    workflow_id = _required_identifier("workflow_id", workflow_id)
    run_id = _required_identifier("run_id", run_id)
    tool_name = _required_identifier("tool_name", tool_name)
    if activity_id is not None and call_id is not None:
        raise ValueError("pass either activity_id or call_id, not both")
    if activity_id is not None:
        activity_id = _required_identifier("activity_id", activity_id)
    if call_id is not None:
        call_id = _required_identifier("call_id", call_id)
    if attempt < 0:
        raise ValueError("attempt must not be negative")
    stable_call_id = call_id or activity_id or f"temporal:{workflow_id}:{run_id}:{tool_name}"
    durable_metadata = _json_safe_mapping(metadata)
    durable_metadata.update(
        {
            "temporal_workflow_id": workflow_id,
            "temporal_run_id": run_id,
            "temporal_activity_id": activity_id,
            "temporal_attempt": attempt,
        }
    )
    return ToolCallContext(
        project=project,
        environment=environment,
        tenant_id=tenant_id,
        session_id=workflow_id,
        run_id=run_id,
        call_id=stable_call_id,
        tool_name=tool_name,
        actor_id=actor_id,
        framework="temporal",
        attempt=attempt,
        context_source="explicit",
        metadata=durable_metadata,
    )


class TemporalActivityRunner:
    """Invoke one ActionLens-governed callable from a Temporal Activity.

    The caller supplies ``activity.heartbeat`` as ``heartbeater`` when desired.
    Activity retries must reuse the same business idempotency key supplied to
    the governed tool; this runner never creates one from a retry attempt.
    """

    def __init__(
        self,
        tool: Callable[..., Any],
        *,
        heartbeater: Callable[[str], Any] | None = None,
    ) -> None:
        if not hasattr(tool, "actionlens_spec"):
            raise TypeError("tool must be wrapped by ActionLens before use in an Activity")
        self.tool = tool
        self.heartbeater = heartbeater

    def run(self, context: ToolCallContext, /, *args: Any, **kwargs: Any) -> Any:
        self._heartbeat("actionlens:preflight")
        result = self.tool(*args, **kwargs, __al_ctx=context)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("async ActionLens tools require await TemporalActivityRunner.arun(...)")
        self._heartbeat("actionlens:completed")
        return result

    async def arun(self, context: ToolCallContext, /, *args: Any, **kwargs: Any) -> Any:
        self._heartbeat("actionlens:preflight")
        result = self.tool(*args, **kwargs, __al_ctx=context)
        if inspect.isawaitable(result):
            result = await result
        self._heartbeat("actionlens:completed")
        return result

    def _heartbeat(self, detail: str) -> None:
        if self.heartbeater is not None:
            result = self.heartbeater(detail)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise TypeError("heartbeater must be synchronous")


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
