from __future__ import annotations

import functools
import inspect
import json
from collections.abc import Callable, Mapping
from typing import Any, Literal, get_type_hints
from uuid import uuid4

from pydantic import ConfigDict, create_model

from actionlens.context import get_current_context
from actionlens.models import StructuredToolOutput, ToolCallContext
from actionlens.repository import canonical_operation_hash


class ActionLensToolAdapter:
    """Framework-neutral bridge around an ActionLens-governed callable."""

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        framework: str,
        output_mode: Literal["model", "dict", "json"] = "json",
    ) -> None:
        if not hasattr(func, "actionlens_spec"):
            raise TypeError("func must be wrapped by ActionLens before creating an adapter")
        self.func = func
        self.spec = func.actionlens_spec
        self.framework = framework
        self.output_mode = output_mode
        self.name = self.spec.name
        self.description = self.spec.description or inspect.getdoc(func) or ""
        self.parameters_json_schema = signature_json_schema(func)
        self.callable = self._build_callable()

    def invoke(
        self,
        arguments: Mapping[str, Any],
        *,
        framework_context: Any = None,
        actionlens_context: ToolCallContext | None = None,
    ) -> StructuredToolOutput | dict[str, Any] | str:
        context = actionlens_context
        if context is None and framework_context is not None:
            context = context_from_framework(framework_context, framework=self.framework, tool_name=self.name)
        payload = dict(arguments)
        if "__al_ctx" in payload or "_ActionLens__al_ctx" in payload:
            raise ValueError("internal context must be supplied through actionlens_context")
        if context is not None:
            payload["__al_ctx"] = context
        result = self.func(**payload)
        if inspect.isawaitable(result):
            raise TypeError("async tool requires await adapter.ainvoke(...)")
        return self._serialize(result)

    async def ainvoke(
        self,
        arguments: Mapping[str, Any],
        *,
        framework_context: Any = None,
        actionlens_context: ToolCallContext | None = None,
    ) -> StructuredToolOutput | dict[str, Any] | str:
        context = actionlens_context
        if context is None and framework_context is not None:
            context = context_from_framework(framework_context, framework=self.framework, tool_name=self.name)
        payload = dict(arguments)
        if "__al_ctx" in payload or "_ActionLens__al_ctx" in payload:
            raise ValueError("internal context must be supplied through actionlens_context")
        if context is not None:
            payload["__al_ctx"] = context
        result = self.func(**payload)
        if inspect.isawaitable(result):
            result = await result
        return self._serialize(result)

    def openai_schema(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters_json_schema,
            },
        }

    def _build_callable(self) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(self.func):
            @functools.wraps(self.func)
            async def async_bridge(*args: Any, **kwargs: Any) -> Any:
                result = await self.func(*args, **kwargs)
                return self._serialize(result)

            async_bridge.__signature__ = inspect.signature(self.func)  # type: ignore[attr-defined]
            async_bridge.__annotations__ = _signature_annotations(self.func)
            return async_bridge

        @functools.wraps(self.func)
        def bridge(*args: Any, **kwargs: Any) -> Any:
            return self._serialize(self.func(*args, **kwargs))

        bridge.__signature__ = inspect.signature(self.func)  # type: ignore[attr-defined]
        bridge.__annotations__ = _signature_annotations(self.func)
        return bridge

    def _serialize(self, result: Any) -> StructuredToolOutput | dict[str, Any] | str:
        output = StructuredToolOutput.model_validate(result)
        if self.output_mode == "model":
            return output
        if self.output_mode == "dict":
            return output.model_dump(mode="json", exclude_none=True)
        return output.model_dump_json(exclude_none=True)


def signature_json_schema(func: Callable[..., Any]) -> dict[str, Any]:
    signature = inspect.signature(func)
    try:
        hints = get_type_hints(func)
    except (NameError, TypeError):
        hints = {}
    fields = {}
    additional = False
    for name, parameter in signature.parameters.items():
        if name == "__al_ctx":
            continue
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            additional = True
            continue
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        annotation = hints.get(name, parameter.annotation)
        if annotation is inspect.Parameter.empty:
            annotation = Any
        fields[name] = (annotation, ... if parameter.default is inspect.Parameter.empty else parameter.default)
    model = create_model("ActionLensArguments", __config__=ConfigDict(extra="allow" if additional else "forbid"), **fields)
    return model.model_json_schema()


def durable_step_idempotency_key(arguments: dict[str, Any], context: ToolCallContext) -> str:
    """Opt-in stable operation identity for independent durable steps.

    Attempts are excluded. Workflow run, step and tool scope are included;
    changed arguments for the same step conflict rather than create an effect.
    """
    step = context.metadata.get("temporal_activity_id") if context.framework == "temporal" else context.metadata.get("dbos_step_id") if context.framework == "dbos" else None
    if step is None or not str(step).strip():
        raise ValueError("a stable durable activity_id or step_id is required")
    return "step:v1:" + canonical_operation_hash({
        "project": context.project, "environment": context.environment,
        "tenant_id": context.tenant_id, "workflow": context.session_id,
        "run": context.run_id, "tool": context.tool_name, "step": str(step),
    })


def _signature_annotations(func: Callable[..., Any]) -> dict[str, Any]:
    annotations = dict(getattr(func, "__annotations__", {}))
    for name, parameter in inspect.signature(func).parameters.items():
        if parameter.annotation is not inspect.Parameter.empty:
            annotations[name] = parameter.annotation
    return annotations


def context_from_framework(
    source: Any,
    *,
    framework: str,
    tool_name: str,
    project: str = "default",
) -> ToolCallContext:
    values = _context_values(source)
    ambient = get_current_context()
    if ambient is not None:
        base = ambient.model_dump()
        metadata = base.pop("metadata")
        values = {**metadata, **base, **values}
    known = {
        "project",
        "environment",
        "tenant_id",
        "session_id",
        "run_id",
        "call_id",
        "parent_call_id",
        "actor_id",
        "attempt",
    }
    return ToolCallContext(
        project=str(values.get("project") or project),
        environment=str(values.get("environment") or "default"),
        tenant_id=_optional_str(values.get("tenant_id")),
        session_id=str(values.get("session_id") or values.get("thread_id") or "generated"),
        run_id=str(values.get("run_id") or f"run-{uuid4().hex}"),
        call_id=str(values.get("call_id") or values.get("tool_call_id") or f"call-{uuid4().hex}"),
        parent_call_id=_optional_str(values.get("parent_call_id")),
        actor_id=_optional_str(values.get("actor_id") or values.get("user_id")),
        tool_name=tool_name,
        framework=framework,
        attempt=int(values.get("attempt") or 0),
        context_source="explicit",
        metadata={key: _json_safe(value) for key, value in values.items() if key not in known},
    )


def _context_values(source: Any) -> dict[str, Any]:
    if source is None:
        return {}
    if isinstance(source, Mapping):
        values = dict(source)
    else:
        values = {}
        for name in (
            "project", "environment", "tenant_id", "session_id", "thread_id",
            "run_id", "call_id", "tool_call_id", "parent_call_id", "actor_id",
            "user_id", "attempt", "metadata",
        ):
            if hasattr(source, name):
                values[name] = getattr(source, name)
        deps = getattr(source, "deps", None)
        if isinstance(deps, Mapping):
            values = {**dict(deps), **values}
    metadata = values.pop("metadata", None)
    if isinstance(metadata, Mapping):
        values = {**dict(metadata), **values}
    configurable = values.pop("configurable", None)
    if isinstance(configurable, Mapping):
        values = {**dict(configurable), **values}
    return values


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)
