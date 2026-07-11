from __future__ import annotations

import functools
import inspect
import json
from collections.abc import Callable, Mapping
from typing import Any, Literal, get_type_hints
from uuid import uuid4

from pydantic import TypeAdapter

from actionlens.models import StructuredToolOutput, ToolCallContext


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
        context = actionlens_context or context_from_framework(
            framework_context, framework=self.framework, tool_name=self.name
        )
        result = self.func(**dict(arguments), __al_ctx=context)
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
        context = actionlens_context or context_from_framework(
            framework_context, framework=self.framework, tool_name=self.name
        )
        result = self.func(**dict(arguments), __al_ctx=context)
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
    properties: dict[str, Any] = {}
    required: list[str] = []
    additional_properties = False
    for name, parameter in signature.parameters.items():
        if name in {"__al_ctx", "context"}:
            continue
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            additional_properties = True
            continue
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        annotation = hints.get(name, parameter.annotation)
        if annotation is inspect.Parameter.empty:
            schema: dict[str, Any] = {}
        else:
            try:
                schema = TypeAdapter(annotation).json_schema()
            except Exception:  # noqa: BLE001 - unsupported annotations remain unconstrained.
                schema = {}
        if parameter.default is not inspect.Parameter.empty:
            schema = dict(schema)
            schema["default"] = parameter.default
        else:
            required.append(name)
        properties[name] = schema
    result: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": additional_properties,
    }
    if required:
        result["required"] = required
    return result


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
