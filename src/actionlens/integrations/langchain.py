from __future__ import annotations

from collections.abc import Callable, Mapping
import inspect
from typing import Any

from actionlens.models import ToolCallContext

from .common import ActionLensToolAdapter, context_from_framework


def wrap_langchain_tool(func: Callable[..., Any]) -> ActionLensToolAdapter:
    """Return a stable-JSON bridge for LangChain and LangGraph."""
    return ActionLensToolAdapter(func, framework="langchain", output_mode="json")


def context_from_langgraph_state(
    state: Mapping[str, Any], *, tool_name: str, project: str = "default"
) -> ToolCallContext:
    values = dict(state)
    config = values.get("config")
    if isinstance(config, Mapping):
        configurable = config.get("configurable")
        if isinstance(configurable, Mapping):
            values = {**dict(configurable), **values}
    return context_from_framework(
        values,
        framework="langgraph",
        tool_name=tool_name,
        project=project,
    )


def as_langchain_tool(adapter: ActionLensToolAdapter) -> Any:
    """Create a native StructuredTool using a lazy optional import."""
    try:
        from langchain_core.tools import StructuredTool
    except ImportError as exc:
        raise ImportError("Install ActionLens with the 'langchain' extra") from exc
    return StructuredTool.from_function(
        func=adapter.callable,
        coroutine=adapter.callable if inspect.iscoroutinefunction(adapter.callable) else None,
        name=adapter.name,
        description=adapter.description or adapter.name,
    )
