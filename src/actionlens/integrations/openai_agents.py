from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .common import ActionLensToolAdapter


def wrap_openai_agent_tool(func: Callable[..., Any]) -> ActionLensToolAdapter:
    """Return a stable-JSON bridge for OpenAI Agents SDK function tools."""
    return ActionLensToolAdapter(func, framework="openai-agents", output_mode="json")


def as_openai_agents_tool(adapter: ActionLensToolAdapter) -> Any:
    """Create a native Agents SDK FunctionTool using a lazy optional import."""
    try:
        from agents import function_tool
    except ImportError as exc:
        raise ImportError("Install ActionLens with the 'openai-agents' extra") from exc
    return function_tool(
        adapter.callable,
        name_override=adapter.name,
        description_override=adapter.description,
    )
