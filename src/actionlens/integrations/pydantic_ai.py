from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .common import ActionLensToolAdapter


def wrap_pydantic_ai_tool(func: Callable[..., Any]) -> ActionLensToolAdapter:
    """Return a typed-result bridge suitable for PydanticAI tool registration."""
    return ActionLensToolAdapter(func, framework="pydantic-ai", output_mode="model")


def as_pydantic_ai_tool(adapter: ActionLensToolAdapter) -> Any:
    """Create a native PydanticAI Tool without importing it from the core package."""
    try:
        from pydantic_ai import Tool
    except ImportError as exc:
        raise ImportError("Install ActionLens with the 'pydantic-ai' extra") from exc
    return Tool(adapter.callable, name=adapter.name, description=adapter.description)
