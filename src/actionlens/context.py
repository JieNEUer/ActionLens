from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from .models import ToolCallContext

_current_context: ContextVar[ToolCallContext | None] = ContextVar(
    "actionlens_current_context", default=None
)


def get_current_context() -> ToolCallContext | None:
    return _current_context.get()


def make_generated_context(project: str, tool_name: str) -> ToolCallContext:
    return ToolCallContext(
        project=project,
        session_id="generated",
        run_id=f"run-{uuid4().hex}",
        call_id=f"call-{uuid4().hex}",
        tool_name=tool_name,
        context_source="generated",
    )


@dataclass
class SessionContext:
    context: ToolCallContext
    _token: Any = None

    def __enter__(self) -> ToolCallContext:
        self._token = _current_context.set(self.context)
        return self.context

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._token is not None:
            _current_context.reset(self._token)

    async def __aenter__(self) -> ToolCallContext:
        return self.__enter__()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.__exit__(exc_type, exc, tb)
