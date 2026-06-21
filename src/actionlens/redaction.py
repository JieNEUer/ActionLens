from __future__ import annotations

import re
from typing import Any, Protocol

from .models import ToolCallContext

REDACTED = "[REDACTED]"


class Redactor(Protocol):
    def redact(self, value: Any, context: ToolCallContext | None = None) -> Any:
        ...


class KeyRedactor:
    def __init__(self, keys: list[str] | None = None):
        self.keys = [key.lower() for key in (keys or [])]

    def redact(self, value: Any, context: ToolCallContext | None = None) -> Any:
        return _redact(value, self.keys, [])


class RegexRedactor:
    def __init__(self, patterns: list[str] | None = None):
        self.patterns = [re.compile(pattern) for pattern in (patterns or [])]

    def redact(self, value: Any, context: ToolCallContext | None = None) -> Any:
        return _redact(value, [], self.patterns)


class CompositeRedactor:
    def __init__(self, redactors: list[Redactor]):
        self.redactors = redactors

    def redact(self, value: Any, context: ToolCallContext | None = None) -> Any:
        current = value
        for redactor in self.redactors:
            current = redactor.redact(current, context)
        return current


def redact_value(
    value: Any,
    *,
    keys: list[str] | None = None,
    patterns: list[str] | None = None,
) -> Any:
    return CompositeRedactor(
        [KeyRedactor(keys), RegexRedactor(patterns)]
    ).redact(value)


def _redact(value: Any, keys: list[str], patterns: list[re.Pattern[str]]) -> Any:
    if isinstance(value, dict):
        redacted: dict[Any, Any] = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(token in key_text for token in keys) or any(
                pattern.search(str(key)) for pattern in patterns
            ):
                redacted[key] = REDACTED
            else:
                redacted[key] = _redact(item, keys, patterns)
        return redacted
    if isinstance(value, list):
        return [_redact(item, keys, patterns) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact(item, keys, patterns) for item in value)
    if isinstance(value, str):
        result = value
        for pattern in patterns:
            result = pattern.sub(REDACTED, result)
        return result
    return value
