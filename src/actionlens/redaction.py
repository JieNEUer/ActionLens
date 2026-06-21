from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"


def redact_value(
    value: Any,
    *,
    keys: list[str] | None = None,
    patterns: list[str] | None = None,
) -> Any:
    keys = [k.lower() for k in (keys or [])]
    compiled = [re.compile(p) for p in (patterns or [])]
    return _redact(value, keys, compiled)


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
