"""Bounded JSON normalization before any audit or visible-value redaction."""

from __future__ import annotations

import json
import math
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import PurePath
from typing import Any
from uuid import UUID

from pydantic import BaseModel


def json_value(value: Any) -> Any:
    remaining = 100_000
    active: set[int] = set()

    def visit(item: Any, depth: int = 0) -> Any:
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > 32:
            return "[projection limit]"
        if item is None or isinstance(item, (str, bool, int)):
            return item
        if isinstance(item, float):
            return item if math.isfinite(item) else None
        if isinstance(item, Enum):
            return visit(item.value, depth + 1)
        if isinstance(item, (PurePath, UUID, date, datetime)):
            return str(item)
        if isinstance(item, (bytes, bytearray, memoryview)):
            return bytes(item).decode("utf-8", errors="replace")
        identity = id(item)
        if identity in active:
            return "[circular reference]"
        active.add(identity)
        try:
            if isinstance(item, BaseModel):
                item = dict(item)
            elif is_dataclass(item) and not isinstance(item, type):
                item = {field.name: getattr(item, field.name) for field in fields(item)}
            if isinstance(item, dict):
                result = {}
                for key, child in item.items():
                    if remaining <= 0:
                        result["__projection_limit__"] = True
                        break
                    safe_key = (
                        key
                        if isinstance(key, str)
                        else str(key)
                        if isinstance(key, (int, float, bool))
                        else "[opaque key]"
                    )
                    result[safe_key] = visit(child, depth + 1)
                return result
            if isinstance(item, (list, tuple, set, frozenset)):
                result = []
                for child in item:
                    if remaining <= 0:
                        result.append("[projection limit]")
                        break
                    result.append(visit(child, depth + 1))
                return (
                    sorted(result, key=lambda child: json.dumps(child, sort_keys=True))
                    if isinstance(item, (set, frozenset))
                    else result
                )
            # Arbitrary repr/str/model serializers may expose credentials or
            # execute unbounded host code. Unknown objects remain opaque.
            return "[opaque object]"
        finally:
            active.remove(identity)

    return visit(value)
