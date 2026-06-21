from __future__ import annotations

import json
import asyncio
import concurrent.futures

from pydantic import ValidationError

from .models import ErrorRecord


def classify_exception(exc: BaseException) -> ErrorRecord:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, concurrent.futures.TimeoutError)):
        return ErrorRecord(
            taxonomy="Timeout",
            message=str(exc) or "Tool execution timed out.",
            type_name=type(exc).__name__,
            retryable=True,
        )
    if isinstance(exc, (json.JSONDecodeError, ValueError)):
        return ErrorRecord(
            taxonomy="FormatError",
            message=str(exc),
            type_name=type(exc).__name__,
            retryable=True,
        )
    if isinstance(exc, ValidationError):
        return ErrorRecord(
            taxonomy="ValidationError",
            message=str(exc),
            type_name=type(exc).__name__,
            retryable=True,
        )
    if isinstance(exc, PermissionError):
        return ErrorRecord(
            taxonomy="PermissionDenied",
            message=str(exc),
            type_name=type(exc).__name__,
            retryable=False,
        )
    return ErrorRecord(
        taxonomy="UnknownError",
        message=str(exc),
        type_name=type(exc).__name__,
        retryable=False,
    )


def recovery_hint(error: ErrorRecord) -> str:
    hints: dict[str, str] = {
        "Timeout": "工具执行超时。请不要立即重复高成本调用，可缩小输入范围或改用备用工具。",
        "FormatError": "工具返回或参数格式不正确。请修正格式后再试，不要重复已成功的写操作。",
        "ValidationError": "工具参数校验失败。请根据错误修正参数后重试。",
        "PermissionDenied": "权限不足。请不要绕过权限，向用户请求授权或改用只读方案。",
        "UnknownError": "工具发生未分类错误。请保守停止或请求人工协助。",
    }
    return hints.get(error.taxonomy, hints["UnknownError"])
