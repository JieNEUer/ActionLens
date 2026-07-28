from __future__ import annotations

import asyncio
import concurrent.futures
import json

from pydantic import ValidationError

from .models import ErrorRecord


class SideEffectUncertainError(RuntimeError):
    """The external side effect may have completed but cannot be confirmed."""


class RemoteToolExecutionError(RuntimeError):
    """A remote runner reported a terminal failure without a local traceback."""


def classify_exception(exc: BaseException) -> ErrorRecord:
    if isinstance(exc, SideEffectUncertainError):
        return ErrorRecord(
            taxonomy="SideEffectUncertain",
            message=str(exc) or "External side effect completion is uncertain.",
            type_name=type(exc).__name__,
            retryable=False,
        )
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, concurrent.futures.TimeoutError)):
        return ErrorRecord(
            taxonomy="Timeout",
            message=str(exc) or "Tool execution timed out.",
            type_name=type(exc).__name__,
            retryable=True,
        )
    # Pydantic v2's ValidationError is a ValueError subclass. It must be
    # checked first or callers receive the wrong recovery guidance.
    if isinstance(exc, ValidationError):
        return ErrorRecord(
            taxonomy="ValidationError",
            message=str(exc),
            type_name=type(exc).__name__,
            retryable=True,
        )
    if isinstance(exc, RemoteToolExecutionError):
        return ErrorRecord(
            taxonomy="RemoteExecutionError",
            message=str(exc) or "Remote tool execution failed.",
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
        "Timeout": "Tool execution timed out. Do not immediately retry expensive calls; narrow the input or use an alternative tool.",
        "FormatError": "Tool returned or received an incorrectly formatted value. Correct the format and retry; do not repeat a successful write.",
        "ValidationError": "Tool argument validation failed. Correct the arguments based on the error and retry.",
        "PermissionDenied": "Permission denied. Do not bypass access controls; request authorization from the user or switch to a read-only approach.",
        "UnknownError": "An unclassified tool error occurred. Stop conservatively or request human assistance.",
        "SideEffectUncertain": "The external side effect may have completed. Query the business system or request human confirmation first; do not auto-retry.",
        "RemoteExecutionError": "The remote runner reported a failure. Inspect its job status or retry a read-only operation when appropriate.",
    }
    return hints.get(error.taxonomy, hints["UnknownError"])
