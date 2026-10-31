"""One async entry for native async and tracked, bounded sync tool work."""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
from typing import Any


async def invoke_async(tool, *args, **kwargs) -> Any:
    if inspect.iscoroutinefunction(tool):
        return await tool(*args, **kwargs)
    runtime = tool.actionlens_runtime
    copied = contextvars.copy_context()
    future = runtime.lens._executor_for_async_bridge().submit(
        copied.run, functools.partial(tool, *args, **kwargs)
    )
    task = asyncio.wrap_future(future)
    try:
        result = await asyncio.shield(task)
    except asyncio.CancelledError:
        # A running mutation cannot be stopped by cancelling its Python future.
        # Keep repository/files alive until its real terminal fact is recorded.
        if future.cancel():
            task.cancel()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()
        raise
    return await result if inspect.isawaitable(result) else result
