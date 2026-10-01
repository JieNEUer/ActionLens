from __future__ import annotations

import atexit
import os
import queue
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from actionlens.models import TrajectoryEvent


class JsonlSink:
    def __init__(
        self,
        root: str | Path,
        *,
        queue_maxsize: int | None = None,
        drop_policy: Literal["block", "drop_oldest", "drop_newest"] = "block",
        strict: bool = False,
        flush_interval_sec: float = 0.25,
    ):
        if queue_maxsize is not None and queue_maxsize <= 0:
            raise ValueError("queue_maxsize must be greater than zero")
        if drop_policy not in {"block", "drop_oldest", "drop_newest"}:
            raise ValueError("drop_policy must be block, drop_oldest, or drop_newest")
        if flush_interval_sec <= 0:
            raise ValueError("flush_interval_sec must be greater than zero")
        self.root = Path(root)
        self.trajectory_dir = self.root / "trajectories"
        self.trajectory_dir.mkdir(parents=True, exist_ok=True)
        self.queue_maxsize = queue_maxsize
        self.drop_policy = drop_policy
        self.strict = strict
        self.flush_interval_sec = flush_interval_sec
        self.dropped_count = 0
        self.write_error_count = 0
        self._lock = threading.Lock()
        self._closed = False
        self._worker_error: BaseException | None = None
        self._queue: queue.Queue[TrajectoryEvent | object] | None = None
        self._sentinel = object()
        self._worker: threading.Thread | None = None
        if queue_maxsize is not None:
            self._queue = queue.Queue(maxsize=queue_maxsize)
            self._worker = threading.Thread(
                target=self._run_worker,
                name="actionlens-jsonl-sink",
                daemon=True,
            )
            self._worker.start()
            atexit.register(self.close)

    def emit(self, event: TrajectoryEvent) -> None:
        if self._closed:
            if self.strict:
                raise RuntimeError("JsonlSink is closed")
            self.dropped_count += 1
            return
        self._raise_worker_error_if_strict()
        if self._queue is None:
            self._write_safely(event)
            return
        if self.drop_policy == "block":
            self._queue.put(event)
            return
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            if self.drop_policy == "drop_newest":
                self.dropped_count += 1
                return
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except queue.Empty:
                pass
            self.dropped_count += 1
            try:
                self._queue.put_nowait(event)
            except queue.Full:
                self.dropped_count += 1

    def flush(self) -> None:
        if self._queue is not None:
            self._queue.join()
        self._raise_worker_error_if_strict()

    def deliver(self, event: TrajectoryEvent) -> None:
        """Reliable outbox delivery: synchronous write, flush and fsync.

        Delivery bypasses the lossy observation queue and propagates errors.
        """
        self._write(event, durable=True)

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            self._closed = True
        if self._queue is not None and self._worker is not None:
            self._queue.put(self._sentinel)
            self._worker.join()
        self._raise_worker_error_if_strict()

    def stats(self) -> dict[str, int]:
        return {
            "dropped": self.dropped_count,
            "write_errors": self.write_error_count,
            "queued": self._queue.qsize() if self._queue is not None else 0,
        }

    def _run_worker(self) -> None:
        assert self._queue is not None
        while True:
            try:
                item = self._queue.get(timeout=self.flush_interval_sec)
            except queue.Empty:
                continue
            try:
                if item is self._sentinel:
                    return
                assert isinstance(item, TrajectoryEvent)
                self._write_safely(item)
            finally:
                self._queue.task_done()

    def _write_safely(self, event: TrajectoryEvent) -> None:
        try:
            self._write(event, durable=False)
        except Exception as exc:  # noqa: BLE001 - sink failure is isolated by default.
            self.write_error_count += 1
            if self._worker_error is None:
                self._worker_error = exc
            if self.strict and self._queue is None:
                raise

    def _write(self, event: TrajectoryEvent, *, durable: bool) -> None:
        path = self._path_for(event.timestamp)
        line = event.model_dump_json(exclude_none=True)
        with self._lock:
            if durable and self._closed:
                raise RuntimeError("JsonlSink is closed")
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                if durable:
                    handle.flush()
                    os.fsync(handle.fileno())

    def _raise_worker_error_if_strict(self) -> None:
        if self.strict and self._worker_error is not None:
            raise RuntimeError("JsonlSink writer failed") from self._worker_error

    def _path_for(self, timestamp: datetime | None = None) -> Path:
        timestamp = timestamp or datetime.now(timezone.utc)
        return self.trajectory_dir / f"actionlens-{timestamp.date().isoformat()}.jsonl"
