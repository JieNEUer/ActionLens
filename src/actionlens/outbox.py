from __future__ import annotations

import random
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4


class OutboxDispatcher:
    """Claims and delivers transactional outbox events with bounded backoff."""

    def __init__(
        self, repository: Any, sink: Any, *, worker_id: str | None = None,
        claim_seconds: float = 30.0, base_retry_seconds: float = 0.25,
        max_retry_seconds: float = 300.0,
        max_attempts: int = 8,
        readiness_lag_seconds: float | None = None,
    ) -> None:
        if claim_seconds <= 0 or base_retry_seconds < 0 or max_retry_seconds <= 0:
            raise ValueError("invalid outbox timing values")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        self.repository = repository
        self.sink = sink
        self.worker_id = worker_id or f"dispatcher-{uuid4().hex}"
        self.claim_seconds = claim_seconds
        self.base_retry_seconds = base_retry_seconds
        self.max_retry_seconds = max_retry_seconds
        self.max_attempts = max_attempts
        if readiness_lag_seconds is not None and readiness_lag_seconds <= 0:
            raise ValueError("readiness_lag_seconds must be positive")
        self.readiness_lag_seconds = readiness_lag_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._last_success_at: datetime | None = None
        self._last_error: str | None = None

    def start(self, *, interval_seconds: float = 1.0, batch_size: int = 100) -> None:
        if interval_seconds <= 0 or batch_size <= 0:
            raise ValueError("interval_seconds and batch_size must be positive")
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("outbox dispatcher is already running")
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                kwargs={"interval_seconds": interval_seconds, "batch_size": batch_size},
                name=f"actionlens-{self.worker_id}",
                daemon=True,
            )
            self._thread.start()

    def stop(self, *, timeout: float = 10.0) -> bool:
        self._stop_event.set()
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=max(0.0, timeout))
        return not thread.is_alive()

    close = stop

    def health(self) -> dict[str, Any]:
        thread = self._thread
        stats_method = getattr(self.repository, "outbox_stats", None)
        stats_error = None
        try:
            stats = stats_method() if stats_method is not None else {}
        except Exception as exc:  # health probes must report outages, not propagate them
            stats = {}
            stats_error = _error_type(exc)
        lag = stats.get("oldest_pending_lag_seconds")
        lag_ready = (
            self.readiness_lag_seconds is None
            or lag is None
            or float(lag) <= self.readiness_lag_seconds
        )
        return {
            "running": bool(thread and thread.is_alive()),
            "ready": self._last_error is None and stats_error is None and lag_ready,
            "last_success_at": self._last_success_at,
            "last_error": self._last_error,
            "stats_error": stats_error,
            "outbox": stats,
        }

    def _run(self, *, interval_seconds: float, batch_size: int) -> None:
        while not self._stop_event.is_set():
            try:
                self.dispatch_once(limit=batch_size)
            except Exception as exc:  # repository outage must not kill the worker silently
                with self._state_lock:
                    self._last_error = _error_type(exc)
            else:
                with self._state_lock:
                    self._last_success_at = datetime.now(timezone.utc)
                    self._last_error = None
            self._stop_event.wait(interval_seconds)

    def dispatch_once(self, *, limit: int = 100) -> dict[str, int]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        delivered = failed = dead_lettered = 0
        records = self.repository.claim_outbox(
            worker_id=self.worker_id, limit=limit, claim_seconds=self.claim_seconds
        )
        for record in records:
            try:
                errors_before = self._sink_error_count()
                self.sink.emit(record.event)
                if self._sink_error_count() > errors_before:
                    raise RuntimeError("sink reported a best-effort delivery error")
            except Exception as exc:  # delivery remains durable for retry
                failed += 1
                # A sink or transport exception can contain a DSN, token, or
                # endpoint detail. Persist a stable classification only.
                message = _error_type(exc)
                if record.attempt + 1 >= self.max_attempts:
                    if self.repository.dead_letter_outbox(
                        record.delivery_id, worker_id=self.worker_id, error=message
                    ):
                        dead_lettered += 1
                    continue
                retry_after = getattr(exc, "retry_after", None)
                delay = min(
                    self.max_retry_seconds,
                    max(0.0, retry_after) if retry_after is not None else
                    self.base_retry_seconds * (2 ** min(record.attempt, 12)),
                )
                if retry_after is None:
                    delay *= random.uniform(0.8, 1.2)
                self.repository.retry_outbox(
                    record.delivery_id,
                    worker_id=self.worker_id,
                    error=message,
                    next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=delay),
                )
            else:
                if self.repository.ack_outbox(record.delivery_id, worker_id=self.worker_id):
                    delivered += 1
        return {"claimed": len(records), "delivered": delivered, "failed": failed,
                "dead_lettered": dead_lettered}

    def _sink_error_count(self) -> int:
        return sum(
            int(getattr(self.sink, name, 0))
            for name in ("write_error_count", "error_count")
        )


def _error_type(exc: BaseException) -> str:
    return type(exc).__name__
