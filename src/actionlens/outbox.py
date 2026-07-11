from __future__ import annotations

import random
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
    ) -> None:
        self.repository = repository
        self.sink = sink
        self.worker_id = worker_id or f"dispatcher-{uuid4().hex}"
        self.claim_seconds = claim_seconds
        self.base_retry_seconds = base_retry_seconds
        self.max_retry_seconds = max_retry_seconds
        self.max_attempts = max_attempts

    def dispatch_once(self, *, limit: int = 100) -> dict[str, int]:
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
                message = f"{type(exc).__name__}: {exc}"
                if record.attempt + 1 >= self.max_attempts:
                    if self.repository.dead_letter_outbox(
                        record.delivery_id, worker_id=self.worker_id, error=message
                    ):
                        dead_lettered += 1
                    continue
                delay = min(
                    self.max_retry_seconds,
                    self.base_retry_seconds * (2 ** min(record.attempt, 12)),
                )
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
