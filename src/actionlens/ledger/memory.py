from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Literal


LedgerStatus = Literal[
    "PENDING",
    "APPROVAL_PENDING",
    "APPROVED",
    "SUCCEEDED",
    "FAILED",
]


@dataclass
class LedgerRecord:
    key: str
    status: LedgerStatus
    call_id: str
    created_at: datetime
    updated_at: datetime
    hit_count: int = 0
    output: dict[str, Any] | None = None
    ticket_id: str | None = None


class MemoryLedger:
    def __init__(self) -> None:
        self._records: dict[str, LedgerRecord] = {}
        self._lock = RLock()

    def begin(self, key: str, *, call_id: str) -> tuple[str, LedgerRecord]:
        now = datetime.now(timezone.utc)
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                existing.hit_count += 1
                existing.updated_at = now
                if existing.status == "APPROVED":
                    existing.status = "PENDING"
                    existing.call_id = call_id
                    return "created", existing
                return "hit", existing
            record = LedgerRecord(
                key=key,
                status="PENDING",
                call_id=call_id,
                created_at=now,
                updated_at=now,
            )
            self._records[key] = record
            return "created", record

    def mark_approval_pending(
        self, key: str, *, call_id: str, ticket_id: str
    ) -> LedgerRecord:
        now = datetime.now(timezone.utc)
        with self._lock:
            record = self._records.get(key) or LedgerRecord(
                key=key,
                status="APPROVAL_PENDING",
                call_id=call_id,
                created_at=now,
                updated_at=now,
            )
            record.status = "APPROVAL_PENDING"
            record.ticket_id = ticket_id
            record.updated_at = now
            self._records[key] = record
            return record

    def approve(self, key: str) -> LedgerRecord | None:
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return None
            record.status = "APPROVED"
            record.updated_at = datetime.now(timezone.utc)
            return record

    def succeed(self, key: str, output: dict[str, Any]) -> None:
        with self._lock:
            record = self._records.get(key)
            if record is not None:
                record.status = "SUCCEEDED"
                record.output = output
                record.updated_at = datetime.now(timezone.utc)

    def fail(self, key: str) -> None:
        with self._lock:
            record = self._records.get(key)
            if record is not None:
                record.status = "FAILED"
                record.updated_at = datetime.now(timezone.utc)

    def get(self, key: str) -> LedgerRecord | None:
        with self._lock:
            return self._records.get(key)

    def records(self) -> list[LedgerRecord]:
        with self._lock:
            return list(self._records.values())
