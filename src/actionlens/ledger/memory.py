from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Literal

LedgerStatus = Literal[
    "PENDING",  # v0.3 compatibility alias for EXECUTING
    "EXECUTING",
    "APPROVAL_PENDING",
    "APPROVED",
    "SUCCEEDED",
    "FAILED",  # v0.3 compatibility alias for FAILED_RETRYABLE
    "FAILED_RETRYABLE",
    "FAILED_TERMINAL",
    "DENIED",
    "EXPIRED",
    "UNCERTAIN",
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
    project: str = ""
    environment: str = ""
    tenant_id: str | None = None
    session_id: str = ""
    run_id: str = ""
    tool_name: str = ""
    args_hash: str = ""
    tool_schema_hash: str = ""
    owner_id: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    attempt: int = 0
    fencing_token: int = 0
    last_error: str | None = None


class MemoryLedger:
    def __init__(self) -> None:
        self._records: dict[str, LedgerRecord] = {}
        self._lock = RLock()

    def begin(
        self,
        key: str,
        *,
        call_id: str,
        context: Any = None,
        spec: Any = None,
        args_hash: str = "",
        tool_schema_hash: str = "",
    ) -> tuple[str, LedgerRecord]:
        now = datetime.now(timezone.utc)
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                existing.hit_count += 1
                existing.updated_at = now
                if (
                    args_hash
                    and existing.args_hash
                    and existing.args_hash != args_hash
                ) or (
                    tool_schema_hash
                    and existing.tool_schema_hash
                    and existing.tool_schema_hash != tool_schema_hash
                ):
                    return "conflict", existing
                if existing.status in {"APPROVED", "FAILED", "FAILED_RETRYABLE"}:
                    existing.status = "PENDING"
                    existing.call_id = call_id
                    if args_hash:
                        existing.args_hash = args_hash
                    if tool_schema_hash:
                        existing.tool_schema_hash = tool_schema_hash
                    return "created", existing
                return "hit", existing
            record = LedgerRecord(
                key=key,
                status="PENDING",
                call_id=call_id,
                created_at=now,
                updated_at=now,
                args_hash=args_hash,
                tool_schema_hash=tool_schema_hash,
                **_context_fields(context, spec),
            )
            self._records[key] = record
            return "created", record

    def mark_approval_pending(
        self, key: str, *, call_id: str, ticket_id: str, context: Any = None, spec: Any = None
    ) -> LedgerRecord:
        now = datetime.now(timezone.utc)
        with self._lock:
            record = self._records.get(key) or LedgerRecord(
                key=key,
                status="APPROVAL_PENDING",
                call_id=call_id,
                created_at=now,
                updated_at=now,
                **_context_fields(context, spec),
            )
            if record.status == "APPROVAL_PENDING" and record.ticket_id:
                return record
            record.status = "APPROVAL_PENDING"
            record.ticket_id = ticket_id
            record.updated_at = now
            for name, value in _context_fields(context, spec).items():
                setattr(record, name, value)
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
        self.mark_failed(key, retryable=True)

    def mark_failed(self, key: str, *, retryable: bool) -> None:
        with self._lock:
            record = self._records.get(key)
            if record is not None:
                record.status = "FAILED_RETRYABLE" if retryable else "FAILED_TERMINAL"
                record.updated_at = datetime.now(timezone.utc)

    def mark_uncertain(self, key: str) -> None:
        with self._lock:
            record = self._records.get(key)
            if record is not None:
                record.status = "UNCERTAIN"
                record.updated_at = datetime.now(timezone.utc)

    def get(self, key: str) -> LedgerRecord | None:
        with self._lock:
            return self._records.get(key)

    def records(self) -> list[LedgerRecord]:
        with self._lock:
            return list(self._records.values())


def _context_fields(context: Any, spec: Any) -> dict[str, Any]:
    if context is None:
        return {}
    return {
        "project": context.project,
        "environment": context.environment,
        "tenant_id": context.tenant_id,
        "session_id": context.session_id,
        "run_id": context.run_id,
        "tool_name": getattr(spec, "name", context.tool_name),
    }
