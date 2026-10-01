from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Protocol, runtime_checkable

from .ledger.memory import LedgerRecord
from .models import ApprovalTicket, OutboxRecord, TrajectoryEvent

BeginKind = Literal["created", "hit", "conflict", "uncertain"]


class RepositoryLedgerView:
    """Compatibility view built solely from the public repository protocol."""

    def __init__(self, repository: GovernanceRepository):
        self.repository = repository

    def get(self, key: str) -> LedgerRecord | None:
        return self.repository.get_ledger(key)

    def records(self) -> list[LedgerRecord]:
        return self.repository.list_ledger()


class RepositoryTicketView:
    def __init__(self, repository: GovernanceRepository):
        self.repository = repository

    def get(self, ticket_id: str) -> ApprovalTicket | None:
        return self.repository.get_ticket(ticket_id)

    def list(self, *, status: str | None = None) -> list[ApprovalTicket]:
        return self.repository.list_tickets(status=status)


class RepositoryConflictError(RuntimeError):
    """Raised when an idempotency key is reused for a different operation."""


class StaleFenceError(RuntimeError):
    """Raised when a superseded worker attempts to commit a transition."""


@runtime_checkable
class GovernanceRepository(Protocol):
    """Atomic persistence boundary for ledger, approval tickets, and outbox."""

    def begin(
        self,
        key: str,
        *,
        call_id: str,
        context: Any,
        spec: Any,
        args_hash: str,
        tool_schema_hash: str,
        owner_id: str,
        lease_seconds: float,
    ) -> tuple[BeginKind, LedgerRecord]: ...

    def heartbeat(self, key: str, *, owner_id: str, fencing_token: int, lease_seconds: float) -> bool: ...

    def create_approval(
        self,
        key: str,
        *,
        ticket: ApprovalTicket,
        context: Any,
        spec: Any,
        args_hash: str,
        tool_schema_hash: str,
        event: TrajectoryEvent,
    ) -> tuple[ApprovalTicket, LedgerRecord, bool]: ...

    def decide_approval(
        self,
        ticket_id: str,
        *,
        status: Literal["APPROVED", "DENIED"],
        approved_by: str | None,
        decision_note: str | None,
        modified_args: dict[str, Any] | None,
        event: TrajectoryEvent,
    ) -> ApprovalTicket | None: ...

    def finish(
        self,
        key: str,
        *,
        owner_id: str,
        fencing_token: int,
        status: Literal["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_TERMINAL", "UNCERTAIN"],
        output: dict[str, Any] | None,
        error: str | None,
        event: TrajectoryEvent,
    ) -> LedgerRecord: ...

    def set_ledger_status(
        self, key: str, *, status: Literal["FAILED_TERMINAL", "DENIED", "EXPIRED", "UNCERTAIN"],
        error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord | None: ...

    def get_ledger(self, key: str) -> LedgerRecord | None: ...
    def list_ledger(self) -> list[LedgerRecord]: ...
    def get_ticket(self, ticket_id: str) -> ApprovalTicket | None: ...
    def list_tickets(self, *, status: str | None = None) -> list[ApprovalTicket]: ...
    def expire_ticket(self, ticket_id: str, *, event: TrajectoryEvent | None = None) -> ApprovalTicket | None: ...
    def claim_outbox(self, *, worker_id: str, limit: int, claim_seconds: float) -> list[OutboxRecord]: ...
    def ack_outbox(self, delivery_id: str, *, worker_id: str) -> bool: ...
    def retry_outbox(self, delivery_id: str, *, worker_id: str, error: str, next_retry_at: datetime) -> bool: ...
    def dead_letter_outbox(self, delivery_id: str, *, worker_id: str, error: str) -> bool: ...
    def resolve_uncertain(
        self, key: str, *, status: Literal["SUCCEEDED", "FAILED_RETRYABLE", "UNCERTAIN"],
        output: dict[str, Any] | None, error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord | None: ...
    def list_outbox(self, *, state: Literal["pending", "delivered", "dead_letter"] | None = None,
                    limit: int = 100) -> list[OutboxRecord]: ...
    def replay_dead_letter(self, delivery_id: str) -> bool: ...
    def terminate_dead_letter(self, delivery_id: str, *, reason: str) -> bool: ...


def canonical_operation_hash(value: Any) -> str:
    import hashlib
    import json

    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def approval_expiry_event(ticket: ApprovalTicket, record: LedgerRecord) -> TrajectoryEvent:
    from uuid import uuid4

    identity = uuid4().hex
    return TrajectoryEvent(
        event_id=f"evt-{identity}", timestamp=datetime.now(timezone.utc),
        project=record.project, session_id=record.session_id, run_id=record.run_id,
        call_id=record.call_id, tool_name=record.tool_name, sequence=f"expiry-{identity}",
        event_type="approval.expired", phase="PRE_FLIGHT", metadata={"ticket_id": ticket.ticket_id},
    )
