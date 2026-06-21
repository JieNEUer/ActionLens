from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from actionlens.models import ApprovalTicket, RiskLevel


class MemoryApprovalTicketStore:
    def __init__(self) -> None:
        self._tickets: dict[str, ApprovalTicket] = {}

    def create(self, ticket: ApprovalTicket) -> ApprovalTicket:
        self._tickets[ticket.ticket_id] = ticket
        return ticket

    def get(self, ticket_id: str) -> ApprovalTicket | None:
        return self._tickets.get(ticket_id)

    def approve(
        self,
        ticket_id: str,
        *,
        approved_by: str | None = None,
        decision_note: str | None = None,
        modified_args: dict[str, Any] | None = None,
    ) -> ApprovalTicket | None:
        ticket = self._tickets.get(ticket_id)
        if ticket is None:
            return None
        ticket = ticket.model_copy(
            update={
                "status": "APPROVED",
                "approved_by": approved_by,
                "decision_note": decision_note,
                "modified_args": modified_args,
                "approved_at": datetime.now(timezone.utc),
            }
        )
        self._tickets[ticket_id] = ticket
        return ticket

    def deny(
        self, ticket_id: str, *, decision_note: str | None = None
    ) -> ApprovalTicket | None:
        ticket = self._tickets.get(ticket_id)
        if ticket is None:
            return None
        ticket = ticket.model_copy(
            update={"status": "DENIED", "decision_note": decision_note}
        )
        self._tickets[ticket_id] = ticket
        return ticket


class SQLiteApprovalTicketStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def create(self, ticket: ApprovalTicket) -> ApprovalTicket:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO actionlens_approval_tickets (
                  ticket_id, idempotency_key, call_id, tool_name, safe_args_json,
                  risk, reason, status, requested_at, expires_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticket.ticket_id,
                    ticket.idempotency_key,
                    ticket.call_id,
                    ticket.tool_name,
                    json.dumps(ticket.safe_args, ensure_ascii=False, default=str),
                    ticket.risk.value,
                    ticket.reason,
                    ticket.status,
                    ticket.requested_at.isoformat(),
                    ticket.expires_at.isoformat() if ticket.expires_at else None,
                    json.dumps(ticket.metadata, ensure_ascii=False, default=str),
                ),
            )
        return ticket

    def get(self, ticket_id: str) -> ApprovalTicket | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM actionlens_approval_tickets WHERE ticket_id = ?",
                (ticket_id,),
            ).fetchone()
            return _ticket_from_row(row) if row is not None else None

    def approve(
        self,
        ticket_id: str,
        *,
        approved_by: str | None = None,
        decision_note: str | None = None,
        modified_args: dict[str, Any] | None = None,
    ) -> ApprovalTicket | None:
        approved_at = datetime.now(timezone.utc)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE actionlens_approval_tickets
                SET status = 'APPROVED',
                    approved_by = ?,
                    decision_note = ?,
                    modified_args_json = ?,
                    approved_at = ?
                WHERE ticket_id = ? AND status = 'PENDING'
                """,
                (
                    approved_by,
                    decision_note,
                    json.dumps(modified_args, ensure_ascii=False, default=str)
                    if modified_args is not None
                    else None,
                    approved_at.isoformat(),
                    ticket_id,
                ),
            )
        return self.get(ticket_id)

    def deny(
        self, ticket_id: str, *, decision_note: str | None = None
    ) -> ApprovalTicket | None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE actionlens_approval_tickets
                SET status = 'DENIED', decision_note = ?
                WHERE ticket_id = ? AND status = 'PENDING'
                """,
                (decision_note, ticket_id),
            )
        return self.get(ticket_id)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS actionlens_approval_tickets (
                  ticket_id TEXT PRIMARY KEY,
                  idempotency_key TEXT,
                  call_id TEXT NOT NULL,
                  tool_name TEXT NOT NULL,
                  safe_args_json TEXT NOT NULL,
                  risk TEXT NOT NULL,
                  reason TEXT NOT NULL,
                  status TEXT NOT NULL,
                  approved_by TEXT,
                  decision_note TEXT,
                  modified_args_json TEXT,
                  requested_at TEXT NOT NULL,
                  approved_at TEXT,
                  expires_at TEXT,
                  metadata_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_actionlens_ticket_key "
                "ON actionlens_approval_tickets(idempotency_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_actionlens_ticket_status "
                "ON actionlens_approval_tickets(status)"
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn


def _ticket_from_row(row: sqlite3.Row) -> ApprovalTicket:
    modified_args_json = row["modified_args_json"]
    return ApprovalTicket(
        ticket_id=row["ticket_id"],
        idempotency_key=row["idempotency_key"],
        call_id=row["call_id"],
        tool_name=row["tool_name"],
        safe_args=json.loads(row["safe_args_json"]),
        risk=RiskLevel(row["risk"]),
        reason=row["reason"],
        status=row["status"],
        approved_by=row["approved_by"],
        decision_note=row["decision_note"],
        modified_args=json.loads(modified_args_json) if modified_args_json else None,
        requested_at=datetime.fromisoformat(row["requested_at"]),
        approved_at=datetime.fromisoformat(row["approved_at"])
        if row["approved_at"]
        else None,
        expires_at=datetime.fromisoformat(row["expires_at"])
        if row["expires_at"]
        else None,
        metadata=json.loads(row["metadata_json"]),
    )
