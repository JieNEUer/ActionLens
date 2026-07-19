from __future__ import annotations

import json
import queue
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from actionlens.ledger.memory import LedgerRecord
from actionlens.models import ApprovalTicket, OutboxRecord, RiskLevel, TrajectoryEvent
from actionlens.repository import BeginKind, StaleFenceError


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


class SQLiteGovernanceRepository:
    """SQLite reference implementation of the v1.0 atomic repository contract."""

    def __init__(
        self, path: str | Path, *, synchronous: Literal["FULL", "NORMAL"] = "FULL",
        pool_size: int = 4, acquire_timeout: float = 5.0,
    ):
        if synchronous not in {"FULL", "NORMAL"}:
            raise ValueError("synchronous must be FULL or NORMAL")
        if pool_size <= 0 or acquire_timeout <= 0:
            raise ValueError("pool_size and acquire_timeout must be positive")
        self.path = Path(path)
        self.synchronous = synchronous
        self.pool_size = pool_size
        self.acquire_timeout = acquire_timeout
        self._pool: queue.LifoQueue[sqlite3.Connection] = queue.LifoQueue(pool_size)
        self._pool_lock = threading.Lock()
        self._connections_created = 0
        self._closed = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.ledger = _LedgerView(self)
        self.tickets = _TicketView(self)

    def begin(
        self, key: str, *, call_id: str, context: Any, spec: Any,
        args_hash: str, tool_schema_hash: str, owner_id: str, lease_seconds: float,
    ) -> tuple[BeginKind, LedgerRecord]:
        now = _now()
        lease = now + timedelta(seconds=lease_seconds)
        fields = _context_fields(context, spec)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """INSERT OR IGNORE INTO actionlens_governance_ledger
                   (key, project, environment, tenant_id, session_id, run_id, tool_name,
                    call_id, status, hit_count, args_hash, tool_schema_hash, owner_id,
                    lease_expires_at, heartbeat_at, attempt, fencing_token, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'EXECUTING', 0, ?, ?, ?, ?, ?, 1, 1, ?, ?)""",
                (key, *fields, call_id, args_hash, tool_schema_hash, owner_id,
                 _iso(lease), _iso(now), _iso(now), _iso(now)),
            )
            if cursor.rowcount == 1:
                record = self._ledger_conn(conn, key)
                conn.commit()
                return "created", record  # type: ignore[return-value]
            record = self._ledger_conn(conn, key)
            if record is None:
                conn.rollback()
                raise RuntimeError("repository invariant violated: missing ledger row")
            if (
                record.args_hash != args_hash
                or record.tool_schema_hash != tool_schema_hash
                or _record_scope(record) != fields[:3]
            ):
                conn.execute(
                    "UPDATE actionlens_governance_ledger SET hit_count=hit_count+1, updated_at=? WHERE key=?",
                    (_iso(now), key),
                )
                conn.commit()
                return "conflict", record
            expired = record.lease_expires_at is not None and record.lease_expires_at <= now
            reacquire = record.status in {"APPROVED", "FAILED_RETRYABLE", "FAILED"}
            if record.status in {"EXECUTING", "PENDING"} and expired:
                if not bool(getattr(spec, "fencing_supported", False)):
                    conn.execute(
                        """UPDATE actionlens_governance_ledger
                           SET status='UNCERTAIN', hit_count=hit_count+1, updated_at=? WHERE key=?""",
                        (_iso(now), key),
                    )
                    record = self._ledger_conn(conn, key)
                    conn.commit()
                    return "uncertain", record  # type: ignore[return-value]
                reacquire = True
            if reacquire:
                conn.execute(
                    """UPDATE actionlens_governance_ledger SET status='EXECUTING', call_id=?,
                       owner_id=?, lease_expires_at=?, heartbeat_at=?, attempt=attempt+1,
                       fencing_token=fencing_token+1, hit_count=hit_count+1, updated_at=? WHERE key=?""",
                    (call_id, owner_id, _iso(lease), _iso(now), _iso(now), key),
                )
                record = self._ledger_conn(conn, key)
                conn.commit()
                return "created", record  # type: ignore[return-value]
            conn.execute(
                "UPDATE actionlens_governance_ledger SET hit_count=hit_count+1, updated_at=? WHERE key=?",
                (_iso(now), key),
            )
            record = self._ledger_conn(conn, key)
            conn.commit()
            return ("uncertain" if record and record.status == "UNCERTAIN" else "hit"), record  # type: ignore[return-value]

    def heartbeat(self, key: str, *, owner_id: str, fencing_token: int, lease_seconds: float) -> bool:
        now = _now()
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE actionlens_governance_ledger SET heartbeat_at=?, lease_expires_at=?, updated_at=?
                   WHERE key=? AND status='EXECUTING' AND owner_id=? AND fencing_token=?""",
                (_iso(now), _iso(now + timedelta(seconds=lease_seconds)), _iso(now), key, owner_id, fencing_token),
            )
            return cursor.rowcount == 1

    def create_approval(
        self, key: str, *, ticket: ApprovalTicket, context: Any, spec: Any,
        args_hash: str, tool_schema_hash: str, event: TrajectoryEvent,
    ) -> tuple[ApprovalTicket, LedgerRecord, bool]:
        now = _now()
        fields = _context_fields(context, spec)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = self._ledger_conn(conn, key)
            if existing is not None:
                if (
                    existing.args_hash != args_hash
                    or existing.tool_schema_hash != tool_schema_hash
                    or _record_scope(existing) != fields[:3]
                ):
                    conn.rollback()
                    return ticket, existing, False
                if existing.ticket_id:
                    stored = self._ticket_conn(conn, existing.ticket_id)
                    conn.commit()
                    return stored or ticket, existing, False
            conn.execute(
                """INSERT INTO actionlens_governance_ledger
                   (key, project, environment, tenant_id, session_id, run_id, tool_name, call_id,
                    status, hit_count, ticket_id, args_hash, tool_schema_hash, attempt, fencing_token,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'APPROVAL_PENDING', 0, ?, ?, ?, 0, 0, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET status='APPROVAL_PENDING', ticket_id=excluded.ticket_id,
                    call_id=excluded.call_id, updated_at=excluded.updated_at""",
                (key, *fields, ticket.call_id, ticket.ticket_id, args_hash, tool_schema_hash, _iso(now), _iso(now)),
            )
            self._insert_ticket(conn, ticket)
            self._insert_outbox(conn, event)
            record = self._ledger_conn(conn, key)
            conn.commit()
            return ticket, record, True  # type: ignore[return-value]

    def decide_approval(
        self, ticket_id: str, *, status: Literal["APPROVED", "DENIED"],
        approved_by: str | None, decision_note: str | None,
        modified_args: dict[str, Any] | None, event: TrajectoryEvent,
    ) -> ApprovalTicket | None:
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = self._ticket_conn(conn, ticket_id)
            if current is None or current.status != "PENDING" or (
                current.expires_at is not None and current.expires_at <= now
            ):
                conn.rollback()
                return None
            conn.execute(
                """UPDATE actionlens_approval_tickets SET status=?, approved_by=?, decision_note=?,
                   modified_args_json=?, approved_at=? WHERE ticket_id=? AND status='PENDING'""",
                (status, approved_by, decision_note,
                 json.dumps(modified_args, ensure_ascii=False, default=str) if modified_args is not None else None,
                 _iso(now) if status == "APPROVED" else None, ticket_id),
            )
            ledger_status = "APPROVED" if status == "APPROVED" else "DENIED"
            conn.execute(
                "UPDATE actionlens_governance_ledger SET status=?, updated_at=? WHERE ticket_id=? AND status='APPROVAL_PENDING'",
                (ledger_status, _iso(now), ticket_id),
            )
            self._insert_outbox(conn, event)
            result = self._ticket_conn(conn, ticket_id)
            conn.commit()
            return result

    def finish(
        self, key: str, *, owner_id: str, fencing_token: int,
        status: Literal["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_TERMINAL", "UNCERTAIN"],
        output: dict[str, Any] | None, error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord:
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE actionlens_governance_ledger SET status=?, output_json=?, last_error=?,
                   lease_expires_at=NULL, updated_at=? WHERE key=? AND status='EXECUTING'
                   AND owner_id=? AND fencing_token=?""",
                (status, json.dumps(output, ensure_ascii=False, default=str) if output is not None else None,
                 error, _iso(now), key, owner_id, fencing_token),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise StaleFenceError(f"stale fencing token for idempotency key {key!r}")
            self._insert_outbox(conn, event)
            record = self._ledger_conn(conn, key)
            conn.commit()
            return record  # type: ignore[return-value]

    def set_ledger_status(
        self, key: str, *, status: Literal["FAILED_TERMINAL", "DENIED", "EXPIRED", "UNCERTAIN"],
        error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord | None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE actionlens_governance_ledger SET status=?,last_error=?,lease_expires_at=NULL,updated_at=?
                   WHERE key=? AND status NOT IN ('SUCCEEDED','DENIED','EXPIRED','FAILED_TERMINAL')""",
                (status, error, _iso(_now()), key),
            )
            if cursor.rowcount == 0:
                conn.rollback()
                return self._ledger_conn(conn, key)
            self._insert_outbox(conn, event)
            result = self._ledger_conn(conn, key)
            conn.commit()
            return result

    def get_ledger(self, key: str) -> LedgerRecord | None:
        with self._connect() as conn:
            return self._ledger_conn(conn, key)

    def list_ledger(self) -> list[LedgerRecord]:
        with self._connect() as conn:
            return [_ledger_from_row(row) for row in conn.execute(
                "SELECT * FROM actionlens_governance_ledger ORDER BY created_at, key"
            ).fetchall()]

    def get_ticket(self, ticket_id: str) -> ApprovalTicket | None:
        with self._connect() as conn:
            ticket = self._ticket_conn(conn, ticket_id)
            if ticket and ticket.status == "PENDING" and ticket.expires_at and ticket.expires_at <= _now():
                conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE ticket_id=?", (ticket_id,))
                conn.execute("UPDATE actionlens_governance_ledger SET status='EXPIRED', updated_at=? WHERE ticket_id=?", (_iso(_now()), ticket_id))
                ticket = self._ticket_conn(conn, ticket_id)
            return ticket

    def list_tickets(self, *, status: str | None = None) -> list[ApprovalTicket]:
        with self._connect() as conn:
            now = _iso(_now())
            conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE status='PENDING' AND expires_at IS NOT NULL AND expires_at<=?", (now,))
            conn.execute("UPDATE actionlens_governance_ledger SET status='EXPIRED', updated_at=? WHERE status='APPROVAL_PENDING' AND ticket_id IN (SELECT ticket_id FROM actionlens_approval_tickets WHERE status='EXPIRED')", (now,))
            query = "SELECT * FROM actionlens_approval_tickets"
            args: tuple[Any, ...] = ()
            if status is not None:
                query += " WHERE status=?"
                args = (status,)
            query += " ORDER BY requested_at"
            return [_ticket_from_row(row) for row in conn.execute(query, args).fetchall()]

    def expire_ticket(self, ticket_id: str, *, event: TrajectoryEvent | None = None) -> ApprovalTicket | None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE ticket_id=? AND status='PENDING'", (ticket_id,))
            conn.execute("UPDATE actionlens_governance_ledger SET status='EXPIRED', updated_at=? WHERE ticket_id=? AND status IN ('APPROVAL_PENDING','FAILED_RETRYABLE')", (_iso(_now()), ticket_id))
            if event is not None:
                self._insert_outbox(conn, event)
            result = self._ticket_conn(conn, ticket_id)
            conn.commit()
            return result

    def claim_outbox(self, *, worker_id: str, limit: int = 100, claim_seconds: float = 30.0) -> list[OutboxRecord]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if claim_seconds <= 0:
            raise ValueError("claim_seconds must be positive")
        now = _now()
        claim_until = now + timedelta(seconds=claim_seconds)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT delivery_id FROM actionlens_outbox WHERE delivered_at IS NULL
                   AND dead_letter_at IS NULL AND next_retry_at<=? AND (claim_expires_at IS NULL OR claim_expires_at<=?)
                   ORDER BY created_at LIMIT ?""", (_iso(now), _iso(now), limit)
            ).fetchall()
            ids = [row["delivery_id"] for row in rows]
            for delivery_id in ids:
                conn.execute("UPDATE actionlens_outbox SET claimed_by=?, claim_expires_at=? WHERE delivery_id=?", (worker_id, _iso(claim_until), delivery_id))
            result = [self._outbox_conn(conn, item) for item in ids]
            conn.commit()
            return [item for item in result if item is not None]

    def ack_outbox(self, delivery_id: str, *, worker_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("UPDATE actionlens_outbox SET delivered_at=?, claimed_by=NULL, claim_expires_at=NULL WHERE delivery_id=? AND claimed_by=? AND delivered_at IS NULL", (_iso(_now()), delivery_id, worker_id))
            return cursor.rowcount == 1

    def retry_outbox(self, delivery_id: str, *, worker_id: str, error: str, next_retry_at: datetime) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("UPDATE actionlens_outbox SET attempt=attempt+1, last_error=?, next_retry_at=?, claimed_by=NULL, claim_expires_at=NULL WHERE delivery_id=? AND claimed_by=? AND delivered_at IS NULL", (error[:2000], _iso(next_retry_at), delivery_id, worker_id))
            return cursor.rowcount == 1

    def dead_letter_outbox(self, delivery_id: str, *, worker_id: str, error: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("UPDATE actionlens_outbox SET attempt=attempt+1,last_error=?,dead_letter_at=?,claimed_by=NULL,claim_expires_at=NULL WHERE delivery_id=? AND claimed_by=? AND delivered_at IS NULL", (error[:2000],_iso(_now()),delivery_id,worker_id))
            return cursor.rowcount == 1

    def resolve_uncertain(
        self, key: str, *, status: Literal["SUCCEEDED", "FAILED_RETRYABLE", "UNCERTAIN"],
        output: dict[str, Any] | None, error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord | None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE actionlens_governance_ledger
                   SET status=?, output_json=?, last_error=?, owner_id=NULL,
                       lease_expires_at=NULL, heartbeat_at=NULL, updated_at=?
                   WHERE key=? AND status='UNCERTAIN'""",
                (status, json.dumps(output, ensure_ascii=False, default=str)
                 if output is not None else None, error, _iso(_now()), key),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return None
            self._insert_outbox(conn, event)
            result = self._ledger_conn(conn, key)
            conn.commit()
            return result

    def list_outbox(
        self, *, state: Literal["pending", "delivered", "dead_letter"] | None = None,
        limit: int = 100,
    ) -> list[OutboxRecord]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        clauses = {
            "pending": "delivered_at IS NULL AND dead_letter_at IS NULL",
            "delivered": "delivered_at IS NOT NULL",
            "dead_letter": "dead_letter_at IS NOT NULL AND terminated_at IS NULL",
        }
        query = "SELECT * FROM actionlens_outbox"
        if state is not None:
            query += f" WHERE {clauses[state]}"
        query += " ORDER BY created_at LIMIT ?"
        with self._connect() as conn:
            return [_outbox_from_row(row) for row in conn.execute(query, (limit,)).fetchall()]

    def outbox_stats(self) -> dict[str, int | float | None]:
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                """SELECT
                   SUM(CASE WHEN delivered_at IS NULL AND dead_letter_at IS NULL
                            AND terminated_at IS NULL THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) AS delivered,
                   SUM(CASE WHEN dead_letter_at IS NOT NULL AND terminated_at IS NULL
                            THEN 1 ELSE 0 END) AS dead_letter,
                   SUM(CASE WHEN terminated_at IS NOT NULL THEN 1 ELSE 0 END) AS terminated,
                   MIN(CASE WHEN delivered_at IS NULL AND dead_letter_at IS NULL
                            AND terminated_at IS NULL THEN created_at END) AS oldest
                   FROM actionlens_outbox"""
            ).fetchone()
        oldest = datetime.fromisoformat(row["oldest"]) if row["oldest"] else None
        return {
            "pending": int(row["pending"] or 0),
            "delivered": int(row["delivered"] or 0),
            "dead_letter": int(row["dead_letter"] or 0),
            "terminated": int(row["terminated"] or 0),
            "oldest_pending_lag_seconds": max(0.0, (now - oldest).total_seconds())
            if oldest else None,
        }

    def cleanup_outbox(self, *, delivered_before: datetime, limit: int = 1000) -> int:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._connect() as conn:
            cursor = conn.execute(
                """DELETE FROM actionlens_outbox WHERE delivery_id IN
                   (SELECT delivery_id FROM actionlens_outbox
                    WHERE delivered_at IS NOT NULL AND delivered_at < ?
                    ORDER BY delivered_at LIMIT ?)""",
                (_iso(delivered_before), limit),
            )
            return cursor.rowcount

    def replay_dead_letter(self, delivery_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE actionlens_outbox SET dead_letter_at=NULL, terminated_at=NULL,
                   attempt=0, next_retry_at=?, last_error=NULL, claimed_by=NULL,
                   claim_expires_at=NULL WHERE delivery_id=? AND dead_letter_at IS NOT NULL
                   AND terminated_at IS NULL AND delivered_at IS NULL""",
                (_iso(_now()), delivery_id),
            )
            return cursor.rowcount == 1

    def terminate_dead_letter(self, delivery_id: str, *, reason: str) -> bool:
        if not reason.strip():
            raise ValueError("termination reason is required")
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE actionlens_outbox SET terminated_at=?, last_error=?
                   WHERE delivery_id=? AND dead_letter_at IS NOT NULL
                   AND terminated_at IS NULL AND delivered_at IS NULL""",
                (_iso(_now()), f"terminated: {reason}"[:2000], delivery_id),
            )
            return cursor.rowcount == 1

    def _insert_outbox(self, conn: sqlite3.Connection, event: TrajectoryEvent) -> None:
        now = _now()
        conn.execute(
            """INSERT OR IGNORE INTO actionlens_outbox
               (delivery_id, event_id, event_json, attempt, next_retry_at, created_at)
               VALUES (?, ?, ?, 0, ?, ?)""",
            (f"delivery-{uuid4().hex}", event.event_id, event.model_dump_json(exclude_none=True), _iso(now), _iso(now)),
        )

    def _insert_ticket(self, conn: sqlite3.Connection, ticket: ApprovalTicket) -> None:
        conn.execute(
            """INSERT INTO actionlens_approval_tickets
               (ticket_id,idempotency_key,call_id,tool_name,safe_args_json,risk,reason,status,
                requested_at,expires_at,metadata_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (ticket.ticket_id, ticket.idempotency_key, ticket.call_id, ticket.tool_name,
             json.dumps(ticket.safe_args, ensure_ascii=False, default=str), ticket.risk.value,
             ticket.reason, ticket.status, _iso(ticket.requested_at), _iso(ticket.expires_at),
             json.dumps(ticket.metadata, ensure_ascii=False, default=str)),
        )

    def _ledger_conn(self, conn: sqlite3.Connection, key: str) -> LedgerRecord | None:
        row = conn.execute("SELECT * FROM actionlens_governance_ledger WHERE key=?", (key,)).fetchone()
        return _ledger_from_row(row) if row else None

    def _ticket_conn(self, conn: sqlite3.Connection, ticket_id: str) -> ApprovalTicket | None:
        row = conn.execute("SELECT * FROM actionlens_approval_tickets WHERE ticket_id=?", (ticket_id,)).fetchone()
        return _ticket_from_row(row) if row else None

    def _outbox_conn(self, conn: sqlite3.Connection, delivery_id: str) -> OutboxRecord | None:
        row = conn.execute("SELECT * FROM actionlens_outbox WHERE delivery_id=?", (delivery_id,)).fetchone()
        return _outbox_from_row(row) if row else None

    @contextmanager
    def _connect(self):
        if self._closed:
            raise RuntimeError("SQLite repository is closed")
        try:
            conn = self._pool.get_nowait()
        except queue.Empty:
            create = False
            with self._pool_lock:
                if self._connections_created < self.pool_size:
                    self._connections_created += 1
                    create = True
            if create:
                try:
                    conn = self._new_connection()
                except Exception:
                    with self._pool_lock:
                        self._connections_created -= 1
                    raise
            else:
                try:
                    conn = self._pool.get(timeout=self.acquire_timeout)
                except queue.Empty as exc:
                    raise TimeoutError("SQLite repository pool exhausted") from exc
        try:
            with conn:
                yield conn
        finally:
            if self._closed:
                conn.close()
                with self._pool_lock:
                    self._connections_created -= 1
            else:
                self._pool.put(conn)

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, timeout=self.acquire_timeout, isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={max(1, round(self.acquire_timeout * 1000))}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA synchronous={self.synchronous}")
        return conn

    def pool_stats(self) -> dict[str, int]:
        with self._pool_lock:
            created = self._connections_created
        return {
            "pool_size": self.pool_size,
            "connections_created": created,
            "connections_available": self._pool.qsize(),
            "connections_in_use": created - self._pool.qsize(),
        }

    def close(self) -> None:
        self._closed = True
        while True:
            try:
                conn = self._pool.get_nowait()
            except queue.Empty:
                break
            conn.close()
            with self._pool_lock:
                self._connections_created -= 1

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS actionlens_governance_ledger (
              key TEXT PRIMARY KEY, project TEXT NOT NULL, environment TEXT NOT NULL,
              tenant_id TEXT, session_id TEXT NOT NULL, run_id TEXT NOT NULL, tool_name TEXT NOT NULL,
              call_id TEXT NOT NULL, status TEXT NOT NULL, hit_count INTEGER NOT NULL DEFAULT 0,
              output_json TEXT, ticket_id TEXT, args_hash TEXT NOT NULL, tool_schema_hash TEXT NOT NULL,
              owner_id TEXT, lease_expires_at TEXT, heartbeat_at TEXT, attempt INTEGER NOT NULL DEFAULT 0,
              fencing_token INTEGER NOT NULL DEFAULT 0, last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_al_gov_status ON actionlens_governance_ledger(status);
            CREATE TABLE IF NOT EXISTS actionlens_approval_tickets (
              ticket_id TEXT PRIMARY KEY, idempotency_key TEXT, call_id TEXT NOT NULL,
              tool_name TEXT NOT NULL, safe_args_json TEXT NOT NULL, risk TEXT NOT NULL,
              reason TEXT NOT NULL, status TEXT NOT NULL, approved_by TEXT, decision_note TEXT,
              modified_args_json TEXT, requested_at TEXT NOT NULL, approved_at TEXT,
              expires_at TEXT, metadata_json TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_al_ticket_active_key
              ON actionlens_approval_tickets(idempotency_key) WHERE status IN ('PENDING','APPROVED');
            CREATE TABLE IF NOT EXISTS actionlens_outbox (
              delivery_id TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, event_json TEXT NOT NULL,
              attempt INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT NOT NULL, last_error TEXT,
              delivered_at TEXT, claimed_by TEXT, claim_expires_at TEXT, dead_letter_at TEXT,
              terminated_at TEXT, created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_al_outbox_ready ON actionlens_outbox(delivered_at,next_retry_at);
            CREATE TABLE IF NOT EXISTS actionlens_schema_migrations (
              version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            );
            INSERT OR IGNORE INTO actionlens_schema_migrations(version,applied_at) VALUES (5, CURRENT_TIMESTAMP);
            INSERT OR IGNORE INTO actionlens_schema_migrations(version,applied_at) VALUES (10, CURRENT_TIMESTAMP);
            """)
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(actionlens_outbox)")}
            if "dead_letter_at" not in columns:
                conn.execute("ALTER TABLE actionlens_outbox ADD COLUMN dead_letter_at TEXT")
            if "terminated_at" not in columns:
                conn.execute("ALTER TABLE actionlens_outbox ADD COLUMN terminated_at TEXT")


class _LedgerView:
    def __init__(self, repository: SQLiteGovernanceRepository): self.repository = repository
    def get(self, key: str) -> LedgerRecord | None: return self.repository.get_ledger(key)
    def records(self) -> list[LedgerRecord]: return self.repository.list_ledger()


class _TicketView:
    def __init__(self, repository: SQLiteGovernanceRepository): self.repository = repository
    def get(self, ticket_id: str) -> ApprovalTicket | None: return self.repository.get_ticket(ticket_id)
    def list(self, *, status: str | None = None) -> list[ApprovalTicket]: return self.repository.list_tickets(status=status)


def _context_fields(context: Any, spec: Any) -> tuple[str, str, str | None, str, str, str]:
    return (context.project, context.environment, context.tenant_id, context.session_id,
            context.run_id, getattr(spec, "name", context.tool_name))


def _record_scope(record: LedgerRecord) -> tuple[str, str, str | None]:
    return record.project, record.environment, record.tenant_id


def _ledger_from_row(row: sqlite3.Row) -> LedgerRecord:
    output = json.loads(row["output_json"]) if row["output_json"] else None
    return LedgerRecord(
        key=row["key"], status=row["status"], call_id=row["call_id"],
        created_at=datetime.fromisoformat(row["created_at"]), updated_at=datetime.fromisoformat(row["updated_at"]),
        hit_count=row["hit_count"], output=output, ticket_id=row["ticket_id"], project=row["project"],
        environment=row["environment"], tenant_id=row["tenant_id"], session_id=row["session_id"],
        run_id=row["run_id"], tool_name=row["tool_name"], args_hash=row["args_hash"],
        tool_schema_hash=row["tool_schema_hash"], owner_id=row["owner_id"],
        lease_expires_at=datetime.fromisoformat(row["lease_expires_at"]) if row["lease_expires_at"] else None,
        heartbeat_at=datetime.fromisoformat(row["heartbeat_at"]) if row["heartbeat_at"] else None,
        attempt=row["attempt"], fencing_token=row["fencing_token"], last_error=row["last_error"],
    )


def _ticket_from_row(row: sqlite3.Row) -> ApprovalTicket:
    return ApprovalTicket(
        ticket_id=row["ticket_id"], idempotency_key=row["idempotency_key"], call_id=row["call_id"],
        tool_name=row["tool_name"], safe_args=json.loads(row["safe_args_json"]), risk=RiskLevel(row["risk"]),
        reason=row["reason"], status=row["status"], approved_by=row["approved_by"],
        decision_note=row["decision_note"], modified_args=json.loads(row["modified_args_json"]) if row["modified_args_json"] else None,
        requested_at=datetime.fromisoformat(row["requested_at"]),
        approved_at=datetime.fromisoformat(row["approved_at"]) if row["approved_at"] else None,
        expires_at=datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None,
        metadata=json.loads(row["metadata_json"]),
    )


def _outbox_from_row(row: sqlite3.Row) -> OutboxRecord:
    return OutboxRecord(
        delivery_id=row["delivery_id"], event=TrajectoryEvent.model_validate_json(row["event_json"]),
        attempt=row["attempt"], next_retry_at=datetime.fromisoformat(row["next_retry_at"]),
        last_error=row["last_error"], delivered_at=datetime.fromisoformat(row["delivered_at"]) if row["delivered_at"] else None,
        claimed_by=row["claimed_by"], claim_expires_at=datetime.fromisoformat(row["claim_expires_at"]) if row["claim_expires_at"] else None,
        dead_letter_at=datetime.fromisoformat(row["dead_letter_at"]) if row["dead_letter_at"] else None,
        terminated_at=datetime.fromisoformat(row["terminated_at"]) if row["terminated_at"] else None,
    )
