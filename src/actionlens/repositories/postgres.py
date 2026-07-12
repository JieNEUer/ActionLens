from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import uuid4

from actionlens.ledger.memory import LedgerRecord
from actionlens.models import ApprovalTicket, OutboxRecord, RiskLevel, TrajectoryEvent
from actionlens.repository import BeginKind, StaleFenceError


POSTGRES_MIGRATION_SQL = """
CREATE TABLE IF NOT EXISTS actionlens_schema_migrations (
  version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS actionlens_governance_ledger (
  key text PRIMARY KEY, project text NOT NULL, environment text NOT NULL, tenant_id text,
  session_id text NOT NULL, run_id text NOT NULL, tool_name text NOT NULL, call_id text NOT NULL,
  status text NOT NULL, hit_count bigint NOT NULL DEFAULT 0, output_json jsonb, ticket_id text,
  args_hash text NOT NULL, tool_schema_hash text NOT NULL, owner_id text,
  lease_expires_at timestamptz, heartbeat_at timestamptz, attempt integer NOT NULL DEFAULT 0,
  fencing_token bigint NOT NULL DEFAULT 0, last_error text,
  created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_al_gov_status ON actionlens_governance_ledger(status);
CREATE TABLE IF NOT EXISTS actionlens_approval_tickets (
  ticket_id text PRIMARY KEY, idempotency_key text, call_id text NOT NULL, tool_name text NOT NULL,
  safe_args_json jsonb NOT NULL, risk text NOT NULL, reason text NOT NULL, status text NOT NULL,
  approved_by text, decision_note text, modified_args_json jsonb, requested_at timestamptz NOT NULL,
  approved_at timestamptz, expires_at timestamptz, metadata_json jsonb NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_al_ticket_active_key ON actionlens_approval_tickets(idempotency_key)
  WHERE status IN ('PENDING','APPROVED');
CREATE TABLE IF NOT EXISTS actionlens_outbox (
  delivery_id text PRIMARY KEY, event_id text NOT NULL UNIQUE, event_json jsonb NOT NULL,
  attempt integer NOT NULL DEFAULT 0, next_retry_at timestamptz NOT NULL, last_error text,
  delivered_at timestamptz, claimed_by text, claim_expires_at timestamptz, dead_letter_at timestamptz,
  terminated_at timestamptz, created_at timestamptz NOT NULL
);
ALTER TABLE actionlens_outbox ADD COLUMN IF NOT EXISTS dead_letter_at timestamptz;
ALTER TABLE actionlens_outbox ADD COLUMN IF NOT EXISTS terminated_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_al_outbox_ready ON actionlens_outbox(delivered_at,next_retry_at);
CREATE INDEX IF NOT EXISTS idx_al_outbox_claim_v11
  ON actionlens_outbox(next_retry_at,created_at)
  WHERE delivered_at IS NULL AND dead_letter_at IS NULL AND terminated_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_al_outbox_retention_v11 ON actionlens_outbox(delivered_at)
  WHERE delivered_at IS NOT NULL;
INSERT INTO actionlens_schema_migrations(version) VALUES (5) ON CONFLICT(version) DO NOTHING;
INSERT INTO actionlens_schema_migrations(version) VALUES (10) ON CONFLICT(version) DO NOTHING;
INSERT INTO actionlens_schema_migrations(version) VALUES (11) ON CONFLICT(version) DO NOTHING;
"""

LATEST_SCHEMA_VERSION = 11
MIN_COMPATIBLE_SCHEMA_VERSION = 10
MIGRATION_LOCK_ID = 0x4143544C454E53


class SchemaCompatibilityError(RuntimeError):
    """The database schema is outside this client's compatibility window."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PostgresGovernanceRepository:
    """PostgreSQL repository; all governance transitions and outbox writes are atomic."""

    def __init__(
        self, dsn: str, *, connect: Any | None = None, pool: Any | None = None,
        auto_migrate: bool = False, min_pool_size: int = 1, max_pool_size: int = 10,
        pool_timeout: float = 30.0, max_idle_seconds: float = 300.0,
        statement_timeout_ms: int = 30_000, lock_timeout_ms: int = 5_000,
        transaction_timeout_ms: int = 60_000, verify_schema: bool = True,
    ):
        if min_pool_size < 0 or max_pool_size < 1 or min_pool_size > max_pool_size:
            raise ValueError("invalid PostgreSQL pool size")
        if min(pool_timeout, max_idle_seconds) <= 0:
            raise ValueError("pool_timeout and max_idle_seconds must be positive")
        if min(statement_timeout_ms, lock_timeout_ms, transaction_timeout_ms) <= 0:
            raise ValueError("PostgreSQL timeouts must be positive")
        self.dsn = dsn
        self._json_adapter = lambda value: value
        self._pool = pool
        self._owns_pool = False
        if connect is None:
            try:
                from psycopg.rows import dict_row
                from psycopg.types.json import Jsonb
            except ImportError as exc:
                raise ImportError("Install ActionLens with the 'postgres' extra") from exc
            if self._pool is None:
                try:
                    from psycopg_pool import ConnectionPool
                except ImportError as exc:
                    raise ImportError(
                        "PostgreSQL pooling requires psycopg-pool; install ActionLens "
                        "with the 'postgres' extra"
                    ) from exc
                options = " ".join((
                    f"-c statement_timeout={statement_timeout_ms}",
                    f"-c lock_timeout={lock_timeout_ms}",
                    f"-c transaction_timeout={transaction_timeout_ms}",
                ))
                self._pool = ConnectionPool(
                    conninfo=dsn, min_size=min_pool_size, max_size=max_pool_size,
                    timeout=pool_timeout, max_idle=max_idle_seconds,
                    kwargs={"row_factory": dict_row, "application_name": "actionlens",
                            "options": options}, open=True,
                )
                self._owns_pool = True
            self._json_adapter = Jsonb
        self._connect_factory = connect
        try:
            if auto_migrate:
                self.migrate()
            elif verify_schema and (connect is None or pool is not None):
                self.verify_schema()
        except Exception:
            self.close()
            raise
        self.ledger = _LedgerView(self)
        self.tickets = _TicketView(self)

    @contextmanager
    def _connection(self):
        if self._pool is not None:
            with self._pool.connection() as conn:
                yield conn
            return
        if self._connect_factory is None:
            raise RuntimeError("PostgreSQL repository has no connection source")
        conn = self._connect_factory(self.dsn)
        try:
            yield conn
        finally:
            conn.close()

    def migrate(self) -> None:
        with self._connection() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
            conn.execute(POSTGRES_MIGRATION_SQL)

    def schema_status(self) -> dict[str, int | bool]:
        with self._connection() as conn:
            exists = conn.execute(
                "SELECT to_regclass('public.actionlens_schema_migrations') AS name"
            ).fetchone()
            if not exists or not exists.get("name"):
                current = 0
            else:
                row = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version "
                    "FROM actionlens_schema_migrations"
                ).fetchone()
                current = int(row["version"])
        return {
            "current": current,
            "minimum_compatible": MIN_COMPATIBLE_SCHEMA_VERSION,
            "latest": LATEST_SCHEMA_VERSION,
            "compatible": MIN_COMPATIBLE_SCHEMA_VERSION <= current <= LATEST_SCHEMA_VERSION,
        }

    def verify_schema(self) -> dict[str, int | bool]:
        status = self.schema_status()
        if not status["compatible"]:
            raise SchemaCompatibilityError(
                f"ActionLens PostgreSQL schema {status['current']} is outside supported "
                f"range {status['minimum_compatible']}..{status['latest']}; "
                "run 'actionlens migrate'"
            )
        return status

    def pool_stats(self) -> dict[str, int | float]:
        if self._pool is None:
            return {"pooled": 0}
        get_stats = getattr(self._pool, "get_stats", None)
        stats = dict(get_stats() if get_stats is not None else {})
        stats["pooled"] = 1
        return stats

    def close(self) -> None:
        if self._owns_pool and self._pool is not None:
            self._pool.close()
            self._owns_pool = False

    def __enter__(self) -> "PostgresGovernanceRepository":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def begin(
        self, key: str, *, call_id: str, context: Any, spec: Any, args_hash: str,
        tool_schema_hash: str, owner_id: str, lease_seconds: float,
    ) -> tuple[BeginKind, LedgerRecord]:
        now = _now()
        lease = now + timedelta(seconds=lease_seconds)
        fields = _context_fields(context, spec)
        with self._connection() as conn, conn.transaction():
            row = conn.execute(
                """INSERT INTO actionlens_governance_ledger
                   (key,project,environment,tenant_id,session_id,run_id,tool_name,call_id,status,
                    args_hash,tool_schema_hash,owner_id,lease_expires_at,heartbeat_at,attempt,fencing_token,created_at,updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'EXECUTING',%s,%s,%s,%s,%s,1,1,%s,%s)
                   ON CONFLICT(key) DO NOTHING RETURNING *""",
                (key, *fields, call_id, args_hash, tool_schema_hash, owner_id, lease, now, now, now),
            ).fetchone()
            if row is not None:
                return "created", _ledger(row)
            row = conn.execute("SELECT * FROM actionlens_governance_ledger WHERE key=%s FOR UPDATE", (key,)).fetchone()
            record = _ledger(row)
            if (
                record.args_hash != args_hash
                or record.tool_schema_hash != tool_schema_hash
                or _record_scope(record) != fields[:3]
            ):
                conn.execute("UPDATE actionlens_governance_ledger SET hit_count=hit_count+1,updated_at=%s WHERE key=%s", (now, key))
                return "conflict", record
            expired = record.lease_expires_at is not None and record.lease_expires_at <= now
            reacquire = record.status in {"APPROVED", "FAILED_RETRYABLE", "FAILED"}
            if record.status in {"EXECUTING", "PENDING"} and expired:
                if not bool(getattr(spec, "fencing_supported", False)):
                    row = conn.execute("UPDATE actionlens_governance_ledger SET status='UNCERTAIN',hit_count=hit_count+1,updated_at=%s WHERE key=%s RETURNING *", (now, key)).fetchone()
                    return "uncertain", _ledger(row)
                reacquire = True
            if reacquire:
                row = conn.execute(
                    """UPDATE actionlens_governance_ledger SET status='EXECUTING',call_id=%s,owner_id=%s,
                       lease_expires_at=%s,heartbeat_at=%s,attempt=attempt+1,fencing_token=fencing_token+1,
                       hit_count=hit_count+1,updated_at=%s WHERE key=%s RETURNING *""",
                    (call_id, owner_id, lease, now, now, key),
                ).fetchone()
                return "created", _ledger(row)
            row = conn.execute("UPDATE actionlens_governance_ledger SET hit_count=hit_count+1,updated_at=%s WHERE key=%s RETURNING *", (now, key)).fetchone()
            updated = _ledger(row)
            return ("uncertain" if updated.status == "UNCERTAIN" else "hit"), updated

    def heartbeat(self, key: str, *, owner_id: str, fencing_token: int, lease_seconds: float) -> bool:
        now = _now()
        with self._connection() as conn, conn.transaction():
            cursor = conn.execute(
                """UPDATE actionlens_governance_ledger SET heartbeat_at=%s,lease_expires_at=%s,updated_at=%s
                   WHERE key=%s AND status='EXECUTING' AND owner_id=%s AND fencing_token=%s""",
                (now, now + timedelta(seconds=lease_seconds), now, key, owner_id, fencing_token),
            )
            return cursor.rowcount == 1

    def create_approval(
        self, key: str, *, ticket: ApprovalTicket, context: Any, spec: Any,
        args_hash: str, tool_schema_hash: str, event: TrajectoryEvent,
    ) -> tuple[ApprovalTicket, LedgerRecord, bool]:
        now = _now()
        with self._connection() as conn, conn.transaction():
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (key,)
            )
            row = conn.execute("SELECT * FROM actionlens_governance_ledger WHERE key=%s FOR UPDATE", (key,)).fetchone()
            if row is not None:
                record = _ledger(row)
                fields = _context_fields(context, spec)
                if (
                    record.args_hash != args_hash
                    or record.tool_schema_hash != tool_schema_hash
                    or _record_scope(record) != fields[:3]
                ):
                    return ticket, record, False
                if record.ticket_id:
                    stored = conn.execute("SELECT * FROM actionlens_approval_tickets WHERE ticket_id=%s", (record.ticket_id,)).fetchone()
                    return _ticket(stored), record, False
            fields = _context_fields(context, spec)
            conn.execute(
                """INSERT INTO actionlens_governance_ledger
                   (key,project,environment,tenant_id,session_id,run_id,tool_name,call_id,status,ticket_id,
                    args_hash,tool_schema_hash,created_at,updated_at) VALUES
                   (%s,%s,%s,%s,%s,%s,%s,%s,'APPROVAL_PENDING',%s,%s,%s,%s,%s)""",
                (key, *fields, ticket.call_id, ticket.ticket_id, args_hash, tool_schema_hash, now, now),
            )
            conn.execute(
                """INSERT INTO actionlens_approval_tickets
                   (ticket_id,idempotency_key,call_id,tool_name,safe_args_json,risk,reason,status,requested_at,expires_at,metadata_json)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (ticket.ticket_id,ticket.idempotency_key,ticket.call_id,ticket.tool_name,self._json_adapter(ticket.safe_args),
                 ticket.risk.value,ticket.reason,ticket.status,ticket.requested_at,ticket.expires_at,self._json_adapter(ticket.metadata)),
            )
            self._outbox(conn, event)
            row = conn.execute("SELECT * FROM actionlens_governance_ledger WHERE key=%s", (key,)).fetchone()
            return ticket, _ledger(row), True

    def decide_approval(
        self, ticket_id: str, *, status: Literal["APPROVED", "DENIED"], approved_by: str | None,
        decision_note: str | None, modified_args: dict[str, Any] | None, event: TrajectoryEvent,
    ) -> ApprovalTicket | None:
        now = _now()
        with self._connection() as conn, conn.transaction():
            row = conn.execute("SELECT * FROM actionlens_approval_tickets WHERE ticket_id=%s FOR UPDATE", (ticket_id,)).fetchone()
            if row is None:
                return None
            current = _ticket(row)
            if current.status != "PENDING" or (current.expires_at and current.expires_at <= now):
                return None
            row = conn.execute(
                """UPDATE actionlens_approval_tickets SET status=%s,approved_by=%s,decision_note=%s,
                   modified_args_json=%s,approved_at=%s WHERE ticket_id=%s RETURNING *""",
                (status, approved_by, decision_note, self._json_adapter(modified_args) if modified_args is not None else None,
                 now if status == "APPROVED" else None, ticket_id),
            ).fetchone()
            conn.execute("UPDATE actionlens_governance_ledger SET status=%s,updated_at=%s WHERE ticket_id=%s AND status='APPROVAL_PENDING'", (status, now, ticket_id))
            self._outbox(conn, event)
            return _ticket(row)

    def finish(
        self, key: str, *, owner_id: str, fencing_token: int,
        status: Literal["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_TERMINAL", "UNCERTAIN"],
        output: dict[str, Any] | None, error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord:
        with self._connection() as conn, conn.transaction():
            row = conn.execute(
                """UPDATE actionlens_governance_ledger SET status=%s,output_json=%s,last_error=%s,
                   lease_expires_at=NULL,updated_at=%s WHERE key=%s AND status='EXECUTING'
                   AND owner_id=%s AND fencing_token=%s RETURNING *""",
                (status, self._json_adapter(output) if output is not None else None, error, _now(), key, owner_id, fencing_token),
            ).fetchone()
            if row is None:
                raise StaleFenceError(f"stale fencing token for idempotency key {key!r}")
            self._outbox(conn, event)
            return _ledger(row)

    def set_ledger_status(
        self, key: str, *, status: Literal["FAILED_TERMINAL", "DENIED", "EXPIRED", "UNCERTAIN"],
        error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord | None:
        with self._connection() as conn, conn.transaction():
            row = conn.execute(
                """UPDATE actionlens_governance_ledger SET status=%s,last_error=%s,lease_expires_at=NULL,updated_at=%s
                   WHERE key=%s AND status NOT IN ('SUCCEEDED','DENIED','EXPIRED','FAILED_TERMINAL') RETURNING *""",
                (status,error,_now(),key),
            ).fetchone()
            if row is None:
                row=conn.execute("SELECT * FROM actionlens_governance_ledger WHERE key=%s",(key,)).fetchone()
                return _ledger(row) if row else None
            self._outbox(conn,event)
            return _ledger(row)

    def get_ledger(self, key: str) -> LedgerRecord | None:
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM actionlens_governance_ledger WHERE key=%s", (key,)).fetchone()
            return _ledger(row) if row else None

    def list_ledger(self) -> list[LedgerRecord]:
        with self._connection() as conn:
            return [_ledger(row) for row in conn.execute("SELECT * FROM actionlens_governance_ledger ORDER BY created_at,key").fetchall()]

    def get_ticket(self, ticket_id: str) -> ApprovalTicket | None:
        with self._connection() as conn, conn.transaction():
            row = conn.execute("SELECT * FROM actionlens_approval_tickets WHERE ticket_id=%s FOR UPDATE", (ticket_id,)).fetchone()
            if row is None:
                return None
            result = _ticket(row)
            if result.status == "PENDING" and result.expires_at and result.expires_at <= _now():
                row = conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE ticket_id=%s RETURNING *", (ticket_id,)).fetchone()
                conn.execute("UPDATE actionlens_governance_ledger SET status='EXPIRED',updated_at=%s WHERE ticket_id=%s", (_now(),ticket_id))
                result = _ticket(row)
            return result

    def list_tickets(self, *, status: str | None = None) -> list[ApprovalTicket]:
        with self._connection() as conn, conn.transaction():
            conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE status='PENDING' AND expires_at<=now()")
            conn.execute(
                """UPDATE actionlens_governance_ledger SET status='EXPIRED',updated_at=%s
                   WHERE status='APPROVAL_PENDING' AND ticket_id IN
                   (SELECT ticket_id FROM actionlens_approval_tickets WHERE status='EXPIRED')""",
                (_now(),),
            )
            query = "SELECT * FROM actionlens_approval_tickets" + (" WHERE status=%s" if status else "") + " ORDER BY requested_at"
            return [_ticket(row) for row in conn.execute(query, (status,) if status else ()).fetchall()]

    def expire_ticket(self, ticket_id: str, *, event: TrajectoryEvent | None = None) -> ApprovalTicket | None:
        with self._connection() as conn, conn.transaction():
            row = conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE ticket_id=%s AND status='PENDING' RETURNING *", (ticket_id,)).fetchone()
            if row is None:
                row = conn.execute("SELECT * FROM actionlens_approval_tickets WHERE ticket_id=%s", (ticket_id,)).fetchone()
                if row is None:
                    return None
            conn.execute("UPDATE actionlens_governance_ledger SET status='EXPIRED',updated_at=%s WHERE ticket_id=%s AND status IN ('APPROVAL_PENDING','FAILED_RETRYABLE')", (_now(),ticket_id))
            if event:
                self._outbox(conn, event)
            return _ticket(row)

    def claim_outbox(self, *, worker_id: str, limit: int = 100, claim_seconds: float = 30.0) -> list[OutboxRecord]:
        now = _now()
        with self._connection() as conn, conn.transaction():
            rows = conn.execute(
                """WITH candidates AS (
                     SELECT delivery_id FROM actionlens_outbox
                     WHERE delivered_at IS NULL AND next_retry_at<=%s
                       AND dead_letter_at IS NULL AND terminated_at IS NULL
                       AND (claim_expires_at IS NULL OR claim_expires_at<=%s)
                     ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT %s
                   )
                   UPDATE actionlens_outbox AS outbox
                   SET claimed_by=%s, claim_expires_at=%s
                   FROM candidates WHERE outbox.delivery_id=candidates.delivery_id
                   RETURNING outbox.*""",
                (now, now, limit, worker_id, now + timedelta(seconds=claim_seconds)),
            ).fetchall()
            return [_outbox(row) for row in rows]

    def ack_outbox(self, delivery_id: str, *, worker_id: str) -> bool:
        with self._connection() as conn, conn.transaction():
            return conn.execute("UPDATE actionlens_outbox SET delivered_at=%s,claimed_by=NULL,claim_expires_at=NULL WHERE delivery_id=%s AND claimed_by=%s AND delivered_at IS NULL", (_now(),delivery_id,worker_id)).rowcount == 1

    def retry_outbox(self, delivery_id: str, *, worker_id: str, error: str, next_retry_at: datetime) -> bool:
        with self._connection() as conn, conn.transaction():
            return conn.execute("UPDATE actionlens_outbox SET attempt=attempt+1,last_error=%s,next_retry_at=%s,claimed_by=NULL,claim_expires_at=NULL WHERE delivery_id=%s AND claimed_by=%s AND delivered_at IS NULL", (error[:2000],next_retry_at,delivery_id,worker_id)).rowcount == 1

    def dead_letter_outbox(self, delivery_id: str, *, worker_id: str, error: str) -> bool:
        with self._connection() as conn, conn.transaction():
            return conn.execute("UPDATE actionlens_outbox SET attempt=attempt+1,last_error=%s,dead_letter_at=%s,claimed_by=NULL,claim_expires_at=NULL WHERE delivery_id=%s AND claimed_by=%s AND delivered_at IS NULL", (error[:2000],_now(),delivery_id,worker_id)).rowcount == 1

    def resolve_uncertain(
        self, key: str, *, status: Literal["SUCCEEDED", "FAILED_RETRYABLE", "UNCERTAIN"],
        output: dict[str, Any] | None, error: str | None, event: TrajectoryEvent,
    ) -> LedgerRecord | None:
        with self._connection() as conn, conn.transaction():
            row = conn.execute(
                """UPDATE actionlens_governance_ledger SET status=%s,output_json=%s,
                   last_error=%s,owner_id=NULL,lease_expires_at=NULL,heartbeat_at=NULL,updated_at=%s
                   WHERE key=%s AND status='UNCERTAIN' RETURNING *""",
                (status, self._json_adapter(output) if output is not None else None,
                 error, _now(), key),
            ).fetchone()
            if row is None:
                return None
            self._outbox(conn, event)
            return _ledger(row)

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
        args: tuple[Any, ...]
        if state is None:
            args = (limit,)
        else:
            query += f" WHERE {clauses[state]}"
            args = (limit,)
        query += " ORDER BY created_at LIMIT %s"
        with self._connection() as conn:
            return [_outbox(row) for row in conn.execute(query, args).fetchall()]

    def outbox_stats(self) -> dict[str, int | float | None]:
        with self._connection() as conn:
            row = conn.execute(
                """SELECT
                   COUNT(*) FILTER (WHERE delivered_at IS NULL AND dead_letter_at IS NULL
                                    AND terminated_at IS NULL) AS pending,
                   COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered,
                   COUNT(*) FILTER (WHERE dead_letter_at IS NOT NULL
                                    AND terminated_at IS NULL) AS dead_letter,
                   COUNT(*) FILTER (WHERE terminated_at IS NOT NULL) AS terminated,
                   EXTRACT(EPOCH FROM now() - MIN(created_at) FILTER
                     (WHERE delivered_at IS NULL AND dead_letter_at IS NULL
                      AND terminated_at IS NULL)) AS oldest_pending_lag_seconds
                   FROM actionlens_outbox"""
            ).fetchone()
        return {
            "pending": int(row["pending"]), "delivered": int(row["delivered"]),
            "dead_letter": int(row["dead_letter"]), "terminated": int(row["terminated"]),
            "oldest_pending_lag_seconds": float(row["oldest_pending_lag_seconds"])
            if row["oldest_pending_lag_seconds"] is not None else None,
        }

    def cleanup_outbox(self, *, delivered_before: datetime, limit: int = 1000) -> int:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._connection() as conn, conn.transaction():
            cursor = conn.execute(
                """WITH victims AS (
                     SELECT delivery_id FROM actionlens_outbox
                     WHERE delivered_at IS NOT NULL AND delivered_at < %s
                     ORDER BY delivered_at LIMIT %s FOR UPDATE SKIP LOCKED
                   ) DELETE FROM actionlens_outbox AS outbox USING victims
                     WHERE outbox.delivery_id = victims.delivery_id""",
                (delivered_before, limit),
            )
            return cursor.rowcount

    def replay_dead_letter(self, delivery_id: str) -> bool:
        with self._connection() as conn, conn.transaction():
            return conn.execute(
                """UPDATE actionlens_outbox SET dead_letter_at=NULL,terminated_at=NULL,
                   attempt=0,next_retry_at=%s,last_error=NULL,claimed_by=NULL,claim_expires_at=NULL
                   WHERE delivery_id=%s AND dead_letter_at IS NOT NULL
                   AND terminated_at IS NULL AND delivered_at IS NULL""",
                (_now(), delivery_id),
            ).rowcount == 1

    def terminate_dead_letter(self, delivery_id: str, *, reason: str) -> bool:
        if not reason.strip():
            raise ValueError("termination reason is required")
        with self._connection() as conn, conn.transaction():
            return conn.execute(
                """UPDATE actionlens_outbox SET terminated_at=%s,last_error=%s
                   WHERE delivery_id=%s AND dead_letter_at IS NOT NULL
                   AND terminated_at IS NULL AND delivered_at IS NULL""",
                (_now(), f"terminated: {reason}"[:2000], delivery_id),
            ).rowcount == 1

    def _outbox(self, conn: Any, event: TrajectoryEvent) -> None:
        now=_now()
        conn.execute("INSERT INTO actionlens_outbox(delivery_id,event_id,event_json,next_retry_at,created_at) VALUES (%s,%s,%s,%s,%s) ON CONFLICT(event_id) DO NOTHING", (f"delivery-{uuid4().hex}",event.event_id,self._json_adapter(event.model_dump(mode="json", exclude_none=True)),now,now))


class _LedgerView:
    def __init__(self, repository: PostgresGovernanceRepository): self.repository=repository
    def get(self,key:str): return self.repository.get_ledger(key)
    def records(self): return self.repository.list_ledger()


class _TicketView:
    def __init__(self, repository: PostgresGovernanceRepository): self.repository=repository
    def get(self,ticket_id:str): return self.repository.get_ticket(ticket_id)
    def list(self,*,status:str|None=None): return self.repository.list_tickets(status=status)


def _context_fields(context: Any, spec: Any) -> tuple[str,str,str|None,str,str,str]:
    return context.project,context.environment,context.tenant_id,context.session_id,context.run_id,getattr(spec,"name",context.tool_name)


def _record_scope(record: LedgerRecord) -> tuple[str, str, str | None]:
    return record.project, record.environment, record.tenant_id


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value,str) else value


def _ledger(row: Any) -> LedgerRecord:
    return LedgerRecord(key=row["key"],status=row["status"],call_id=row["call_id"],created_at=row["created_at"],updated_at=row["updated_at"],hit_count=row["hit_count"],output=_json_value(row["output_json"]),ticket_id=row["ticket_id"],project=row["project"],environment=row["environment"],tenant_id=row["tenant_id"],session_id=row["session_id"],run_id=row["run_id"],tool_name=row["tool_name"],args_hash=row["args_hash"],tool_schema_hash=row["tool_schema_hash"],owner_id=row["owner_id"],lease_expires_at=row["lease_expires_at"],heartbeat_at=row["heartbeat_at"],attempt=row["attempt"],fencing_token=row["fencing_token"],last_error=row["last_error"])


def _ticket(row: Any) -> ApprovalTicket:
    return ApprovalTicket(ticket_id=row["ticket_id"],idempotency_key=row["idempotency_key"],call_id=row["call_id"],tool_name=row["tool_name"],safe_args=_json_value(row["safe_args_json"]),risk=RiskLevel(row["risk"]),reason=row["reason"],status=row["status"],approved_by=row["approved_by"],decision_note=row["decision_note"],modified_args=_json_value(row["modified_args_json"]),requested_at=row["requested_at"],approved_at=row["approved_at"],expires_at=row["expires_at"],metadata=_json_value(row["metadata_json"]))


def _outbox(row: Any) -> OutboxRecord:
    event=_json_value(row["event_json"])
    return OutboxRecord(delivery_id=row["delivery_id"],event=TrajectoryEvent.model_validate(event),attempt=row["attempt"],next_retry_at=row["next_retry_at"],last_error=row["last_error"],delivered_at=row["delivered_at"],claimed_by=row["claimed_by"],claim_expires_at=row["claim_expires_at"],dead_letter_at=row.get("dead_letter_at"),terminated_at=row.get("terminated_at"))
