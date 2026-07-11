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
  created_at timestamptz NOT NULL
);
ALTER TABLE actionlens_outbox ADD COLUMN IF NOT EXISTS dead_letter_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_al_outbox_ready ON actionlens_outbox(delivered_at,next_retry_at);
INSERT INTO actionlens_schema_migrations(version) VALUES (5) ON CONFLICT(version) DO NOTHING;
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PostgresGovernanceRepository:
    """PostgreSQL repository; all governance transitions and outbox writes are atomic."""

    def __init__(self, dsn: str, *, connect: Any | None = None, auto_migrate: bool = True):
        self.dsn = dsn
        self._json_adapter = lambda value: value
        if connect is None:
            try:
                import psycopg
                from psycopg.rows import dict_row
                from psycopg.types.json import Jsonb
            except ImportError as exc:
                raise ImportError("Install ActionLens with the 'postgres' extra") from exc

            def connect(value: str):  # type: ignore[no-redef]
                return psycopg.connect(value, row_factory=dict_row)
            self._json_adapter = Jsonb
        self._connect_factory = connect
        if auto_migrate:
            self.migrate()
        self.ledger = _LedgerView(self)
        self.tickets = _TicketView(self)

    @contextmanager
    def _connection(self):
        conn = self._connect_factory(self.dsn)
        try:
            yield conn
        finally:
            conn.close()

    def migrate(self) -> None:
        with self._connection() as conn, conn.transaction():
            conn.execute(POSTGRES_MIGRATION_SQL)

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
            if record.args_hash != args_hash or record.tool_schema_hash != tool_schema_hash:
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
            row = conn.execute("SELECT * FROM actionlens_governance_ledger WHERE key=%s FOR UPDATE", (key,)).fetchone()
            if row is not None:
                record = _ledger(row)
                if record.args_hash != args_hash or record.tool_schema_hash != tool_schema_hash:
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
            if row is None: return None
            result = _ticket(row)
            if result.status == "PENDING" and result.expires_at and result.expires_at <= _now():
                row = conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE ticket_id=%s RETURNING *", (ticket_id,)).fetchone()
                conn.execute("UPDATE actionlens_governance_ledger SET status='EXPIRED',updated_at=%s WHERE ticket_id=%s", (_now(),ticket_id))
                result = _ticket(row)
            return result

    def list_tickets(self, *, status: str | None = None) -> list[ApprovalTicket]:
        with self._connection() as conn, conn.transaction():
            conn.execute("UPDATE actionlens_approval_tickets SET status='EXPIRED' WHERE status='PENDING' AND expires_at<=now()")
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
            if event: self._outbox(conn,event)
            return _ticket(row)

    def claim_outbox(self, *, worker_id: str, limit: int = 100, claim_seconds: float = 30.0) -> list[OutboxRecord]:
        now = _now()
        with self._connection() as conn, conn.transaction():
            rows = conn.execute(
                """SELECT * FROM actionlens_outbox WHERE delivered_at IS NULL AND next_retry_at<=%s
                   AND dead_letter_at IS NULL AND (claim_expires_at IS NULL OR claim_expires_at<=%s) ORDER BY created_at
                   FOR UPDATE SKIP LOCKED LIMIT %s""", (now,now,limit)).fetchall()
            result=[]
            for row in rows:
                updated=conn.execute("UPDATE actionlens_outbox SET claimed_by=%s,claim_expires_at=%s WHERE delivery_id=%s RETURNING *", (worker_id,now+timedelta(seconds=claim_seconds),row["delivery_id"])).fetchone()
                result.append(_outbox(updated))
            return result

    def ack_outbox(self, delivery_id: str, *, worker_id: str) -> bool:
        with self._connection() as conn, conn.transaction():
            return conn.execute("UPDATE actionlens_outbox SET delivered_at=%s,claimed_by=NULL,claim_expires_at=NULL WHERE delivery_id=%s AND claimed_by=%s AND delivered_at IS NULL", (_now(),delivery_id,worker_id)).rowcount == 1

    def retry_outbox(self, delivery_id: str, *, worker_id: str, error: str, next_retry_at: datetime) -> bool:
        with self._connection() as conn, conn.transaction():
            return conn.execute("UPDATE actionlens_outbox SET attempt=attempt+1,last_error=%s,next_retry_at=%s,claimed_by=NULL,claim_expires_at=NULL WHERE delivery_id=%s AND claimed_by=%s AND delivered_at IS NULL", (error[:2000],next_retry_at,delivery_id,worker_id)).rowcount == 1

    def dead_letter_outbox(self, delivery_id: str, *, worker_id: str, error: str) -> bool:
        with self._connection() as conn, conn.transaction():
            return conn.execute("UPDATE actionlens_outbox SET attempt=attempt+1,last_error=%s,dead_letter_at=%s,claimed_by=NULL,claim_expires_at=NULL WHERE delivery_id=%s AND claimed_by=%s AND delivered_at IS NULL", (error[:2000],_now(),delivery_id,worker_id)).rowcount == 1

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


def _json_value(value: Any) -> Any:
    return json.loads(value) if isinstance(value,str) else value


def _ledger(row: Any) -> LedgerRecord:
    return LedgerRecord(key=row["key"],status=row["status"],call_id=row["call_id"],created_at=row["created_at"],updated_at=row["updated_at"],hit_count=row["hit_count"],output=_json_value(row["output_json"]),ticket_id=row["ticket_id"],project=row["project"],environment=row["environment"],tenant_id=row["tenant_id"],session_id=row["session_id"],run_id=row["run_id"],tool_name=row["tool_name"],args_hash=row["args_hash"],tool_schema_hash=row["tool_schema_hash"],owner_id=row["owner_id"],lease_expires_at=row["lease_expires_at"],heartbeat_at=row["heartbeat_at"],attempt=row["attempt"],fencing_token=row["fencing_token"],last_error=row["last_error"])


def _ticket(row: Any) -> ApprovalTicket:
    return ApprovalTicket(ticket_id=row["ticket_id"],idempotency_key=row["idempotency_key"],call_id=row["call_id"],tool_name=row["tool_name"],safe_args=_json_value(row["safe_args_json"]),risk=RiskLevel(row["risk"]),reason=row["reason"],status=row["status"],approved_by=row["approved_by"],decision_note=row["decision_note"],modified_args=_json_value(row["modified_args_json"]),requested_at=row["requested_at"],approved_at=row["approved_at"],expires_at=row["expires_at"],metadata=_json_value(row["metadata_json"]))


def _outbox(row: Any) -> OutboxRecord:
    event=_json_value(row["event_json"])
    return OutboxRecord(delivery_id=row["delivery_id"],event=TrajectoryEvent.model_validate(event),attempt=row["attempt"],next_retry_at=row["next_retry_at"],last_error=row["last_error"],delivered_at=row["delivered_at"],claimed_by=row["claimed_by"],claim_expires_at=row["claim_expires_at"],dead_letter_at=row.get("dead_letter_at"))
