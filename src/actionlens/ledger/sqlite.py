from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .memory import LedgerRecord


class SQLiteLedger:
    """SQLite-backed idempotency ledger with atomic key ownership."""

    def __init__(self, path: str | Path, *, stale_pending_sec: float | None = 300.0):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stale_pending_sec = stale_pending_sec
        self._init_db()

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
        now = _now()
        fields = _context_fields(context, spec)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT OR IGNORE INTO actionlens_idempotency (
                  key, project, environment, tenant_id, session_id, run_id, tool_name,
                  call_id, status, hit_count, args_hash, tool_schema_hash, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?, ?, ?)
                """,
                (key, *fields, call_id, args_hash, tool_schema_hash, now, now),
            )
            if conn.total_changes > 0:
                record = self._get_conn(conn, key)
                conn.commit()
                return "created", record

            row = conn.execute(
                "SELECT status, hit_count, updated_at, args_hash, tool_schema_hash FROM actionlens_idempotency WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise RuntimeError("SQLiteLedger invariant violated: missing key.")

            existing_args_hash = row["args_hash"] or ""
            existing_schema_hash = row["tool_schema_hash"] or ""
            if (
                args_hash
                and existing_args_hash
                and existing_args_hash != args_hash
            ) or (
                tool_schema_hash
                and existing_schema_hash
                and existing_schema_hash != tool_schema_hash
            ):
                conn.execute(
                    "UPDATE actionlens_idempotency SET hit_count = hit_count + 1, updated_at = ? WHERE key = ?",
                    (now, key),
                )
                record = self._get_conn(conn, key)
                conn.commit()
                return "conflict", record

            status = row["status"]
            hit_count = int(row["hit_count"]) + 1
            stale = (
                status == "PENDING"
                and self.stale_pending_sec is not None
                and datetime.fromisoformat(row["updated_at"])
                <= datetime.now(timezone.utc) - timedelta(seconds=self.stale_pending_sec)
            )
            if stale:
                conn.execute("UPDATE actionlens_idempotency SET status='UNCERTAIN', hit_count=?, updated_at=? WHERE key=?", (hit_count, now, key))
                record = self._get_conn(conn, key)
                conn.commit()
                return "uncertain", record
            if status in {"APPROVED", "FAILED", "FAILED_RETRYABLE"}:
                conn.execute(
                    """
                    UPDATE actionlens_idempotency
                    SET status = 'PENDING', project = ?, environment = ?, tenant_id = ?,
                        session_id = ?, run_id = ?, tool_name = ?, call_id = ?, hit_count = ?,
                        args_hash = ?, tool_schema_hash = ?, updated_at = ?
                    WHERE key = ?
                    """,
                    (*fields, call_id, hit_count, args_hash, tool_schema_hash, now, key),
                )
                record = self._get_conn(conn, key)
                conn.commit()
                return "created", record

            conn.execute(
                """
                UPDATE actionlens_idempotency
                SET hit_count = ?, updated_at = ?
                WHERE key = ?
                """,
                (hit_count, now, key),
            )
            record = self._get_conn(conn, key)
            conn.commit()
            return "hit", record

    def mark_approval_pending(
        self, key: str, *, call_id: str, ticket_id: str, context: Any = None, spec: Any = None
    ) -> LedgerRecord:
        now = _now()
        fields = _context_fields(context, spec)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO actionlens_idempotency (
                  key, project, environment, tenant_id, session_id, run_id, tool_name,
                  call_id, status, hit_count, ticket_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'APPROVAL_PENDING', 0, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  status = 'APPROVAL_PENDING',
                  call_id = excluded.call_id,
                  ticket_id = excluded.ticket_id,
                  project = excluded.project,
                  environment = excluded.environment,
                  tenant_id = excluded.tenant_id,
                  session_id = excluded.session_id,
                  run_id = excluded.run_id,
                  tool_name = excluded.tool_name,
                  updated_at = excluded.updated_at
                WHERE actionlens_idempotency.status IN ('FAILED', 'FAILED_RETRYABLE')
                """,
                (key, *fields, call_id, ticket_id, now, now),
            )
            record = self._get_conn(conn, key)
            conn.commit()
            return record

    def approve(self, key: str) -> LedgerRecord | None:
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE actionlens_idempotency
                SET status = 'APPROVED', updated_at = ?
                WHERE key = ? AND status IN ('APPROVAL_PENDING', 'FAILED', 'FAILED_RETRYABLE')
                """,
                (now, key),
            )
            if cursor.rowcount == 0:
                conn.rollback()
                return None
            record = self._get_conn(conn, key)
            conn.commit()
            return record

    def succeed(self, key: str, output: dict[str, Any]) -> None:
        self._update_terminal(key, status="SUCCEEDED", output=output)

    def fail(self, key: str) -> None:
        self.mark_failed(key, retryable=True)

    def mark_failed(self, key: str, *, retryable: bool) -> None:
        self._update_terminal(
            key,
            status="FAILED_RETRYABLE" if retryable else "FAILED_TERMINAL",
            output=None,
        )

    def mark_uncertain(self, key: str) -> None:
        self._update_terminal(key, status="UNCERTAIN", output=None)

    def get(self, key: str) -> LedgerRecord | None:
        with self._connect() as conn:
            return self._get_conn(conn, key)

    def records(self) -> list[LedgerRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM actionlens_idempotency ORDER BY created_at, key"
            ).fetchall()
            return [_record_from_row(row) for row in rows]

    def _update_terminal(
        self, key: str, *, status: str, output: dict[str, Any] | None
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE actionlens_idempotency
                SET status = ?, output_json = ?, updated_at = ?
                WHERE key = ?
                """,
                (
                    status,
                    json.dumps(output, ensure_ascii=False, default=str)
                    if output is not None
                    else None,
                    _now(),
                    key,
                ),
            )

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS actionlens_idempotency (
                  key TEXT PRIMARY KEY,
                  project TEXT NOT NULL,
                  environment TEXT NOT NULL,
                  tenant_id TEXT,
                  session_id TEXT NOT NULL,
                  run_id TEXT NOT NULL,
                  tool_name TEXT NOT NULL,
                  call_id TEXT NOT NULL,
                  status TEXT NOT NULL,
                  hit_count INTEGER NOT NULL DEFAULT 0,
                  output_json TEXT,
                  ticket_id TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            _ensure_columns(
                conn,
                "actionlens_idempotency",
                {
                    "project": "TEXT NOT NULL DEFAULT ''",
                    "environment": "TEXT NOT NULL DEFAULT ''",
                    "tenant_id": "TEXT",
                    "session_id": "TEXT NOT NULL DEFAULT ''",
                    "run_id": "TEXT NOT NULL DEFAULT ''",
                    "tool_name": "TEXT NOT NULL DEFAULT ''",
                    "hit_count": "INTEGER NOT NULL DEFAULT 0",
                    "output_json": "TEXT",
                    "ticket_id": "TEXT",
                    "args_hash": "TEXT NOT NULL DEFAULT ''",
                    "tool_schema_hash": "TEXT NOT NULL DEFAULT ''",
                    "updated_at": "TEXT NOT NULL DEFAULT ''",
                },
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_actionlens_session "
                "ON actionlens_idempotency(session_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_actionlens_tool "
                "ON actionlens_idempotency(tool_name)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_actionlens_status "
                "ON actionlens_idempotency(status)"
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=5.0,
            isolation_level=None,
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _get_conn(
        self, conn: sqlite3.Connection, key: str
    ) -> LedgerRecord | None:
        row = conn.execute(
            "SELECT * FROM actionlens_idempotency WHERE key = ?", (key,)
        ).fetchone()
        return _record_from_row(row) if row is not None else None


def _record_from_row(row: sqlite3.Row) -> LedgerRecord:
    output_json = row["output_json"]
    output = json.loads(output_json) if output_json else None
    return LedgerRecord(
        key=row["key"],
        status=row["status"],
        call_id=row["call_id"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        hit_count=int(row["hit_count"]),
        output=output,
        ticket_id=row["ticket_id"],
        project=row["project"],
        environment=row["environment"],
        tenant_id=row["tenant_id"],
        session_id=row["session_id"],
        run_id=row["run_id"],
        tool_name=row["tool_name"],
        args_hash=row["args_hash"],
        tool_schema_hash=row["tool_schema_hash"],
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_columns(
    conn: sqlite3.Connection, table: str, columns: dict[str, str]
) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    for name, declaration in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")


def _context_fields(context: Any, spec: Any) -> tuple[str, str, str | None, str, str, str]:
    if context is None:
        return "", "", None, "", "", ""
    return (
        context.project,
        context.environment,
        context.tenant_id,
        context.session_id,
        context.run_id,
        getattr(spec, "name", context.tool_name),
    )
