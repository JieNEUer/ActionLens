from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .memory import LedgerRecord


class SQLiteLedger:
    """SQLite-backed idempotency ledger with atomic key ownership."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def begin(self, key: str, *, call_id: str) -> tuple[str, LedgerRecord]:
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT OR IGNORE INTO actionlens_idempotency (
                  key, project, environment, session_id, run_id, tool_name,
                  call_id, status, hit_count, created_at, updated_at
                )
                VALUES (?, '', '', '', '', '', ?, 'PENDING', 0, ?, ?)
                """,
                (key, call_id, now, now),
            )
            if conn.total_changes > 0:
                record = self._get_conn(conn, key)
                conn.commit()
                return "created", record

            row = conn.execute(
                "SELECT status, hit_count FROM actionlens_idempotency WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise RuntimeError("SQLiteLedger invariant violated: missing key.")

            status = row["status"]
            hit_count = int(row["hit_count"]) + 1
            if status == "APPROVED":
                conn.execute(
                    """
                    UPDATE actionlens_idempotency
                    SET status = 'PENDING', call_id = ?, hit_count = ?,
                        updated_at = ?
                    WHERE key = ? AND status = 'APPROVED'
                    """,
                    (call_id, hit_count, now, key),
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
        self, key: str, *, call_id: str, ticket_id: str
    ) -> LedgerRecord:
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO actionlens_idempotency (
                  key, project, environment, session_id, run_id, tool_name,
                  call_id, status, hit_count, ticket_id, created_at, updated_at
                )
                VALUES (?, '', '', '', '', '', ?, 'APPROVAL_PENDING', 0, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  status = 'APPROVAL_PENDING',
                  call_id = excluded.call_id,
                  ticket_id = excluded.ticket_id,
                  updated_at = excluded.updated_at
                """,
                (key, call_id, ticket_id, now, now),
            )
            record = self._get_conn(conn, key)
            conn.commit()
            return record

    def approve(self, key: str) -> LedgerRecord | None:
        now = _now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE actionlens_idempotency
                SET status = 'APPROVED', updated_at = ?
                WHERE key = ? AND status IN ('APPROVAL_PENDING', 'FAILED')
                """,
                (now, key),
            )
            record = self._get_conn(conn, key)
            conn.commit()
            return record

    def succeed(self, key: str, output: dict[str, Any]) -> None:
        self._update_terminal(key, status="SUCCEEDED", output=output)

    def fail(self, key: str) -> None:
        self._update_terminal(key, status="FAILED", output=None)

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
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
