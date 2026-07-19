"""Capture an auditable PostgreSQL outbox claim plan from an isolated cluster."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import actionlens as al


def _isolated_dsn() -> str:
    dsn = os.environ.get("ACTIONLENS_POSTGRES_DSN")
    if not dsn:
        raise SystemExit("ACTIONLENS_POSTGRES_DSN is required")
    try:
        parsed = urlsplit(dsn)
        allowed = (
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.hostname == "127.0.0.1"
            and parsed.port == 55432
            and bool(parsed.path.strip("/"))
        )
    except ValueError:
        allowed = False
    if not allowed:
        raise SystemExit(
            "refusing to benchmark a non-isolated database; use 127.0.0.1:55432"
        )
    return dsn


def _index_nodes(value: object) -> list[str]:
    names: list[str] = []
    if isinstance(value, dict):
        name = value.get("Index Name")
        if isinstance(name, str):
            names.append(name)
        for child in value.values():
            if isinstance(child, (dict, list)):
                names.extend(_index_nodes(child))
    elif isinstance(value, list):
        for child in value:
            names.extend(_index_nodes(child))
    return names


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--delivered-rows", type=int, default=100_000)
    args = parser.parse_args()
    if args.delivered_rows <= 0:
        parser.error("--delivered-rows must be positive")
    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit("install ActionLens with the postgres extra") from exc

    dsn = _isolated_dsn()
    repository = al.PostgresGovernanceRepository(dsn, auto_migrate=True)
    prefix = f"query-plan-{uuid4().hex}"
    try:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(
                """INSERT INTO actionlens_outbox
                   (delivery_id,event_id,event_json,next_retry_at,delivered_at,created_at)
                   SELECT %s || '-delivered-' || item, %s || '-event-delivered-' || item,
                          '{}'::jsonb, now(), now(), now()
                   FROM generate_series(1, %s) AS item""",
                (prefix, prefix, args.delivered_rows),
            )
            connection.execute(
                """INSERT INTO actionlens_outbox
                   (delivery_id,event_id,event_json,next_retry_at,created_at)
                   SELECT %s || '-ready-' || item, %s || '-event-ready-' || item,
                          '{}'::jsonb, now(), now()
                   FROM generate_series(1, 20) AS item""",
                (prefix, prefix),
            )
            connection.execute("ANALYZE actionlens_outbox")
            plan_value = connection.execute(
                """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
                   WITH candidates AS (
                     SELECT delivery_id FROM actionlens_outbox
                     WHERE delivered_at IS NULL AND next_retry_at <= now()
                       AND dead_letter_at IS NULL AND terminated_at IS NULL
                       AND (claim_expires_at IS NULL OR claim_expires_at <= now())
                     ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 10
                   )
                   UPDATE actionlens_outbox AS outbox
                   SET claimed_by='query-plan', claim_expires_at=now() + interval '30 seconds'
                   FROM candidates WHERE outbox.delivery_id=candidates.delivery_id"""
            ).fetchone()[0]
            version = connection.execute("SHOW server_version").fetchone()[0]
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(
                "DELETE FROM actionlens_outbox WHERE delivery_id LIKE %s", (f"{prefix}%",)
            )
        repository.close()

    plan = json.loads(plan_value) if isinstance(plan_value, str) else plan_value
    index_nodes = _index_nodes(plan)
    report = {
        "schema": "actionlens.postgres-query-plan.v1",
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "actionlens": al.__version__,
            "postgres_server_version": version,
        },
        "index_nodes": index_nodes,
        "plan": plan,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if "idx_al_outbox_claim_v11" not in index_nodes:
        raise RuntimeError("claim query did not use idx_al_outbox_claim_v11")
    print(json.dumps({"index_nodes": index_nodes, "postgres_server_version": version}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
