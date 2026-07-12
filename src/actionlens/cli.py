from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .artifacts import FileArtifactStore
from .exporters import (
    export_events,
    export_inspect_ai,
    export_sft,
    render_html_report,
    summarize_events,
)
from .ledger import SQLiteApprovalTicketStore, SQLiteLedger
from .repositories import PostgresGovernanceRepository, SQLiteGovernanceRepository


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="actionlens")
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary = subparsers.add_parser("summary", help="Summarize local trajectory JSONL.")
    summary.add_argument("--storage-dir", default=".actionlens")
    _add_filters(summary)

    export = subparsers.add_parser("export", help="Export local trajectory data.")
    export.add_argument("--storage-dir", default=".actionlens")
    export.add_argument(
        "--format",
        choices=["actionlens-jsonl", "summary-json", "inspect-ai", "sft-jsonl"],
        default="actionlens-jsonl",
    )
    export.add_argument("--output", required=True)
    _add_filters(export)

    report = subparsers.add_parser("report", help="Create a static local trajectory report.")
    report.add_argument("--storage-dir", default=".actionlens")
    report.add_argument("--html", action="store_true", required=True)
    report.add_argument("--output", required=True)
    _add_filters(report)

    tickets = subparsers.add_parser("tickets", help="Inspect local approval tickets.")
    tickets.add_argument("--storage-dir", default=".actionlens")
    tickets.add_argument("--status", choices=["PENDING", "APPROVED", "DENIED", "EXPIRED"])

    ledger = subparsers.add_parser("inspect-ledger", help="Inspect idempotency ledger records.")
    ledger.add_argument("--storage-dir", default=".actionlens")

    gc = subparsers.add_parser("gc", help="Remove old local artifacts.")
    gc.add_argument("--storage-dir", default=".actionlens")
    gc.add_argument("--older-than", default="7d")
    gc.add_argument("--max-bytes", type=int, default=None)
    gc.add_argument("--dry-run", action="store_true")

    outbox = subparsers.add_parser("outbox", help="Inspect or recover local outbox records.")
    outbox.add_argument("--storage-dir", default=".actionlens")
    outbox.add_argument("action", choices=["list", "status", "cleanup", "replay", "terminate"])
    outbox.add_argument("--delivery-id")
    outbox.add_argument("--reason")
    outbox.add_argument("--limit", type=int, default=100)
    outbox.add_argument("--retention", default="30d")

    migrate = subparsers.add_parser("migrate", help="Apply PostgreSQL schema migrations.")
    migrate.add_argument("--dsn", default=None)

    schema_status = subparsers.add_parser(
        "schema-status", help="Check PostgreSQL schema compatibility without applying DDL."
    )
    schema_status.add_argument("--dsn", default=None)

    args = parser.parse_args(argv)
    if args.command == "summary":
        print(json.dumps(_summary(Path(args.storage_dir), **_filters(args)), ensure_ascii=False, indent=2))
        return 0
    if args.command == "export":
        result = _export(
            Path(args.storage_dir),
            format_name=args.format,
            output=Path(args.output),
            **_filters(args),
        )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if args.command == "report":
        result = render_html_report(args.storage_dir, args.output, **_filters(args))
        print(json.dumps(result, ensure_ascii=False))
        return 0
    if args.command == "tickets":
        store = SQLiteApprovalTicketStore(_database_path(Path(args.storage_dir)))
        data = [
            ticket.model_dump(mode="json", exclude={"modified_args"})
            for ticket in store.list(status=args.status)
        ]
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0
    if args.command == "inspect-ledger":
        database = _database_path(Path(args.storage_dir))
        records = SQLiteGovernanceRepository(database).list_ledger()
        if not records:
            records = SQLiteLedger(database).records()
        print(json.dumps([record.__dict__ for record in records], ensure_ascii=False, indent=2, default=str))
        return 0
    if args.command == "gc":
        seconds = _parse_duration(args.older_than)
        result = FileArtifactStore(args.storage_dir).gc(
            older_than_seconds=seconds,
            max_bytes=args.max_bytes,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.command == "outbox":
        repository = SQLiteGovernanceRepository(_database_path(Path(args.storage_dir)))
        if args.action == "list":
            records = repository.list_outbox(state="dead_letter", limit=args.limit)
            print(json.dumps([item.model_dump(mode="json") for item in records], ensure_ascii=False, indent=2))
            return 0
        if args.action == "status":
            print(json.dumps(repository.outbox_stats(), ensure_ascii=False, indent=2))
            return 0
        if args.action == "cleanup":
            retention = _parse_duration(args.retention)
            deleted = repository.cleanup_outbox(
                delivered_before=datetime.now(timezone.utc) - timedelta(seconds=retention),
                limit=args.limit,
            )
            print(json.dumps({"deleted": deleted, "retention_seconds": retention}))
            return 0
        if not args.delivery_id:
            parser.error("--delivery-id is required for replay and terminate")
        if args.action == "replay":
            changed = repository.replay_dead_letter(args.delivery_id)
        else:
            if not args.reason:
                parser.error("--reason is required for terminate")
            changed = repository.terminate_dead_letter(args.delivery_id, reason=args.reason)
        print(json.dumps({"changed": changed, "delivery_id": args.delivery_id}))
        return 0 if changed else 1
    if args.command in {"migrate", "schema-status"}:
        dsn = args.dsn or os.environ.get("ACTIONLENS_POSTGRES_DSN")
        if not dsn:
            parser.error("--dsn or ACTIONLENS_POSTGRES_DSN is required")
        repository = PostgresGovernanceRepository(
            dsn, auto_migrate=args.command == "migrate", verify_schema=False
        )
        try:
            status = repository.verify_schema()
            print(json.dumps(status, ensure_ascii=False, indent=2))
            return 0
        finally:
            repository.close()
    return 1


def _summary(storage_dir: Path, **filters: str | None) -> dict[str, Any]:
    return summarize_events(storage_dir, **filters)


def _export(
    storage_dir: Path,
    *,
    format_name: str,
    output: Path,
    **filters: str | None,
) -> dict[str, int]:
    if format_name == "summary-json":
        summary = _summary(storage_dir, **filters)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {"exported": summary["events"], "skipped": summary["skipped_lines"]}
    if format_name == "inspect-ai":
        return export_inspect_ai(storage_dir, output, **filters)
    if format_name == "sft-jsonl":
        return export_sft(storage_dir, output, **filters)
    return export_events(storage_dir, output, **filters)


def _add_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--session", dest="session_id")
    parser.add_argument("--run", dest="run_id")
    parser.add_argument("--project")


def _filters(args: argparse.Namespace) -> dict[str, str | None]:
    return {
        "session_id": getattr(args, "session_id", None),
        "run_id": getattr(args, "run_id", None),
        "project": getattr(args, "project", None),
    }


def _database_path(storage_dir: Path) -> Path:
    return storage_dir / "ledger" / "actionlens.sqlite3"


def _parse_duration(value: str) -> int:
    match = re.fullmatch(r"(\d+)([smhd])", value.strip())
    if not match:
        raise SystemExit("--older-than must look like 30s, 10m, 12h, or 7d")
    amount = int(match.group(1))
    unit = match.group(2)
    return amount * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


if __name__ == "__main__":
    raise SystemExit(main())
