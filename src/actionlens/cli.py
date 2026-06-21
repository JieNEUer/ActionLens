from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .artifacts import FileArtifactStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="actionlens")
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary = subparsers.add_parser("summary", help="Summarize local trajectory JSONL.")
    summary.add_argument("--storage-dir", default=".actionlens")

    export = subparsers.add_parser("export", help="Export local trajectory data.")
    export.add_argument("--storage-dir", default=".actionlens")
    export.add_argument(
        "--format",
        choices=["actionlens-jsonl", "summary-json"],
        default="actionlens-jsonl",
    )
    export.add_argument("--output", required=True)

    gc = subparsers.add_parser("gc", help="Remove old local artifacts.")
    gc.add_argument("--storage-dir", default=".actionlens")
    gc.add_argument("--older-than", default="7d")
    gc.add_argument("--max-bytes", type=int, default=None)
    gc.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "summary":
        print(json.dumps(_summary(Path(args.storage_dir)), ensure_ascii=False, indent=2))
        return 0
    if args.command == "export":
        _export(Path(args.storage_dir), format_name=args.format, output=Path(args.output))
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
    return 1


def _summary(storage_dir: Path) -> dict[str, Any]:
    trajectory_dir = storage_dir / "trajectories"
    events = 0
    by_type: Counter[str] = Counter()
    by_tool_status: dict[str, Counter[str]] = defaultdict(Counter)
    sessions: set[str] = set()
    if trajectory_dir.exists():
        for path in trajectory_dir.glob("*.jsonl"):
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    events += 1
                    by_type[event.get("event_type", "unknown")] += 1
                    sessions.add(event.get("session_id", "unknown"))
                    tool = event.get("tool_name")
                    if tool:
                        by_tool_status[tool][event.get("event_type", "unknown")] += 1
    return {
        "events": events,
        "sessions": sorted(sessions),
        "by_type": dict(by_type),
        "by_tool": {tool: dict(counts) for tool, counts in by_tool_status.items()},
    }


def _export(storage_dir: Path, *, format_name: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if format_name == "summary-json":
        output.write_text(
            json.dumps(_summary(storage_dir), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return

    trajectory_dir = storage_dir / "trajectories"
    with output.open("w", encoding="utf-8") as target:
        if not trajectory_dir.exists():
            return
        for path in sorted(trajectory_dir.glob("*.jsonl")):
            with path.open("r", encoding="utf-8") as source:
                for line in source:
                    if line.strip():
                        target.write(line if line.endswith("\n") else line + "\n")


def _parse_duration(value: str) -> int:
    match = re.fullmatch(r"(\d+)([smhd])", value.strip())
    if not match:
        raise SystemExit("--older-than must look like 30s, 10m, 12h, or 7d")
    amount = int(match.group(1))
    unit = match.group(2)
    return amount * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


if __name__ == "__main__":
    raise SystemExit(main())
