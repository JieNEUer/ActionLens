"""Configurable bounded soak probe; use --duration 86400 for the release gate."""

from __future__ import annotations

import argparse
import json
import tempfile
import threading
import time
import tracemalloc
from pathlib import Path

import actionlens as al


class _CountingSink:
    strict = True

    def __init__(self) -> None:
        self.events = 0

    def emit(self, event: object) -> None:
        self.events += 1

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--interval", type=float, default=0.01)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.duration <= 0 or args.interval < 0:
        parser.error("duration must be positive and interval must not be negative")

    baseline_threads = threading.active_count()
    tracemalloc.start()
    calls = 0
    with tempfile.TemporaryDirectory(prefix="actionlens-soak-") as directory:
        root = Path(directory)
        repository = al.SQLiteGovernanceRepository(root / "governance.sqlite3")
        sink = _CountingSink()
        lens = al.ActionLens(storage_dir=root / "lens", sink=sink, repository=repository)

        @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
        def probe(value: int) -> int:
            return value

        deadline = time.monotonic() + args.duration
        while time.monotonic() < deadline:
            output = probe(calls, idempotency_key=f"soak-{calls}")
            if output.status != "SUCCESS":
                raise RuntimeError(f"soak call failed: {output.model_dump()}")
            calls += 1
            if args.interval:
                time.sleep(args.interval)
        lens.close()
        current, peak = tracemalloc.get_traced_memory()
        pending = repository.outbox_stats()["pending"]
        temporary_files = len(list(root.rglob("*.tmp")))
        repository.close()
    tracemalloc.stop()

    report = {
        "schema": "actionlens.soak.v1",
        "duration_seconds": args.duration,
        "calls": calls,
        "threads_baseline": baseline_threads,
        "threads_final": threading.active_count(),
        "traced_current_bytes": current,
        "traced_peak_bytes": peak,
        "outbox_pending": pending,
        "temporary_files": temporary_files,
        "events_delivered": sink.events,
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload)
    return int(
        report["threads_final"] > report["threads_baseline"] + 1
        or pending != 0
        or temporary_files != 0
    )


if __name__ == "__main__":
    raise SystemExit(main())
