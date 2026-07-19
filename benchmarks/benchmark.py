"""Reproducible ActionLens microbenchmarks with machine-readable output."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path
from typing import Callable

import actionlens as al
from actionlens.repository import canonical_operation_hash


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


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * percentile)))
    return ordered[index]


def _measure(name: str, operation: Callable[[int], None], iterations: int) -> dict[str, float | str]:
    latencies: list[float] = []
    started = time.perf_counter()
    for index in range(iterations):
        call_started = time.perf_counter_ns()
        operation(index)
        latencies.append((time.perf_counter_ns() - call_started) / 1_000_000)
    elapsed = time.perf_counter() - started
    return {
        "name": name,
        "iterations": iterations,
        "throughput_ops_s": iterations / elapsed,
        "mean_ms": statistics.fmean(latencies),
        "p50_ms": _percentile(latencies, 0.50),
        "p95_ms": _percentile(latencies, 0.95),
        "p99_ms": _percentile(latencies, 0.99),
    }


async def _measure_async(operation: Callable[[int], object], iterations: int) -> dict[str, float | str]:
    latencies: list[float] = []
    started = time.perf_counter()
    for index in range(iterations):
        call_started = time.perf_counter_ns()
        await operation(index)
        latencies.append((time.perf_counter_ns() - call_started) / 1_000_000)
    elapsed = time.perf_counter() - started
    return {
        "name": "async_sqlite_small_result",
        "iterations": iterations,
        "throughput_ops_s": iterations / elapsed,
        "mean_ms": statistics.fmean(latencies),
        "p50_ms": _percentile(latencies, 0.50),
        "p95_ms": _percentile(latencies, 0.95),
        "p99_ms": _percentile(latencies, 0.99),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.iterations <= 0:
        parser.error("--iterations must be positive")

    with tempfile.TemporaryDirectory(prefix="actionlens-benchmark-") as directory:
        root = Path(directory)
        repository = al.SQLiteGovernanceRepository(
            root / "governance.sqlite3", synchronous="NORMAL"
        )
        lens = al.ActionLens(storage_dir=root, sink=_CountingSink(), repository=repository)

        @lens.tool
        def small(value: int) -> dict[str, int]:
            return {"value": value}

        @lens.tool
        async def async_small(value: int) -> dict[str, int]:
            return {"value": value}

        @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
        def idempotent(value: int) -> int:
            return value

        idempotent(1, idempotency_key="benchmark-hit")
        one_kib = b"x" * 1024

        tracemalloc.start()
        results = [
            _measure(
                "canonical_hash",
                lambda index: canonical_operation_hash(
                    {"index": index, "stable": [1, 2, 3], "text": "actionlens"}
                ),
                args.iterations,
            ),
            _measure("sqlite_small_result", lambda index: small(index), args.iterations),
            _measure(
                "sqlite_normal_idempotency_hit",
                lambda index: idempotent(1, idempotency_key="benchmark-hit"),
                args.iterations,
            ),
            _measure(
                "artifact_1kib_deduplicated",
                lambda index: lens.artifact_store.put(one_kib),
                args.iterations,
            ),
        ]
        results.append(asyncio.run(_measure_async(async_small, args.iterations)))
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        lens.close()
        repository.close()

    report = {
        "schema": "actionlens.benchmark.v1",
        "seed": 0,
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
            "actionlens": al.__version__,
        },
        "peak_traced_bytes": peak,
        "results": results,
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
