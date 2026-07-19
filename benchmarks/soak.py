"""Long-running ActionLens lifecycle probe with trend-oriented evidence.

Use ``--duration 86400`` for a 24-hour release soak. The default is deliberately
short so the script can also serve as a local and CI smoke probe.
"""

from __future__ import annotations

import argparse
import ctypes
import gc
import json
import os
import platform
import sys
import tempfile
import threading
import time
import tracemalloc
from pathlib import Path
from typing import Any
from ctypes import wintypes

import actionlens as al


class _CountingSink:
    strict = True

    def __init__(self) -> None:
        self.events = 0

    def emit(self, event: object) -> None:
        self.events += 1

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


def _rss_bytes() -> int | None:
    if os.name == "nt":
        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        get_process_memory_info = psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        get_process_memory_info.restype = wintypes.BOOL
        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if get_process_memory_info(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            return int(counters.WorkingSetSize)
        return None
    statm = Path("/proc/self/statm")
    if statm.exists():
        try:
            resident_pages = int(statm.read_text(encoding="ascii").split()[1])
            return resident_pages * os.sysconf("SC_PAGE_SIZE")
        except (IndexError, OSError, ValueError):
            return None
    return None


def _slope(samples: list[dict[str, Any]], field: str) -> float | None:
    points = [
        (float(sample["elapsed_seconds"]), float(sample[field]))
        for sample in samples
        if sample.get(field) is not None
    ]
    if len(points) < 2:
        return None
    average_x = sum(point[0] for point in points) / len(points)
    average_y = sum(point[1] for point in points) / len(points)
    denominator = sum((point[0] - average_x) ** 2 for point in points)
    if denominator == 0:
        return None
    return sum(
        (point[0] - average_x) * (point[1] - average_y) for point in points
    ) / denominator


def _sample(
    *, started: float, repository: al.SQLiteGovernanceRepository, root: Path
) -> dict[str, Any]:
    traced_current, traced_peak = tracemalloc.get_traced_memory()
    return {
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "rss_bytes": _rss_bytes(),
        "traced_current_bytes": traced_current,
        "traced_peak_bytes": traced_peak,
        "threads": threading.active_count(),
        "gc_counts": list(gc.get_count()),
        "outbox": repository.outbox_stats(),
        "pool": repository.pool_stats(),
        "temporary_files": len(list(root.rglob("*.tmp"))),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--interval", type=float, default=0.01)
    parser.add_argument("--sample-interval", type=float, default=60.0)
    parser.add_argument("--max-rss-slope-bytes-per-second", type=float, default=None)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.duration <= 0 or args.interval < 0 or args.sample_interval <= 0:
        parser.error("duration and sample interval must be positive; interval must not be negative")
    if (
        args.max_rss_slope_bytes_per_second is not None
        and args.max_rss_slope_bytes_per_second < 0
    ):
        parser.error("max RSS slope must not be negative")

    baseline_threads = threading.active_count()
    gc.collect()
    tracemalloc.start()
    calls = 0
    samples: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="actionlens-soak-") as directory:
        root = Path(directory)
        repository = al.SQLiteGovernanceRepository(root / "governance.sqlite3")
        sink = _CountingSink()
        lens = al.ActionLens(storage_dir=root / "lens", sink=sink, repository=repository)

        @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
        def probe(value: int) -> int:
            return value

        started = time.monotonic()
        deadline = started + args.duration
        next_sample = started
        while time.monotonic() < deadline:
            output = probe(calls, idempotency_key=f"soak-{calls}")
            if output.status != "SUCCESS":
                raise RuntimeError(f"soak call failed: {output.model_dump()}")
            calls += 1
            now = time.monotonic()
            if now >= next_sample:
                samples.append(_sample(started=started, repository=repository, root=root))
                next_sample = now + args.sample_interval
            if args.interval:
                time.sleep(args.interval)
        lens.dispatch_outbox(limit=1_000)
        samples.append(_sample(started=started, repository=repository, root=root))
        final_outbox = repository.outbox_stats()
        final_pool = repository.pool_stats()
        temporary_files = len(list(root.rglob("*.tmp")))
        lens.close()
        repository.close()
    tracemalloc.stop()

    rss_slope = _slope(samples, "rss_bytes")
    thread_limit_exceeded = threading.active_count() > baseline_threads + 1
    rss_limit_exceeded = (
        args.max_rss_slope_bytes_per_second is not None
        and rss_slope is not None
        and rss_slope > args.max_rss_slope_bytes_per_second
    )
    report = {
        "schema": "actionlens.soak.v2",
        "duration_seconds": args.duration,
        "calls": calls,
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "actionlens": al.__version__,
        },
        "threads_baseline": baseline_threads,
        "threads_final": threading.active_count(),
        "events_delivered": sink.events,
        "outbox_final": final_outbox,
        "pool_final": final_pool,
        "temporary_files": temporary_files,
        "rss_slope_bytes_per_second": rss_slope,
        "samples": samples,
        "passed": not (
            thread_limit_exceeded
            or final_outbox["pending"] != 0
            or temporary_files != 0
            or rss_limit_exceeded
        ),
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
