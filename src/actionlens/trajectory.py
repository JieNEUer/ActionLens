from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class TrajectoryReadStats:
    files: int = 0
    lines: int = 0
    events: int = 0
    skipped: int = 0


def iter_trajectory_events(
    storage_dir: str | Path,
    *,
    stats: TrajectoryReadStats | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield valid trajectory objects while accounting for corrupt or partial lines."""
    stats = stats or TrajectoryReadStats()
    trajectory_dir = Path(storage_dir) / "trajectories"
    if not trajectory_dir.exists():
        return
    for path in sorted(trajectory_dir.glob("*.jsonl")):
        stats.files += 1
        try:
            handle = path.open("r", encoding="utf-8")
        except OSError:
            stats.skipped += 1
            continue
        with handle:
            for line in handle:
                stats.lines += 1
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    stats.skipped += 1
                    continue
                if not isinstance(event, dict):
                    stats.skipped += 1
                    continue
                stats.events += 1
                yield event
