from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .schema import read_event


@dataclass
class TrajectoryReadStats:
    files: int = 0
    lines: int = 0
    events: int = 0
    skipped: int = 0
    invalid: int = 0
    duplicates: int = 0
    conflicts: int = 0
    unsupported: int = 0
    source_files: list[dict[str, Any]] = field(default_factory=list)
    issues: list[dict[str, Any]] = field(default_factory=list)

    def record_issue(self, kind: str, path: Path, line: int) -> None:
        if len(self.issues) < 100:
            self.issues.append({"kind": kind, "file": path.name, "line": line})


def iter_trajectory_events(
    storage_dir: str | Path,
    *,
    stats: TrajectoryReadStats | None = None,
    max_line_bytes: int = 16 * 1024 * 1024,
) -> Iterator[dict[str, Any]]:
    """Yield canonical events from fixed source byte ranges using a disk index.

    Conflicting IDs are quarantined in their entirety. Source hashes describe
    exactly the bytes read, and original delivery files remain untouched.
    """
    stats = stats or TrajectoryReadStats()
    if max_line_bytes <= 0:
        raise ValueError("max_line_bytes must be positive")
    trajectory_dir = Path(storage_dir) / "trajectories"
    if not trajectory_dir.exists():
        return
    with tempfile.TemporaryDirectory(prefix="actionlens-events-") as temporary:
        conn = sqlite3.connect(Path(temporary) / "canonical.sqlite3")
        try:
            conn.execute("CREATE TABLE events (id TEXT PRIMARY KEY, payload TEXT, conflicted INTEGER DEFAULT 0)")
            for path in sorted(trajectory_dir.glob("*.jsonl")):
                stats.files += 1
                try:
                    handle = path.open("rb")
                except OSError:
                    stats.skipped += 1
                    continue
                digest = hashlib.sha256()
                consumed = 0
                with handle:
                    remaining = path.stat().st_size
                    file_line = 0
                    while remaining:
                        line = handle.readline(min(remaining, max_line_bytes + 1))
                        if not line:
                            break
                        digest.update(line)
                        remaining -= len(line)
                        consumed += len(line)
                        stats.lines += 1
                        file_line += 1
                        if len(line) > max_line_bytes:
                            while remaining and not line.endswith(b"\n"):
                                line = handle.readline(min(remaining, 256 * 1024))
                                if not line:
                                    break
                                digest.update(line)
                                remaining -= len(line)
                                consumed += len(line)
                            stats.invalid += 1
                            stats.skipped += 1
                            stats.record_issue("oversized", path, file_line)
                            continue
                        if not line.strip():
                            continue
                        try:
                            event = json.loads(line.decode("utf-8"))
                            if not isinstance(event, dict):
                                raise ValueError("event must be an object")
                            if event.get("schema_version", "actionlens.event.v1") != "actionlens.event.v1":
                                stats.unsupported += 1
                                stats.skipped += 1
                                stats.record_issue("unsupported", path, file_line)
                                continue
                            read_event(event)
                            canonical = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
                        except (UnicodeError, ValueError, TypeError):
                            stats.invalid += 1
                            stats.skipped += 1
                            stats.record_issue("invalid", path, file_line)
                            continue
                        existing = conn.execute("SELECT payload, conflicted FROM events WHERE id=?", (event["event_id"],)).fetchone()
                        if existing is None:
                            conn.execute("INSERT INTO events(id,payload) VALUES (?,?)", (event["event_id"], canonical))
                        elif existing[0] == canonical:
                            stats.duplicates += 1
                            stats.record_issue("duplicate", path, file_line)
                        else:
                            if not existing[1]:
                                stats.conflicts += 1
                                stats.skipped += 1  # quarantine the previously indexed first occurrence
                            stats.skipped += 1
                            stats.record_issue("conflict", path, file_line)
                            conn.execute("UPDATE events SET conflicted=1 WHERE id=?", (event["event_id"],))
                stats.source_files.append({"path": str(path.relative_to(Path(storage_dir))), "sha256": digest.hexdigest(),
                                           "size_bytes": consumed, "snapshot": "fixed_byte_range"})
            conn.commit()
            for (canonical,) in conn.execute("SELECT payload FROM events WHERE conflicted=0 ORDER BY rowid"):
                stats.events += 1
                yield json.loads(canonical)
        finally:
            conn.close()
