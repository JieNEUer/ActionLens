from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from actionlens.models import TrajectoryEvent


class JsonlSink:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.trajectory_dir = self.root / "trajectories"
        self.trajectory_dir.mkdir(parents=True, exist_ok=True)

    def emit(self, event: TrajectoryEvent) -> None:
        path = self._path_for(event.timestamp)
        line = event.model_dump_json(exclude_none=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None

    def _path_for(self, timestamp: datetime | None = None) -> Path:
        timestamp = timestamp or datetime.now(timezone.utc)
        return self.trajectory_dir / f"actionlens-{timestamp.date().isoformat()}.jsonl"
