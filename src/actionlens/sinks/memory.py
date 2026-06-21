from __future__ import annotations

from actionlens.models import TrajectoryEvent


class MemorySink:
    def __init__(self) -> None:
        self.events: list[TrajectoryEvent] = []

    def emit(self, event: TrajectoryEvent) -> None:
        self.events.append(event)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None
