from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from actionlens.models import TrajectoryEvent


@dataclass(frozen=True)
class SinkBinding:
    sink: Any
    strict: bool = False


class CompositeSink:
    def __init__(self, *sinks: Any | SinkBinding):
        self.bindings = [item if isinstance(item, SinkBinding) else SinkBinding(item) for item in sinks]
        self.error_count = 0

    def emit(self, event: TrajectoryEvent) -> None:
        for binding in self.bindings:
            try:
                binding.sink.emit(event)
            except Exception:
                self.error_count += 1
                if binding.strict:
                    raise

    def flush(self) -> None:
        self._lifecycle("flush")

    def close(self) -> None:
        self._lifecycle("close")

    def _lifecycle(self, method: str) -> None:
        for binding in self.bindings:
            try:
                getattr(binding.sink, method)()
            except Exception:
                self.error_count += 1
                if binding.strict:
                    raise
