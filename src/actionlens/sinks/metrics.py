from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from threading import Lock

from actionlens.models import TrajectoryEvent


@dataclass
class LatencySummary:
    count: int = 0
    sum: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    # Cumulative fixed millisecond buckets, plus +Inf.
    buckets: list[int] = field(default_factory=lambda: [0] * 9)

    def observe(self, value: float) -> None:
        self.count += 1
        self.sum += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        for index, boundary in enumerate((1, 5, 10, 50, 100, 500, 1000, 5000, float("inf"))):
            if value <= boundary:
                self.buckets[index] += 1


class MetricsSink:
    """Low-cardinality in-process metrics mapper suitable for custom exporters."""

    def __init__(self, *, max_tools: int = 1000) -> None:
        if max_tools <= 0:
            raise ValueError("max_tools must be positive")
        self.max_tools = max_tools
        self._tools: set[str] = set()
        self._lock = Lock()
        self.tool_calls: Counter[tuple[str, str]] = Counter()
        self.idempotency_hits: Counter[tuple[str, str]] = Counter()
        self.approvals: Counter[tuple[str, str]] = Counter()
        self.latency_ms: dict[tuple[str, str], LatencySummary] = {}

    def emit(self, event: TrajectoryEvent) -> None:
        with self._lock:
            self._emit(event)

    def _emit(self, event: TrajectoryEvent) -> None:
        tool = event.tool_name or "unknown"
        if tool not in self._tools:
            if len(self._tools) >= self.max_tools:
                tool = "__other__"
            else:
                self._tools.add(tool)
        if event.event_type in {"tool_call.completed", "tool_call.failed"}:
            status = "failed" if event.error else "success"
            self.tool_calls[(tool, status)] += 1
            if "latency_ms" in event.metrics:
                self.latency_ms.setdefault((tool, status), LatencySummary()).observe(float(event.metrics["latency_ms"]))
        elif event.event_type == "idempotency.hit":
            status = str(event.metadata.get("status", "unknown"))
            if status not in {"SUCCEEDED", "EXECUTING", "APPROVAL_PENDING", "APPROVED", "FAILED_RETRYABLE", "FAILED_TERMINAL", "UNCERTAIN", "DENIED", "EXPIRED"}:
                status = "unknown"
            self.idempotency_hits[(tool, status)] += 1
        elif event.event_type.startswith("approval."):
            status = event.event_type.removeprefix("approval.")
            self.approvals[(tool, status if status in {"pending", "approved", "denied", "expired"} else "other")] += 1

    def flush(self) -> None: ...
    def close(self) -> None: ...
