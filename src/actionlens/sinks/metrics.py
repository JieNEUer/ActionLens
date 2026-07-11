from __future__ import annotations

from collections import Counter

from actionlens.models import TrajectoryEvent


class MetricsSink:
    """Low-cardinality in-process metrics mapper suitable for custom exporters."""

    def __init__(self) -> None:
        self.tool_calls: Counter[tuple[str, str]] = Counter()
        self.idempotency_hits: Counter[tuple[str, str]] = Counter()
        self.approvals: Counter[tuple[str, str]] = Counter()
        self.latency_ms: dict[tuple[str, str], list[float]] = {}

    def emit(self, event: TrajectoryEvent) -> None:
        tool = event.tool_name or "unknown"
        if event.event_type in {"tool_call.completed", "tool_call.failed"}:
            status = "failed" if event.error else "success"
            self.tool_calls[(tool, status)] += 1
            if "latency_ms" in event.metrics:
                self.latency_ms.setdefault((tool, status), []).append(float(event.metrics["latency_ms"]))
        elif event.event_type == "idempotency.hit":
            self.idempotency_hits[(tool, str(event.metadata.get("status", "unknown")))] += 1
        elif event.event_type.startswith("approval."):
            self.approvals[(tool, event.event_type.removeprefix("approval."))] += 1

    def flush(self) -> None: ...
    def close(self) -> None: ...
