from __future__ import annotations

from typing import Any

from actionlens.models import TrajectoryEvent


class OpenTelemetrySink:
    """Thin trajectory-to-span mapping; trajectory remains the source of truth."""

    def __init__(self, tracer: Any | None = None, *, strict: bool = False):
        if tracer is None:
            try:
                from opentelemetry import trace
            except ImportError as exc:
                raise ImportError("Install ActionLens with the 'otel' extra") from exc
            tracer = trace.get_tracer("actionlens")
        self.tracer = tracer
        self.strict = strict
        self._spans: dict[str, Any] = {}
        self.error_count = 0

    def emit(self, event: TrajectoryEvent) -> None:
        try:
            self._emit(event)
        except Exception:
            self.error_count += 1
            if self.strict:
                raise

    def _emit(self, event: TrajectoryEvent) -> None:
        if not event.call_id:
            return
        if event.event_type == "tool_call.started":
            span = self.tracer.start_span(f"actionlens.tool.{event.tool_name or 'unknown'}")
            for key, value in {
                "actionlens.project": event.project,
                "actionlens.session_id": event.session_id,
                "actionlens.run_id": event.run_id,
                "actionlens.call_id": event.call_id,
                "actionlens.tool": event.tool_name or "",
            }.items():
                span.set_attribute(key, value)
            self._spans[event.call_id] = span
            return
        span = self._spans.pop(event.call_id, None)
        if span is None:
            return
        span.add_event(event.event_type, attributes={"actionlens.event_id": event.event_id})
        if event.error is not None:
            try:
                from opentelemetry.trace import Status, StatusCode
                span.set_status(Status(StatusCode.ERROR, event.error.taxonomy))
            except ImportError:
                pass
        span.end()

    def flush(self) -> None: ...
    def close(self) -> None:
        for span in self._spans.values():
            span.end()
        self._spans.clear()
