from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from actionlens.models import TrajectoryEvent


@dataclass(frozen=True)
class OpenTelemetryMappingProfile:
    """Pinned mapping contract for a specific GenAI semantic-conventions snapshot."""

    profile_id: str
    semconv_revision: str
    operation_name_attribute: str = "gen_ai.operation.name"
    tool_name_attribute: str = "gen_ai.tool.name"
    tool_call_id_attribute: str = "gen_ai.tool_call.id"


OTEL_GENAI_PROFILE_V1 = OpenTelemetryMappingProfile(
    profile_id="actionlens.otel-genai.v1",
    semconv_revision="opentelemetry/semantic-conventions-genai@2026-07-21-development",
)


_TERMINAL_EVENTS = {
    "approval.denied",
    "approval.expired",
    "approval.pending",
    "tool_call.completed",
    "tool_call.failed",
    "tool_call.preflight_resolved",
}


class OpenTelemetrySink:
    """Safe GenAI span mapping; trajectories remain the unsampled source of truth."""

    def __init__(
        self,
        tracer: Any | None = None,
        *,
        strict: bool = False,
        profile: OpenTelemetryMappingProfile = OTEL_GENAI_PROFILE_V1,
    ):
        if tracer is None:
            try:
                from opentelemetry import trace
            except ImportError as exc:
                raise ImportError("Install ActionLens with the 'otel' extra") from exc
            tracer = trace.get_tracer("actionlens")
        self.tracer = tracer
        self.strict = strict
        self.profile = profile
        self._spans: dict[str, Any] = {}
        self._spans_lock = threading.Lock()
        self._closed = False
        self._error_count = 0
        self._error_lock = threading.Lock()

    @property
    def error_count(self) -> int:
        """Return the number of isolated sink failures observed so far."""

        with self._error_lock:
            return self._error_count

    def emit(self, event: TrajectoryEvent) -> None:
        try:
            self._emit(event)
        except Exception:
            with self._error_lock:
                self._error_count += 1
            if self.strict:
                raise

    def _emit(self, event: TrajectoryEvent) -> None:
        if not event.call_id:
            return
        if event.event_type == "tool_call.started":
            tool_name = event.tool_name or "unknown"
            span = self.tracer.start_span(f"execute_tool {tool_name}")
            for key, value in {
                self.profile.operation_name_attribute: "execute_tool",
                self.profile.tool_name_attribute: tool_name,
                self.profile.tool_call_id_attribute: event.call_id,
                "actionlens.project": event.project,
                "actionlens.session_id": event.session_id,
                "actionlens.run_id": event.run_id,
                "actionlens.call_id": event.call_id,
                "actionlens.otel.mapping_profile": self.profile.profile_id,
                "actionlens.otel.semconv_revision": self.profile.semconv_revision,
            }.items():
                span.set_attribute(key, value)
            previous = None
            with self._spans_lock:
                if self._closed:
                    close_new_span = True
                else:
                    close_new_span = False
                    previous = self._spans.get(event.call_id)
                    self._spans[event.call_id] = span
            if previous is not None:
                previous.end()
            if close_new_span:
                span.end()
            return
        terminal = event.event_type in _TERMINAL_EVENTS
        with self._spans_lock:
            span = (
                self._spans.pop(event.call_id, None)
                if terminal
                else self._spans.get(event.call_id)
            )
            if span is None:
                return
            # Keep non-terminal span updates under the same lock as close().
            # A terminal span is removed first and is then owned exclusively
            # by this call, so duplicate terminal events cannot end it twice.
            span.add_event(
                event.event_type,
                attributes={
                    "actionlens.event_id": event.event_id,
                    "actionlens.event.phase": event.phase,
                },
            )
            if event.error is not None:
                try:
                    from opentelemetry.trace import Status, StatusCode

                    span.set_status(Status(StatusCode.ERROR, event.error.taxonomy))
                except ImportError:
                    pass
        if terminal:
            span.set_attribute("actionlens.tool.terminal_event", event.event_type)
            span.end()

    def flush(self) -> None: ...
    def close(self) -> None:
        with self._spans_lock:
            self._closed = True
            spans = list(self._spans.values())
            self._spans.clear()
        for span in spans:
            span.end()
