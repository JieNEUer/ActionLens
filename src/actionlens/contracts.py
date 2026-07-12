from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .models import ToolCallContext, ToolSpec, TrajectoryEvent
from .repository import StaleFenceError


def verify_sink_contract(sink: Any) -> dict[str, bool]:
    """Run the stable sink lifecycle contract against an isolated sink instance."""
    event = _event(f"contract-sink-{uuid4().hex}")
    sink.emit(event)
    sink.flush()
    sink.close()
    sink.close()
    return {"emit": True, "flush": True, "close": True}


def verify_repository_contract(repository: Any) -> dict[str, bool]:
    """Exercise atomic ledger, conflict, terminal transition, and outbox semantics.

    The supplied repository is mutated. Consumers should pass a dedicated test database.
    """
    suffix = uuid4().hex
    key = f"contract-{suffix}"
    context = ToolCallContext(
        project="actionlens-contract", environment="test", tenant_id="tenant-a",
        session_id=f"session-{suffix}", run_id=f"run-{suffix}", call_id=f"call-{suffix}",
        tool_name="contract_tool",
    )
    spec = ToolSpec(name="contract_tool", fencing_supported=True)
    kind, record = repository.begin(
        key, call_id=context.call_id, context=context, spec=spec, args_hash="args-v1",
        tool_schema_hash="schema-v1", owner_id="contract-worker", lease_seconds=30,
    )
    _require(kind == "created", "first begin must create the ledger record")
    conflict, _ = repository.begin(
        key, call_id=f"other-{suffix}", context=context, spec=spec, args_hash="args-v2",
        tool_schema_hash="schema-v1", owner_id="other-worker", lease_seconds=30,
    )
    _require(conflict == "conflict", "different args must produce a conflict")
    event = _event(f"contract-repository-{suffix}", context=context)
    finished = repository.finish(
        key, owner_id="contract-worker", fencing_token=record.fencing_token,
        status="SUCCEEDED", output={"status": "SUCCESS", "result_summary": "ok"},
        error=None, event=event,
    )
    _require(finished.status == "SUCCEEDED", "finish must persist SUCCEEDED")
    claimed = repository.claim_outbox(
        worker_id=f"dispatcher-{suffix}", limit=100, claim_seconds=30
    )
    delivery = next((item for item in claimed if item.event.event_id == event.event_id), None)
    _require(delivery is not None, "terminal event must be atomically available in outbox")
    _require(
        repository.ack_outbox(delivery.delivery_id, worker_id=f"dispatcher-{suffix}"),
        "the claiming worker must be able to acknowledge delivery",
    )
    takeover_key = f"contract-takeover-{suffix}"
    _, stale = repository.begin(
        takeover_key, call_id=context.call_id, context=context, spec=spec,
        args_hash="takeover", tool_schema_hash="schema-v1", owner_id="stale-worker",
        lease_seconds=-1,
    )
    kind, current = repository.begin(
        takeover_key, call_id=f"takeover-{suffix}", context=context, spec=spec,
        args_hash="takeover", tool_schema_hash="schema-v1", owner_id="current-worker",
        lease_seconds=30,
    )
    _require(kind == "created", "an expired fenceable lease must be acquirable")
    try:
        repository.finish(
            takeover_key, owner_id="stale-worker", fencing_token=stale.fencing_token,
            status="SUCCEEDED", output={}, error=None,
            event=_event(f"contract-stale-{suffix}", context=context),
        )
    except StaleFenceError:
        pass
    else:
        raise AssertionError("ActionLens contract violation: stale fence must not commit")
    repository.finish(
        takeover_key, owner_id="current-worker", fencing_token=current.fencing_token,
        status="SUCCEEDED", output={}, error=None,
        event=_event(f"contract-takeover-complete-{suffix}", context=context),
    )
    return {"begin": True, "conflict": True, "finish": True, "outbox": True}


def _event(event_id: str, *, context: ToolCallContext | None = None) -> TrajectoryEvent:
    context = context or ToolCallContext(
        project="actionlens-contract", session_id="sink", run_id="sink",
        call_id="sink", tool_name="contract_sink",
    )
    return TrajectoryEvent(
        event_id=event_id,
        timestamp=datetime.now(timezone.utc),
        project=context.project,
        session_id=context.session_id,
        run_id=context.run_id,
        sequence=1,
        event_type="contract.probe",
        phase="POST_FLIGHT",
        call_id=context.call_id,
        tool_name=context.tool_name,
    )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(f"ActionLens contract violation: {message}")
