from __future__ import annotations

import concurrent.futures
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

import actionlens as al
from actionlens.cli import main
from actionlens.exporters import (
    export_events,
    export_inspect_ai,
    export_sft,
    render_html_report,
)
from actionlens.integrations.langchain import (
    as_langchain_tool,
    context_from_langgraph_state,
    wrap_langchain_tool,
)
from actionlens.integrations.openai_agents import wrap_openai_agent_tool
from actionlens.integrations.pydantic_ai import wrap_pydantic_ai_tool
from actionlens.ledger import SQLiteLedger
from actionlens.models import TrajectoryEvent
from actionlens.sinks import JsonlSink, MemorySink


def _event(index: int = 1) -> TrajectoryEvent:
    return TrajectoryEvent(
        event_id=f"e{index}",
        timestamp=datetime.now(timezone.utc),
        project="demo",
        session_id="s1",
        run_id="r1",
        sequence=index,
        event_type="tool_call.started",
        phase="PRE_FLIGHT",
        call_id=f"c{index}",
        tool_name="ping",
    )


def test_jsonl_queue_close_flushes_all_events(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path, queue_maxsize=4)
    for index in range(3):
        sink.emit(_event(index))
    sink.close()
    lines = next((tmp_path / "trajectories").glob("*.jsonl")).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert sink.stats() == {"dropped": 0, "write_errors": 0, "queued": 0}


def test_jsonl_drop_newest_is_counted(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path, queue_maxsize=1, drop_policy="drop_newest")
    entered = threading.Event()
    release = threading.Event()
    original = sink._write_safely

    def blocked(event: TrajectoryEvent) -> None:
        entered.set()
        release.wait(2)
        original(event)

    sink._write_safely = blocked  # type: ignore[method-assign]
    sink.emit(_event(1))
    assert entered.wait(1)
    sink.emit(_event(2))
    sink.emit(_event(3))
    assert sink.dropped_count == 1
    release.set()
    sink.close()


def test_jsonl_drop_oldest_keeps_newest(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path, queue_maxsize=1, drop_policy="drop_oldest")
    entered = threading.Event()
    release = threading.Event()
    original = sink._write_safely

    def blocked(event: TrajectoryEvent) -> None:
        entered.set()
        release.wait(2)
        original(event)

    sink._write_safely = blocked  # type: ignore[method-assign]
    sink.emit(_event(1))
    assert entered.wait(1)
    sink.emit(_event(2))
    sink.emit(_event(3))
    release.set()
    sink.close()
    payload = next((tmp_path / "trajectories").glob("*.jsonl")).read_text(encoding="utf-8")
    assert '"event_id":"e3"' in payload
    assert '"event_id":"e2"' not in payload


@pytest.mark.parametrize("queue_size", [0, -1])
def test_jsonl_rejects_invalid_queue_size(tmp_path: Path, queue_size: int) -> None:
    with pytest.raises(ValueError):
        JsonlSink(tmp_path, queue_maxsize=queue_size)


def test_sink_failure_is_nonfatal_by_default(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path)
    sink.trajectory_dir = tmp_path / "missing" / "nested"
    sink.emit(_event())
    assert sink.write_error_count == 1


def test_sink_failure_raises_in_strict_mode(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path, strict=True)
    sink.trajectory_dir = tmp_path / "missing" / "nested"
    with pytest.raises(OSError):
        sink.emit(_event())


def test_arbitrary_sink_failure_does_not_break_tool(tmp_path: Path) -> None:
    class BrokenSink:
        strict = False

        def emit(self, event: TrajectoryEvent) -> None:
            raise OSError("disk unavailable")

        def flush(self) -> None: ...
        def close(self) -> None: ...

    lens = al.ActionLens(storage_dir=tmp_path, sink=BrokenSink())

    @lens.tool
    def ping() -> str:
        return "pong"

    assert ping().result == "pong"


def test_ticket_expiration_is_terminal_and_queryable(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(approval_required=True, approval_ttl_sec=-1)
    def erase() -> str:
        return "done"

    pending = erase()
    ticket_id = pending.result["ticket_id"]
    assert lens.approve(ticket_id=ticket_id) is False
    assert lens.get_ticket(ticket_id).status == "EXPIRED"  # type: ignore[union-attr]
    assert lens.tickets(status="EXPIRED")[0].ticket_id == ticket_id


def test_concurrent_approval_requests_share_one_ticket(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(
        risk=al.RiskLevel.DESTRUCTIVE,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def erase(name: str) -> str:
        return name

    def invoke() -> al.StructuredToolOutput:
        context = al.ToolCallContext(
            project="default", session_id="s", run_id="r", call_id="caller", tool_name="erase"
        )
        return erase("x", idempotency_key="same", __al_ctx=context)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        outputs = list(pool.map(lambda _: invoke(), range(2)))
    assert {output.result["ticket_id"] for output in outputs} == {lens.tickets()[0].ticket_id}
    assert len(lens.tickets()) == 1


def test_approval_rejects_unknown_modified_arg(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(approval_required=True)
    def erase(name: str) -> str:
        return name

    pending = erase("x")
    with pytest.raises(ValueError, match="unknown parameters"):
        lens.approve(ticket_id=pending.result["ticket_id"], modified_args={"other": "y"})


def test_approval_rejects_wrong_modified_arg_type(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(approval_required=True)
    def erase(count: int) -> int:
        return count

    pending = erase(1)
    with pytest.raises(ValueError, match="annotation"):
        lens.approve(ticket_id=pending.result["ticket_id"], modified_args={"count": "many"})


def test_denial_emits_redacted_decision_event(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(storage_dir=tmp_path, sink=sink)

    @lens.tool(approval_required=True)
    def erase() -> str:
        return "done"

    pending = erase()
    assert lens.deny(ticket_id=pending.result["ticket_id"], decision_note="Bearer top-secret")
    event = next(event for event in sink.events if event.event_type == "approval.denied")
    assert event.metadata["decision_note"] == "[REDACTED]"


def test_sqlite_ledger_persists_context_fields(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(idempotency=al.IdempotencyPolicy.AUTO_HASH)
    def ping() -> str:
        return "pong"

    with lens.session(session_id="s1", run_id="r1", tenant_id="t1", environment="prod"):
        ping()
    record = lens.ledger.records()[0]
    assert (record.project, record.environment, record.tenant_id) == ("demo", "prod", "t1")
    assert (record.session_id, record.run_id, record.tool_name) == ("s1", "r1", "ping")


def test_sqlite_stale_pending_can_be_reacquired(tmp_path: Path) -> None:
    ledger = SQLiteLedger(tmp_path / "ledger.sqlite3", stale_pending_sec=0)
    assert ledger.begin("k", call_id="c1")[0] == "created"
    assert ledger.begin("k", call_id="c2")[0] == "created"
    assert ledger.get("k").call_id == "c2"  # type: ignore[union-attr]


def test_sqlite_migrates_v02_context_columns(tmp_path: Path) -> None:
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE actionlens_idempotency (
            key TEXT PRIMARY KEY, call_id TEXT NOT NULL, status TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"""
        )
    ledger = SQLiteLedger(path)
    columns = {row[1] for row in sqlite3.connect(path).execute("PRAGMA table_info(actionlens_idempotency)")}
    assert {"project", "environment", "tenant_id", "session_id", "run_id", "tool_name"} <= columns
    assert ledger.records() == []


def test_corrupt_jsonl_is_counted_and_skipped(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool
    def ping() -> str:
        return "pong"

    ping()
    path = next((tmp_path / "trajectories").glob("*.jsonl"))
    with path.open("a", encoding="utf-8") as handle:
        handle.write("{partial\n")
    summary = json.loads(_capture_cli(["summary", "--storage-dir", str(tmp_path)]))
    assert summary["events"] == 2
    assert summary["skipped_lines"] == 1


def test_native_export_omits_corrupt_lines(tmp_path: Path) -> None:
    trajectory = tmp_path / "trajectories"
    trajectory.mkdir()
    (trajectory / "x.jsonl").write_text('{"event_type":"ok"}\nnot-json\n', encoding="utf-8")
    output = tmp_path / "out.jsonl"
    result = export_events(tmp_path, output)
    assert result == {"exported": 1, "skipped": 1}
    assert output.read_text(encoding="utf-8").count("\n") == 1


def test_inspect_export_groups_run_and_keeps_governance(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool
    def ping() -> str:
        return "pong"

    with lens.session(session_id="s1", run_id="r1"):
        ping()
    output = tmp_path / "inspect.jsonl"
    assert export_inspect_ai(tmp_path, output)["exported"] == 1
    sample = json.loads(output.read_text(encoding="utf-8"))
    assert sample["id"] == "r1"
    assert [item["event"] for item in sample["transcript"]] == ["tool_call.started", "tool_call.completed"]


def test_sft_export_only_includes_successful_completed_calls(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool(output=al.OutputPolicy(include_raw_in_trajectory=True))
    def ping(secret: str) -> dict[str, str]:
        return {"token": secret, "value": "pong"}

    @lens.tool(approval_required=True)
    def erase() -> str:
        return "done"

    ping("hidden")
    erase()
    output = tmp_path / "sft.jsonl"
    result = export_sft(tmp_path, output)
    assert result["exported"] == 1
    sample = json.loads(output.read_text(encoding="utf-8"))
    tool_content = json.loads(sample["messages"][1]["content"])
    assert tool_content["result"]["token"] == "[REDACTED]"


def test_export_marks_dangling_artifact_without_reading_it(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool(max_bytes=2)
    def large() -> str:
        return "secret raw value"

    result = large()
    Path(result.artifact_refs[0].uri).unlink()
    output = tmp_path / "inspect.jsonl"
    export_inspect_ai(tmp_path, output)
    data = json.loads(output.read_text(encoding="utf-8"))
    artifact = next(item["artifact"] for item in data["transcript"] if "artifact" in item)
    assert artifact["dangling"] is True


def test_large_secret_artifact_uses_redacted_preview(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(max_bytes=2)
    def large() -> dict[str, str]:
        return {"token": "sk-very-secret", "payload": "x" * 100}

    output = large()
    ref = output.artifact_refs[0]
    assert ref.redacted is True
    assert "sk-very-secret" not in (ref.preview or "")
    meta = json.loads(Path(ref.uri + ".meta.json").read_text(encoding="utf-8"))
    assert "sk-very-secret" not in meta["preview"]


def test_html_report_escapes_untrusted_values(tmp_path: Path) -> None:
    sink = JsonlSink(tmp_path)
    event = _event()
    event.tool_name = "<script>alert(1)</script>"
    sink.emit(event)
    output = tmp_path / "report.html"
    render_html_report(tmp_path, output)
    html = output.read_text(encoding="utf-8")
    assert "&lt;script&gt;" in html
    assert "<script>alert" not in html


def test_cli_export_filter_selects_session(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool
    def ping() -> str:
        return "pong"

    with lens.session(session_id="keep"):
        ping()
    with lens.session(session_id="drop"):
        ping()
    output = tmp_path / "filtered.jsonl"
    assert main(["export", "--storage-dir", str(tmp_path), "--session", "keep", "--output", str(output)]) == 0
    assert '"session_id": "keep"' in output.read_text(encoding="utf-8")
    assert '"session_id": "drop"' not in output.read_text(encoding="utf-8")


def test_cli_report_tickets_and_ledger(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool(approval_required=True)
    def erase() -> str:
        return "done"

    erase()
    report = tmp_path / "report.html"
    assert main(
        ["report", "--storage-dir", str(tmp_path), "--html", "--output", str(report)]
    ) == 0
    assert report.exists()
    tickets = json.loads(
        _capture_cli(["tickets", "--storage-dir", str(tmp_path), "--status", "PENDING"])
    )
    ledger = json.loads(
        _capture_cli(["inspect-ledger", "--storage-dir", str(tmp_path)])
    )
    assert len(tickets) == 1
    assert tickets[0]["status"] == "PENDING"
    assert len(ledger) == 1
    assert ledger[0]["status"] == "APPROVAL_PENDING"


def test_pydantic_adapter_schema_and_model_output(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(idempotency=al.IdempotencyPolicy.REQUIRED)
    def mutate(value: int) -> int:
        return value

    adapter = wrap_pydantic_ai_tool(mutate)
    assert set(adapter.parameters_json_schema["required"]) == {"value", "idempotency_key"}
    output = adapter.invoke({"value": 1, "idempotency_key": "k"})
    assert isinstance(output, al.StructuredToolOutput)


def test_pydantic_adapter_maps_run_context_metadata(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(storage_dir=tmp_path, sink=sink)

    @lens.tool
    def ping() -> str:
        return "pong"

    context = type("RunContext", (), {"metadata": {"session_id": "pyd", "run_id": "r1", "trace": "t"}})()
    wrap_pydantic_ai_tool(ping).invoke({}, framework_context=context)
    started = sink.events[0]
    assert started.session_id == "pyd"
    assert started.metadata["context_source"] == "explicit"


def test_pydantic_adapter_preserves_pending_approval(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(approval_required=True)
    def erase() -> str:
        return "done"

    output = wrap_pydantic_ai_tool(erase).invoke({})
    assert output.status == "PENDING_APPROVAL"  # type: ignore[union-attr]


def test_openai_adapter_schema_hides_runtime_context(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(idempotency=al.IdempotencyPolicy.REQUIRED)
    def mutate(value: str) -> str:
        return value

    schema = wrap_openai_agent_tool(mutate).openai_schema()["function"]["parameters"]
    assert "idempotency_key" in schema["required"]
    assert "__al_ctx" not in schema["properties"]


def test_openai_adapter_returns_stable_json_with_recovery_hint(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(approval_required=True)
    def erase() -> str:
        return "done"

    payload = json.loads(wrap_openai_agent_tool(erase).invoke({}))
    assert payload["status"] == "PENDING_APPROVAL"
    assert payload["recovery_hint"]


def test_openai_adapter_explicit_context_reaches_trajectory(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(storage_dir=tmp_path, sink=sink)

    @lens.tool
    def ping() -> str:
        return "pong"

    wrap_openai_agent_tool(ping).invoke({}, framework_context={"session_id": "oa", "run_id": "r"})
    assert sink.events[0].session_id == "oa"


def test_langgraph_context_uses_checkpoint_config() -> None:
    context = context_from_langgraph_state(
        {"config": {"configurable": {"thread_id": "thread-1", "run_id": "run-1"}}},
        tool_name="ping",
    )
    assert context.session_id == "thread-1"
    assert context.run_id == "run-1"
    assert context.framework == "langgraph"


def test_langchain_adapter_native_schema_contains_idempotency(tmp_path: Path) -> None:
    pytest.importorskip("langchain_core")
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(idempotency=al.IdempotencyPolicy.REQUIRED)
    def mutate(value: int) -> int:
        """Mutate a value."""
        return value

    native = as_langchain_tool(wrap_langchain_tool(mutate))
    schema = native.args_schema.model_json_schema()
    assert "idempotency_key" in schema["required"]


def test_langgraph_tool_node_executes_native_actionlens_tool(tmp_path: Path) -> None:
    pytest.importorskip("langgraph")
    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(idempotency=al.IdempotencyPolicy.REQUIRED)
    def mutate(value: str) -> dict[str, str]:
        return {"value": value}

    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([as_langchain_tool(wrap_langchain_tool(mutate))]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    app = graph.compile()
    result = app.invoke(
        {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "mutate",
                            "args": {"value": "x", "idempotency_key": "graph-key"},
                            "id": "call-1",
                            "type": "tool_call",
                        }
                    ],
                )
            ]
        }
    )
    payload = json.loads(result["messages"][-1].content)
    assert payload["status"] == "SUCCESS"
    assert payload["result"] == {"value": "x"}


def test_langchain_adapter_returns_json(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool
    def ping() -> str:
        return "pong"

    payload = json.loads(wrap_langchain_tool(ping).invoke({}))
    assert payload["status"] == "SUCCESS"


def _capture_cli(argv: list[str]) -> str:
    from contextlib import redirect_stdout
    from io import StringIO

    stream = StringIO()
    with redirect_stdout(stream):
        assert main(argv) == 0
    return stream.getvalue()
