from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path

import pytest

import actionlens as al
from actionlens.sinks import MemorySink


def test_result_truncation_writes_artifact(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=sink)

    @lens.tool(max_bytes=20)
    def scrape() -> str:
        return "x" * 200

    with lens.session(session_id="s1"):
        output = scrape()

    assert output.status == "SUCCESS"
    assert output.governance["truncated"] is True
    assert output.artifact_refs
    artifact_path = Path(output.artifact_refs[0].uri)
    assert artifact_path.exists()
    assert artifact_path.read_text(encoding="utf-8") == "x" * 200
    assert any(event.event_type == "tool_call.completed" for event in sink.events)


def test_required_idempotency_key_is_in_public_signature(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
    def send_message(message: str) -> dict:
        return {"sent": message}

    signature = inspect.signature(send_message)
    assert "idempotency_key" in signature.parameters
    assert "__al_ctx" not in signature.parameters

    with pytest.raises(TypeError):
        signature.bind("hello")

    with lens.session(session_id="s1"):
        first = send_message("hello", idempotency_key="msg-1")
        second = send_message("hello", idempotency_key="msg-1")

    assert first.status == "SUCCESS"
    assert second.status == "SUCCESS"
    assert second.result == first.result


def test_explicit_context_is_hidden_and_used(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=sink)

    @lens.tool()
    def lookup(query: str) -> str:
        return query.upper()

    ctx = al.ToolCallContext(
        project="demo",
        session_id="serialized-session",
        run_id="resume-run",
        call_id="old-call",
        tool_name="lookup",
    )
    output = lookup("abc", __al_ctx=ctx)

    assert output.status == "SUCCESS"
    started = next(event for event in sink.events if event.event_type == "tool_call.started")
    assert started.session_id == "serialized-session"
    assert started.metadata["context_source"] == "explicit"


def test_hash_ignore_keys_prevents_random_timestamp_bypass(tmp_path: Path) -> None:
    calls = 0
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(
        risk=al.RiskLevel.MUTATION,
        idempotency=al.IdempotencyPolicy.AUTO_HASH,
        hash_ignore_keys=["timestamp"],
    )
    def write_note(message: str, timestamp: int) -> dict:
        nonlocal calls
        calls += 1
        return {"message": message, "calls": calls}

    with lens.session(session_id="s1"):
        first = write_note("hello", timestamp=1)
        second = write_note("hello", timestamp=2)

    assert calls == 1
    assert second.result == first.result


def test_approval_resume_flow(tmp_path: Path) -> None:
    calls = 0
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(
        risk=al.RiskLevel.DESTRUCTIVE,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def drop_table(name: str) -> dict:
        nonlocal calls
        calls += 1
        return {"dropped": name}

    with lens.session(session_id="s1"):
        pending = drop_table("users", idempotency_key="drop-users")

    assert pending.status == "PENDING_APPROVAL"
    assert calls == 0
    ticket_id = pending.result["ticket_id"]
    assert lens.approve(ticket_id=ticket_id) is True

    with lens.session(session_id="s1"):
        success = drop_table("users", idempotency_key="drop-users")

    assert success.status == "SUCCESS"
    assert success.result == {"dropped": "users"}
    assert calls == 1


def test_async_tool_timeout(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(timeout_sec=0.01)
    async def slow() -> str:
        await asyncio.sleep(0.1)
        return "done"

    async def run() -> al.StructuredToolOutput:
        async with lens.session(session_id="s1"):
            return await slow()

    output = asyncio.run(run())
    assert output.status == "TIMEOUT"
    assert output.error_taxonomy == "Timeout"


def test_cli_summary_reads_jsonl(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path)

    @lens.tool()
    def ping() -> str:
        return "pong"

    with lens.session(session_id="s1"):
        ping()

    from actionlens.cli import main

    assert main(["summary", "--storage-dir", str(tmp_path)]) == 0
    captured = capsys.readouterr().out
    data = json.loads(captured)
    assert data["events"] >= 2
    assert "s1" in data["sessions"]


def test_cli_export_writes_jsonl_and_summary(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path)

    @lens.tool()
    def ping() -> str:
        return "pong"

    with lens.session(session_id="s1"):
        ping()

    from actionlens.cli import main

    jsonl_out = tmp_path / "out" / "events.jsonl"
    summary_out = tmp_path / "out" / "summary.json"

    assert (
        main(
            [
                "export",
                "--storage-dir",
                str(tmp_path),
                "--format",
                "actionlens-jsonl",
                "--output",
                str(jsonl_out),
            ]
        )
        == 0
    )
    assert jsonl_out.exists()
    assert "tool_call.completed" in jsonl_out.read_text(encoding="utf-8")

    assert (
        main(
            [
                "export",
                "--storage-dir",
                str(tmp_path),
                "--format",
                "summary-json",
                "--output",
                str(summary_out),
            ]
        )
        == 0
    )
    data = json.loads(summary_out.read_text(encoding="utf-8"))
    assert data["events"] >= 2
