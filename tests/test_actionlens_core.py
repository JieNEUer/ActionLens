from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import json
import time
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


def test_sqlite_ledger_concurrent_same_key_executes_once(tmp_path: Path) -> None:
    calls = 0
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
    def mutate(value: str) -> dict:
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return {"value": value, "calls": calls}

    def run_one() -> al.StructuredToolOutput:
        ctx = al.ToolCallContext(
            project="demo",
            session_id="s1",
            run_id="r1",
            call_id="caller",
            tool_name="mutate",
        )
        return mutate("x", idempotency_key="same-key", __al_ctx=ctx)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(lambda _: run_one(), range(2)))

    assert calls == 1
    assert {first.status, second.status} <= {"SUCCESS", "SKIPPED"}


def test_approval_ticket_persists_across_lens_instances(tmp_path: Path) -> None:
    calls: list[str] = []
    lens1 = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens1.tool(
        name="drop_table",
        risk=al.RiskLevel.DESTRUCTIVE,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def drop_table_v1(name: str) -> dict:
        calls.append(name)
        return {"dropped": name}

    with lens1.session(session_id="s1"):
        pending = drop_table_v1("users", idempotency_key="drop-users")

    ticket_id = pending.result["ticket_id"]

    lens2 = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())
    assert lens2.approve(
        ticket_id=ticket_id,
        approved_by="ops",
        modified_args={"name": "archived_users"},
    )

    lens3 = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens3.tool(
        name="drop_table",
        risk=al.RiskLevel.DESTRUCTIVE,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def drop_table_v3(name: str) -> dict:
        calls.append(name)
        return {"dropped": name}

    with lens3.session(session_id="s1"):
        success = drop_table_v3("users", idempotency_key="drop-users")

    assert success.status == "SUCCESS"
    assert success.result == {"dropped": "archived_users"}
    assert calls == ["archived_users"]


def test_approval_denied_returns_denied(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(
        risk=al.RiskLevel.DESTRUCTIVE,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def erase(name: str) -> dict:
        return {"erased": name}

    with lens.session(session_id="s1"):
        pending = erase("users", idempotency_key="erase-users")

    assert lens.deny(ticket_id=pending.result["ticket_id"], decision_note="too risky")

    with lens.session(session_id="s1"):
        denied = erase("users", idempotency_key="erase-users")

    assert denied.status == "DENIED"
    assert denied.error_taxonomy == "ApprovalDenied"


def test_regex_redaction_applies_to_args_and_inline_result(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=sink)

    @lens.tool(output=al.OutputPolicy(redact_patterns=[r"sk-[A-Za-z0-9]+"]))
    def echo(secret: str) -> dict:
        return {"token": secret, "message": f"using {secret}"}

    with lens.session(session_id="s1"):
        output = echo("sk-abc123")

    assert output.result["token"] == "[REDACTED]"
    assert output.result["message"] == "using [REDACTED]"
    started = next(event for event in sink.events if event.event_type == "tool_call.started")
    assert started.metadata["args"]["secret"] == "[REDACTED]"


def test_artifact_gc_dry_run_and_max_bytes(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(max_bytes=5)
    def large() -> str:
        return "x" * 100

    with lens.session(session_id="s1"):
        output = large()

    artifact_path = Path(output.artifact_refs[0].uri)
    dry = lens.artifact_store.gc(older_than_seconds=0, max_bytes=1, dry_run=True)
    assert dry["would_delete"] >= 1
    assert artifact_path.exists()

    real = lens.artifact_store.gc(older_than_seconds=0, max_bytes=1)
    assert real["deleted"] >= 1
    assert not artifact_path.exists()


def test_budget_policy_denies_after_limit(tmp_path: Path) -> None:
    lens = al.ActionLens(
        project="demo",
        storage_dir=tmp_path,
        sink=MemorySink(),
        policies=[al.BudgetPolicy(max_calls_per_run=1)],
    )

    @lens.tool()
    def ping() -> str:
        return "pong"

    with lens.session(session_id="s1", run_id="r1"):
        first = ping()
        second = ping()

    assert first.status == "SUCCESS"
    assert second.status == "DENIED"
    assert second.error_taxonomy == "BudgetExceeded"


def test_sync_tool_timeout_thread_mode(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(timeout_sec=0.01, run_sync_in_thread=True)
    def slow() -> str:
        time.sleep(0.05)
        return "done"

    with lens.session(session_id="s1"):
        output = slow()

    assert output.status == "TIMEOUT"
    assert output.error_taxonomy == "Timeout"


def test_invoke_many_parallel_safe_reads_preserves_order(tmp_path: Path) -> None:
    lens = al.ActionLens(project="demo", storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(concurrency=al.ConcurrencyPolicy.SAFE)
    def read_value(value: int) -> int:
        time.sleep(0.02 if value == 1 else 0.0)
        return value

    with lens.session(session_id="s1"):
        outputs = lens.invoke_many(
            [
                (read_value, (1,), {}),
                (read_value, (2,), {}),
            ]
        )

    assert [output.result for output in outputs] == [1, 2]
