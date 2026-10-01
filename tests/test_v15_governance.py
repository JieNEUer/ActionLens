from __future__ import annotations

import asyncio
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

import actionlens as al
import actionlens.sinks.webhook as wh
from actionlens.errors import classify_exception
from actionlens.sinks import MemorySink


def _context(run_id: str) -> al.ToolCallContext:
    return al.ToolCallContext(
        project="v15",
        session_id="session",
        run_id=run_id,
        call_id=f"call-{run_id}",
        tool_name="ping",
    )


def test_validation_error_is_not_misclassified_as_format_error() -> None:
    class Payload(BaseModel):
        count: int

    with pytest.raises(ValidationError) as raised:
        Payload(count="not-an-int")

    assert classify_exception(raised.value).taxonomy == "ValidationError"


def test_mutation_timeout_becomes_uncertain_and_blocks_retry(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    calls = 0

    def slow_mutation() -> str:
        nonlocal calls
        calls += 1
        time.sleep(0.08)
        return "completed"

    with pytest.warns(UserWarning, match="cannot stop"):
        mutate = lens.tool(
            risk=al.RiskLevel.MUTATION,
            idempotency=al.IdempotencyPolicy.REQUIRED,
            timeout_sec=0.01,
            run_sync_in_thread=True,
        )(slow_mutation)

    first = mutate(idempotency_key="mutation-1")
    second = mutate(idempotency_key="mutation-1")
    assert first.status == "UNCERTAIN"
    assert first.error_taxonomy == "Timeout"
    assert first.governance["background_execution_may_continue"] is True
    assert second.status == "UNCERTAIN"
    assert lens.repository.get_ledger("mutation-1").status == "UNCERTAIN"  # type: ignore[union-attr]
    time.sleep(0.1)
    assert calls == 1


def test_read_timeout_remains_retryable_timeout(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(
        risk=al.RiskLevel.READ,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        timeout_sec=0.01,
        run_sync_in_thread=True,
    )
    def slow_read() -> str:
        time.sleep(0.05)
        return "later"

    output = slow_read(idempotency_key="read-1")
    assert output.status == "TIMEOUT"
    assert lens.repository.get_ledger("read-1").status == "FAILED_RETRYABLE"  # type: ignore[union-attr]


def test_legacy_ledgers_reacquire_retryable_failures(tmp_path: Path) -> None:
    memory = al.MemoryLedger()
    sqlite = al.SQLiteLedger(tmp_path / "legacy-ledger.sqlite3")
    for ledger in (memory, sqlite):
        assert ledger.begin("retry-key", call_id="first")[0] == "created"
        ledger.mark_failed("retry-key", retryable=True)
        assert ledger.begin("retry-key", call_id="second")[0] == "created"
        assert ledger.get("retry-key").call_id == "second"


def test_heartbeat_keeps_long_running_execution_owned(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(
        risk=al.RiskLevel.MUTATION,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        lease_seconds=0.02,
    )
    def long_running() -> str:
        time.sleep(0.09)
        return "ok"

    output = long_running(idempotency_key="heartbeat-1")
    assert output.status == "SUCCESS"
    assert lens.repository.get_ledger("heartbeat-1").status == "SUCCEEDED"  # type: ignore[union-attr]


def test_budget_policy_preserves_active_runs_and_supports_explicit_reset() -> None:
    policy = al.BudgetPolicy(max_calls_per_run=3, max_tracked_runs=2)
    spec = al.ToolSpec(name="ping")
    for run_id in ("one", "two"):
        assert policy.decide(spec=spec, args={}, context=_context(run_id)).action == "ALLOW"
    assert policy.tracked_run_count == 2
    assert policy.decide(spec=spec, args={}, context=_context("three")).action == "DENY"
    policy.reset_run(project="v15", run_id="one")
    assert policy.tracked_run_count == 1


def test_actionlens_reset_run_releases_artifact_and_builtin_budget_state(tmp_path: Path) -> None:
    budget = al.BudgetPolicy(max_calls_per_run=1)
    lens = al.ActionLens(
        project="v15",
        storage_dir=tmp_path,
        sink=MemorySink(),
        artifact_policy=al.ArtifactPolicy(max_bytes_per_run=4),
        policies=[budget],
    )
    context = _context("run-to-reset")
    spec = al.ToolSpec(name="ping")

    lens.artifact_store.put(b"1234", metadata={"run_id": context.run_id})
    assert budget.decide(spec=spec, args={}, context=context).action == "ALLOW"
    assert lens.artifact_store.tracked_run_count == 1
    assert budget.tracked_run_count == 1

    lens.reset_run(context.run_id)

    assert lens.artifact_store.tracked_run_count == 0
    assert budget.tracked_run_count == 0
    lens.artifact_store.put(b"1234", metadata={"run_id": context.run_id})
    assert budget.decide(spec=spec, args={}, context=context).action == "ALLOW"


def test_output_policy_uses_utf8_bytes_and_item_limit(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(output=al.OutputPolicy(max_inline_bytes=13, max_inline_items=2))
    def unicode_payload() -> list[str]:
        return ["ni", "hao", "shi", "jie"]

    output = unicode_payload()
    assert output.artifact_refs
    assert isinstance(output.result, str)
    assert len(output.result.encode("utf-8")) <= 13
    assert "top-level items" in output.result_summary


def test_confidential_summary_and_trajectory_do_not_store_content(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(
        storage_dir=tmp_path,
        sink=sink,
        artifact_policy=al.ArtifactPolicy(raw_mode="deny"),
    )

    @lens.tool
    def secret() -> str:
        return "classified-content"

    output = secret()
    completed = next(event for event in sink.events if event.event_type == "tool_call.completed")
    assert "classified-content" not in output.result_summary
    assert completed.metadata["output"]["result"] is None


def test_encrypted_artifact_never_persists_plaintext_preview(tmp_path: Path) -> None:
    class Cipher:
        provider_id = "test-cipher"
        key_version = "v1"

        def encrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload[::-1]

        def decrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload[::-1]

    secret = "restricted-preview-content"
    lens = al.ActionLens(
        storage_dir=tmp_path,
        sink=MemorySink(),
        artifact_policy=al.ArtifactPolicy(encryption="provider"),
        encryption_provider=Cipher(),
    )

    @lens.tool(max_bytes=8)
    def result() -> str:
        return secret

    output = result()
    ref = output.artifact_refs[0]
    sidecar = Path(ref.uri).with_suffix(Path(ref.uri).suffix + ".meta.json")
    assert output.result == secret[:8]
    assert ref.preview is None
    assert ref.confidentiality["preview_withheld"] is True
    assert secret not in sidecar.read_text(encoding="utf-8")

    # Reusing content-addressed ciphertext also removes a sidecar preview left
    # by an older ActionLens version.
    legacy_sidecar = json.loads(sidecar.read_text(encoding="utf-8"))
    legacy_sidecar["preview"] = secret
    sidecar.write_text(json.dumps(legacy_sidecar), encoding="utf-8")
    reused = lens.artifact_store.put(secret, preview=secret)
    assert reused.preview is None
    assert secret not in sidecar.read_text(encoding="utf-8")


def test_duplicate_tool_name_fails_fast(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(name="duplicate")
    def first() -> str:
        return "first"

    with pytest.raises(ValueError, match="already registered"):

        @lens.tool(name="duplicate")
        def second() -> str:
            return "second"


def test_new_event_sequences_are_process_unique_and_exportable(tmp_path: Path) -> None:
    first = al.ActionLens(storage_dir=tmp_path / "one", sink=MemorySink())
    second = al.ActionLens(storage_dir=tmp_path / "two", sink=MemorySink())
    event_one = first._make_event(  # type: ignore[attr-defined]
        context=_context("one"), event_type="test", phase="PRE_FLIGHT"
    )
    event_two = second._make_event(  # type: ignore[attr-defined]
        context=_context("two"), event_type="test", phase="PRE_FLIGHT"
    )
    assert isinstance(event_one.sequence, str)
    assert event_one.sequence != event_two.sequence


def test_streaming_tool_writes_artifact_and_returns_tail(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(output=al.OutputPolicy(streaming_tail_lines=2))
    def stream() -> object:
        yield "first\n"
        yield "second\n"
        yield "third\n"

    output = stream()
    assert output.status == "SUCCESS"
    assert output.governance["streaming"] is True
    assert output.result == "second\nthird"
    assert output.artifact_refs
    assert Path(output.artifact_refs[0].uri).read_text(encoding="utf-8") == "first\nsecond\nthird\n"


def test_async_streaming_tool_writes_artifact(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(output=al.OutputPolicy(streaming_tail_lines=1))
    async def stream() -> object:
        yield "first\n"
        await asyncio.sleep(0)
        yield "second\n"

    output = asyncio.run(stream())
    assert output.status == "SUCCESS"
    assert output.result == "second"
    assert output.artifact_refs


def test_artifact_navigation_is_bounded_authorized_and_governed(tmp_path: Path) -> None:
    class Allow:
        def authorize(self, artifact: object, *, context: dict[str, object]) -> bool:
            return True

    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink(), artifact_authorizer=Allow())
    artifact = lens.artifact_store.put("alpha\nbeta\ngamma\n")
    page = lens.read_artifact_page(artifact, offset=6, limit=4)
    assert page["content"] == "beta"
    matches = lens.grep_artifact(artifact, "gamma")
    assert matches["matches"][0]["line"] == "gamma"
    tools = lens.artifact_navigation_tools()
    output = tools["artifact_read"](artifact, offset=0, limit=5)
    assert output.status == "SUCCESS"
    assert output.result["content"] == "alpha"


def test_synchronous_approval_resolver_executes_without_pending_roundtrip(tmp_path: Path) -> None:
    def approve(ticket: al.ApprovalTicket, context: al.ToolCallContext) -> dict[str, str]:
        assert ticket.safe_args["name"] == "target"
        assert context.tool_name == "erase"
        return {"action": "APPROVE", "approved_by": "operator"}

    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink(), approval_resolver=approve)
    calls = 0

    @lens.tool(
        risk=al.RiskLevel.DESTRUCTIVE,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def erase(name: str) -> str:
        nonlocal calls
        calls += 1
        return name

    output = erase("target", idempotency_key="erase-1")
    assert output.status == "SUCCESS"
    assert calls == 1


def test_approved_retryable_failure_reuses_existing_approval(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    attempts = 0

    @lens.tool(
        risk=al.RiskLevel.MUTATION,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def retryable_mutation(value: str) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise al.NoSideEffectError("provider confirmed no operation was applied")
        return value

    pending = retryable_mutation("ok", idempotency_key="approved-retry")
    assert pending.status == "PENDING_APPROVAL"
    assert lens.approve(ticket_id=pending.result["ticket_id"])

    failed = retryable_mutation("ok", idempotency_key="approved-retry")
    assert failed.status == "FAILED"
    assert lens.repository.get_ledger("approved-retry").status == "FAILED_RETRYABLE"  # type: ignore[union-attr]

    retried = retryable_mutation("ok", idempotency_key="approved-retry")
    assert retried.status == "SUCCESS"
    assert retried.result == "ok"
    assert attempts == 2


def test_required_idempotency_key_fn_supplies_operation_identity(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    calls = 0

    def key_for_order(args: dict[str, object], context: al.ToolCallContext) -> str:
        return f"{context.project}:order:{args['order_id']}"

    @lens.tool(
        risk=al.RiskLevel.MUTATION,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        idempotency_key_fn=key_for_order,
    )
    def submit(order_id: str, idempotency_key: str | None = None) -> str:
        nonlocal calls
        calls += 1
        return order_id

    first = submit("42")
    second = submit("42")
    assert first.status == second.status == "SUCCESS"
    assert second.result == "42"
    assert calls == 1
    assert lens.repository.get_ledger("default:order:42") is not None  # type: ignore[union-attr]


def test_required_idempotency_without_a_key_fails_before_execution(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    calls = 0

    @lens.tool(
        risk=al.RiskLevel.MUTATION,
        idempotency=al.IdempotencyPolicy.REQUIRED,
    )
    def mutate(value: str, idempotency_key: str | None = None) -> str:
        nonlocal calls
        calls += 1
        return value

    output = mutate("write")
    assert output.status == "FAILED"
    assert output.error_taxonomy == "IdempotencyKeyRequired"
    assert calls == 0


def test_opaque_python_result_is_normalized_for_durable_output(tmp_path: Path) -> None:
    class OpaqueResult:
        def __str__(self) -> str:
            return "opaque-result"

    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool
    def produce() -> object:
        return OpaqueResult()

    try:
        output = produce()
        lens.flush()
        events = list((tmp_path / "trajectories").glob("*.jsonl"))
        assert output.status == "SUCCESS"
        assert output.result == "opaque-result"
        assert events
        assert "tool_call.completed" in events[0].read_text(encoding="utf-8")
    finally:
        lens.close()


def test_read_governance_failure_acknowledges_that_execution_already_happened(
    tmp_path: Path,
) -> None:
    class FinishFails(al.SQLiteGovernanceRepository):
        def finish(self, *args: object, **kwargs: object) -> object:
            raise OSError("storage unavailable")

    lens = al.ActionLens(
        storage_dir=tmp_path,
        sink=MemorySink(),
        repository=FinishFails(tmp_path / "governance.sqlite3"),
    )
    calls = 0

    @lens.tool(idempotency=al.IdempotencyPolicy.REQUIRED)
    def lookup() -> str:
        nonlocal calls
        calls += 1
        return "answer"

    output = lookup(idempotency_key="read-finish-failure")
    assert output.status == "FAILED"
    assert calls == 1
    assert "Tool executed" in output.result_summary


def test_child_context_creates_a_durable_parent_link(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(storage_dir=tmp_path, sink=sink)

    @lens.tool
    def child_tool() -> str:
        return "ok"

    with lens.session(session_id="session", run_id="run") as parent:
        child = lens.child_context(parent=parent, tool_name="child_tool")
        output = child_tool(__al_ctx=child)

    completed = next(event for event in sink.events if event.event_type == "tool_call.completed")
    assert output.status == "SUCCESS"
    assert completed.parent_call_id == parent.call_id
    assert completed.run_id == parent.run_id


def test_cache_read_is_time_bounded_and_shared_across_sessions(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    calls = 0

    @lens.tool(idempotency=al.IdempotencyPolicy.CACHE_READ, cache_ttl_sec=60)
    def lookup(value: str) -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"calls": calls}

    with lens.session(session_id="one"):
        first = lookup("same")
    with lens.session(session_id="two"):
        second = lookup("same")
    assert first.result == second.result == {"calls": 1}
    assert calls == 1


def test_mcp_proxy_validates_arguments_and_governs_jsonrpc_calls(tmp_path: Path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def transport(name: str, arguments: dict[str, object]) -> dict[str, object]:
        calls.append((name, arguments))
        return {"remote": name, "arguments": arguments}

    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    proxy = al.MCPGovernanceProxy(lens, transport)
    proxy.register_tool(
        "lookup",
        input_schema={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
    )

    invalid = proxy.call("lookup", {"unexpected": "value"})
    assert invalid.status == "FAILED"
    assert invalid.error_taxonomy == "ValidationError"
    assert calls == []

    response = proxy.handle_request(
        {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "lookup", "arguments": {"query": "x"}}}
    )
    payload = json.loads(response["result"]["content"][0]["text"])
    assert response["id"] == 7
    assert payload["status"] == "SUCCESS"
    assert calls == [("lookup", {"query": "x"})]


def test_mcp_approval_can_modify_schema_validated_dynamic_arguments(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def resolver(ticket: al.ApprovalTicket, _: al.ToolCallContext) -> dict[str, object]:
        assert ticket.safe_args == {"query": "before"}
        return {
            "action": "APPROVE",
            "approved_by": "operator",
            "modified_args": {"query": "after"},
        }

    def transport(_: str, arguments: dict[str, object]) -> dict[str, object]:
        calls.append(arguments)
        return {"accepted": True}

    lens = al.ActionLens(
        storage_dir=tmp_path,
        sink=MemorySink(),
        approval_resolver=resolver,
    )
    proxy = al.MCPGovernanceProxy(lens, transport)
    proxy.register_tool(
        "update",
        risk=al.RiskLevel.MUTATION,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
        input_schema={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
    )

    output = proxy.call(
        "update", {"query": "before", "idempotency_key": "mcp-update-1"}
    )
    assert output.status == "SUCCESS"
    assert calls == [{"query": "after"}]


def test_remote_tool_runner_is_executed_through_governed_runtime(tmp_path: Path) -> None:
    class Runner:
        request: al.RemoteToolRequest | None = None

        def submit(self, request: al.RemoteToolRequest) -> al.RemoteJobRef:
            self.request = request
            return al.RemoteJobRef(job_id="job-1", submitted_at=datetime.now(timezone.utc))

        def status(self, _: al.RemoteJobRef) -> al.RemoteJobStatus:
            return al.RemoteJobStatus(status="COMPLETED")

        def result(self, _: al.RemoteJobRef) -> al.StructuredToolOutput:
            return al.StructuredToolOutput(
                status="SUCCESS",
                result_summary="remote completed",
                result={"answer": "ok"},
                artifact_refs=[
                    al.ArtifactRef(
                        uri="https://artifacts.example.test/job-1",
                        media_type="text/plain",
                        size_bytes=10,
                        sha256="a" * 64,
                        preview="remote-secret-preview",
                    )
                ],
            )

        def cancel(self, _: al.RemoteJobRef) -> al.CancelResult:
            return al.CancelResult(
                accepted=True,
                status=al.RemoteJobStatus(status="CANCELLED"),
            )

    runner = Runner()
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    remote_lookup = lens.remote_tool(
        runner,
        name="remote_lookup",
        schema_fingerprint={"type": "object", "properties": {"query": {"type": "string"}}},
    )

    output = remote_lookup(query="status")
    assert output.status == "SUCCESS"
    assert output.result == {"answer": "ok"}
    assert output.governance["delegated"] is True
    assert output.governance["delegated_artifact_previews_withheld"] is True
    assert output.artifact_refs[0].preview is None
    assert runner.request is not None
    assert runner.request.args == {"query": "status"}
    assert runner.request.idempotency_key
    assert runner.request.args_hash
    assert runner.request.tool_schema_hash


def test_invoke_many_parallel_tools_preserve_session_context(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(storage_dir=tmp_path, sink=sink)

    @lens.tool(concurrency=al.ConcurrencyPolicy.SAFE)
    def read_value(value: int) -> int:
        return value

    with lens.session(session_id="parallel-session", run_id="parallel-run"):
        outputs = lens.invoke_many(
            [
                (read_value, (1,), {}),
                (read_value, (2,), {}),
            ]
        )

    assert [output.result for output in outputs] == [1, 2]
    completed = [event for event in sink.events if event.event_type == "tool_call.completed"]
    assert len(completed) == 2
    assert len({event.call_id for event in completed}) == 2
    for event in completed:
        assert event.session_id == "parallel-session"
        assert event.run_id == "parallel-run"


def test_legacy_memory_ledger_detects_args_conflict() -> None:
    ledger = al.MemoryLedger()
    spec = al.ToolSpec(name="mutate")
    context = al.ToolCallContext(
        project="p", session_id="s", run_id="r", call_id="c", tool_name="mutate"
    )
    ledger.begin(
        "key",
        call_id="c1",
        context=context,
        spec=spec,
        args_hash="hash-a",
        tool_schema_hash="schema",
    )
    kind, record = ledger.begin(
        "key",
        call_id="c2",
        context=context,
        spec=spec,
        args_hash="hash-b",
        tool_schema_hash="schema",
    )
    assert kind == "conflict"
    assert record.args_hash == "hash-a"


def test_legacy_sqlite_ledger_detects_args_conflict(tmp_path: Path) -> None:
    ledger = al.SQLiteLedger(tmp_path / "conflict.sqlite3")
    spec = al.ToolSpec(name="mutate")
    context = al.ToolCallContext(
        project="p", session_id="s", run_id="r", call_id="c", tool_name="mutate"
    )
    ledger.begin(
        "key",
        call_id="c1",
        context=context,
        spec=spec,
        args_hash="hash-a",
        tool_schema_hash="schema",
    )
    kind, record = ledger.begin(
        "key",
        call_id="c2",
        context=context,
        spec=spec,
        args_hash="hash-b",
        tool_schema_hash="schema",
    )
    assert kind == "conflict"


def test_custom_legacy_ledger_without_hash_parameters_remains_compatible(
    tmp_path: Path,
) -> None:
    class CustomLegacyLedger(al.MemoryLedger):
        def begin(self, key, *, call_id, context=None, spec=None):
            return super().begin(
                key,
                call_id=call_id,
                context=context,
                spec=spec,
            )

    calls = 0
    lens = al.ActionLens(storage_dir=tmp_path, ledger=CustomLegacyLedger())

    @lens.tool(idempotency=al.IdempotencyPolicy.AUTO_HASH)
    def lookup(value: str) -> str:
        nonlocal calls
        calls += 1
        return value

    assert lookup("same").status == "SUCCESS"
    assert lookup("same").status == "SUCCESS"
    assert calls == 1


def test_webhook_sink_caches_and_pins_validated_dns_addresses(monkeypatch) -> None:
    call_count = 0

    def counting_getaddrinfo(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("93.184.216.34", 443),
            )
        ]

    sink = wh.WebhookSink(
        "https://example.com/hook",
        secret=lambda: "test-secret",
        dns_cache_ttl_sec=300,
    )
    monkeypatch.setattr(socket, "getaddrinfo", counting_getaddrinfo)

    first = sink._resolved_addresses()
    second = sink._resolved_addresses()

    assert call_count == 1
    assert first == second
    assert first[0].sockaddr == ("93.184.216.34", 443)


def test_webhook_pinned_connection_uses_validated_ip_with_domain_sni(
    monkeypatch,
) -> None:
    connected: list[tuple[str, int]] = []
    server_names: list[str] = []

    class FakeSocket:
        def settimeout(self, _timeout) -> None: ...

        def connect(self, address) -> None:
            connected.append(address)

        def close(self) -> None: ...

    class FakeContext:
        verify_mode = wh.ssl.CERT_REQUIRED
        check_hostname = True

        def wrap_socket(self, sock, *, server_hostname):
            server_names.append(server_hostname)
            return sock

    monkeypatch.setattr(wh.socket, "socket", lambda *_args: FakeSocket())
    monkeypatch.setattr(wh.ssl, "create_default_context", lambda: FakeContext())
    address = wh._ResolvedAddress(
        socket.AF_INET,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        ("93.184.216.34", 443),
    )
    connection = wh._PinnedHTTPSConnection(
        "example.com",
        port=443,
        timeout=5,
        resolved_address=address,
    )

    connection.connect()

    assert connected == [("93.184.216.34", 443)]
    assert server_names == ["example.com"]


def test_webhook_pinned_response_preserves_retry_after(monkeypatch) -> None:
    class Response:
        status = 429
        headers = {"Retry-After": "7"}

        def close(self) -> None: ...

    sink = wh.WebhookSink(
        "https://example.com/hook",
        secret=lambda: "test-secret",
    )
    monkeypatch.setattr(sink, "_open_pinned", lambda request, *, timeout: Response())

    with pytest.raises(wh.WebhookDeliveryError) as exc_info:
        sink.emit(
            al.TrajectoryEvent(
                event_id="retry-after-event",
                timestamp=datetime.now(timezone.utc),
                project="test",
                session_id="session",
                run_id="run",
                sequence="1",
                event_type="tool_call.completed",
                phase="POST_FLIGHT",
            )
        )

    assert exc_info.value.retry_after == 7


def test_remote_tool_timeout_cancels_and_marks_high_risk_result_uncertain(tmp_path: Path) -> None:
    class RunningRunner:
        cancelled = False

        def submit(self, _: al.RemoteToolRequest) -> al.RemoteJobRef:
            return al.RemoteJobRef(job_id="job-timeout", submitted_at=datetime.now(timezone.utc))

        def status(self, _: al.RemoteJobRef) -> al.RemoteJobStatus:
            return al.RemoteJobStatus(status="RUNNING")

        def result(self, _: al.RemoteJobRef) -> al.StructuredToolOutput:
            raise AssertionError("result must not be requested before completion")

        def cancel(self, _: al.RemoteJobRef) -> al.CancelResult:
            self.cancelled = True
            return al.CancelResult(
                accepted=True,
                status=al.RemoteJobStatus(status="CANCELLED"),
            )

    runner = RunningRunner()
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    remote_mutation = lens.remote_tool(
        runner,
        name="remote_mutation",
        risk=al.RiskLevel.MUTATION,
        timeout_sec=0.01,
        poll_interval_sec=0.001,
    )

    output = remote_mutation(value="x")
    assert output.status == "UNCERTAIN"
    assert output.error_taxonomy == "Timeout"
    assert runner.cancelled is True
