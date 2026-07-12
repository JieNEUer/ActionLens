from __future__ import annotations

import inspect
import importlib.util
import json
import os
import subprocess
import sys
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import actionlens as al
from actionlens.artifacts import FileArtifactStore
from actionlens.integrations.openai_agents import as_openai_agents_tool, wrap_openai_agent_tool
from actionlens.integrations.pydantic_ai import as_pydantic_ai_tool, wrap_pydantic_ai_tool
from actionlens.models import ApprovalTicket, RiskLevel, ToolSpec, TrajectoryEvent
from actionlens.repositories.postgres import POSTGRES_MIGRATION_SQL
from actionlens.sinks import MemorySink, MetricsSink, OpenTelemetrySink, WebhookDeliveryError, WebhookSink
from actionlens.exporters import export_sft
from actionlens.schema import read_event


def _context(tool: str = "mutate") -> al.ToolCallContext:
    return al.ToolCallContext(
        project="demo", session_id="s1", run_id="r1", call_id="c1", tool_name=tool
    )


def _event(event_id: str = "e1") -> TrajectoryEvent:
    return TrajectoryEvent(
        event_id=event_id, timestamp=datetime.now(timezone.utc), project="demo",
        session_id="s1", run_id="r1", sequence=1, event_type="tool_call.completed",
        phase="POST_FLIGHT", call_id="c1", tool_name="mutate",
    )


def test_same_key_different_args_is_conflict(tmp_path: Path) -> None:
    calls = 0
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(idempotency=al.IdempotencyPolicy.REQUIRED, risk=al.RiskLevel.MUTATION)
    def mutate(value: str) -> str:
        nonlocal calls
        calls += 1
        return value

    first = mutate("a", idempotency_key="shared")
    conflict = mutate("b", idempotency_key="shared")
    assert first.status == "SUCCESS"
    assert conflict.status == "DENIED"
    assert conflict.error_taxonomy == "IdempotencyConflict"
    assert calls == 1


def test_expired_lease_without_fencing_becomes_uncertain(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(tmp_path / "repo.sqlite3")
    spec = ToolSpec(name="mutate", risk=RiskLevel.MUTATION, lease_seconds=-1)
    first_kind, first = repository.begin(
        "key", call_id="c1", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash="s", owner_id="w1", lease_seconds=-1,
    )
    second_kind, second = repository.begin(
        "key", call_id="c2", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash="s", owner_id="w2", lease_seconds=30,
    )
    assert first_kind == "created" and first.fencing_token == 1
    assert second_kind == "uncertain" and second.status == "UNCERTAIN"


def test_fencing_rejects_old_owner_after_takeover(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(tmp_path / "repo.sqlite3")
    spec = ToolSpec(name="mutate", fencing_supported=True)
    _, first = repository.begin(
        "key", call_id="c1", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash="s", owner_id="w1", lease_seconds=-1,
    )
    kind, second = repository.begin(
        "key", call_id="c2", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash="s", owner_id="w2", lease_seconds=30,
    )
    assert kind == "created" and second.fencing_token == first.fencing_token + 1
    with pytest.raises(al.StaleFenceError):
        repository.finish(
            "key", owner_id="w1", fencing_token=first.fencing_token, status="SUCCEEDED",
            output={"status": "SUCCESS", "result_summary": "old"}, error=None, event=_event(),
        )


def test_heartbeat_extends_only_current_owner_lease(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(tmp_path / "repo.sqlite3")
    _, record = repository.begin(
        "key", call_id="c1", context=_context(), spec=ToolSpec(name="mutate"),
        args_hash="a", tool_schema_hash="s", owner_id="owner", lease_seconds=1,
    )
    previous_expiry = record.lease_expires_at
    assert repository.heartbeat(
        "key", owner_id="owner", fencing_token=record.fencing_token, lease_seconds=30
    )
    assert not repository.heartbeat(
        "key", owner_id="stale", fencing_token=record.fencing_token, lease_seconds=30
    )
    assert repository.get_ledger("key").lease_expires_at > previous_expiry


def test_approval_ticket_ledger_and_outbox_rollback_together(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(tmp_path / "repo.sqlite3")
    ticket = ApprovalTicket(
        ticket_id="ticket-1", idempotency_key="key", call_id="c1", tool_name="mutate",
        safe_args={"value": "x"}, risk=RiskLevel.MUTATION, reason="approval",
    )
    original = repository._insert_outbox

    def fail(*args: object) -> None:
        raise OSError("simulated outbox failure")

    repository._insert_outbox = fail  # type: ignore[method-assign]
    with pytest.raises(OSError):
        repository.create_approval(
            "key", ticket=ticket, context=_context(), spec=ToolSpec(name="mutate"),
            args_hash="a", tool_schema_hash="s", event=_event(),
        )
    repository._insert_outbox = original  # type: ignore[method-assign]
    assert repository.get_ledger("key") is None
    assert repository.get_ticket("ticket-1") is None
    assert repository.claim_outbox(worker_id="w", limit=10, claim_seconds=10) == []


def test_outbox_retries_after_sink_failure(tmp_path: Path) -> None:
    class FlakySink:
        def __init__(self) -> None:
            self.calls = 0
        def emit(self, event: TrajectoryEvent) -> None:
            self.calls += 1
            if self.calls == 1:
                raise OSError("offline")

    repository = al.SQLiteGovernanceRepository(tmp_path / "repo.sqlite3")
    spec = ToolSpec(name="mutate")
    _, record = repository.begin(
        "key", call_id="c1", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash="s", owner_id="w", lease_seconds=30,
    )
    repository.finish(
        "key", owner_id="w", fencing_token=record.fencing_token, status="SUCCEEDED",
        output={"status": "SUCCESS", "result_summary": "ok"}, error=None, event=_event(),
    )
    sink = FlakySink()
    dispatcher = al.OutboxDispatcher(repository, sink, base_retry_seconds=0)
    assert dispatcher.dispatch_once()["failed"] == 1
    assert dispatcher.dispatch_once()["delivered"] == 1


def test_outbox_moves_poison_event_to_dead_letter(tmp_path: Path) -> None:
    class BrokenSink:
        def emit(self, event: TrajectoryEvent) -> None:
            raise OSError("permanent")

    repository = al.SQLiteGovernanceRepository(tmp_path / "repo.sqlite3")
    _, record = repository.begin(
        "key", call_id="c1", context=_context(), spec=ToolSpec(name="mutate"),
        args_hash="a", tool_schema_hash="s", owner_id="w", lease_seconds=30,
    )
    repository.finish(
        "key", owner_id="w", fencing_token=record.fencing_token, status="SUCCEEDED",
        output={"status": "SUCCESS", "result_summary": "ok"}, error=None, event=_event(),
    )
    result = al.OutboxDispatcher(repository, BrokenSink(), max_attempts=1).dispatch_once()
    assert result["dead_lettered"] == 1
    assert repository.claim_outbox(worker_id="new", limit=10, claim_seconds=10) == []


def test_outbox_does_not_ack_sink_that_swallows_error(tmp_path: Path) -> None:
    class SoftFailSink:
        def __init__(self) -> None: self.error_count = 0
        def emit(self, event: TrajectoryEvent) -> None: self.error_count += 1

    repository = al.SQLiteGovernanceRepository(tmp_path / "repo.sqlite3")
    _, record = repository.begin(
        "key", call_id="c1", context=_context(), spec=ToolSpec(name="mutate"),
        args_hash="a", tool_schema_hash="s", owner_id="w", lease_seconds=30,
    )
    repository.finish(
        "key", owner_id="w", fencing_token=record.fencing_token, status="SUCCEEDED",
        output={"status": "SUCCESS", "result_summary": "ok"}, error=None, event=_event(),
    )
    result = al.OutboxDispatcher(repository, SoftFailSink(), max_attempts=1).dispatch_once()
    assert result["dead_lettered"] == 1


def test_redact_then_store_never_writes_raw_secret(tmp_path: Path) -> None:
    lens = al.ActionLens(
        storage_dir=tmp_path, sink=MemorySink(),
        artifact_policy=al.ArtifactPolicy(raw_mode="redact_then_store"),
    )

    @lens.tool(max_bytes=8)
    def secret() -> dict[str, str]:
        return {"password": "never-write-this"}

    output = secret()
    data = Path(output.artifact_refs[0].uri).read_text(encoding="utf-8")
    assert "never-write-this" not in data
    assert "[REDACTED]" in data
    assert not any("never-write-this" in path.read_text(encoding="utf-8", errors="ignore")
                   for path in (tmp_path / "artifacts").rglob("*") if path.is_file())


def test_artifact_atomic_write_cleans_temporary_file_on_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FileArtifactStore(tmp_path)

    def fail_replace(source: object, target: object) -> None:
        raise OSError("simulated crash before rename")

    monkeypatch.setattr("actionlens.artifacts.fs.os.replace", fail_replace)
    with pytest.raises(OSError):
        store.put("payload")
    assert not any(path.is_file() for path in (tmp_path / "artifacts").rglob("*"))


def test_reference_only_rejects_raw_and_accepts_ref(tmp_path: Path) -> None:
    lens = al.ActionLens(
        storage_dir=tmp_path, sink=MemorySink(),
        artifact_policy=al.ArtifactPolicy(raw_mode="reference_only"),
    )

    @lens.tool(max_bytes=2)
    def raw() -> str: return "too large"

    @lens.tool()
    def reference() -> al.ArtifactRef:
        return al.ArtifactRef(uri="s3://bucket/key", media_type="text/plain", size_bytes=4, sha256="abcd")

    assert raw().error_taxonomy == "ArtifactPolicyDenied"
    assert reference().artifact_refs[0].uri == "s3://bucket/key"


def test_encryption_provider_receives_plaintext_without_key_in_ref(tmp_path: Path) -> None:
    class ReverseEncryption:
        provider_id = "test-kms"
        def encrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload[::-1]

    store = FileArtifactStore(
        tmp_path, policy=al.ArtifactPolicy(encryption="provider"),
        encryption_provider=ReverseEncryption(),
    )
    ref = store.put("secret", metadata={"run_id": "r1"})
    assert Path(ref.uri).read_bytes() == b"terces"
    assert ref.confidentiality["provider_id"] == "test-kms"


def test_artifact_checksum_mismatch_is_reported_and_audited(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(storage_dir=tmp_path, sink=sink)

    @lens.tool(max_bytes=2)
    def large() -> str: return "payload"

    ref = large().artifact_refs[0]
    Path(ref.uri).write_text("tampered", encoding="utf-8")
    result = lens.inspect_artifact(ref)
    assert result["status"] == "checksum_mismatch"
    event = next(item for item in sink.events if item.event_type == "artifact.accessed")
    assert event.metadata["status"] == "checksum_mismatch"
    assert "?" not in event.metadata["uri"]


def test_webhook_hmac_and_retry_classification() -> None:
    requests: list[object] = []
    def success(request: object, *, timeout: float) -> SimpleNamespace:
        requests.append(request)
        return SimpleNamespace(status=204, close=lambda: None)
    sink = WebhookSink("https://example.invalid/hook", secret=lambda: "hidden", opener=success)
    sink.emit(_event())
    request = requests[0]
    assert getattr(request, "headers")["X-actionlens-event-id"] == "e1"
    assert "hidden" not in repr(sink.__dict__)

    def rate_limited(request: object, *, timeout: float) -> object:
        raise urllib.error.HTTPError("url", 429, "slow", {}, None)
    with pytest.raises(WebhookDeliveryError):
        WebhookSink("https://example.invalid", secret=lambda: b"x", opener=rate_limited).emit(_event())

    def timeout(request: object, *, timeout: float) -> object:
        raise TimeoutError("slow")
    with pytest.raises(WebhookDeliveryError):
        WebhookSink("https://example.invalid", secret=lambda: b"x", opener=timeout).emit(_event())


def test_otel_export_failure_isolated() -> None:
    class BrokenTracer:
        def start_span(self, name: str) -> object:
            raise RuntimeError("exporter broken")
    sink = OpenTelemetrySink(BrokenTracer())
    started = _event().model_copy(update={"event_type": "tool_call.started"})
    sink.emit(started)
    assert sink.error_count == 1


def test_metrics_labels_exclude_high_cardinality_context() -> None:
    sink = MetricsSink()
    sink.emit(_event())
    labels = next(iter(sink.tool_calls))
    assert labels == ("mutate", "success")
    assert all(value not in labels for value in ("s1", "r1", "c1", "demo"))


def test_core_import_does_not_load_optional_frameworks() -> None:
    """Core import must not crash; asserts optional frameworks stay unloaded only when absent."""
    has_pydantic_ai = importlib.util.find_spec("pydantic_ai") is not None
    has_agents = importlib.util.find_spec("agents") is not None
    has_langchain = importlib.util.find_spec("langchain_core") is not None
    checks = []
    if not has_pydantic_ai:
        checks.append("assert 'pydantic_ai' not in sys.modules, 'pydantic_ai leaked into core import'")
    if not has_agents:
        checks.append("assert 'agents' not in sys.modules, 'agents leaked into core import'")
    if not has_langchain:
        checks.append("assert 'langchain_core' not in sys.modules, 'langchain_core leaked into core import'")
    script = "import sys; import actionlens; " + "; ".join(checks)
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    subprocess.run([sys.executable, "-c", script], check=True, env=environment)


def test_remote_runner_protocol_and_unknown_state() -> None:
    class Runner:
        def submit(self, request: al.RemoteToolRequest) -> al.RemoteJobRef:
            return al.RemoteJobRef(job_id=request.idempotency_key, submitted_at=datetime.now(timezone.utc))
        def status(self, job: al.RemoteJobRef) -> al.RemoteJobStatus:
            return al.RemoteJobStatus(status="UNKNOWN")
        def result(self, job: al.RemoteJobRef) -> al.StructuredToolOutput:
            return al.StructuredToolOutput(status="UNCERTAIN", result_summary="unknown")
        def cancel(self, job: al.RemoteJobRef) -> al.CancelResult:
            return al.CancelResult(accepted=False, status=self.status(job))

    request = al.RemoteToolRequest(
        call_id="c1", idempotency_key="key", args={}, args_hash="a",
        tool_schema_hash="s", context=_context(),
    )
    runner = Runner()
    first = runner.submit(request)
    second = runner.submit(request)
    assert isinstance(runner, al.RemoteToolRunner)
    assert first.job_id == second.job_id == "key"
    assert runner.result(first).status == "UNCERTAIN"


def test_native_sdk_objects_use_real_installed_frameworks(tmp_path: Path) -> None:
    pytest.importorskip("pydantic_ai")
    pytest.importorskip("agents")
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(idempotency=al.IdempotencyPolicy.REQUIRED)
    def mutate(value: int) -> int: return value

    pydantic_tool = as_pydantic_ai_tool(wrap_pydantic_ai_tool(mutate))
    openai_tool = as_openai_agents_tool(wrap_openai_agent_tool(mutate))
    assert type(pydantic_tool).__module__.startswith("pydantic_ai")
    assert type(openai_tool).__module__.startswith("agents")
    assert "idempotency_key" in openai_tool.params_json_schema["required"]
    assert "__al_ctx" not in inspect.signature(pydantic_tool.function).parameters


def test_postgres_is_lazy_optional_and_migration_has_atomic_tables() -> None:
    assert "actionlens_governance_ledger" in POSTGRES_MIGRATION_SQL
    assert "actionlens_approval_tickets" in POSTGRES_MIGRATION_SQL
    assert "actionlens_outbox" in POSTGRES_MIGRATION_SQL
    if importlib.util.find_spec("psycopg") is None:
        with pytest.raises(ImportError, match="postgres"):
            al.PostgresGovernanceRepository("postgresql://unavailable", auto_migrate=False)
    else:
        repository = al.PostgresGovernanceRepository(
            "postgresql://unavailable", auto_migrate=False, verify_schema=False
        )
        assert repository.dsn == "postgresql://unavailable"


@pytest.mark.skipif(not os.getenv("ACTIONLENS_POSTGRES_DSN"), reason="isolated PostgreSQL DSN not configured")
def test_postgres_repository_real_transaction_and_outbox() -> None:
    repository = al.PostgresGovernanceRepository(
        os.environ["ACTIONLENS_POSTGRES_DSN"], auto_migrate=True
    )
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    key = f"integration-{suffix}"
    spec = ToolSpec(name="mutate")
    kind, record = repository.begin(
        key, call_id="c1", context=_context(), spec=spec, args_hash="args",
        tool_schema_hash="schema", owner_id="worker", lease_seconds=30,
    )
    assert kind == "created"
    repository.finish(
        key, owner_id="worker", fencing_token=record.fencing_token, status="SUCCEEDED",
        output={"status": "SUCCESS", "result_summary": "ok"}, error=None,
        event=_event(f"event-{suffix}"),
    )
    claimed = repository.claim_outbox(worker_id="dispatcher", limit=10, claim_seconds=30)
    assert any(item.event.event_id == f"event-{suffix}" for item in claimed)
    conflict, _ = repository.begin(
        key, call_id="c2", context=_context(), spec=spec, args_hash="different",
        tool_schema_hash="schema", owner_id="worker2", lease_seconds=30,
    )
    assert conflict == "conflict"


def test_v01_through_v05_golden_events_are_readable() -> None:
    fixture_dir = Path(__file__).parent / "fixtures" / "schemas"
    events = [read_event(json.loads(path.read_text(encoding="utf-8"))) for path in sorted(fixture_dir.glob("*.json"))]
    assert [event.event_id for event in events] == ["v01", "v02", "v03", "v05", "v10"]


def test_sft_manifest_is_reproducible_and_tracks_sources(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path)

    @lens.tool
    def ping() -> str: return "pong"

    ping()
    output = tmp_path / "dataset.jsonl"
    export_sft(tmp_path, output)
    first = Path(str(output) + ".manifest.json").read_bytes()
    export_sft(tmp_path, output)
    second = Path(str(output) + ".manifest.json").read_bytes()
    manifest = json.loads(second)
    assert first == second
    assert manifest["source_files"] and manifest["output_sha256"]
    assert "source_event_ids" in json.loads(output.read_text(encoding="utf-8"))["metadata"]


def test_expired_ticket_resume_is_terminal_with_repository(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(approval_required=True, approval_ttl_sec=-1)
    def erase() -> str: return "done"

    first = erase()
    second = erase()
    assert first.status == "PENDING_APPROVAL"
    assert second.error_taxonomy == "ApprovalExpired"
    assert lens.repository.get_ledger(first.governance["idempotency_key"]).status == "EXPIRED"


def test_cross_process_invalid_approval_args_terminate_ledger(tmp_path: Path) -> None:
    lens1 = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens1.tool(name="erase", approval_required=True)
    def erase_v1(count: int) -> int: return count

    pending = erase_v1(1)
    lens2 = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    assert lens2.approve(ticket_id=pending.result["ticket_id"], modified_args={"count": "bad"})
    lens3 = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens3.tool(name="erase", approval_required=True)
    def erase_v3(count: int) -> int: return count

    denied = erase_v3(1)
    assert denied.error_taxonomy == "ApprovalArgsInvalid"
    assert lens3.repository.get_ledger(pending.governance["idempotency_key"]).status == "FAILED_TERMINAL"
