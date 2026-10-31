"""Regression contracts for the 1.5.4 diagnostic report (R01–R12)."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Event
from uuid import uuid4

import pytest
from pydantic import BaseModel

import actionlens as al
from actionlens.artifacts import ArtifactAccessDenied
from actionlens.exporters import core
from actionlens.integrations.common import ActionLensToolAdapter
from actionlens.integrations.dbos import DBOSStepRunner
from actionlens.integrations.temporal import TemporalActivityRunner
from actionlens.models import PolicyDecision
from actionlens.repository import canonical_operation_hash
from actionlens.sinks import MemorySink, OpenTelemetrySink
from actionlens.sinks.webhook import verify_webhook_signature


class AllowArtifacts:
    def authorize(self, artifact, *, context):
        return True


@pytest.fixture(params=["sqlite", "postgres"])
def make_lens(request, tmp_path):
    instances = []

    def make(**kwargs):
        storage = tmp_path / str(len(instances))
        repository = (
            al.PostgresGovernanceRepository(
                request.getfixturevalue("isolated_postgres_dsn"), auto_migrate=True
            )
            if request.param == "postgres"
            else al.SQLiteGovernanceRepository(storage / "db.sqlite3")
        )
        lens = al.ActionLens(
            project=uuid4().hex,
            storage_dir=storage,
            repository=repository,
            artifact_authorizer=AllowArtifacts(),
            **kwargs,
        )
        instances.append(lens)
        return lens

    yield make
    for lens in instances:
        lens.close()
        lens.repository.close()


def ctx(lens, **updates):
    values = dict(
        project=lens.project,
        environment="test",
        tenant_id="tenant",
        actor_id="actor",
        session_id="session",
        run_id="run",
        call_id="call",
        tool_name="read",
    )
    return al.ToolCallContext(**(values | updates))


def test_security_identity_survives_every_export_and_legacy_is_separate(
    make_lens, tmp_path
):
    lens = make_lens()

    @lens.tool(output=al.OutputPolicy(include_raw_in_trajectory=True))
    def read(value: str):
        return value

    scopes = [{}, {"tenant_id": "other"}, {"environment": "prod"}, {"project": "other"}]
    for index, scope in enumerate(scopes):
        read(str(index), __al_ctx=ctx(lens, **scope))
    lens.flush()
    events, _ = core.load_events(lens.storage_dir)
    assert len(events) == 8
    assert all(
        event["actor_id"] == "actor" and event["identity_version"] for event in events
    )
    legacy = dict(events[0], event_id="legacy-" + uuid4().hex)
    for key in ("identity_version", "environment", "tenant_id", "actor_id"):
        legacy.pop(key)
    path = next((lens.storage_dir / "trajectories").glob("*.jsonl"))
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(legacy) + "\n")
    candidates, _ = core.build_eval_case_candidates(lens.storage_dir)
    assert len({candidate.case_id for candidate in candidates}) == 5
    assert {candidate.provenance.identity_version for candidate in candidates} == {
        "legacy-unknown",
        "actionlens.identity.v1",
    }
    assert len(core.load_events(lens.storage_dir, tenant_id="tenant")[0]) == 6
    transcript = tmp_path / "transcript.jsonl"
    assert core.export_inspect_ai(lens.storage_dir, transcript)["exported"] == 5
    sft = tmp_path / "sft.jsonl"
    assert core.export_sft(lens.storage_dir, sft)["exported"] == 4
    for row in map(json.loads, sft.read_text(encoding="utf-8").splitlines()):
        args = json.loads(row["messages"][0]["tool_calls"][0]["function"]["arguments"])
        output = json.loads(row["messages"][1]["content"])
        assert args["value"] == output["result"]
        assert row["metadata"]["identity_version"] == "actionlens.identity.v1"


def test_tenant_eval_context_cannot_use_ambiguous_run_shorthand(make_lens):
    lens = make_lens()

    @lens.tool
    def read():
        return "ok"

    read(__al_ctx=ctx(lens))
    lens.flush()
    cases, _ = core.build_eval_case_candidates(
        lens.storage_dir,
        contexts={"run": al.EvalCaseContext(task_input="wrong tenant")},
    )
    assert cases[0].task_input is None
    cases, _ = core.build_eval_case_candidates(
        lens.storage_dir,
        contexts={cases[0].case_id: al.EvalCaseContext(task_input="scoped")},
    )
    assert cases[0].task_input == "scoped"


def test_failing_redactor_withholds_sensitive_audit_and_never_dispatches(make_lens):
    class BrokenRedactor:
        def redact(self, value, context=None):
            raise RuntimeError("synthetic-secret")

    lens = make_lens(redactor=BrokenRedactor())

    @lens.tool
    def read(note: str):
        raise AssertionError("unsafe redaction must stop preflight")

    assert read("synthetic-secret", __al_ctx=ctx(lens)).status == "FAILED"
    lens.flush()
    events, _ = core.load_events(lens.storage_dir)
    assert "synthetic-secret" not in json.dumps(events)


def test_normalization_bounds_cyclic_and_deep_results_without_calling_repr(make_lens):
    class Opaque:
        def __str__(self):
            raise AssertionError("must not stringify opaque objects")

        __repr__ = __str__

    lens = make_lens()
    value = {"opaque": Opaque()}
    value["cycle"] = value

    @lens.tool
    def read():
        return value

    output = read(__al_ctx=ctx(lens))
    assert output.status == "SUCCESS"
    assert output.result == {
        "opaque": "[opaque object]",
        "cycle": "[circular reference]",
    }


@pytest.mark.parametrize("kind", ["dict", "model", "dataclass", "reference"])
def test_equivalent_visible_values_are_redacted_before_serialization(make_lens, kind):
    lens = make_lens()

    class ProviderResult(BaseModel):
        token: str
        message: str

    @dataclass
    class Record:
        token: str
        message: str

    values = {
        "dict": {"token": "synthetic-secret", "message": "synthetic-secret"},
        "model": ProviderResult(token="synthetic-secret", message="synthetic-secret"),
        "dataclass": Record(token="synthetic-secret", message="synthetic-secret"),
        "reference": al.ArtifactRef(
            uri="https://example.invalid/result",
            media_type="text/plain",
            size_bytes=16,
            sha256="digest",
            preview="synthetic-secret",
        ),
    }

    @lens.tool(
        output=al.OutputPolicy(
            redact_patterns=["synthetic-secret"], include_raw_in_trajectory=True
        )
    )
    def read():
        return {"nested": values[kind]} if kind != "reference" else values[kind]

    output = read(__al_ctx=ctx(lens))
    assert output.status == "SUCCESS"
    assert "synthetic-secret" not in output.model_dump_json()
    lens.flush()
    events, _ = core.load_events(lens.storage_dir)
    assert "synthetic-secret" not in json.dumps(events)


def test_contextual_argument_redaction_preserves_execution_credentials(make_lens):
    class ScopedRedactor:
        def redact(self, value, context=None):
            assert context is not None
            if isinstance(value, dict):
                return {key: self.redact(item, context) for key, item in value.items()}
            if isinstance(value, list):
                return [self.redact(item, context) for item in value]
            return (
                value.replace("private-note", "[REDACTED]")
                if isinstance(value, str)
                else value
            )

    lens = make_lens(redactor=ScopedRedactor())
    calls = []

    @lens.tool(approval_required=True, idempotency="REQUIRED")
    def write(note: str):
        calls.append(note)
        return note

    key = uuid4().hex
    pending = write("private-note", idempotency_key=key, __al_ctx=ctx(lens))
    ticket = lens.get_ticket(pending.result["ticket_id"])
    assert ticket.safe_args == {"note": "[REDACTED]"}
    lens.approve(ticket_id=ticket.ticket_id, approved_by="reviewer")
    assert (
        write("private-note", idempotency_key=key, __al_ctx=ctx(lens)).result
        == "[REDACTED]"
    )
    assert calls == ["private-note"]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_stream_redaction_is_independent_of_every_utf8_split(tmp_path, asynchronous):
    text = "前缀 synthetic-secret 尾部"
    payload = text.encode("utf-8")
    lens = al.ActionLens(
        storage_dir=tmp_path,
        artifact_policy=al.ArtifactPolicy(raw_mode="redact_then_store"),
        artifact_authorizer=AllowArtifacts(),
    )
    split = 0

    def chunks():
        yield payload[:split]
        yield b""
        yield payload[split:]

    async def achunks():
        for item in chunks():
            yield item

    tool = lens.tool(
        chunks if not asynchronous else achunks,
        output=al.OutputPolicy(redact_patterns=["synthetic-secret"]),
    )
    try:
        for split in range(len(payload) + 1):
            assert 0 <= split <= len(payload)
            output = (
                asyncio.run(tool(__al_ctx=ctx(lens)))
                if asynchronous
                else tool(__al_ctx=ctx(lens))
            )
            assert output.status == "SUCCESS"
            assert lens.read_artifact(
                output.artifact_refs[0], context=ctx(lens)
            ).decode("utf-8") == text.replace("synthetic-secret", "[REDACTED]")
            assert "synthetic-secret" not in output.model_dump_json()
    finally:
        lens.close()


def test_cache_scope_is_actor_and_permission_sensitive_and_can_explicitly_share(
    make_lens,
):
    lens = make_lens(sink=MemorySink())
    calls = []

    @lens.tool(idempotency="CACHE_READ")
    def read(__al_ctx):
        calls.append(__al_ctx.actor_id)
        return {
            "actor": __al_ctx.actor_id,
            "permission": __al_ctx.authorization_fingerprint,
        }

    first = read(__al_ctx=ctx(lens))
    assert read(__al_ctx=ctx(lens)).result == first.result
    assert read(__al_ctx=ctx(lens, actor_id="other")).result["actor"] == "other"
    assert (
        read(__al_ctx=ctx(lens, authorization_fingerprint="v2")).result["permission"]
        == "v2"
    )
    assert calls == ["actor", "other", "actor"]

    @lens.tool(
        idempotency="CACHE_READ", cache_scope_fn=lambda _args, _context: "tenant-public"
    )
    def shared():
        calls.append("shared")
        return "public"

    shared(__al_ctx=ctx(lens))
    shared(__al_ctx=ctx(lens, actor_id="other"))
    assert calls.count("shared") == 1


def test_cache_hit_rechecks_current_policy_and_rejects_invalid_host_scope(make_lens):
    class Policy:
        revoked = False

        def decide(self, **kwargs):
            return PolicyDecision(
                action="DENY" if self.revoked else "ALLOW", reason="current permissions"
            )

    policy = Policy()
    lens = make_lens(policies=[policy])
    calls = []

    @lens.tool(idempotency="CACHE_READ")
    def read():
        calls.append(True)
        return "private"

    assert read(__al_ctx=ctx(lens)).status == "SUCCESS"
    policy.revoked = True
    assert read(__al_ctx=ctx(lens)).status == "DENIED"
    assert len(calls) == 1
    policy.revoked = False
    read(__al_ctx=ctx(lens, actor_id=None))
    read(__al_ctx=ctx(lens, actor_id=None))
    assert len(calls) == 3

    @lens.tool(idempotency="CACHE_READ", cache_scope_fn=lambda _args, _ctx: "")
    def invalid():
        raise AssertionError("invalid scope cannot dispatch")

    assert invalid(__al_ctx=ctx(lens)).error_taxonomy == "CacheScopeInvalid"


@pytest.mark.parametrize("risk", ["READ", "MUTATION"])
@pytest.mark.parametrize("limit", [32, 4096])
@pytest.mark.parametrize("model", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_mcp_error_fact_precedes_schema_validation_and_output_shaping(
    tmp_path, risk, limit, model, asynchronous
):
    class Response(BaseModel):
        isError: bool = True
        content: list[dict[str, str]]

    response = {
        "isError": True,
        "content": [{"type": "text", "text": "synthetic-secret " * 20}],
    }
    if model:
        response = Response.model_validate(response)
    calls = []

    def transport(_name, _args):
        calls.append(True)
        return response

    async def atransport(name, args):
        return transport(name, args)

    lens = al.ActionLens(storage_dir=tmp_path, artifact_authorizer=AllowArtifacts())
    proxy = al.MCPGovernanceProxy(
        lens, atransport if asynchronous else transport, asynchronous=asynchronous
    )
    tool = proxy.register_tool(
        "write",
        risk=risk,
        idempotency="REQUIRED",
        max_bytes=limit,
        output_schema={"type": "object", "required": ["success_only"]},
        output=al.OutputPolicy(redact_patterns=["synthetic-secret"]),
    )
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "write", "arguments": {"idempotency_key": "mcp-error"}},
    }
    try:
        response = (
            asyncio.run(proxy.ahandle_request(request))
            if asynchronous
            else proxy.handle_request(request)
        )
        assert response["result"]["isError"] is True
        assert "synthetic-secret" not in json.dumps(response)
        governed = al.StructuredToolOutput.model_validate_json(
            response["result"]["content"][0]["text"]
        )
        for ref in governed.artifact_refs:
            assert b"synthetic-secret" not in lens.read_artifact(ref)
        record = lens.ledger.get("mcp-error")
        assert record.status == (
            "UNCERTAIN" if risk == "MUTATION" else "FAILED_RETRYABLE"
        )
        assert record.status != "SUCCEEDED"
        if risk == "MUTATION":
            output = tool.actionlens_runtime.invoke(idempotency_key="mcp-error")
            assert output.status == "UNCERTAIN"
            assert len(calls) == 1
        lens.flush()
        assert core.export_sft(tmp_path, tmp_path / "sft.jsonl")["exported"] == 0
    finally:
        lens.close()


def test_remote_effective_hash_and_original_conflict_identity_are_independent(
    make_lens,
):
    class Runner:
        requests = []

        def submit(self, request):
            assert (
                request.args_hash
                == request.execution_args_hash
                == canonical_operation_hash(request.args)
            )
            self.requests.append(request)
            return al.RemoteJobRef(
                job_id="job", submitted_at=datetime.now(timezone.utc)
            )

        def status(self, job):
            return al.RemoteJobStatus(status="COMPLETED")

        def result(self, job):
            return al.StructuredToolOutput(
                status="SUCCESS", result_summary="ok", result="ok"
            )

        def cancel(self, job):
            raise AssertionError("unexpected cancellation")

    lens = make_lens()
    runner = Runner()
    remote = lens.remote_tool(
        runner,
        name="charge",
        risk="MUTATION",
        idempotency="REQUIRED",
        approval_required=True,
    )
    key = uuid4().hex
    pending = remote(amount=10, idempotency_key=key, __al_ctx=ctx(lens))
    lens.approve(
        ticket_id=pending.result["ticket_id"],
        approved_by="reviewer",
        modified_args={"amount": 20},
    )
    assert (
        remote(amount=10, idempotency_key=key, __al_ctx=ctx(lens)).status == "SUCCESS"
    )
    assert (
        remote(amount=10, idempotency_key=key, __al_ctx=ctx(lens)).status == "SUCCESS"
    )
    assert remote(amount=30, idempotency_key=key, __al_ctx=ctx(lens)).status == "DENIED"
    assert len(runner.requests) == 1
    assert runner.requests[0].args == {"amount": 20}
    assert runner.requests[0].requested_args_hash == canonical_operation_hash(
        {"amount": 10}
    )
    assert lens.ledger.get(key).args_hash == runner.requests[0].requested_args_hash
    assert (
        lens.ledger.get(key).output["governance"]["execution_args_hash"]
        == runner.requests[0].args_hash
    )
    completed = [
        row.event
        for row in lens.repository.list_outbox(limit=10000)
        if row.event.event_type == "tool_call.completed"
        and row.event.project == lens.project
    ]
    assert (
        completed[0].metadata["output"]["governance"]["execution_args_hash"]
        == runner.requests[0].args_hash
    )


@pytest.mark.parametrize("operation", ["dispatch", "decision", "lease"])
@pytest.mark.parametrize("wait", ["lock", "pool"])
def test_repository_time_is_sampled_after_connection_and_row_lock_waits(
    make_lens, monkeypatch, operation, wait
):
    lens = make_lens(sink=MemorySink())
    key = uuid4().hex

    @lens.tool(approval_required=True, idempotency="REQUIRED", approval_ttl_sec=60)
    def write():
        raise AssertionError("repository probe must not execute the handler")

    pending = write(idempotency_key=key, __al_ctx=ctx(lens))
    ticket_id = pending.result["ticket_id"]
    if operation != "decision":
        lens.approve(ticket_id=ticket_id, approved_by="reviewer")
    record = lens.ledger.get(key)
    repository = lens.repository
    postgres = isinstance(repository, al.PostgresGovernanceRepository)
    module = __import__(type(repository).__module__, fromlist=["_now"])
    now = datetime.now(timezone.utc)
    clock = [now]
    monkeypatch.setattr(module, "_now", lambda: clock[0])
    reached, release = Event(), Event()
    connection_name = "_connection" if postgres else "_connect"
    connect = getattr(repository, connection_name)

    @contextmanager
    def tracked_connection():
        if wait == "pool":
            reached.set()
            assert release.wait(2)
        with connect() as connection:
            if wait == "lock":
                reached.set()
            yield connection

    blocker = None
    if wait == "lock":
        if postgres:
            import psycopg

            blocker = psycopg.connect(repository.dsn)
            blocker.execute(
                "SELECT key FROM actionlens_governance_ledger WHERE key=%s FOR UPDATE",
                (key,),
            )
        else:
            blocker = sqlite3.connect(repository.path)
            blocker.execute("BEGIN IMMEDIATE")
    monkeypatch.setattr(repository, connection_name, tracked_connection)

    def execute():
        if operation == "decision":
            event = lens._make_event(
                context=ctx(lens), event_type="approval.approved", phase="PRE_FLIGHT"
            )
            return repository.decide_approval(
                ticket_id,
                status="APPROVED",
                approved_by="reviewer",
                decision_note=None,
                modified_args=None,
                event=event,
            )
        return repository.begin(
            key,
            call_id="new-call",
            context=ctx(lens),
            spec=write.actionlens_spec,
            args_hash=record.args_hash,
            tool_schema_hash=record.tool_schema_hash,
            owner_id="worker",
            lease_seconds=30,
        )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(execute)
            assert reached.wait(2)
            clock[0] = now + timedelta(seconds=5 if operation == "lease" else 120)
            if blocker is not None:
                blocker.commit()
            release.set()
            result = future.result(timeout=3)
        if operation == "lease":
            assert result[0] == "created"
            assert result[1].lease_expires_at == clock[0] + timedelta(seconds=30)
        else:
            assert result is None if operation == "decision" else result[0] == "hit"
            assert repository.get_ledger(key).status == "EXPIRED"
            expired = [
                row
                for row in repository.list_outbox(limit=10_000)
                if row.event.event_type == "approval.expired"
                and row.event.metadata.get("ticket_id") == ticket_id
            ]
            assert len(expired) == 1
            repository.expire_ticket(ticket_id)
            assert (
                len(
                    [
                        row
                        for row in repository.list_outbox(limit=10_000)
                        if row.event.event_id == expired[0].event.event_id
                    ]
                )
                == 1
            )
    finally:
        release.set()
        if blocker is not None:
            blocker.close()


@pytest.mark.parametrize(
    "failure", ["claim", "sink", "ack", "ack_false", "finish_before", "finish_receipt"]
)
def test_commit_and_delivery_failures_report_distinct_facts(
    make_lens, monkeypatch, failure
):
    lens = make_lens(sink=MemorySink())
    key = uuid4().hex
    calls = []

    @lens.tool(risk="MUTATION", idempotency="REQUIRED")
    def write():
        calls.append(True)
        return "confirmed"

    def fail(*args, **kwargs):
        raise ConnectionError("synthetic outage")

    if failure == "claim":
        monkeypatch.setattr(lens.repository, "claim_outbox", fail)
    elif failure == "sink":
        monkeypatch.setattr(lens.sink, "emit", fail)
    elif failure.startswith("ack"):
        monkeypatch.setattr(
            lens.repository,
            "ack_outbox",
            (lambda *a, **k: False) if failure == "ack_false" else fail,
        )
    else:
        finish = lens.repository.finish

        def broken_finish(*args, **kwargs):
            if failure == "finish_receipt":
                finish(*args, **kwargs)
            fail()

        monkeypatch.setattr(lens.repository, "finish", broken_finish)
    # Observation failures are non-fatal; the sink delivery fault is injected
    # only after dispatch starts, to test a confirmed business commit.
    output = write(idempotency_key=key, __al_ctx=ctx(lens))
    if failure.startswith("finish"):
        assert output.status == "UNCERTAIN"
        assert lens.ledger.get(key).status == (
            "SUCCEEDED" if failure == "finish_receipt" else "EXECUTING"
        )
    else:
        assert output.status == "SUCCESS"
        assert output.governance["audit_delivery_pending"] is True
        assert lens.ledger.get(key).status == "SUCCEEDED"
        assert write(idempotency_key=key, __al_ctx=ctx(lens)).status == "SUCCESS"
    assert calls == [True]


def test_webhook_authenticated_identity_precedes_deduplication():
    payload = b'{"event_id":"one"}'
    timestamp = str(int(time.time()))
    signature = hmac.new(
        b"secret", timestamp.encode() + b"." + payload, hashlib.sha256
    ).hexdigest()
    headers = {
        "X-ActionLens-Timestamp": timestamp,
        "X-ActionLens-Event-Id": "one",
        "X-ActionLens-Key-Id": "rotated",
        "X-ActionLens-Signature": "sha256=" + signature,
    }
    seen = set()

    def check_and_mark(event_id):
        duplicate = event_id in seen
        seen.add(event_id)
        return duplicate

    def verify(values):
        return verify_webhook_signature(
            payload, values, secrets={"rotated": "secret"}, seen_event=check_and_mark
        )

    assert not verify(headers | {"X-ActionLens-Signature": "invalid"})
    assert not seen
    assert not verify(headers | {"X-ActionLens-Event-Id": "swapped"})
    assert not seen
    assert verify(headers)
    assert not verify(headers)


@pytest.mark.parametrize(
    "exporter",
    [
        "export_eval_candidates",
        "export_inspect_samples",
        "export_sft",
        "export_evidence_bundle",
    ],
)
def test_all_export_manifests_bind_consumed_source_snapshot(
    make_lens, tmp_path, monkeypatch, exporter
):
    lens = make_lens()

    @lens.tool(output=al.OutputPolicy(include_raw_in_trajectory=True))
    def read(value: str):
        return value

    read("first", __al_ctx=ctx(lens))
    lens.flush()
    source = next((lens.storage_dir / "trajectories").glob("*.jsonl"))
    before = source.read_bytes()
    load = core.load_events

    def load_then_append(*args, **kwargs):
        result = load(*args, **kwargs)
        read("later", __al_ctx=ctx(lens, call_id=uuid4().hex))
        lens.flush()
        return result

    monkeypatch.setattr(core, "load_events", load_then_append)
    target = tmp_path / "export"
    getattr(core, exporter)(lens.storage_dir, target, tenant_id="tenant")
    manifest_path = (
        target / "manifest.json"
        if exporter == "export_evidence_bundle"
        else tmp_path / "export.manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipt = manifest["source_files"][0]
    assert receipt["sha256"] == hashlib.sha256(before).hexdigest()
    assert receipt["size_bytes"] == len(before) < source.stat().st_size
    assert manifest["filters"]["tenant_id"] == "tenant"
    assert manifest["source_validation"]["events"] == 2


def test_html_summary_and_timeline_share_one_reader_snapshot(
    make_lens, tmp_path, monkeypatch
):
    from actionlens.exporters import html

    lens = make_lens()

    @lens.tool
    def read():
        return "ok"

    read(__al_ctx=ctx(lens))
    lens.flush()
    load = core.load_events
    calls = []

    def tracked(*args, **kwargs):
        calls.append(True)
        return load(*args, **kwargs)

    def unexpected(*args, **kwargs):
        raise AssertionError("HTML summary cannot reread the changing source")

    monkeypatch.setattr(html, "load_events", tracked)
    monkeypatch.setattr(core, "load_events", unexpected)
    assert (
        html.render_html_report(lens.storage_dir, tmp_path / "report.html")["events"]
        == 2
    )
    assert calls == [True]


@pytest.mark.parametrize("encrypted", [False, True])
def test_exported_artifact_identity_round_trips_with_original_authorization(
    make_lens, tmp_path, encrypted
):
    class Cipher:
        provider_id = "test-only-cipher"

        def encrypt(self, payload, *, context):
            return payload[::-1]

        def decrypt(self, payload, *, context):
            return payload[::-1]

    lens = make_lens(
        artifact_policy=al.ArtifactPolicy(
            encryption="provider" if encrypted else "none"
        ),
        encryption_provider=Cipher() if encrypted else None,
    )

    @lens.tool(max_bytes=8)
    def read():
        return "large result" * 30

    output = read(__al_ctx=ctx(lens))
    lens.flush()
    bundle = tmp_path / "bundle"
    core.export_evidence_bundle(lens.storage_dir, bundle)
    rows = [
        json.loads(line)
        for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    ref = al.ArtifactRef.model_validate(
        next(row["output_ref"] for row in rows if row.get("output_ref"))
    )
    assert ref.descriptor_id == output.artifact_refs[0].descriptor_id
    assert ref.access_scope == output.artifact_refs[0].access_scope
    assert lens.read_artifact(ref, context=ctx(lens)) == lens.read_artifact(
        output.artifact_refs[0], context=ctx(lens)
    )
    with pytest.raises(ArtifactAccessDenied):
        lens.read_artifact(ref, context=ctx(lens, tenant_id="other"))


@pytest.mark.parametrize("bridge", ["common", "mcp", "dbos", "temporal", "runtime"])
@pytest.mark.parametrize("cancel", [False, True])
def test_async_bridges_keep_loop_live_and_track_sync_mutation_to_completion(
    make_lens, bridge, cancel
):
    lens = make_lens(sink=MemorySink())
    entered, release = Event(), Event()
    key = uuid4().hex

    def body():
        entered.set()
        assert release.wait(2)
        return "done"

    tool = lens.tool(body, name="write", risk="MUTATION", idempotency="REQUIRED")
    context = ctx(lens, tool_name="write")
    if bridge == "mcp":
        proxy = al.MCPGovernanceProxy(lens, lambda _name, _args: body())
        proxy.register_tool("remote-write", risk="MUTATION", idempotency="REQUIRED")

    async def run():
        if bridge == "common":
            invocation = ActionLensToolAdapter(
                tool, framework="test", output_mode="model"
            ).ainvoke({"idempotency_key": key}, actionlens_context=context)
        elif bridge == "mcp":
            invocation = proxy.acall(
                "remote-write", {"idempotency_key": key}, context=context
            )
        elif bridge == "runtime":
            invocation = tool.actionlens_runtime.ainvoke(
                idempotency_key=key, __al_ctx=context
            )
        else:
            runner = (
                DBOSStepRunner(tool)
                if bridge == "dbos"
                else TemporalActivityRunner(tool)
            )
            invocation = runner.arun(context, idempotency_key=key)
        task = asyncio.create_task(invocation)
        try:
            for _ in range(200):
                if entered.is_set():
                    break
                await asyncio.sleep(0.001)
            assert entered.is_set()
            assert not task.done()
            if cancel:
                task.cancel()
                await asyncio.sleep(0.01)
                assert not task.done()  # cancellation must retain live resources
            release.set()
            if cancel:
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                assert (await task).status == "SUCCESS"
        finally:
            release.set()
            if not task.done():
                await task

    asyncio.run(run())
    assert lens.ledger.get(key).status == "SUCCEEDED"


def test_async_bridge_propagates_ambient_context_with_a_single_timeout_worker(
    make_lens,
):
    lens = make_lens(sync_max_workers=1)

    @lens.tool(run_sync_in_thread=True, timeout_sec=1)
    def read(__al_ctx):
        from actionlens.context import get_current_context

        assert get_current_context().actor_id == __al_ctx.actor_id
        return __al_ctx.actor_id

    adapter = ActionLensToolAdapter(read, framework="test", output_mode="model")

    async def run():
        with lens.session(
            session_id="ambient", actor_id="ambient-actor", tenant_id="tenant"
        ):
            output = await adapter.ainvoke({})
            assert output.status == "SUCCESS"
            assert output.result == "ambient-actor"

    asyncio.run(run())


def test_otel_spans_isolate_tenant_environment_project_and_actor(make_lens):
    class Span:
        ended = False

        def set_attribute(self, *args):
            pass

        def add_event(self, *args, **kwargs):
            pass

        def end(self):
            self.ended = True

    class Tracer:
        spans = []

        def start_span(self, name):
            span = Span()
            self.spans.append(span)
            return span

    tracer = Tracer()
    sink = OpenTelemetrySink(tracer=tracer)
    lens = make_lens(sink=sink)
    scopes = [
        {},
        {"tenant_id": "other"},
        {"environment": "prod"},
        {"project": "other"},
        {"actor_id": "other"},
    ]
    for scope in scopes:
        sink.emit(
            lens._make_event(
                context=ctx(lens, **scope),
                event_type="tool_call.started",
                phase="PRE_FLIGHT",
            )
        )
    assert len(sink._spans) == 5
    assert not any(span.ended for span in tracer.spans)
    for index, scope in enumerate(scopes):
        sink.emit(
            lens._make_event(
                context=ctx(lens, **scope),
                event_type="tool_call.completed",
                phase="POST_FLIGHT",
            )
        )
        assert tracer.spans[index].ended
    assert not sink._spans
