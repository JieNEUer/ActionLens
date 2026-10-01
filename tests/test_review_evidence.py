from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import multiprocessing
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import pytest

import actionlens as al
from actionlens.artifacts import (
    ArtifactAccessDenied,
    ArtifactPolicyError,
    FileArtifactStore,
)
from actionlens.exporters.core import export_evidence_bundle, load_events
from actionlens.integrations.common import ActionLensToolAdapter, signature_json_schema
from actionlens.models import TrajectoryEvent
from actionlens.outbox import OutboxDispatcher
from actionlens.sinks import (
    CompositeSink,
    JsonlSink,
    MemorySink,
    MetricsSink,
    SinkBinding,
)
from actionlens.sinks.webhook import WebhookSink
from actionlens.trajectory import TrajectoryReadStats, iter_trajectory_events


class AllowArtifacts:
    def authorize(self, artifact, *, context):
        return True


def event(event_id="one", **kwargs):
    values = {"event_id": event_id, "timestamp": datetime.now(timezone.utc), "project": "p", "session_id": "s",
              "run_id": "r", "call_id": "c", "tool_name": "tool", "sequence": 1,
              "phase": "POST_FLIGHT", "event_type": "tool_call.completed"}
    return TrajectoryEvent(**(values | kwargs))


def seed_outbox(repository, item):
    context = al.ToolCallContext(project="p", session_id="s", run_id="r", call_id="c", tool_name="tool")
    _, record = repository.begin(item.event_id, call_id="c", context=context, spec=al.ToolSpec(name="tool"),
                                 args_hash="a", tool_schema_hash="s", owner_id="worker", lease_seconds=30)
    repository.finish(item.event_id, owner_id="worker", fencing_token=record.fencing_token, status="SUCCEEDED",
                      output={"status": "SUCCESS", "result_summary": "ok"}, error=None, event=item)


@pytest.mark.parametrize("uri_kind", ["dot_segments", "outside", "file_uri"])
def test_artifact_path_boundary_rejects_traversal_even_with_valid_checksum(tmp_path, uri_kind):
    store = FileArtifactStore(tmp_path / "store", authorizer=AllowArtifacts())
    outside = tmp_path / "synthetic-secret.txt"
    outside.write_bytes(b"synthetic-secret")
    uri = str(store.artifacts_dir / ".." / ".." / outside.name) if uri_kind == "dot_segments" else str(outside)
    if uri_kind == "file_uri":
        uri = outside.as_uri()
    artifact = al.ArtifactRef(uri=uri, sha256=hashlib.sha256(outside.read_bytes()).hexdigest(), size_bytes=16, media_type="text/plain")
    with pytest.raises(ArtifactPolicyError):
        store.read(artifact)


def test_artifact_navigation_schema_hides_internal_identity(tmp_path):
    actors = []

    class Authorizer:
        def authorize(self, artifact, *, context):
            actors.append(context["actor_id"])
            return context["actor_id"] == "allowed"

    with closing(al.ActionLens(storage_dir=tmp_path, sink=MemorySink(), artifact_authorizer=Authorizer())) as lens:
        artifact = lens.artifact_store.put("payload")
        read = lens.artifact_navigation_tools()["artifact_read"]
        assert "__al_ctx" not in str(inspect.signature(read))
        assert "_ActionLens__al_ctx" not in json.dumps(signature_json_schema(read))
        with lens.session(session_id="s", actor_id="blocked"):
            result = read(artifact)
            assert result.status == "FAILED" and actors == ["blocked"]
            assert read(artifact, _ActionLens__al_ctx={"actor_id": "allowed"}).error_taxonomy == "ValidationError"
        assert actors == ["blocked"]


def test_authoritative_descriptors_keep_expiry_crypto_and_tenant_scope(tmp_path):
    store = FileArtifactStore(tmp_path, authorizer=AllowArtifacts(), policy=al.ArtifactPolicy(retention_days=0))
    artifact = store.put("payload", metadata={"project": "p", "tenant_id": "a"})
    forged = artifact.model_copy(update={"expires_at": None, "confidentiality": {}, "size_bytes": 1, "access_scope": {}})
    with pytest.raises(ArtifactAccessDenied, match="expired"):
        store.read(forged, context={"project": "p", "tenant_id": "a"})
    with pytest.raises(ArtifactPolicyError, match="identity"):
        store.read(forged.model_copy(update={"descriptor_id": None}))
    other_store = FileArtifactStore(tmp_path, authorizer=AllowArtifacts())
    other = other_store.put("payload", metadata={"project": "p", "tenant_id": "b"})
    assert artifact.uri == other.uri and artifact.descriptor_id != other.descriptor_id
    assert other_store.read(other, context={"project": "p", "tenant_id": "b"}) == b"payload"
    with pytest.raises(ArtifactAccessDenied, match="scope"):
        other_store.read(other.model_copy(update={"access_scope": {}}), context={"project": "p", "tenant_id": "a"})


def _slow_reader(root, payload, entered, release, results):
    class SlowAuthorizer:
        def authorize(self, artifact, *, context):
            entered.set()
            if not release.wait(15):
                raise RuntimeError("reader synchronization timed out")
            return True

    store = FileArtifactStore(root, authorizer=SlowAuthorizer())
    results.put(store.read(al.ArtifactRef.model_validate(payload)))


def _concurrent_gc(root, started, done, results):
    store = FileArtifactStore(root)
    started.set()
    results.put(store.gc(older_than_seconds=1))
    done.set()


def test_cross_process_gc_cannot_delete_an_active_reader(tmp_path):
    store = FileArtifactStore(tmp_path)
    artifact = store.put("payload")
    old = time.time() - 1000
    os.utime(artifact.uri, (old, old))
    context = multiprocessing.get_context("spawn")
    entered, release, gc_started, done = context.Event(), context.Event(), context.Event(), context.Event()
    reads, deletions = context.Queue(), context.Queue()
    reader = context.Process(target=_slow_reader, args=(str(tmp_path), artifact.model_dump(), entered, release, reads))
    gc = context.Process(target=_concurrent_gc, args=(str(tmp_path), gc_started, done, deletions))
    try:
        reader.start()
        assert entered.wait(15)
        gc.start()
        assert gc_started.wait(15)
        assert not done.wait(0.3)
        assert Path(artifact.uri).exists()
        release.set()
        assert reads.get(timeout=15) == b"payload"
        assert deletions.get(timeout=15)["deleted"] == 1
    finally:
        release.set()
        for process in (reader, gc):
            if process.pid:
                process.join(15)
                if process.is_alive():
                    process.terminate()
                    process.join(5)
        reads.close()
        deletions.close()
    assert reader.exitcode == gc.exitcode == 0


def test_open_rejects_parent_symlink_replacement(tmp_path):
    root = tmp_path / "store"
    store = FileArtifactStore(root)
    artifact = store.put("payload")
    source_dir = Path(artifact.uri).parent
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / Path(artifact.uri).name).write_bytes(b"payload")
    try:
        (tmp_path / "probe-link").symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation unavailable")

    class SwapAuthorizer:
        def authorize(self, artifact, *, context):
            source_dir.rename(source_dir.with_name(source_dir.name + "-old"))
            source_dir.symlink_to(outside, target_is_directory=True)
            return True

    store.authorizer = SwapAuthorizer()
    with pytest.raises(ArtifactPolicyError):
        store.read(artifact)


@pytest.mark.parametrize("async_tool", [False, True])
def test_stream_tail_is_bounded_in_utf8_bytes_and_stage_quota_stops_early(tmp_path, async_tool):
    with closing(al.ActionLens(storage_dir=tmp_path / "tail", sink=MemorySink())) as lens:
        if async_tool:
            @lens.tool(max_bytes=10)
            async def tool():
                yield "汉" * 10000
            output = asyncio.run(tool())
        else:
            @lens.tool(max_bytes=10)
            def tool():
                yield "汉" * 10000
            output = tool()
        assert output.status == "SUCCESS" and len((output.result or "").encode()) <= 10
    observed, closed = [], []
    with closing(al.ActionLens(storage_dir=tmp_path / "quota", sink=MemorySink(), artifact_policy=al.ArtifactPolicy(max_bytes_per_run=10))) as lens:
        @lens.tool
        def bounded():
            try:
                for index in range(100):
                    observed.append(index)
                    yield b"12345678"
            finally:
                closed.append(True)

        output = bounded()
        assert output.status == "FAILED" and output.error_taxonomy == "ArtifactPolicyDenied"
        assert observed == [0, 1] and closed == [True]
        assert lens.artifact_store.tracked_run_count == 0


def test_budget_capacity_never_forgets_active_runs_and_scope_is_tenant_aware(tmp_path):
    budget = al.BudgetPolicy(max_calls_per_run=1, max_tracked_runs=2)
    spec = al.ToolSpec(name="read")
    base = al.ToolCallContext(project="p", session_id="s", run_id="r", call_id="c", tool_name="read", tenant_id="a")
    assert budget.decide(spec=spec, args={}, context=base).action == "ALLOW"
    assert budget.decide(spec=spec, args={}, context=base.model_copy(update={"tenant_id": "b"})).action == "ALLOW"
    assert budget.decide(spec=spec, args={}, context=base.model_copy(update={"run_id": "new"})).action == "DENY"
    assert budget.decide(spec=spec, args={}, context=base).action == "DENY"
    budget.reset_run(project="p", run_id="r", context=base)
    assert budget.tracked_run_count == 1
    assert budget.decide(spec=spec, args={}, context=base.model_copy(update={"tenant_id": "b"})).action == "DENY"
    budget.reset_run(project="p", run_id="r")
    assert budget.tracked_run_count == 0
    with closing(al.ActionLens(storage_dir=tmp_path, sink=MemorySink())) as lens:
        assert lens.capabilities()["call_budget_scope"] == "local_instance"


def test_reliable_queued_delivery_is_not_acked_until_write_and_fsync(tmp_path, monkeypatch):
    repository = al.SQLiteGovernanceRepository(tmp_path / "governance.sqlite3")
    sink = JsonlSink(tmp_path, queue_maxsize=1, drop_policy="drop_newest")
    seed_outbox(repository, event())
    entered, release = Event(), Event()
    original = sink._write

    def slow_write(item, *, durable):
        entered.set()
        assert release.wait(5)
        return original(item, durable=durable)

    monkeypatch.setattr(sink, "_write", slow_write)
    dispatcher = OutboxDispatcher(repository, sink)
    try:
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(dispatcher.dispatch_once)
            assert entered.wait(5)
            assert repository.list_outbox()[0].delivered_at is None
            release.set()
            assert future.result()["delivered"] == 1
        assert load_events(tmp_path)[0][0]["event_id"] == "one"
        seed_outbox(repository, event("two"))
        sink.close()
        assert dispatcher.dispatch_once()["failed"] == 1
        assert next(item for item in repository.list_outbox() if item.event.event_id == "two").delivered_at is None
    finally:
        release.set()
        sink.close()
        repository.close()


def test_reliable_write_failure_remains_replayable_and_optional_sink_does_not_block(tmp_path, monkeypatch):
    repository = al.SQLiteGovernanceRepository(tmp_path / "governance.sqlite3")
    sink = JsonlSink(tmp_path, queue_maxsize=1)
    seed_outbox(repository, event())

    def fail(*args, **kwargs):
        raise OSError("synthetic-secret")

    monkeypatch.setattr(sink, "_write", fail)
    dispatcher = OutboxDispatcher(repository, sink, max_attempts=1)
    assert dispatcher.dispatch_once()["dead_lettered"] == 1
    assert repository.list_outbox()[0].last_error == "OSError"
    monkeypatch.undo()
    assert repository.replay_dead_letter(repository.list_outbox()[0].delivery_id)
    optional = MemorySink()
    optional.emit = fail
    dispatcher.sink = CompositeSink(SinkBinding(sink), SinkBinding(optional, required=False))
    assert dispatcher.dispatch_once()["delivered"] == 1
    sink.close()
    repository.close()


@pytest.mark.parametrize("status", [200, 400, 401, 403, 413, 429, 503])
def test_webhook_rejection_is_never_acknowledged_as_success(tmp_path, status):
    class Response:
        headers = {"Retry-After": "2"}
        def __init__(self):
            self.status = status
        def close(self):
            pass

    repository = al.SQLiteGovernanceRepository(tmp_path / "governance.sqlite3")
    sink = WebhookSink("https://audit.invalid/events", secret=lambda: "synthetic-secret", opener=lambda *args, **kwargs: Response())
    seed_outbox(repository, event())
    dispatcher = OutboxDispatcher(repository, sink)
    result = dispatcher.dispatch_once()
    record = repository.list_outbox()[0]
    if status == 200:
        assert result["delivered"] == 1
    else:
        assert result["delivered"] == 0 and result["failed"] == 1
        assert record.delivered_at is None
        if status not in {429, 503}:
            assert result["dead_lettered"] == 1
        elif status == 429:
            assert record.next_retry_at >= datetime.now(timezone.utc)
    repository.close()


def test_nested_adapter_schema_has_root_definitions(tmp_path):
    jsonschema = pytest.importorskip("jsonschema")
    with closing(al.ActionLens(storage_dir=tmp_path, sink=MemorySink())) as lens:
        @lens.tool
        def read(artifact: al.ArtifactRef):
            return artifact.size_bytes

        adapter = ActionLensToolAdapter(read, framework="test")
        schema = adapter.parameters_json_schema
        assert "$defs" in schema
        payload = {"artifact": al.ArtifactRef(uri="s3://bucket/key", size_bytes=4, media_type="image/png", sha256="hash",
                                             media_metadata=al.MediaMetadata(width=12)).model_dump(mode="json")}
        jsonschema.validate(payload, schema)
        assert json.loads(adapter.invoke(payload))["result"] == 4


def test_canonical_reader_validates_deduplicates_and_quarantines_conflicts(tmp_path):
    directory = tmp_path / "trajectories"
    directory.mkdir()
    first, conflicting, valid = event(), event().model_copy(update={"metrics": {"latency_ms": 7}}), event("valid")
    # Keep all fields identical except the conflicting payload.
    conflicting = first.model_copy(update={"metrics": {"latency_ms": 7}})
    path = directory / "events.jsonl"
    path.write_bytes(first.model_dump_json().encode() + b"\n" + first.model_dump_json().encode() + b"\n"
                     + conflicting.model_dump_json().encode() + b"\n" + valid.model_dump_json().encode() + b"\n"
                     + b'{"event_id":"invalid"}\n\xff\n'
                     + valid.model_copy(update={"schema_version": "future"}).model_dump_json().encode() + b"\n")
    events, stats = load_events(tmp_path)
    assert [item["event_id"] for item in events] == ["valid"]
    assert (stats.invalid, stats.duplicates, stats.conflicts, stats.unsupported) == (2, 1, 1, 1)
    assert stats.skipped == 5 and stats.issues[0]["line"] == 2
    assert stats.source_files[0]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("verified", [False, True])
def test_signature_verification_must_bind_the_bundle_and_chain_root(tmp_path, verified):
    def verifier(ref, *, bundle_id, chain_root_sha256):
        return {"verified": True, "bundle_id": bundle_id, "chain_root_sha256": chain_root_sha256 if verified else "wrong",
                "key_id": "test-key", "algorithm": "test-signature"}

    output = tmp_path / "evidence"
    export_evidence_bundle(tmp_path, output, signature_manifest_ref="https://audit.invalid/signature", signature_verifier=verifier)
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest["integrity"]["signed"] is verified
    assert manifest["integrity"]["signature_provided"] is True
    assert ("verified_signature_manifest" in manifest["missing_evidence"]) is not verified


def test_metrics_aggregate_history_with_bounded_storage_and_labels():
    sink = MetricsSink(max_tools=2)
    item = event(metrics={"latency_ms": 7})
    for _ in range(10000):
        sink.emit(item)
    summary = sink.latency_ms[("tool", "success")]
    assert (summary.count, summary.sum, summary.minimum, summary.maximum) == (10000, 70000, 7, 7)
    assert len(summary.buckets) == 9 and summary.buckets[-1] == 10000
    for index in range(100):
        sink.emit(item.model_copy(update={"tool_name": f"tool-{index}"}))
    assert len(sink.latency_ms) <= 3


def test_async_stream_staging_runs_outside_the_event_loop(tmp_path, monkeypatch):
    from threading import get_ident

    from actionlens.runtime import _StreamCollector

    threads = []
    loop_thread = get_ident()
    original = _StreamCollector.write

    def observe(*args, **kwargs):
        threads.append(get_ident())
        return original(*args, **kwargs)

    monkeypatch.setattr(_StreamCollector, "write", observe)
    with closing(al.ActionLens(storage_dir=tmp_path, sink=MemorySink())) as lens:
        @lens.tool
        async def stream():
            for _ in range(4):
                yield "payload"

        assert asyncio.run(stream()).status == "SUCCESS"
    assert threads and all(thread != loop_thread for thread in threads)


def test_output_reference_credentials_are_rejected_in_every_storage_mode(tmp_path):
    with closing(al.ActionLens(storage_dir=tmp_path, sink=MemorySink())) as lens:
        @lens.tool
        def read():
            return al.StructuredToolOutput(status="SUCCESS", result_summary="remote result",
                                           artifact_refs=[al.ArtifactRef(uri="https://user:synthetic-secret@audit.invalid/item?token=secret",
                                                                        media_type="text/plain", size_bytes=1, sha256="h")])

        output = read()
        assert output.error_taxonomy == "ArtifactPolicyDenied"
        assert "synthetic-secret" not in output.model_dump_json()


def test_empty_infinite_stream_is_bounded_and_closed(tmp_path):
    closed = []
    with closing(al.ActionLens(storage_dir=tmp_path, sink=MemorySink())) as lens:
        @lens.tool(output=al.OutputPolicy(max_stream_chunks=3))
        def stream():
            try:
                while True:
                    yield ""
            finally:
                closed.append(True)

        assert stream().error_taxonomy == "ArtifactPolicyDenied"
        assert closed == [True]


def test_oversized_event_line_does_not_hide_the_next_valid_event(tmp_path):
    directory = tmp_path / "trajectories"
    directory.mkdir()
    path = directory / "events.jsonl"
    path.write_bytes(b"x" * 2048 + b"\n" + event("valid").model_dump_json().encode() + b"\n")
    stats = TrajectoryReadStats()
    rows = list(iter_trajectory_events(tmp_path, stats=stats, max_line_bytes=1000))
    assert [row["event_id"] for row in rows] == ["valid"]
    assert stats.invalid == stats.skipped == 1
    assert stats.issues == [{"kind": "oversized", "file": "events.jsonl", "line": 1}]
    assert stats.source_files[0]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_file_uri_resolves_to_the_same_trusted_artifact(tmp_path):
    store = FileArtifactStore(tmp_path / "with spaces", authorizer=AllowArtifacts())
    artifact = store.put("payload")
    uri_ref = artifact.model_copy(update={"uri": Path(artifact.uri).as_uri()})
    assert store.read(uri_ref) == b"payload"


@pytest.mark.skipif(os.name != "nt", reason="Windows junction boundary")
def test_windows_junction_cannot_replace_an_artifact_parent(tmp_path):
    store = FileArtifactStore(tmp_path / "store")
    artifact = store.put("payload")
    parent = Path(artifact.uri).parent
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / Path(artifact.uri).name).write_bytes(b"payload")
    observed = []

    class SwapAuthorizer:
        def authorize(self, artifact, *, context):
            parent.rename(parent.with_name(parent.name + "-old"))
            result = subprocess.run(["cmd", "/c", "mklink", "/J", str(parent), str(outside)], capture_output=True, check=False)
            if result.returncode:
                pytest.skip("junction creation unavailable")
            observed.append(True)
            return True

    store.authorizer = SwapAuthorizer()
    with pytest.raises(ArtifactPolicyError, match="reparse"):
        store.read(artifact)
    assert observed == [True]
