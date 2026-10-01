from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

import actionlens as al
import actionlens.artifacts.fs as artifact_fs
from actionlens.artifacts import ArtifactPolicyError, FileArtifactStore
from actionlens.integrations import (
    DBOSStepRunner,
    TemporalActivityRunner,
    context_from_dbos_workflow,
    context_from_temporal_workflow,
)
from actionlens.sinks import MemorySink


def test_explicit_context_preserves_call_id_and_reaches_original_tool(tmp_path: Path) -> None:
    sink = MemorySink()
    lens = al.ActionLens(storage_dir=tmp_path, sink=sink)
    seen: list[al.ToolCallContext] = []

    @lens.tool()
    def observe(*, __al_ctx: al.ToolCallContext) -> str:
        seen.append(__al_ctx)
        return __al_ctx.call_id

    context = al.ToolCallContext(
        project="demo",
        session_id="workflow-17",
        run_id="run-3",
        call_id="activity-charge-card",
        tool_name="observe",
    )
    output = observe(__al_ctx=context)

    assert output.result == "activity-charge-card"
    assert [item.call_id for item in seen] == ["activity-charge-card"]
    assert seen[0].context_source == "explicit"
    started = next(event for event in sink.events if event.event_type == "tool_call.started")
    assert started.call_id == "activity-charge-card"


def test_declared_context_parameter_supports_contextvar_and_positional_binding(
    tmp_path: Path,
) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    seen: list[al.ToolCallContext] = []

    @lens.tool()
    def observe(value: str, __al_ctx: al.ToolCallContext | None = None) -> str:
        assert __al_ctx is not None
        seen.append(__al_ctx)
        return value

    with lens.session(session_id="session-context", run_id="run-context", actor_id="actor-7"):
        output = observe("ok")

    assert output.result == "ok"
    assert seen[0].session_id == "session-context"
    assert seen[0].run_id == "run-context"
    assert seen[0].actor_id == "actor-7"
    assert seen[0].tool_name == "observe"
    assert seen[0].context_source == "contextvar"
    assert seen[0].call_id.startswith("call-")


def test_explicit_declared_context_wins_over_ambient_session(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    seen: list[al.ToolCallContext] = []

    @lens.tool()
    def observe(*, __al_ctx: al.ToolCallContext) -> str:
        seen.append(__al_ctx)
        return __al_ctx.session_id

    explicit = al.ToolCallContext(
        project="demo",
        session_id="durable-session",
        run_id="durable-run",
        call_id="durable-step",
        tool_name="observe",
        actor_id="durable-actor",
    )
    with lens.session(session_id="ambient-session", actor_id="ambient-actor"):
        output = observe(__al_ctx=explicit)

    assert output.result == "durable-session"
    assert seen[0].call_id == "durable-step"
    assert seen[0].actor_id == "durable-actor"
    assert seen[0].context_source == "explicit"


def test_temporal_context_keeps_retry_attempt_out_of_idempotency_identity(tmp_path: Path) -> None:
    calls = 0
    heartbeats: list[str] = []
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.AUTO_HASH)
    def send_message(text: str) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"text": text, "calls": calls}

    first_context = context_from_temporal_workflow(
        "order-42",
        "temporal-run-a",
        tool_name="send_message",
        activity_id="send-confirmation",
        attempt=1,
    )
    retry_context = context_from_temporal_workflow(
        "order-42",
        "temporal-run-a",
        tool_name="send_message",
        activity_id="send-confirmation",
        attempt=2,
    )
    runner = TemporalActivityRunner(send_message, heartbeater=heartbeats.append)

    first = runner.run(first_context, "confirmed")
    retry = runner.run(retry_context, "confirmed")

    assert first_context.session_id == "order-42"
    assert first_context.run_id == "temporal-run-a"
    assert first_context.call_id == retry_context.call_id == "send-confirmation"
    assert first_context.attempt == 1
    assert retry_context.attempt == 2
    assert calls == 1
    assert retry.result == first.result
    assert heartbeats == [
        "actionlens:preflight",
        "actionlens:completed",
        "actionlens:preflight",
        "actionlens:completed",
    ]


def test_temporal_retry_does_not_create_a_second_approval_ticket(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool(
        risk=al.RiskLevel.DESTRUCTIVE,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
    )
    def delete_account(account_id: str) -> dict[str, str]:
        return {"deleted": account_id}

    runner = TemporalActivityRunner(delete_account)
    first = runner.run(
        context_from_temporal_workflow(
            "account-7", "run-1", tool_name="delete_account", activity_id="delete"
        ),
        "account-7",
        idempotency_key="delete-account-7",
    )
    retry = runner.run(
        context_from_temporal_workflow(
            "account-7", "run-1", tool_name="delete_account", activity_id="delete", attempt=2
        ),
        "account-7",
        idempotency_key="delete-account-7",
    )

    assert first.status == retry.status == "PENDING_APPROVAL"
    assert retry.result == first.result


def test_temporal_runner_supports_async_tools_and_rejects_unwrapped_callables(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool()
    async def lookup(value: str) -> str:
        await asyncio.sleep(0)
        return value.upper()

    context = context_from_temporal_workflow(
        "lookup-1", "run-1", tool_name="lookup", activity_id="lookup-step"
    )
    output = asyncio.run(TemporalActivityRunner(lookup).arun(context, "ok"))

    assert output.result == "OK"
    with pytest.raises(TypeError, match="async ActionLens tools"):
        TemporalActivityRunner(lookup).run(context, "ok")
    with pytest.raises(TypeError, match="wrapped by ActionLens"):
        TemporalActivityRunner(lambda: None)


def test_dbos_context_and_step_runner_are_durable_runtime_agnostic(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())

    @lens.tool()
    def increment(value: int) -> int:
        return value + 1

    context = context_from_dbos_workflow(
        "dbos-operation-9",
        tool_name="increment",
        step_id="increment-step",
        attempt=3,
        metadata={"request": object()},
    )
    output = DBOSStepRunner(increment).run(context, 4)

    assert context.session_id == "dbos-operation-9"
    assert context.run_id == "dbos-operation-9"
    assert context.call_id == "increment-step"
    assert context.framework == "dbos"
    assert context.attempt == 3
    assert isinstance(context.metadata["request"], str)
    assert output.result == 5


def test_media_metadata_and_provenance_are_persisted_without_sidecar_overwrite(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    source = store.put(b"raw-image", media_type="image/png")
    provenance = al.ArtifactProvenance.from_source(
        source,
        operation="thumbnail",
        operation_version="thumbnailer-2.1",
        parameters={"max_width": 320},
        created_by_tool="create_thumbnail",
    )
    derived = store.put(
        b"thumbnail-image",
        media_type="image/jpeg",
        media_metadata=al.MediaMetadata(width=320, height=180, codec="jpeg"),
        provenance=provenance,
        metadata={"run_id": "media-run", "sha256": "caller-cannot-overwrite"},
    )

    assert derived.media_metadata is not None
    assert derived.media_metadata.width == 320
    assert derived.provenance is not None
    assert derived.provenance.source_ref.sha256 == source.sha256
    sidecar = json.loads(Path(derived.uri + ".meta.json").read_text(encoding="utf-8"))
    assert sidecar["sha256"] == derived.sha256
    assert sidecar["metadata"]["sha256"] == "caller-cannot-overwrite"
    assert sidecar["provenance"]["source_ref"]["uri"] == source.uri


def test_media_metadata_extractor_is_optional_best_effort_and_never_reads_ciphertext(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    class Extractor:
        def __init__(self) -> None:
            self.calls: list[tuple[Path, str]] = []

        def extract(self, path: Path, media_type: str) -> al.MediaMetadata:
            self.calls.append((path, media_type))
            assert path.exists()
            return al.MediaMetadata(width=640, height=480)

    extractor = Extractor()
    store = FileArtifactStore(tmp_path / "plain", media_metadata_extractor=extractor)
    image = store.put(b"image", media_type="image/png")
    store.put(b"text", media_type="text/plain")

    assert image.media_metadata == al.MediaMetadata(width=640, height=480)
    assert [media_type for _, media_type in extractor.calls] == ["image/png"]

    class FailingExtractor:
        def extract(self, path: Path, media_type: str) -> al.MediaMetadata | None:
            raise RuntimeError("broken decoder")

    best_effort = FileArtifactStore(tmp_path / "best-effort", media_metadata_extractor=FailingExtractor())
    with caplog.at_level(logging.WARNING, logger="actionlens.artifacts.fs"):
        assert best_effort.put(b"image", media_type="image/png").media_metadata is None
    assert "stored without extracted metadata" in caplog.text
    assert "broken decoder" in caplog.text

    class ByteCipher:
        provider_id = "test-cipher"

        def encrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return b"x" + payload

        def decrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload[1:]

        def rewrap(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload

    encrypted = FileArtifactStore(
        tmp_path / "encrypted",
        policy=al.ArtifactPolicy(encryption="provider"),
        encryption_provider=ByteCipher(),
        media_metadata_extractor=extractor,
    )
    encrypted_image = encrypted.put(
        b"image",
        media_type="image/png",
        media_metadata={"width": 10, "height": 20},
    )
    assert encrypted_image.media_metadata == al.MediaMetadata(width=10, height=20)
    assert len(extractor.calls) == 1

    with pytest.raises(ValueError, match="supplied artifact_store"):
        al.ActionLens(
            storage_dir=tmp_path / "lens",
            artifact_store=store,
            media_metadata_extractor=extractor,
        )


def test_media_and_provenance_reject_non_finite_json_numbers() -> None:
    for value in (math.nan, math.inf, -math.inf):
        with pytest.raises(ValueError):
            al.MediaMetadata(duration_sec=value)

    source = al.ArtifactRef(
        uri="https://artifacts.invalid/source",
        media_type="image/png",
        size_bytes=1,
        sha256="0" * 64,
    )
    with pytest.raises(ValueError, match="JSON-serializable"):
        al.ArtifactProvenance.from_source(
            source,
            operation="resize",
            operation_version="v1",
            parameters={"scale": math.nan},
        )


def test_lineage_gc_preserves_source_until_derived_artifact_is_eligible(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    source = store.put(b"source", media_type="image/png")
    derived = store.put(
        b"derived",
        media_type="image/jpeg",
        provenance=al.ArtifactProvenance.from_source(
            source,
            operation="thumbnail",
            operation_version="v1",
        ),
    )
    source_path = Path(source.uri)
    derived_path = Path(derived.uri)
    os.utime(source_path, (1, 1))

    protected = store.gc(older_than_seconds=60)
    assert protected["deleted"] == 0
    assert protected["lineage_protected"] == 1
    assert source_path.exists()
    assert derived_path.exists()

    cascaded = store.gc(older_than_seconds=60, cascade_derived=True)
    assert cascaded["deleted"] == 2
    assert cascaded["lineage_cascaded"] == 1
    assert not source_path.exists()
    assert not derived_path.exists()


def test_cli_gc_exposes_cascade_derived(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from actionlens.cli import main

    store = FileArtifactStore(tmp_path)
    source = store.put(b"source", media_type="image/png")
    derived = store.put(
        b"derived",
        media_type="image/jpeg",
        provenance=al.ArtifactProvenance.from_source(
            source, operation="thumbnail", operation_version="v1"
        ),
    )
    os.utime(Path(source.uri), (1, 1))

    assert main(
        [
            "gc",
            "--storage-dir",
            str(tmp_path),
            "--older-than",
            "60s",
            "--cascade-derived",
        ]
    ) == 0
    result = json.loads(capsys.readouterr().out)

    assert result["deleted"] == 2
    assert result["lineage_cascaded"] == 1
    assert not Path(source.uri).exists()
    assert not Path(derived.uri).exists()


def test_gc_without_delete_candidates_skips_lineage_sidecars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FileArtifactStore(tmp_path)
    store.put(b"recent", media_type="image/png")

    def fail_if_called(files: set[Path]) -> dict[Path, set[Path]]:
        raise AssertionError(f"unexpected lineage scan for {len(files)} files")

    monkeypatch.setattr(store, "_lineage_parents", fail_if_called)
    result = store.gc(older_than_seconds=10**12)

    assert result["deleted"] == 0
    assert result["lineage_protected"] == 0


def test_deduplicated_derived_content_retains_all_local_provenance_sources(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    first_source = store.put(b"first", media_type="image/png")
    second_source = store.put(b"second", media_type="image/png")
    first_derived = store.put(
        b"same-thumbnail",
        media_type="image/jpeg",
        provenance=al.ArtifactProvenance.from_source(
            first_source, operation="thumbnail", operation_version="v1"
        ),
    )
    second_derived = store.put(
        b"same-thumbnail",
        media_type="image/jpeg",
        provenance=al.ArtifactProvenance.from_source(
            second_source, operation="thumbnail", operation_version="v1"
        ),
    )

    assert first_derived.uri == second_derived.uri
    sidecar = json.loads(Path(first_derived.uri + ".meta.json").read_text(encoding="utf-8"))
    assert {
        record["source_ref"]["sha256"] for record in sidecar["provenance_records"]
    } == {first_source.sha256, second_source.sha256}

    os.utime(Path(first_source.uri), (1, 1))
    os.utime(Path(second_source.uri), (1, 1))
    result = store.gc(older_than_seconds=60)

    assert result["deleted"] == 0
    assert result["lineage_protected"] == 2
    assert Path(first_source.uri).exists()
    assert Path(second_source.uri).exists()


def test_concurrent_deduplicated_derivations_merge_their_provenance(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    sources = [store.put(value, media_type="image/png") for value in (b"first", b"second")]
    barrier = Barrier(2)

    def write(source: al.ArtifactRef) -> al.ArtifactRef:
        barrier.wait()
        return store.put(
            b"same-thumbnail",
            media_type="image/jpeg",
            provenance=al.ArtifactProvenance.from_source(
                source, operation="thumbnail", operation_version="v1"
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = list(executor.map(write, sources))

    sidecar = json.loads(Path(first.uri + ".meta.json").read_text(encoding="utf-8"))
    assert first.uri == second.uri
    assert {
        record["source_ref"]["sha256"] for record in sidecar["provenance_records"]
    } == {source.sha256 for source in sources}


@pytest.mark.parametrize("streaming", [False, True])
def test_deduplicated_promotion_recovers_windows_style_destination_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, streaming: bool
) -> None:
    """A peer-visible content target turns a Windows replace conflict into dedup reuse."""

    store = FileArtifactStore(tmp_path)
    payload = b"same-content-from-two-workers"
    original_replace = artifact_fs.os.replace

    def windows_conflict_replace(
        source: object, destination: object, *args: object, **kwargs: object
    ) -> None:
        target = Path(destination)
        if target.suffix == ".bin":
            original_replace(source, destination, *args, **kwargs)
            raise PermissionError("[WinError 5] destination was promoted by a peer")
        original_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr(artifact_fs.os, "replace", windows_conflict_replace)

    if streaming:
        artifact = store.put_stream(
            io.BytesIO(payload), metadata={"run_id": "stream-run"}
        )
    else:
        artifact = store.put(payload, metadata={"run_id": "bytes-run"})

    assert artifact.sha256 == hashlib.sha256(payload).hexdigest()
    assert Path(artifact.uri).read_bytes() == payload


def test_deduplicated_promotion_does_not_hide_permission_error_without_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FileArtifactStore(tmp_path)

    def denied_replace(*args: object, **kwargs: object) -> None:
        raise PermissionError("[WinError 5] unrelated access denial")

    monkeypatch.setattr(artifact_fs.os, "replace", denied_replace)

    with pytest.raises(PermissionError, match="unrelated access denial"):
        store.put(b"payload", metadata={"run_id": "failed-run"})


def test_concurrent_deduplicated_writes_initialize_sidecar_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Metadata initialization has the same Windows replace race as content writes."""

    store = FileArtifactStore(tmp_path)
    existing = store.put(b"same-content")
    meta_path = Path(existing.uri + ".meta.json")
    meta_path.unlink()
    original_exists = Path.exists
    original_replace = artifact_fs.os.replace
    barrier = Barrier(2)

    def windows_conflict_replace(
        source: object, destination: object, *args: object, **kwargs: object
    ) -> None:
        target = Path(destination)
        if target == meta_path and original_exists(target):
            raise PermissionError("[WinError 5] metadata sidecar already exists")
        original_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr(artifact_fs.os, "replace", windows_conflict_replace)

    def write(worker: int) -> al.ArtifactRef:
        barrier.wait(timeout=5)
        return store.put(
            b"same-content", metadata={"run_id": "same-run", "worker": worker}
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = list(executor.map(write, (1, 2)))

    sidecar = json.loads(meta_path.read_text(encoding="utf-8"))
    assert first.uri == second.uri == existing.uri
    assert sidecar["sha256"] == existing.sha256
    assert sidecar["metadata"]["worker"] in {1, 2}


def test_artifact_run_quota_requires_identity_and_releases_on_reset(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path, policy=al.ArtifactPolicy(max_bytes_per_run=5))

    with pytest.raises(ArtifactPolicyError, match=r"metadata\['run_id'\]"):
        store.put(b"12345")

    store.put(b"12345", metadata={"run_id": "run-1"})
    assert store.tracked_run_count == 1
    with pytest.raises(ArtifactPolicyError, match="budget exceeded"):
        store.put(b"x", metadata={"run_id": "run-1"})

    store.reset_run("run-1")
    assert store.tracked_run_count == 0
    store.put(b"12345", metadata={"run_id": "run-1"})

    unbounded = FileArtifactStore(tmp_path / "unbounded")
    for run_id in ("one", "two", "three"):
        unbounded.put(b"payload", metadata={"run_id": run_id})
    assert unbounded.tracked_run_count == 0


def test_provenance_record_limit_is_bounded_and_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(artifact_fs, "_MAX_PROVENANCE_RECORDS_PER_ARTIFACT", 2)
    store = FileArtifactStore(tmp_path)
    sources = [store.put(value, media_type="image/png") for value in (b"first", b"second", b"third")]

    for source in sources[:2]:
        store.put(
            b"same-thumbnail",
            media_type="image/jpeg",
            provenance=al.ArtifactProvenance.from_source(
                source, operation="thumbnail", operation_version="v1"
            ),
        )

    with pytest.raises(ArtifactPolicyError, match="provenance record limit"):
        store.put(
            b"same-thumbnail",
            media_type="image/jpeg",
            provenance=al.ArtifactProvenance.from_source(
                sources[2], operation="thumbnail", operation_version="v1"
            ),
        )

    repeated = store.put(
        b"same-thumbnail",
        media_type="image/jpeg",
        provenance=al.ArtifactProvenance.from_source(
            sources[0], operation="thumbnail", operation_version="v1"
        ),
    )
    sidecar = json.loads(Path(repeated.uri + ".meta.json").read_text(encoding="utf-8"))
    assert len(sidecar["provenance_records"]) == 2


def test_media_models_reject_invalid_values_and_non_json_lineage_parameters() -> None:
    with pytest.raises(ValueError):
        al.MediaMetadata(width=-1)
    source = al.ArtifactRef(
        uri="artifact://source",
        media_type="image/png",
        size_bytes=1,
        sha256="source-sha",
    )
    with pytest.raises(ValueError, match="JSON-serializable"):
        al.ArtifactProvenance.from_source(
            source,
            operation="thumbnail",
            operation_version="v1",
            parameters={"bad": {1, 2}},
        )

    signed_source = source.model_copy(
        update={"uri": "https://user:secret@artifact.example/image.png?signature=secret#part"}
    )
    provenance = al.ArtifactProvenance.from_source(
        signed_source,
        operation="thumbnail",
        operation_version="v1",
    )
    assert provenance.source_ref.uri == "https://artifact.example/image.png"

    with pytest.raises(ValueError, match="activity_id"):
        context_from_temporal_workflow("workflow", "run", tool_name="tool", activity_id=" ")
    with pytest.raises(ValueError, match="step_id"):
        context_from_dbos_workflow("workflow", tool_name="tool", step_id=" ")
