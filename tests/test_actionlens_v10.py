from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import actionlens as al
from actionlens.artifacts import (
    ArtifactAccessDenied,
    ArtifactPolicyError,
    FileArtifactStore,
)
from actionlens.models import TrajectoryEvent
from actionlens.outbox import OutboxDispatcher
from actionlens.sinks import MemorySink, WebhookSink, verify_webhook_signature


def _context() -> al.ToolCallContext:
    return al.ToolCallContext(
        project="demo", session_id="s1", run_id="r1", call_id="c1", tool_name="mutate"
    )


def _event(event_id: str = "e1") -> TrajectoryEvent:
    return TrajectoryEvent(
        event_id=event_id,
        timestamp=datetime.now(timezone.utc),
        project="demo",
        session_id="s1",
        run_id="r1",
        sequence=1,
        event_type="test.event",
        phase="POST_FLIGHT",
        call_id="c1",
        tool_name="mutate",
    )


def _uncertain_repository(tmp_path: Path, schema_hash: str = "s") -> tuple[al.SQLiteGovernanceRepository, str]:
    repository = al.SQLiteGovernanceRepository(tmp_path / "governance.sqlite3")
    key = "scope:mutation-1"
    spec = al.ToolSpec(name="mutate", fencing_supported=False)
    assert repository.begin(
        key, call_id="c1", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash=schema_hash, owner_id="w1", lease_seconds=-1,
    )[0] == "created"
    assert repository.begin(
        key, call_id="c2", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash=schema_hash, owner_id="w2", lease_seconds=30,
    )[0] == "uncertain"
    return repository, key


def test_uncertain_reconciliation_is_evidence_backed_and_atomic(tmp_path: Path) -> None:
    lens = al.ActionLens(storage_dir=tmp_path / "lens", sink=MemorySink())
    @lens.tool(name="mutate")
    def mutate():
        return None
    repository, key = _uncertain_repository(tmp_path, mutate.actionlens_runtime._tool_schema_hash())
    lens.close()
    lens = al.ActionLens(storage_dir=tmp_path / "lens", sink=MemorySink(), repository=repository)
    lens.tool(name="mutate")(mutate.__wrapped__)

    class Reconciler:
        def inspect(self, record: object) -> al.ReconciliationResult:
            return al.ReconciliationResult(
                outcome="CONFIRMED_SUCCEEDED",
                summary="provider lookup found the transaction",
                output={"status": "SUCCESS", "result_summary": "confirmed"},
                evidence_ref="https://user:pass@audit.example/transactions/1?token=secret",
            )

    result = lens.reconcile_uncertain(key, Reconciler())
    assert result.outcome == "CONFIRMED_SUCCEEDED"
    record = repository.get_ledger(key)
    assert record is not None and record.status == "SUCCEEDED"
    assert record.output is not None
    assert record.output["status"] == "SUCCESS"
    assert record.output["result"] is None
    reconciliation = next(
        item.event for item in repository.list_outbox()
        if item.event.event_type == "ledger.reconciled"
    )
    assert reconciliation.metadata["evidence_ref"] == "https://audit.example/transactions/1"


def test_manual_reconciliation_requires_actor_reason_and_evidence(tmp_path: Path) -> None:
    repository, key = _uncertain_repository(tmp_path)
    lens = al.ActionLens(storage_dir=tmp_path / "lens", sink=MemorySink(), repository=repository)

    class Override:
        def inspect(self, record: object) -> al.ReconciliationResult:
            return al.ReconciliationResult(
                outcome="MANUAL_OVERRIDE", summary="operator decision",
                override_target="NOT_APPLIED",
            )

    with pytest.raises(ValueError, match="actor_id, reason, and evidence_ref"):
        lens.reconcile_uncertain(key, Override(), actor_id="ops-user")
    assert repository.get_ledger(key).status == "UNCERTAIN"  # type: ignore[union-attr]


def test_dead_letter_can_be_listed_replayed_and_terminated(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(tmp_path / "governance.sqlite3")
    spec = al.ToolSpec(name="mutate")
    _, record = repository.begin(
        "k", call_id="c1", context=_context(), spec=spec, args_hash="a",
        tool_schema_hash="s", owner_id="w", lease_seconds=30,
    )
    repository.finish(
        "k", owner_id="w", fencing_token=record.fencing_token, status="SUCCEEDED",
        output={}, error=None, event=_event(),
    )

    class BrokenSink:
        def emit(self, event: object) -> None:
            raise OSError("offline")

    OutboxDispatcher(repository, BrokenSink(), max_attempts=1).dispatch_once()
    dead = repository.list_outbox(state="dead_letter")
    assert len(dead) == 1
    delivery_id = dead[0].delivery_id
    event_id = dead[0].event.event_id
    assert repository.replay_dead_letter(delivery_id)
    claimed = repository.claim_outbox(worker_id="w2", limit=1, claim_seconds=30)
    assert claimed[0].event.event_id == event_id
    assert repository.dead_letter_outbox(delivery_id, worker_id="w2", error="still bad")
    assert repository.terminate_dead_letter(delivery_id, reason="invalid destination")
    assert repository.list_outbox(state="dead_letter") == []


def test_explicit_key_cannot_cross_tenant_scope(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(tmp_path / "governance.sqlite3")
    spec = al.ToolSpec(name="mutate")
    first = _context().model_copy(update={"tenant_id": "tenant-a"})
    second = _context().model_copy(update={"tenant_id": "tenant-b", "call_id": "c2"})
    assert repository.begin(
        "business-key", call_id="c1", context=first, spec=spec, args_hash="same",
        tool_schema_hash="same", owner_id="w1", lease_seconds=30,
    )[0] == "created"
    assert repository.begin(
        "business-key", call_id="c2", context=second, spec=spec, args_hash="same",
        tool_schema_hash="same", owner_id="w2", lease_seconds=30,
    )[0] == "conflict"


def test_public_repository_contract_is_directly_runnable(tmp_path: Path) -> None:
    report = al.verify_repository_contract(
        al.SQLiteGovernanceRepository(tmp_path / "contract.sqlite3")
    )
    assert all(report.values())
    assert {"begin", "conflict", "finish", "outbox", "heartbeat", "runtime_approval", "runtime_terminal", "runtime_expiry"} == set(report)


def test_dispatcher_background_lifecycle_reports_health() -> None:
    class EmptyRepository:
        def claim_outbox(self, **kwargs: object) -> list[object]:
            return []

    dispatcher = OutboxDispatcher(EmptyRepository(), MemorySink())
    dispatcher.start(interval_seconds=0.01, batch_size=2)
    time.sleep(0.03)
    assert dispatcher.health()["running"] is True
    assert dispatcher.stop(timeout=1)
    assert dispatcher.health()["running"] is False


def test_artifact_read_requires_authorization_and_verifies_both_checksums(tmp_path: Path) -> None:
    class Cipher:
        provider_id = "test-kms"
        key_version = "v2"

        def encrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return b"enc:" + payload

        def decrypt(self, payload: bytes, *, context: dict[str, object]) -> bytes:
            return payload.removeprefix(b"enc:")

    class Authorizer:
        def __init__(self, allowed: bool):
            self.allowed = allowed

        def authorize(self, artifact: object, *, context: dict[str, object]) -> bool:
            return self.allowed

    policy = al.ArtifactPolicy(encryption="provider", policy_id="restricted")
    denied = FileArtifactStore(
        tmp_path, policy=policy, encryption_provider=Cipher(), authorizer=Authorizer(False)
    )
    artifact = denied.put("classified", metadata={"project": "demo"})
    with pytest.raises(ArtifactAccessDenied):
        denied.read(artifact, context={"actor_id": "u1", "project": "demo"})

    allowed = FileArtifactStore(
        tmp_path, policy=policy, encryption_provider=Cipher(), authorizer=Authorizer(True)
    )
    assert allowed.read(artifact, context={"actor_id": "u1", "project": "demo"}) == b"classified"
    Path(artifact.uri).write_bytes(b"tampered")
    with pytest.raises(ArtifactPolicyError, match="ciphertext checksum"):
        allowed.read(artifact, context={"actor_id": "u1", "project": "demo"})


def test_reference_only_rejects_credential_bearing_uri(tmp_path: Path) -> None:
    lens = al.ActionLens(
        storage_dir=tmp_path, sink=MemorySink(),
        artifact_policy=al.ArtifactPolicy(raw_mode="reference_only"),
    )

    @lens.tool
    def external() -> al.ArtifactRef:
        return al.ArtifactRef(
            uri="https://user:password@example.com/a?token=secret",
            media_type="text/plain", size_bytes=1, sha256="0" * 64,
        )

    output = external()
    assert output.status == "FAILED"
    assert output.error_taxonomy == "ArtifactPolicyDenied"


def test_webhook_key_rotation_and_receiver_verification() -> None:
    requests: list[object] = []

    def opener(request: object, *, timeout: float) -> SimpleNamespace:
        requests.append(request)
        return SimpleNamespace(status=204, close=lambda: None)

    WebhookSink(
        "https://hooks.example.test/events", secret=lambda: ("2026-07", "rotated"),
        opener=opener, host_allowlist={"hooks.example.test"},
    ).emit(_event("evt-rotation"))
    request = requests[0]
    headers = dict(request.headers)
    payload = request.data
    assert verify_webhook_signature(payload, headers, secrets={"2026-07": "rotated"})
    assert not verify_webhook_signature(
        payload, headers, secrets={"2026-07": "rotated"}, seen_event=lambda event_id: True
    )
    assert not verify_webhook_signature(
        payload, headers, secrets={"2026-07": "rotated"}, now=time.time() + 600
    )


@pytest.mark.parametrize(
    "endpoint",
    ["http://example.com/hook", "https://user:pass@example.com/hook"],
)
def test_webhook_rejects_unsafe_endpoint(endpoint: str) -> None:
    with pytest.raises(ValueError):
        WebhookSink(endpoint, secret=lambda: "x")
