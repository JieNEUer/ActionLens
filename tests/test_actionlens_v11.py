from __future__ import annotations

import asyncio
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from typing import Any

import pytest

import actionlens as al
from actionlens.artifacts import ArtifactPolicyError, FileArtifactStore
from actionlens.models import TrajectoryEvent
from actionlens.outbox import OutboxDispatcher
from actionlens.repositories.postgres import MIGRATION_LOCK_ID
from actionlens.sinks import MemorySink


def _context() -> al.ToolCallContext:
    return al.ToolCallContext(
        project="v11", session_id="s", run_id="r", call_id="c", tool_name="mutate"
    )


def _event(event_id: str) -> TrajectoryEvent:
    return TrajectoryEvent(
        event_id=event_id, timestamp=datetime.now(timezone.utc), project="v11",
        session_id="s", run_id="r", sequence=1, event_type="test.v11",
        phase="POST_FLIGHT", call_id="c", tool_name="mutate",
    )


def test_async_governance_io_does_not_block_event_loop(tmp_path: Path) -> None:
    entered, release = Event(), Event()
    class SlowRepository(al.SQLiteGovernanceRepository):
        blocking = False

        def begin(self, *args: Any, **kwargs: Any) -> Any:
            self.blocking = True
            try:
                entered.set()
                assert release.wait(5), "event loop did not release governance I/O"
                return super().begin(*args, **kwargs)
            finally:
                self.blocking = False

    repository = SlowRepository(tmp_path / "governance.sqlite3")
    lens = al.ActionLens(storage_dir=tmp_path / "lens", sink=MemorySink(), repository=repository)

    @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
    async def mutate() -> str:
        await asyncio.sleep(0)
        return "ok"

    async def exercise() -> tuple[al.StructuredToolOutput, bool]:
        observed = False

        async def ticker() -> None:
            nonlocal observed
            while not entered.is_set():
                await asyncio.sleep(0.001)
            observed = repository.blocking
            release.set()

        task = asyncio.create_task(ticker())
        output = await mutate(idempotency_key="v11-key")
        await task
        return output, observed

    output, observed = asyncio.run(exercise())
    assert output.status == "SUCCESS"
    assert observed, "event loop did not advance while governance repository was blocked"


def test_governance_failure_is_classified_by_execution_phase(tmp_path: Path) -> None:
    called = 0

    class BeginFails(al.SQLiteGovernanceRepository):
        def begin(self, *args: Any, **kwargs: Any) -> Any:
            raise TimeoutError("pool exhausted")

    before_lens = al.ActionLens(
        storage_dir=tmp_path / "before", sink=MemorySink(),
        repository=BeginFails(tmp_path / "before.sqlite3"),
    )

    @before_lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
    def before() -> str:
        nonlocal called
        called += 1
        return "done"

    before_output = before(idempotency_key="before")
    assert before_output.status == "FAILED"
    assert before_output.error_taxonomy == "GovernanceUnavailable"
    assert called == 0

    class FinishFails(al.SQLiteGovernanceRepository):
        def finish(self, *args: Any, **kwargs: Any) -> Any:
            raise OSError("connection reset during commit")

    after_lens = al.ActionLens(
        storage_dir=tmp_path / "after", sink=MemorySink(),
        repository=FinishFails(tmp_path / "after.sqlite3"),
    )

    @after_lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
    def after() -> str:
        nonlocal called
        called += 1
        return "done"

    after_output = after(idempotency_key="after")
    assert after_output.status == "UNCERTAIN"
    assert after_output.error_taxonomy == "GovernanceCommitUncertain"
    assert called == 1


class _Result:
    def __init__(self, row: dict[str, Any] | None = None):
        self.row = row

    def fetchone(self) -> dict[str, Any] | None:
        return self.row


class _FakeConnection:
    def __init__(self, version: int = 11) -> None:
        self.calls: list[tuple[str, object]] = []
        self.version = version

    @contextmanager
    def transaction(self):
        yield

    def execute(self, sql: str, args: object = None) -> _Result:
        self.calls.append((sql, args))
        if "to_regclass" in sql:
            return _Result({"name": "actionlens_schema_migrations"})
        if "MAX(version)" in sql:
            return _Result({"version": self.version})
        return _Result({})


class _FakePool:
    def __init__(self, version: int = 11) -> None:
        self.conn = _FakeConnection(version)

    @contextmanager
    def connection(self):
        yield self.conn

    def get_stats(self) -> dict[str, int]:
        return {"pool_size": 2, "pool_available": 1, "requests_waiting": 0}


def test_postgres_pool_schema_guard_and_migration_lock() -> None:
    pool = _FakePool()
    repository = al.PostgresGovernanceRepository(
        "postgresql://unused", pool=pool, verify_schema=True
    )
    assert repository.verify_schema()["current"] == 11
    assert repository.pool_stats()["pool_available"] == 1
    repository.migrate()
    assert any(
        "pg_advisory_xact_lock" in sql and args == (MIGRATION_LOCK_ID,)
        for sql, args in pool.conn.calls
    )
    with pytest.raises(al.SchemaCompatibilityError, match="outside supported range"):
        al.PostgresGovernanceRepository(
            "postgresql://unused", pool=_FakePool(version=9), verify_schema=True
        )


def test_postgres_injected_pool_does_not_require_optional_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psycopg", None)
    repository = al.PostgresGovernanceRepository(
        "postgresql://unused", pool=_FakePool(), verify_schema=False
    )
    assert repository.pool_stats()["pooled"] == 1


def test_outbox_stats_retention_and_readiness(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(tmp_path / "outbox.sqlite3")
    _, record = repository.begin(
        "k", call_id="c", context=_context(), spec=al.ToolSpec(name="mutate"),
        args_hash="a", tool_schema_hash="s", owner_id="w", lease_seconds=30,
    )
    repository.finish(
        "k", owner_id="w", fencing_token=record.fencing_token, status="SUCCEEDED",
        output={}, error=None, event=_event("pending"),
    )
    time.sleep(0.01)
    dispatcher = OutboxDispatcher(repository, MemorySink(), readiness_lag_seconds=0.001)
    health = dispatcher.health()
    assert health["ready"] is False
    assert health["outbox"]["pending"] == 1

    assert dispatcher.dispatch_once()["delivered"] == 1
    assert repository.outbox_stats()["delivered"] == 1
    deleted = repository.cleanup_outbox(
        delivered_before=datetime.now(timezone.utc) + timedelta(seconds=1), limit=10
    )
    assert deleted == 1

    class Unavailable:
        def outbox_stats(self) -> dict[str, int]:
            raise OSError("database unavailable")

    unavailable = OutboxDispatcher(Unavailable(), MemorySink()).health()
    assert unavailable["ready"] is False
    assert unavailable["stats_error"] == "OSError"


def test_sqlite_durability_mode_is_explicit(tmp_path: Path) -> None:
    repository = al.SQLiteGovernanceRepository(
        tmp_path / "normal.sqlite3", synchronous="NORMAL", pool_size=1,
        acquire_timeout=0.01,
    )
    with repository._connect() as connection:
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 1
        with pytest.raises(TimeoutError, match="pool exhausted"):
            repository.get_ledger("missing")
    assert repository.pool_stats()["connections_available"] == 1
    repository.close()
    with pytest.raises(RuntimeError, match="closed"):
        repository.get_ledger("missing")
    with pytest.raises(ValueError, match="FULL or NORMAL"):
        al.SQLiteGovernanceRepository(tmp_path / "invalid.sqlite3", synchronous="OFF")


def test_artifact_gc_skips_active_reader_lease_and_serializes_gc(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    artifact = store.put(b"payload")
    path = Path(artifact.uri)
    os.utime(path, (1, 1))
    reader_lease = store.leases_dir / f"{path.name}.reader.lease"
    reader_lease.touch()
    assert store.gc(older_than_seconds=0)["deleted"] == 0
    assert path.exists()
    reader_lease.unlink()

    gc_lock = store.leases_dir / "gc.lock"
    gc_lock.touch()
    with pytest.raises(ArtifactPolicyError, match="already running"):
        store.gc(older_than_seconds=0)
    gc_lock.unlink()
    assert store.gc(older_than_seconds=0)["deleted"] == 1


def test_artifact_gc_rejects_invalid_bounds(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    with pytest.raises(ValueError):
        store.gc(older_than_seconds=-1)
    with pytest.raises(ValueError):
        store.gc(older_than_seconds=0, max_bytes=-1)


def test_artifact_read_rejects_symlink_or_reparse_point(tmp_path: Path) -> None:
    class Allow:
        def authorize(self, artifact: object, *, context: dict[str, Any]) -> bool:
            return True

    store = FileArtifactStore(tmp_path, authorizer=Allow())
    artifact = store.put(b"payload")
    link = Path(artifact.uri).with_name("artifact-link.bin")
    try:
        link.symlink_to(Path(artifact.uri))
    except OSError:
        pytest.skip("creating symlinks is not permitted on this Windows host")
    linked = artifact.model_copy(update={"uri": str(link)})
    with pytest.raises(ArtifactPolicyError, match="symlink or reparse"):
        store.read(linked)


def test_artifact_inspect_preserves_missing_status(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    artifact = store.put(b"payload")
    Path(artifact.uri).unlink()
    assert store.inspect(artifact)["status"] == "missing"
