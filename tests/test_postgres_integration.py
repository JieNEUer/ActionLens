from __future__ import annotations

import concurrent.futures
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

import actionlens as al
from actionlens.models import ToolSpec, TrajectoryEvent
from actionlens.outbox import OutboxDispatcher
from actionlens.sinks import MemorySink


psycopg = pytest.importorskip("psycopg")


def _context(suffix: str) -> al.ToolCallContext:
    return al.ToolCallContext(
        project="postgres-integration",
        environment="test",
        tenant_id="tenant-integration",
        session_id=f"session-{suffix}",
        run_id=f"run-{suffix}",
        call_id=f"call-{suffix}",
        tool_name="mutate",
    )


def _event(event_id: str, suffix: str) -> TrajectoryEvent:
    return TrajectoryEvent(
        event_id=event_id,
        timestamp=datetime.now(timezone.utc),
        project="postgres-integration",
        session_id=f"session-{suffix}",
        run_id=f"run-{suffix}",
        sequence=1,
        event_type="test.postgres.integration",
        phase="POST_FLIGHT",
        call_id=f"call-{suffix}",
        tool_name="mutate",
    )


@pytest.fixture(autouse=True)
def _clean_isolated_cluster(isolated_postgres_dsn: str) -> None:
    def clean() -> None:
        with psycopg.connect(isolated_postgres_dsn, autocommit=True) as connection:
            connection.execute("DELETE FROM actionlens_outbox")
            connection.execute("DELETE FROM actionlens_approval_tickets")
            connection.execute("DELETE FROM actionlens_governance_ledger")
            connection.execute("DROP TABLE IF EXISTS actionlens_test_receiver")

    repository = al.PostgresGovernanceRepository(isolated_postgres_dsn, auto_migrate=True)
    repository.close()
    clean()
    yield
    clean()


def _repository(dsn: str, **kwargs: object) -> al.PostgresGovernanceRepository:
    options: dict[str, object] = {
        "auto_migrate": True,
        "min_pool_size": 0,
        "max_pool_size": 4,
        "pool_timeout": 0.5,
    }
    options.update(kwargs)
    return al.PostgresGovernanceRepository(dsn, **options)


def _completed_record(
    repository: al.PostgresGovernanceRepository, *, suffix: str
) -> tuple[str, str]:
    key = f"outbox-{suffix}"
    event_id = f"event-{suffix}"
    kind, record = repository.begin(
        key,
        call_id=f"call-{suffix}",
        context=_context(suffix),
        spec=ToolSpec(name="mutate"),
        args_hash="args",
        tool_schema_hash="schema",
        owner_id="worker",
        lease_seconds=30,
    )
    assert kind == "created"
    repository.finish(
        key,
        owner_id="worker",
        fencing_token=record.fencing_token,
        status="SUCCEEDED",
        output={"status": "SUCCESS", "result_summary": "ok"},
        error=None,
        event=_event(event_id, suffix),
    )
    return key, event_id


def test_postgres_pool_exhaustion_recovers_after_connection_release(
    isolated_postgres_dsn: str,
) -> None:
    repository = _repository(isolated_postgres_dsn, max_pool_size=1, pool_timeout=0.05)
    try:
        with repository._connection():
            with pytest.raises(Exception) as raised:
                repository.get_ledger("pool-exhausted")
        assert type(raised.value).__name__ == "PoolTimeout"
        assert repository.get_ledger("pool-exhausted") is None
    finally:
        repository.close()


def test_postgres_lock_timeout_preserves_preexisting_ledger_fact(
    isolated_postgres_dsn: str,
) -> None:
    repository = _repository(isolated_postgres_dsn, lock_timeout_ms=100)
    suffix = uuid4().hex
    key = f"locked-{suffix}"
    context = _context(suffix)
    spec = ToolSpec(name="mutate")
    try:
        kind, record = repository.begin(
            key,
            call_id=context.call_id,
            context=context,
            spec=spec,
            args_hash="args",
            tool_schema_hash="schema",
            owner_id="first-worker",
            lease_seconds=30,
        )
        assert kind == "created"
        with psycopg.connect(isolated_postgres_dsn, autocommit=True) as holder:
            with holder.transaction():
                holder.execute(
                    "SELECT key FROM actionlens_governance_ledger WHERE key=%s FOR UPDATE",
                    (key,),
                )
                with pytest.raises(Exception) as raised:
                    repository.begin(
                        key,
                        call_id=f"retry-{suffix}",
                        context=context,
                        spec=spec,
                        args_hash="args",
                        tool_schema_hash="schema",
                        owner_id="second-worker",
                        lease_seconds=30,
                    )
        assert type(raised.value).__name__ == "LockNotAvailable"
        persisted = repository.get_ledger(key)
        assert persisted is not None
        assert persisted.status == "EXECUTING"
        assert persisted.fencing_token == record.fencing_token
    finally:
        repository.close()


def test_postgres_connection_reset_pool_reconnects(
    isolated_postgres_dsn: str,
) -> None:
    repository = _repository(isolated_postgres_dsn)
    connection_failed = False
    try:
        try:
            with repository._connection() as connection:
                backend_pid = connection.execute("SELECT pg_backend_pid() AS pid").fetchone()["pid"]
                with psycopg.connect(isolated_postgres_dsn, autocommit=True) as terminator:
                    assert terminator.execute(
                        "SELECT pg_terminate_backend(%s)", (backend_pid,)
                    ).fetchone()[0]
                with pytest.raises(Exception):
                    connection.execute("SELECT 1")
                connection_failed = True
        except Exception:
            connection_failed = True
        assert connection_failed

        for _ in range(20):
            try:
                assert repository.get_ledger("after-reset") is None
                break
            except Exception:
                time.sleep(0.05)
        else:
            pytest.fail("PostgreSQL pool did not recover after a terminated backend")
    finally:
        repository.close()


def test_postgres_migration_is_concurrent_and_preserves_schema_10_compatibility(
    isolated_postgres_dsn: str,
) -> None:
    repositories = [
        al.PostgresGovernanceRepository(
            isolated_postgres_dsn,
            min_pool_size=0,
            max_pool_size=2,
            verify_schema=False,
        )
        for _ in range(2)
    ]
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(lambda repository: repository.migrate(), repositories))
        assert repositories[0].schema_status()["current"] == 11

        with psycopg.connect(isolated_postgres_dsn, autocommit=True) as connection:
            connection.execute("DELETE FROM actionlens_schema_migrations WHERE version=11")
        assert repositories[0].verify_schema()["current"] == 10
        repositories[1].migrate()
        assert repositories[0].verify_schema()["current"] == 11
    finally:
        for repository in repositories:
            repository.close()


def test_postgres_commit_response_loss_returns_uncertain_with_durable_facts(
    isolated_postgres_dsn: str,
    tmp_path: Path,
) -> None:
    class ResponseLostRepository(al.PostgresGovernanceRepository):
        def finish(self, *args: object, **kwargs: object) -> object:
            super().finish(*args, **kwargs)
            raise ConnectionError("simulated response lost after commit")

    repository = ResponseLostRepository(
        isolated_postgres_dsn,
        auto_migrate=True,
        min_pool_size=0,
        max_pool_size=4,
    )
    calls = 0
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink(), repository=repository)

    @lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
    def mutate() -> str:
        nonlocal calls
        calls += 1
        return "applied"

    try:
        output = mutate(idempotency_key="response-lost")
        assert output.status == "UNCERTAIN"
        assert output.error_taxonomy == "GovernanceCommitUncertain"
        assert calls == 1
        persisted = repository.get_ledger("response-lost")
        assert persisted is not None
        assert persisted.status == "SUCCEEDED"
        assert any(
            item.event.event_type == "tool_call.completed"
            for item in repository.list_outbox(limit=100)
        )
    finally:
        repository.close()


def test_postgres_response_lost_outbox_ack_is_deduplicated_by_persistent_receiver(
    isolated_postgres_dsn: str,
) -> None:
    class PersistentReceiver:
        strict = True

        def __init__(self, dsn: str) -> None:
            self.dsn = dsn
            self.attempts = 0
            with psycopg.connect(dsn, autocommit=True) as connection:
                connection.execute(
                    "CREATE TABLE actionlens_test_receiver (event_id text PRIMARY KEY)"
                )

        def emit(self, event: TrajectoryEvent) -> None:
            self.attempts += 1
            with psycopg.connect(self.dsn, autocommit=True) as connection:
                connection.execute(
                    "INSERT INTO actionlens_test_receiver(event_id) VALUES (%s) "
                    "ON CONFLICT(event_id) DO NOTHING",
                    (event.event_id,),
                )

        def flush(self) -> None:
            return None

        def close(self) -> None:
            return None

    class AckResponseLostRepository:
        def __init__(self, delegate: al.PostgresGovernanceRepository) -> None:
            self.delegate = delegate
            self.ack_attempts = 0

        def __getattr__(self, name: str) -> object:
            return getattr(self.delegate, name)

        def ack_outbox(self, delivery_id: str, *, worker_id: str) -> bool:
            self.ack_attempts += 1
            if self.ack_attempts == 1:
                return False
            return self.delegate.ack_outbox(delivery_id, worker_id=worker_id)

    repository = _repository(isolated_postgres_dsn)
    suffix = uuid4().hex
    _, event_id = _completed_record(repository, suffix=suffix)
    receiver = PersistentReceiver(isolated_postgres_dsn)
    response_lost = AckResponseLostRepository(repository)
    try:
        first = OutboxDispatcher(response_lost, receiver, claim_seconds=0.01)
        assert first.dispatch_once()["delivered"] == 0
        time.sleep(0.03)
        second = OutboxDispatcher(response_lost, receiver, claim_seconds=0.01)
        assert second.dispatch_once()["delivered"] == 1
        assert receiver.attempts == 2
        with psycopg.connect(isolated_postgres_dsn) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM actionlens_test_receiver WHERE event_id=%s", (event_id,)
            ).fetchone()[0]
        assert count == 1
        assert repository.outbox_stats()["delivered"] == 1
    finally:
        repository.close()


def test_postgres_claim_query_uses_the_ready_partial_index(
    isolated_postgres_dsn: str,
) -> None:
    repository = _repository(isolated_postgres_dsn)
    try:
        with psycopg.connect(isolated_postgres_dsn, autocommit=True) as connection:
            connection.execute(
                """INSERT INTO actionlens_outbox
                   (delivery_id,event_id,event_json,next_retry_at,delivered_at,created_at)
                   SELECT 'delivered-' || item, 'delivered-event-' || item, '{}'::jsonb,
                          now(), now(), now()
                   FROM generate_series(1, 2000) AS item"""
            )
            connection.execute(
                """INSERT INTO actionlens_outbox
                   (delivery_id,event_id,event_json,next_retry_at,created_at)
                   SELECT 'ready-' || item, 'ready-event-' || item, '{}'::jsonb,
                          now(), now()
                   FROM generate_series(1, 20) AS item"""
            )
            connection.execute("ANALYZE actionlens_outbox")
            connection.execute("SET enable_seqscan = off")
            rows = connection.execute(
                """EXPLAIN (ANALYZE, BUFFERS)
                   WITH candidates AS (
                     SELECT delivery_id FROM actionlens_outbox
                     WHERE delivered_at IS NULL AND next_retry_at <= now()
                       AND dead_letter_at IS NULL AND terminated_at IS NULL
                       AND (claim_expires_at IS NULL OR claim_expires_at <= now())
                     ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 10
                   )
                   UPDATE actionlens_outbox AS outbox
                   SET claimed_by='query-plan', claim_expires_at=now() + interval '30 seconds'
                   FROM candidates WHERE outbox.delivery_id=candidates.delivery_id"""
            ).fetchall()
            connection.execute("RESET enable_seqscan")
        plan = "\n".join(row[0] for row in rows)
        assert "idx_al_outbox_claim_v11" in plan
    finally:
        repository.close()
