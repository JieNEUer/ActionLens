from __future__ import annotations

import os
from collections.abc import Iterator
from urllib.parse import urlsplit

import pytest


@pytest.fixture(scope="session")
def isolated_postgres_dsn() -> Iterator[str]:
    """Return the only database endpoint integration tests are allowed to mutate."""

    dsn = os.environ.get("ACTIONLENS_POSTGRES_DSN")
    if not dsn:
        pytest.skip("isolated PostgreSQL DSN is not configured")
    try:
        parsed = urlsplit(dsn)
        is_isolated = (
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.hostname == "127.0.0.1"
            and parsed.port == 55432
            and bool(parsed.path.strip("/"))
        )
    except ValueError:
        is_isolated = False
    if not is_isolated:
        pytest.skip(
            "refusing PostgreSQL integration tests unless ACTIONLENS_POSTGRES_DSN targets "
            "the isolated 127.0.0.1:55432 cluster"
        )
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    import psycopg

    from actionlens.repositories.postgres import PostgresGovernanceRepository

    repository = PostgresGovernanceRepository(dsn, auto_migrate=True)
    repository.close()

    def clean() -> None:
        # Outbox claims are cluster-wide. A previous test process must not
        # leave older pending events ahead of this session's test events.
        with psycopg.connect(dsn) as connection:
            connection.execute("DELETE FROM actionlens_outbox")
            connection.execute("DELETE FROM actionlens_approval_tickets")
            connection.execute("DELETE FROM actionlens_governance_ledger")

    clean()
    try:
        yield dsn
    finally:
        clean()
