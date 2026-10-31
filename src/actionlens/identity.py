"""Shared event identity; absent scope is an explicit legacy namespace."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

IDENTITY_VERSION = "actionlens.identity.v1"


def run_identity(event: Mapping[str, Any]) -> tuple[Any, ...]:
    known = event.get("identity_version") == IDENTITY_VERSION
    return (
        IDENTITY_VERSION if known else "legacy-unknown",
        event.get("project"),
        event.get("environment") if known else None,
        event.get("tenant_id") if known else None,
        event.get("session_id"),
        event.get("run_id"),
    )


def call_identity(event: Mapping[str, Any]) -> tuple[Any, ...]:
    return (*run_identity(event), event.get("call_id"), event.get("actor_id"))


def identity_fields(event: Mapping[str, Any]) -> dict[str, Any]:
    version, project, environment, tenant_id, session_id, run_id = run_identity(event)
    return dict(
        identity_version=version,
        project=project,
        environment=environment,
        tenant_id=tenant_id,
        session_id=session_id,
        run_id=run_id,
    )
