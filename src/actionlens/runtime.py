from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import copy
import functools
import inspect
import json
import os
import socket
import tempfile
import threading
import time
import warnings
from collections import OrderedDict, deque
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import ExitStack, suppress
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, get_type_hints
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from pydantic import TypeAdapter, ValidationError

from .artifacts import ArtifactPolicyError, FileArtifactStore, MediaMetadataExtractor
from .context import SessionContext, get_current_context, make_generated_context
from .errors import NoSideEffectError, classify_exception, recovery_hint
from .ledger import (
    SQLiteApprovalTicketStore,
    SQLiteLedger,
)
from .models import (
    ApprovalResolution,
    ApprovalTicket,
    ArtifactPolicy,
    ArtifactRef,
    ConcurrencyPolicy,
    ErrorRecord,
    IdempotencyPolicy,
    OutputPolicy,
    PolicyDecision,
    RiskLevel,
    StructuredToolOutput,
    ToolCallContext,
    ToolSpec,
    TrajectoryEvent,
)
from .outbox import OutboxDispatcher
from .policy import BudgetPolicy, Policy, PolicyChain
from .reconciliation import ReconciliationResult, SideEffectReconciler
from .redaction import Redactor, redact_value
from .repositories import SQLiteGovernanceRepository
from .repository import (
    RepositoryLedgerView,
    RepositoryTicketView,
    canonical_operation_hash,
)
from .sinks import JsonlSink

_INVOCATION_METADATA = {
    "actionlens_idempotency_key", "actionlens_args_hash", "actionlens_tool_schema_hash",
    "actionlens_owner_id", "actionlens_fencing_token", "actionlens_sync_approval_resuming",
    "actionlens_background_execution_may_continue",
    "actionlens_effective_args_hash",
}


class ActionLens:
    def __init__(
        self,
        *,
        project: str = "default",
        storage_dir: str | Path = ".actionlens",
        sink: Any | None = None,
        artifact_store: FileArtifactStore | None = None,
        artifact_policy: ArtifactPolicy | None = None,
        encryption_provider: Any | None = None,
        artifact_authorizer: Any | None = None,
        media_metadata_extractor: MediaMetadataExtractor | None = None,
        ledger: Any | None = None,
        ticket_store: Any | None = None,
        repository: Any | None = None,
        policies: list[Policy] | None = None,
        redactor: Redactor | None = None,
        approval_resolver: Callable[[ApprovalTicket, ToolCallContext], Any] | None = None,
        sync_max_workers: int = 8,
        ticket_cache_max_entries: int = 10_000,
    ) -> None:
        if sync_max_workers <= 0:
            raise ValueError("sync_max_workers must be positive")
        if ticket_cache_max_entries <= 0:
            raise ValueError("ticket_cache_max_entries must be positive")
        self.project = project
        self.storage_dir = Path(storage_dir)
        if artifact_store is not None and media_metadata_extractor is not None:
            raise ValueError(
                "configure media_metadata_extractor on the supplied artifact_store"
            )
        self.artifact_store = artifact_store or FileArtifactStore(
            self.storage_dir,
            policy=artifact_policy,
            encryption_provider=encryption_provider,
            authorizer=artifact_authorizer,
            media_metadata_extractor=media_metadata_extractor,
        )
        self.sink = sink or JsonlSink(self.storage_dir)
        default_db = self.storage_dir / "ledger" / "actionlens.sqlite3"
        self.repository = repository
        self._owns_repository = repository is None and ledger is None and ticket_store is None
        if repository is None and ledger is None and ticket_store is None:
            self.repository = SQLiteGovernanceRepository(default_db)
        if self.repository is not None:
            self.ledger = RepositoryLedgerView(self.repository)
            self.ticket_store = RepositoryTicketView(self.repository)
            self.outbox_dispatcher = OutboxDispatcher(self.repository, self.sink)
        else:
            self.ledger = ledger or SQLiteLedger(default_db)
            self.ticket_store = ticket_store or SQLiteApprovalTicketStore(default_db)
            self.outbox_dispatcher = None
        self.policy_chain = PolicyChain(policies)
        self.redactor = redactor
        self.approval_resolver = approval_resolver
        self._sequence = 0
        self._sequence_lock = threading.Lock()
        self._event_origin = (
            f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:12]}"
        )
        self._sink_error_count = 0
        self._tickets: OrderedDict[str, str] = OrderedDict()
        self._ticket_lock = threading.RLock()
        self._ticket_cache_max_entries = ticket_cache_max_entries
        self._runtimes: dict[str, ToolRuntime] = {}
        self._artifact_navigation_tools: dict[str, Callable[..., Any]] | None = None
        self._sync_max_workers = sync_max_workers
        self._sync_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._executor_lock = threading.Lock()
        self._closed = False

    def session(
        self,
        *,
        session_id: str,
        run_id: str | None = None,
        actor_id: str | None = None,
        environment: str = "default",
        tenant_id: str | None = None,
        framework: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SessionContext:
        context = ToolCallContext(
            project=self.project,
            environment=environment,
            tenant_id=tenant_id,
            session_id=session_id,
            run_id=run_id or f"run-{uuid4().hex}",
            call_id=f"call-{uuid4().hex}",
            tool_name="",
            actor_id=actor_id,
            framework=framework,
            context_source="contextvar",
            metadata=metadata or {},
        )
        return SessionContext(context)

    def reset_run(self, run_id: str, *, context: ToolCallContext | None = None) -> None:
        """Release built-in in-memory accounting after a host-owned run ends.

        ActionLens deliberately does not infer completion from a session scope:
        durable hosts may resume the same run in a later process. Call this only
        after the host has finished all calls and artifact writes for ``run_id``.
        """

        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be a non-empty string")
        run_id = run_id.strip()
        if context is None:
            self.artifact_store.reset_run(run_id)
        else:
            self.artifact_store.reset_run(run_id, metadata=self._budget_scope_metadata(context))
        for policy in self.policy_chain.policies:
            if isinstance(policy, BudgetPolicy):
                policy.reset_run(project=context.project if context else self.project, run_id=run_id, context=context)

    @staticmethod
    def _budget_scope_metadata(context: ToolCallContext) -> dict[str, Any]:
        return {"project": context.project, "environment": context.environment, "tenant_id": context.tenant_id}

    def capabilities(self) -> dict[str, Any]:
        """Expose the active persistence and resource-accounting guarantees."""
        return {
            "atomic_governance": self.repository is not None,
            "distributed_lease": self.repository is not None,
            "durable_outbox": self.outbox_dispatcher is not None,
            "call_budget_scope": "local_instance",
            "artifact_budget_scope": "local_instance",
            "batch_resume_owner": "host",
        }

    def child_context(
        self,
        *,
        parent: ToolCallContext | None = None,
        tool_name: str = "",
        metadata: dict[str, Any] | None = None,
        call_id: str | None = None,
    ) -> ToolCallContext:
        """Create a child call context which shares its parent's run budget.

        Child contexts preserve project, tenant, session, run, actor, and
        framework identity. The new call id is linked through
        ``parent_call_id`` so exporters can reconstruct a call tree without
        assigning orchestration ownership to ActionLens.
        """

        base = parent or get_current_context()
        if base is None:
            raise RuntimeError("child_context requires an explicit or active parent context")
        return base.model_copy(
            update={
                "call_id": call_id or f"call-{uuid4().hex}",
                "parent_call_id": base.call_id,
                "tool_name": tool_name,
                "context_source": "explicit",
                "metadata": {**base.metadata, **(metadata or {})},
            }
        )

    def child_session(
        self,
        *,
        parent: ToolCallContext | None = None,
        tool_name: str = "",
        metadata: dict[str, Any] | None = None,
        call_id: str | None = None,
    ) -> SessionContext:
        return SessionContext(
            self.child_context(
                parent=parent,
                tool_name=tool_name,
                metadata=metadata,
                call_id=call_id,
            )
        )

    def tool(
        self,
        func: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        description: str | None = None,
        risk: RiskLevel | str = RiskLevel.READ,
        idempotency: IdempotencyPolicy | str = IdempotencyPolicy.OFF,
        idempotency_key_param: str = "idempotency_key",
        idempotency_key_fn: Callable[[dict[str, Any], ToolCallContext], str] | None = None,
        hash_ignore_keys: list[str] | None = None,
        timeout_sec: float | None = None,
        run_sync_in_thread: bool = False,
        cache_ttl_sec: float = 300.0,
        max_bytes: int | None = None,
        output: OutputPolicy | None = None,
        approval_required: bool = False,
        approval_ttl_sec: float | None = 86400.0,
        concurrency: ConcurrencyPolicy | str = ConcurrencyPolicy.UNKNOWN,
        lease_seconds: float = 30.0,
        fencing_supported: bool = False,
        validation_mode: Literal["strict", "coerce", "passthrough"] = "strict",
    ):
        def decorate(target: Callable[..., Any]):
            policy = output or OutputPolicy()
            if max_bytes is not None:
                policy = policy.model_copy(
                    update={"max_inline_bytes": max_bytes}
                )
            spec = ToolSpec(
                name=name or target.__name__,
                description=description or inspect.getdoc(target),
                risk=RiskLevel(risk),
                idempotency=IdempotencyPolicy(idempotency),
                idempotency_key_param=idempotency_key_param,
                idempotency_key_fn=idempotency_key_fn,
                hash_ignore_keys=hash_ignore_keys or [],
                timeout_sec=timeout_sec,
                run_sync_in_thread=run_sync_in_thread,
                cache_ttl_sec=cache_ttl_sec,
                concurrency=ConcurrencyPolicy(concurrency),
                approval_required=approval_required,
                approval_ttl_sec=approval_ttl_sec,
                lease_seconds=lease_seconds,
                fencing_supported=fencing_supported,
                validation_mode=validation_mode,
                output=policy,
            )
            return self.wrap(target, spec=spec)

        if func is not None:
            return decorate(func)
        return decorate

    def wrap(self, func: Callable[..., Any], *, spec: ToolSpec) -> Callable[..., Any]:
        if spec.name in self._runtimes:
            raise ValueError(f"a tool named {spec.name!r} is already registered")
        if (
            spec.run_sync_in_thread
            and spec.timeout_sec is not None
            and spec.risk in {RiskLevel.MUTATION, RiskLevel.DESTRUCTIVE}
        ):
            warnings.warn(
                "run_sync_in_thread timeouts cannot stop a running synchronous "
                "function; high-risk timeouts are recorded as UNCERTAIN and the "
                "business operation may still be running",
                UserWarning,
                stacklevel=2,
            )
        runtime = ToolRuntime(self, func, spec)
        self._runtimes[spec.name] = runtime
        return runtime.as_callable()

    def remote_tool(self, runner: Any, **kwargs: Any) -> Callable[..., Any]:
        """Adapt a ``RemoteToolRunner`` into the normal governed tool path.

        The adapter is imported lazily so the core runtime does not acquire a
        dependency on a particular remote transport implementation.
        """

        from .integrations.remote import RemoteToolAdapter

        return RemoteToolAdapter(self, runner, **kwargs).as_callable()

    def approve(
        self,
        *,
        ticket_id: str | None = None,
        key: str | None = None,
        approved_by: str | None = None,
        decision_note: str | None = None,
        modified_args: dict[str, Any] | None = None,
    ) -> bool:
        if ticket_id is None and key is not None:
            record = self.ledger.get(key)
            if record is not None and record.ticket_id:
                ticket_id = record.ticket_id
        if ticket_id is not None:
            current = self.ticket_store.get(ticket_id)
            if current is None:
                return False
            if current.status == "EXPIRED":
                if self.repository is not None:
                    event = self._make_ticket_event(current, "approval.expired")
                    self.repository.expire_ticket(ticket_id, event=event)
                    self.dispatch_outbox()
                elif current.idempotency_key:
                    self.ledger.fail(current.idempotency_key)
                    self._emit_ticket_event(current, "approval.expired")
                return False
            if current.status != "PENDING":
                return False
            self._validate_approval_args(current, modified_args)
            runtime = self._runtimes.get(current.tool_name)
            ticket_context = self._context_from_ticket(current)
            safe_note = redact_value(
                decision_note,
                patterns=[r"sk-[A-Za-z0-9_-]+", r"(?i)bearer\s+\S+"],
            )
            if runtime is not None:
                safe_note = runtime.redact(safe_note, ticket_context)
            if self.repository is not None:
                proposed = current.model_copy(update={
                    "status": "APPROVED", "approved_by": approved_by,
                    "decision_note": safe_note, "modified_args": modified_args,
                    "approved_at": datetime.now(timezone.utc),
                })
                event = self._make_ticket_event(proposed, "approval.approved")
                ticket = self.repository.decide_approval(
                    ticket_id, status="APPROVED", approved_by=approved_by,
                    decision_note=safe_note, modified_args=modified_args, event=event,
                )
                if ticket is None:
                    return False
                self.dispatch_outbox()
                return True
            ticket = self.ticket_store.approve(
                ticket_id,
                approved_by=approved_by,
                decision_note=safe_note,
                modified_args=modified_args,
            )
            if ticket is not None and ticket.status != "APPROVED":
                return False
            key = ticket.idempotency_key if ticket is not None else self._ticket_key(ticket_id)
        if key is None:
            return False
        approved = self.ledger.approve(key) is not None
        if approved and ticket_id is not None and ticket is not None:
            self._emit_ticket_event(ticket, "approval.approved")
        return approved

    def deny(
        self, *, ticket_id: str, decision_note: str | None = None
    ) -> bool:
        current = self.ticket_store.get(ticket_id)
        if current is None or current.status != "PENDING":
            return False
        runtime = self._runtimes.get(current.tool_name)
        context = self._context_from_ticket(current)
        safe_note = redact_value(
            decision_note,
            patterns=[r"sk-[A-Za-z0-9_-]+", r"(?i)bearer\s+\S+"],
        )
        if runtime is not None:
            safe_note = runtime.redact(safe_note, context)
        if self.repository is not None:
            proposed = current.model_copy(
                update={"status": "DENIED", "decision_note": safe_note}
            )
            event = self._make_ticket_event(proposed, "approval.denied")
            ticket = self.repository.decide_approval(
                ticket_id, status="DENIED", approved_by=None,
                decision_note=safe_note, modified_args=None, event=event,
            )
            if ticket is None:
                return False
            self.dispatch_outbox()
            return True
        ticket = self.ticket_store.deny(ticket_id, decision_note=safe_note)
        if ticket is None or ticket.status != "DENIED":
            return False
        if ticket.idempotency_key:
            self.ledger.fail(ticket.idempotency_key)
        self._emit_ticket_event(ticket, "approval.denied")
        return True

    def get_ticket(self, ticket_id: str) -> ApprovalTicket | None:
        return self.ticket_store.get(ticket_id)

    def tickets(self, *, status: str | None = None) -> list[ApprovalTicket]:
        return self.ticket_store.list(status=status)

    def _resolve_sync_approval(
        self, ticket: ApprovalTicket, context: ToolCallContext
    ) -> ApprovalResolution:
        resolver = self.approval_resolver
        if resolver is None:
            return ApprovalResolution(action="PENDING")
        try:
            raw_resolution = resolver(ticket, context)
            if inspect.isawaitable(raw_resolution):
                raise TypeError("approval_resolver must be synchronous")
            if raw_resolution is None:
                resolution = ApprovalResolution(action="PENDING")
            elif isinstance(raw_resolution, bool):
                resolution = ApprovalResolution(
                    action="APPROVE" if raw_resolution else "DENY"
                )
            else:
                resolution = ApprovalResolution.model_validate(raw_resolution)
        except Exception as exc:  # noqa: BLE001 - an approval failure must fail closed.
            self._emit(
                context=context,
                event_type="approval.resolver_failed",
                phase="PRE_FLIGHT",
                metadata={"resolver_error_type": type(exc).__name__},
            )
            return ApprovalResolution(action="PENDING")

        if resolution.action == "APPROVE":
            try:
                approved = self.approve(
                    ticket_id=ticket.ticket_id,
                    approved_by=resolution.approved_by,
                    decision_note=resolution.decision_note,
                    modified_args=resolution.modified_args,
                )
            except Exception as exc:  # noqa: BLE001 - invalid resolver output stays pending.
                self._emit(
                    context=context,
                    event_type="approval.resolver_failed",
                    phase="PRE_FLIGHT",
                    metadata={"resolver_error_type": type(exc).__name__},
                )
                return ApprovalResolution(action="PENDING")
            return resolution if approved else ApprovalResolution(action="PENDING")
        if resolution.action == "DENY":
            denied = self.deny(
                ticket_id=ticket.ticket_id,
                decision_note=resolution.decision_note,
            )
            return resolution if denied else ApprovalResolution(action="PENDING")
        return resolution

    def _validate_approval_args(
        self, ticket: ApprovalTicket, modified_args: dict[str, Any] | None
    ) -> None:
        if modified_args is None:
            return
        runtime = self._runtimes.get(ticket.tool_name)
        if runtime is not None:
            runtime.validate_modified_args(modified_args)
            return
        allowed = set(ticket.metadata.get("allowed_args", ticket.safe_args))
        unknown = set(modified_args) - allowed
        if unknown:
            raise ValueError(
                f"modified_args contains unknown parameters: {', '.join(sorted(unknown))}"
            )

    def _emit_ticket_event(self, ticket: ApprovalTicket, event_type: str) -> None:
        self._deliver_event(self._make_ticket_event(ticket, event_type))

    def _make_ticket_event(
        self, ticket: ApprovalTicket, event_type: str
    ) -> TrajectoryEvent:
        context = self._context_from_ticket(ticket)
        metadata = {
            "ticket_id": ticket.ticket_id,
            "status": ticket.status,
            "approved_by": ticket.approved_by,
            "decision_note": ticket.decision_note,
        }
        runtime = self._runtimes.get(ticket.tool_name)
        if runtime is not None:
            metadata = runtime.redact(metadata, context)
        else:
            metadata = redact_value(
                metadata,
                keys=["password", "token", "secret", "authorization"],
            )
        return self._make_event(
            context=context,
            event_type=event_type,
            phase="PRE_FLIGHT",
            metadata=metadata,
        )

    def _context_from_ticket(self, ticket: ApprovalTicket) -> ToolCallContext:
        try:
            return ToolCallContext.model_validate(ticket.metadata.get("context", {}))
        except (ValidationError, TypeError):
            return make_generated_context(self.project, ticket.tool_name)

    def invoke_many(
        self,
        calls: list[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]],
        *,
        max_workers: int = 8,
    ) -> list[StructuredToolOutput]:
        """Execute ordered segments; independent safe reads share a segment.

        A pending approval or uncertain effect blocks every later segment.
        The host owns persistence and resumption of the batch manifest.
        """
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        for func, _, _ in calls:
            if not hasattr(func, "actionlens_spec") or inspect.iscoroutinefunction(func):
                raise TypeError("invoke_many requires synchronous governed tools")
        results = []
        index = 0
        def parallel_safe(func):
            spec = func.actionlens_spec
            return spec.risk in {RiskLevel.READ, RiskLevel.EXTERNAL_IO} and spec.concurrency == ConcurrencyPolicy.SAFE
        while index < len(calls):
            end = index + 1
            if parallel_safe(calls[index][0]):
                while end < len(calls) and parallel_safe(calls[end][0]):
                    end += 1
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = [pool.submit(contextvars.copy_context().run, func, *args, **kwargs)
                               for func, args, kwargs in calls[index:end]]
                    segment = [future.result() for future in futures]
            else:
                func, args, kwargs = calls[index]
                segment = [func(*args, **kwargs)]
            results.extend(segment)
            index = end
            if any(item.status in {"PENDING_APPROVAL", "UNCERTAIN"} for item in segment):
                results.extend(StructuredToolOutput(status="SKIPPED", result_summary="Batch paused before this tool started.",
                                                    error_taxonomy="BatchPaused") for _ in calls[index:])
                break
        return results

    def inspect_artifact(
        self, artifact: ArtifactRef, *, context: ToolCallContext | None = None
    ) -> dict[str, Any]:
        result = self.artifact_store.inspect(artifact)
        event_context = context or get_current_context() or make_generated_context(
            self.project, "artifact.inspect"
        )
        safe_uri = _safe_reference(artifact.uri)
        self._emit(
            context=event_context,
            event_type="artifact.accessed",
            phase="POST_FLIGHT",
            metadata={"uri": safe_uri, "status": result["status"], "sha256": artifact.sha256},
        )
        return result

    def read_artifact(
        self, artifact: ArtifactRef, *, context: ToolCallContext | None = None
    ) -> bytes:
        event_context = context or get_current_context() or make_generated_context(
            self.project, "artifact.read"
        )
        status = "ok"
        try:
            return self.artifact_store.read(
                artifact,
                context=event_context.model_dump(mode="json"),
            )
        except Exception as exc:
            status = type(exc).__name__
            raise
        finally:
            safe_uri = _safe_reference(artifact.uri)
            self._emit(
                context=event_context,
                event_type="artifact.accessed",
                phase="POST_FLIGHT",
                metadata={"uri": safe_uri, "status": status, "sha256": artifact.sha256},
            )

    def read_artifact_page(
        self,
        artifact: ArtifactRef,
        *,
        offset: int = 0,
        limit: int = 4096,
        context: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        """Read one authorized artifact page without exposing an unbounded body."""

        if limit <= 0 or limit > 64 * 1024:
            raise ValueError("limit must be between 1 and 65536 bytes")
        event_context = context or get_current_context() or make_generated_context(
            self.project, "artifact.read"
        )
        status = "ok"
        payload = b""
        try:
            payload = self.artifact_store.read_range(
                artifact,
                offset=offset,
                limit=limit,
                context=event_context.model_dump(mode="json"),
            )
            return {
                "offset": offset,
                "next_offset": offset + len(payload),
                "bytes": len(payload),
                "eof": len(payload) < limit,
                "content": payload.decode("utf-8", errors="replace"),
            }
        except Exception as exc:
            status = type(exc).__name__
            raise
        finally:
            self._emit(
                context=event_context,
                event_type="artifact.accessed",
                phase="POST_FLIGHT",
                metadata={
                    "uri": _safe_reference(artifact.uri),
                    "status": status,
                    "sha256": artifact.sha256,
                    "offset": offset,
                    "limit": limit,
                    "returned_bytes": len(payload),
                },
            )

    def grep_artifact(
        self,
        artifact: ArtifactRef,
        needle: str,
        *,
        offset: int = 0,
        scan_bytes: int = 16 * 1024,
        max_matches: int = 20,
        context: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        """Search one bounded artifact window using a literal, non-ReDoS query."""

        if not needle:
            raise ValueError("needle must not be empty")
        if max_matches <= 0:
            raise ValueError("max_matches must be positive")
        page = self.read_artifact_page(
            artifact,
            offset=offset,
            limit=scan_bytes,
            context=context,
        )
        content = str(page["content"])
        matches: list[dict[str, Any]] = []
        search_from = 0
        while len(matches) < max_matches:
            index = content.find(needle, search_from)
            if index < 0:
                break
            line_start = content.rfind("\n", 0, index) + 1
            line_end = content.find("\n", index)
            if line_end < 0:
                line_end = len(content)
            line = content[line_start:line_end]
            matches.append(
                {
                    "offset": offset + len(content[:index].encode("utf-8")),
                    "line": redact_value(
                        line,
                        patterns=[r"sk-[A-Za-z0-9_-]+", r"(?i)bearer\s+\S+"],
                    ),
                }
            )
            search_from = index + len(needle)
        return {
            **page,
            "needle": needle,
            "matches": matches,
            "matches_truncated": len(matches) == max_matches,
        }

    def artifact_navigation_tools(self) -> dict[str, Callable[..., Any]]:
        """Return governed read/grep tools that let agents navigate artifacts safely."""

        if self._artifact_navigation_tools is not None:
            return dict(self._artifact_navigation_tools)

        self._artifact_navigation_tools = _make_artifact_navigation_tools(self)
        return dict(self._artifact_navigation_tools)

    def reconcile_uncertain(
        self,
        key: str,
        reconciler: SideEffectReconciler,
        *,
        actor_id: str | None = None,
        reason: str | None = None,
        evidence_ref: str | None = None,
    ) -> ReconciliationResult:
        if self.repository is None:
            raise RuntimeError("UNCERTAIN reconciliation requires a governance repository")
        record = self.repository.get_ledger(key)
        if record is None:
            raise KeyError(f"unknown idempotency key {key!r}")
        if record.status != "UNCERTAIN":
            raise ValueError(f"ledger record is {record.status}, not UNCERTAIN")
        result = reconciler.inspect(record)
        effective_evidence = evidence_ref or result.evidence_ref
        target = result.outcome
        if target == "MANUAL_OVERRIDE":
            if not actor_id or not reason or not effective_evidence:
                raise ValueError("manual override requires actor_id, reason, and evidence_ref")
            if result.override_target is None:
                raise ValueError("manual override requires override_target")
            target = {
                "SUCCEEDED": "CONFIRMED_SUCCEEDED",
                "NOT_APPLIED": "CONFIRMED_NOT_APPLIED",
                "STILL_UNCERTAIN": "STILL_UNCERTAIN",
            }[result.override_target]
        elif target != "STILL_UNCERTAIN" and not effective_evidence:
            raise ValueError("a terminal reconciliation requires evidence_ref")

        status = {
            "CONFIRMED_SUCCEEDED": "SUCCEEDED",
            "CONFIRMED_NOT_APPLIED": "FAILED_RETRYABLE",
            "STILL_UNCERTAIN": "UNCERTAIN",
        }[target]
        reconciled_output = None
        if status == "SUCCEEDED":
            if result.output is None:
                raise ValueError("successful reconciliation requires a StructuredToolOutput payload")
            validated_output = StructuredToolOutput.model_validate(result.output)
            if validated_output.status != "SUCCESS":
                raise ValueError("successful reconciliation output must have status SUCCESS")
            reconciled_output = validated_output.model_dump(mode="json")
        context = ToolCallContext(
            project=record.project,
            environment=record.environment,
            tenant_id=record.tenant_id,
            session_id=record.session_id,
            run_id=record.run_id,
            call_id=record.call_id,
            tool_name=record.tool_name,
            actor_id=actor_id,
            context_source="explicit",
        )
        runtime = self._runtimes.get(record.tool_name)
        if status == "SUCCEEDED":
            if runtime is None or runtime._tool_schema_hash() != record.tool_schema_hash:
                raise ValueError("successful reconciliation requires the matching registered tool schema")
            shaped, _ = runtime._shape_delegated_output(context, validated_output)
            if shaped.status != "SUCCESS":
                raise ValueError("reconciliation output could not satisfy the local output policy")
            reconciled_output = shaped.model_dump(mode="json")
            result = result.model_copy(update={"output": reconciled_output})
        metadata = {
            "idempotency_key": key,
            "outcome": result.outcome,
            "resolved_status": status,
            "actor_id": actor_id,
            "reason": reason,
            "evidence_ref": _safe_reference(effective_evidence),
            "summary": result.summary,
        }
        event = self._make_event(
            context=context,
            event_type="ledger.reconciled" if status != "UNCERTAIN" else "ledger.reconciliation_pending",
            phase="POST_FLIGHT",
            metadata=redact_value(
                metadata,
                keys=["password", "token", "secret", "authorization"],
                patterns=[r"sk-[A-Za-z0-9_-]+", r"(?i)bearer\s+\S+"],
            ),
        )
        updated = self.repository.resolve_uncertain(
            key,
            status=status,
            output=reconciled_output,
            error=None if status == "SUCCEEDED" else (
                _truncate_utf8(str(runtime.redact(result.summary, context)), 2048)
                if runtime is not None else "Reconciliation did not confirm success."
            ),
            event=event,
        )
        if updated is None:
            raise RuntimeError("UNCERTAIN record changed concurrently; reconciliation was not applied")
        self.dispatch_outbox()
        return result

    def dead_letters(self, *, limit: int = 100):
        if self.repository is None:
            return []
        return self.repository.list_outbox(state="dead_letter", limit=limit)

    def replay_dead_letter(self, delivery_id: str) -> bool:
        return bool(self.repository and self.repository.replay_dead_letter(delivery_id))

    def terminate_dead_letter(self, delivery_id: str, *, reason: str) -> bool:
        return bool(
            self.repository
            and self.repository.terminate_dead_letter(delivery_id, reason=reason)
        )

    def _emit(
        self,
        *,
        context: ToolCallContext,
        event_type: str,
        phase: str,
        error: ErrorRecord | None = None,
        decision: PolicyDecision | None = None,
        output_ref: ArtifactRef | None = None,
        metrics: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TrajectoryEvent:
        event = self._make_event(
            context=context, event_type=event_type, phase=phase, error=error,
            decision=decision, output_ref=output_ref, metrics=metrics, metadata=metadata,
        )
        self._deliver_event(event)
        return event

    def _make_event(
        self,
        *,
        context: ToolCallContext,
        event_type: str,
        phase: str,
        error: ErrorRecord | None = None,
        decision: PolicyDecision | None = None,
        output_ref: ArtifactRef | None = None,
        metrics: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TrajectoryEvent:
        with self._sequence_lock:
            self._sequence += 1
            sequence = f"{self._event_origin}-{self._sequence:020d}"
        runtime = self._runtimes.get(context.tool_name)
        if runtime is not None:
            try:
                metadata = runtime.redact(metadata or {}, context)
                if error is not None:
                    error = runtime._finalize_error(error, context)
                if decision is not None:
                    decision = PolicyDecision.model_validate(runtime.redact(decision.model_dump(mode="json"), context))
            except Exception:
                metadata = {"evidence_redaction_failed": True}
                decision = None
                output_ref = None
                if error is not None:
                    error = error.model_copy(update={"message": "Diagnostic details withheld because redaction failed."})
        event = TrajectoryEvent(
            event_id=f"evt-{uuid4().hex}",
            timestamp=datetime.now(timezone.utc),
            project=context.project,
            session_id=context.session_id,
            run_id=context.run_id,
            call_id=context.call_id,
            parent_call_id=context.parent_call_id,
            sequence=sequence,
            event_type=event_type,
            phase=phase,  # type: ignore[arg-type]
            tool_name=context.tool_name,
            error=error,
            decision=decision,
            output_ref=output_ref,
            metrics=metrics or {},
            metadata=metadata or {},
        )
        return event

    def _deliver_event(self, event: TrajectoryEvent) -> None:
        try:
            self.sink.emit(event)
        except Exception:  # noqa: BLE001 - observability is non-fatal unless strict.
            self._sink_error_count += 1
            if getattr(self.sink, "strict", False):
                raise

    def dispatch_outbox(self, *, limit: int = 100) -> dict[str, int]:
        if self.outbox_dispatcher is None:
            return {"claimed": 0, "delivered": 0, "failed": 0, "dead_lettered": 0}
        return self.outbox_dispatcher.dispatch_once(limit=limit)

    def flush(self) -> None:
        self.sink.flush()

    def close(self) -> None:
        with self._executor_lock:
            if self._closed:
                return
            self._closed = True
            executor = self._sync_executor
            self._sync_executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if self.outbox_dispatcher is not None:
            self.outbox_dispatcher.stop()
        self.sink.close()
        if self._owns_repository and self.repository is not None:
            close_repository = getattr(self.repository, "close", None)
            if close_repository is not None:
                close_repository()

    def _executor_for_sync_timeout(self) -> concurrent.futures.ThreadPoolExecutor:
        with self._executor_lock:
            if self._closed:
                raise RuntimeError("ActionLens is closed")
            if self._sync_executor is None:
                self._sync_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self._sync_max_workers,
                    thread_name_prefix="actionlens-sync",
                )
            return self._sync_executor

    def _cache_ticket_key(self, ticket_id: str, key: str) -> None:
        with self._ticket_lock:
            self._tickets[ticket_id] = key
            self._tickets.move_to_end(ticket_id)
            while len(self._tickets) > self._ticket_cache_max_entries:
                self._tickets.popitem(last=False)

    def _ticket_key(self, ticket_id: str) -> str | None:
        with self._ticket_lock:
            key = self._tickets.get(ticket_id)
            if key is not None:
                self._tickets.move_to_end(ticket_id)
            return key


class _IdempotencyKeyError(ValueError):
    """A pre-flight failure while establishing a caller's operation identity."""

    def __init__(self, taxonomy: str, message: str) -> None:
        super().__init__(message)
        self.taxonomy = taxonomy


class ToolRuntime:
    def __init__(self, lens: ActionLens, func: Callable[..., Any], spec: ToolSpec):
        self.lens = lens
        self.func = func
        self.spec = spec
        self.original_signature = inspect.signature(func)
        try:
            hints = get_type_hints(func)
        except (NameError, TypeError):
            hints = {}
            for name, parameter in self.original_signature.parameters.items():
                if name == "__al_ctx" or not isinstance(parameter.annotation, str):
                    continue
                try:
                    hints[name] = eval(parameter.annotation, func.__globals__)
                except (NameError, TypeError) as exc:
                    if self.spec.validation_mode != "passthrough":
                        raise TypeError(f"cannot resolve annotation for tool argument {name!r}") from exc
        self._validators = {
            name: TypeAdapter(hints.get(name, param.annotation))
            for name, param in self.original_signature.parameters.items()
            if name != "__al_ctx" and param.annotation is not inspect.Parameter.empty
            and not isinstance(hints.get(name, param.annotation), str)
        }
        self.public_signature = self._build_public_signature()
        self.original_params = set(self.original_signature.parameters)
        self.var_keyword_param = next(
            (
                name
                for name, parameter in self.original_signature.parameters.items()
                if parameter.kind == inspect.Parameter.VAR_KEYWORD
            ),
            None,
        )
        self.dynamic_arguments = bool(
            getattr(self.func, "actionlens_dynamic_arguments", False)
        ) and self.var_keyword_param is not None
        self.injected_idempotency = (
            self.spec.idempotency == IdempotencyPolicy.REQUIRED
            and self.spec.idempotency_key_param not in self.original_params
        )

    def as_callable(self) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(self.func) or inspect.isasyncgenfunction(self.func):

            @functools.wraps(self.func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> StructuredToolOutput:
                return await self.ainvoke(*args, **kwargs)

            async_wrapper.__signature__ = self.public_signature  # type: ignore[attr-defined]
            async_wrapper.__annotations__ = self._public_annotations()
            async_wrapper.actionlens_spec = self.spec  # type: ignore[attr-defined]
            async_wrapper.actionlens_runtime = self  # type: ignore[attr-defined]
            return async_wrapper

        @functools.wraps(self.func)
        def wrapper(*args: Any, **kwargs: Any) -> StructuredToolOutput:
            return self.invoke(*args, **kwargs)

        wrapper.__signature__ = self.public_signature  # type: ignore[attr-defined]
        wrapper.__annotations__ = self._public_annotations()
        wrapper.actionlens_spec = self.spec  # type: ignore[attr-defined]
        wrapper.actionlens_runtime = self  # type: ignore[attr-defined]
        return wrapper

    def invoke(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        return self._invoke_sync(*args, **kwargs)

    async def ainvoke(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        return await self._invoke_async(*args, **kwargs)

    def _build_public_signature(self) -> inspect.Signature:
        params = [
            param
            for name, param in self.original_signature.parameters.items()
            if name != "__al_ctx"
        ]
        names = {param.name for param in params}
        if (
            self.spec.idempotency == IdempotencyPolicy.REQUIRED
            and self.spec.idempotency_key_param not in names
        ):
            idempotency_param = inspect.Parameter(
                self.spec.idempotency_key_param,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=str,
            )
            insert_at = len(params)
            for index, param in enumerate(params):
                if param.kind == inspect.Parameter.VAR_KEYWORD:
                    insert_at = index
                    break
            params.insert(insert_at, idempotency_param)
        return self.original_signature.replace(parameters=params)

    def _public_annotations(self) -> dict[str, Any]:
        annotations = dict(getattr(self.func, "__annotations__", {}))
        for name, parameter in self.public_signature.parameters.items():
            if parameter.annotation is not inspect.Parameter.empty:
                annotations[name] = parameter.annotation
        return annotations

    def validate_modified_args(self, modified_args: dict[str, Any]) -> None:
        if self.dynamic_arguments:
            validator = getattr(self.func, "actionlens_modified_argument_validator", None)
            if callable(validator):
                validator(dict(modified_args))
            return
        unknown = set(modified_args) - (self.original_params - {"__al_ctx"})
        if unknown:
            raise ValueError(
                f"modified_args contains unknown parameters: {', '.join(sorted(unknown))}"
            )
        for name, value in modified_args.items():
            validator = self._validators.get(name)
            if validator is None:
                continue
            try:
                _validate_argument_value(validator, value, strict=self.spec.validation_mode != "coerce")
            except ValidationError as exc:
                raise ValueError(f"modified_args[{name!r}] does not match its annotation") from exc

    def redact(self, value: Any, context: ToolCallContext) -> Any:
        return self._redact_visible_result(value, context)

    def _apply_modified_args(
        self, bound: inspect.BoundArguments, modified_args: dict[str, Any]
    ) -> None:
        if self.dynamic_arguments:
            assert self.var_keyword_param is not None
            updated = dict(bound.arguments.get(self.var_keyword_param, {}))
            updated.update(modified_args)
            bound.arguments[self.var_keyword_param] = updated
            self._validate_external_arguments(bound)
            return
        for name, value in modified_args.items():
            bound.arguments[name] = value
        self._validate_external_arguments(bound)

    def _prepare(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[ToolCallContext, inspect.BoundArguments | None, StructuredToolOutput | None]:
        kwargs = dict(kwargs)
        explicit_context = self._pop_explicit_context(kwargs)
        try:
            bound = self.public_signature.bind(*args, **kwargs)
            bound.apply_defaults()
        except TypeError as exc:
            context = self._resolve_context(explicit_context)
            error = ErrorRecord(
                taxonomy="ValidationError",
                message=str(exc),
                type_name=type(exc).__name__,
                retryable=True,
            )
            self.lens._emit(
                context=context,
                event_type="tool_call.failed",
                phase="PRE_FLIGHT",
                error=error,
            )
            return context, None, StructuredToolOutput(
                status="FAILED",
                result_summary="Tool argument validation failed.",
                error_taxonomy=error.taxonomy,
                recovery_hint=recovery_hint(error),
            )
        context = self._resolve_context(explicit_context)
        try:
            self._validate_external_arguments(bound)
        except (TypeError, ValueError) as exc:
            error = ErrorRecord(
                taxonomy="ValidationError",
                message=str(exc),
                type_name=type(exc).__name__,
                retryable=True,
            )
            self.lens._emit(
                context=context,
                event_type="tool_call.failed",
                phase="PRE_FLIGHT",
                error=error,
            )
            return context, None, StructuredToolOutput(
                status="FAILED",
                result_summary="Tool argument validation failed.",
                error_taxonomy=error.taxonomy,
                recovery_hint=recovery_hint(error),
            )
        return context, bound, None

    def _validate_external_arguments(self, bound: inspect.BoundArguments) -> None:
        if self.spec.validation_mode != "passthrough":
            for name, adapter in self._validators.items():
                if name not in bound.arguments:
                    continue
                strict = self.spec.validation_mode == "strict"
                kind = self.original_signature.parameters[name].kind
                value = bound.arguments[name]
                if kind == inspect.Parameter.VAR_POSITIONAL:
                    value = tuple(_validate_argument_value(adapter, item, strict=strict) for item in value)
                elif kind == inspect.Parameter.VAR_KEYWORD:
                    value = {key: _validate_argument_value(adapter, item, strict=strict) for key, item in value.items()}
                else:
                    value = _validate_argument_value(adapter, value, strict=strict)
                bound.arguments[name] = value
        validator = getattr(self.func, "actionlens_argument_validator", None)
        if not callable(validator):
            return
        var_keyword = next(
            (
                name
                for name, parameter in self.original_signature.parameters.items()
                if parameter.kind == inspect.Parameter.VAR_KEYWORD
            ),
            None,
        )
        payload = (
            dict(bound.arguments.get(var_keyword, {}))
            if var_keyword is not None
            else dict(bound.arguments)
        )
        validator(payload)

    def _pop_explicit_context(self, kwargs: dict[str, Any]) -> ToolCallContext | None:
        explicit = kwargs.pop("__al_ctx", None)
        if isinstance(explicit, ToolCallContext):
            return explicit.model_copy(update={"context_source": "explicit"})
        if "context" not in self.original_params:
            maybe_context = kwargs.get("context")
            if isinstance(maybe_context, ToolCallContext):
                kwargs.pop("context")
                return maybe_context.model_copy(update={"context_source": "explicit"})
        return None

    def _resolve_context(self, explicit_context: ToolCallContext | None) -> ToolCallContext:
        base = explicit_context or get_current_context()
        if base is None:
            base = make_generated_context(self.lens.project, self.spec.name)
        source = "explicit" if explicit_context is not None else base.context_source
        call_id = (
            base.call_id
            if explicit_context is not None and base.call_id.strip()
            else f"call-{uuid4().hex}"
        )
        return base.model_copy(
            deep=True,
            update={
                "project": base.project or self.lens.project,
                "call_id": call_id,
                "tool_name": self.spec.name,
                "context_source": source,
                "metadata": {
                    key: copy.deepcopy(value) for key, value in base.metadata.items()
                    if key not in _INVOCATION_METADATA
                },
            }
        )

    def _invoke_sync(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        context, bound, early = self._prepare(args, kwargs)
        if early is not None or bound is None:
            return early  # type: ignore[return-value]
        try:
            preflight = self._preflight(context, bound)
        except Exception as exc:  # governance failed before business execution
            return self._governance_failure(context, exc, after_execution=False)
        if preflight is not None:
            self._record_preflight_result(context, preflight)
            return preflight
        started = datetime.now(timezone.utc)
        heartbeat = self._start_lease_heartbeat(context)
        execution_error: Exception | None = None
        result: Any = None
        try:
            original_args = self._original_args(bound, context)
            original_kwargs = self._original_kwargs(bound, context)
            if self.spec.timeout_sec is not None and self.spec.run_sync_in_thread:
                future = self.lens._executor_for_sync_timeout().submit(
                    self._call_sync_and_collect_stream,
                    context,
                    original_args,
                    original_kwargs,
                )
                try:
                    result = future.result(timeout=self.spec.timeout_sec)
                except concurrent.futures.TimeoutError:
                    context.metadata["actionlens_background_execution_may_continue"] = (
                        not future.cancel()
                    )
                    raise
            else:
                result = self.func(*original_args, **original_kwargs)
            if inspect.isgenerator(result):
                result = self._collect_stream_sync(context, result)
        except Exception as exc:  # noqa: BLE001 - mapped into tool protocol.
            execution_error = exc
        finally:
            if heartbeat is not None:
                heartbeat.stop()
        if execution_error is not None:
            try:
                return self._handle_exception(context, execution_error)
            except Exception as governance_exc:
                return self._governance_failure(context, governance_exc, after_execution=True)
        if heartbeat is not None and heartbeat.failed:
            return self._governance_failure(
                context,
                heartbeat.failure or RuntimeError("lease heartbeat lost ownership"),
                after_execution=True,
            )
        try:
            return self._handle_success(context, result, started)
        except Exception as exc:  # business result exists but durable commit did not complete
            return self._governance_failure(context, exc, after_execution=True)

    async def _invoke_async(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        # Governance may perform synchronous database, filesystem, and sink I/O.
        # Keep it off the host framework's event loop; the tool coroutine itself
        # still runs in the caller's event loop.
        context, bound, early = await _await_thread(self._prepare, args, kwargs)
        if early is not None or bound is None:
            return early  # type: ignore[return-value]
        try:
            preflight = await _await_thread(self._preflight, context, bound)
        except Exception as exc:  # governance failed before business execution
            return self._governance_failure(context, exc, after_execution=False)
        if preflight is not None:
            await _await_thread(self._record_preflight_result, context, preflight)
            return preflight
        started = datetime.now(timezone.utc)
        heartbeat = self._start_lease_heartbeat(context)
        execution_error: Exception | None = None
        result: Any = None
        try:
            invocation = self.func(
                *self._original_args(bound, context),
                **self._original_kwargs(bound, context),
            )
            if inspect.isasyncgen(invocation):
                stream_result = self._collect_stream_async(context, invocation)
                if self.spec.timeout_sec is not None:
                    result = await asyncio.wait_for(
                        stream_result, timeout=self.spec.timeout_sec
                    )
                else:
                    result = await stream_result
            else:
                if self.spec.timeout_sec is not None:
                    result = await asyncio.wait_for(invocation, timeout=self.spec.timeout_sec)
                else:
                    result = await invocation
                if inspect.isgenerator(result):
                    result = await _await_thread(
                        self._collect_stream_sync, context, result
                    )
        except asyncio.CancelledError:
            try:
                await _await_thread(self._handle_exception, context, asyncio.CancelledError("Tool execution cancelled."))
            except Exception as exc:
                self._governance_failure(context, exc, after_execution=True)
            raise
        except Exception as exc:  # noqa: BLE001 - mapped into tool protocol.
            execution_error = exc
        finally:
            if heartbeat is not None:
                await _await_thread(heartbeat.stop)
        if execution_error is not None:
            try:
                return await _await_thread(
                    self._handle_exception, context, execution_error
                )
            except Exception as governance_exc:
                return self._governance_failure(context, governance_exc, after_execution=True)
        if heartbeat is not None and heartbeat.failed:
            return self._governance_failure(
                context,
                heartbeat.failure or RuntimeError("lease heartbeat lost ownership"),
                after_execution=True,
            )
        try:
            return await _await_thread(self._handle_success, context, result, started)
        except Exception as exc:  # business result exists but durable commit did not complete
            return self._governance_failure(context, exc, after_execution=True)

    def _governance_failure(
        self, context: ToolCallContext, exc: Exception, *, after_execution: bool
    ) -> StructuredToolOutput:
        side_effect_risk = self.spec.risk in {RiskLevel.MUTATION, RiskLevel.DESTRUCTIVE}
        uncertain = after_execution and side_effect_risk
        output = StructuredToolOutput(
            status="UNCERTAIN" if uncertain else "FAILED",
            result_summary=(
                "Tool executed but governance state could not be persisted; outcome is uncertain."
                if uncertain else "Governance storage is temporarily unavailable; tool was not executed."
                if not after_execution
                else "Tool executed, but its governance state could not be persisted."
            ),
            error_taxonomy=(
                "GovernanceCommitUncertain" if uncertain else "GovernanceUnavailable"
            ),
            recovery_hint=(
                "Verify the business system and ledger state first; do not auto-retry."
                if uncertain
                else "Retry after governance storage is restored; the tool business logic was not executed."
                if not after_execution
                else "Restore governance storage before retrying; the tool body already ran."
            ),
            governance={"failure_type": type(exc).__name__, "after_execution": after_execution},
        )
        event = self.lens._make_event(
            context=context,
            event_type="tool_call.failed",
            phase="POST_FLIGHT" if after_execution else "PRE_FLIGHT",
            error=ErrorRecord(
                taxonomy=output.error_taxonomy or "GovernanceUnavailable",
                message=output.result_summary,
                type_name=type(exc).__name__,
                retryable=not uncertain,
            ),
            metadata={"after_execution": after_execution},
        )
        key = self._context_key(context)
        owner = context.metadata.get("actionlens_owner_id")
        fence = context.metadata.get("actionlens_fencing_token")
        if self.lens.repository is not None and key is not None and owner is not None and fence is not None:
            try:
                self.lens.repository.finish(
                    key, owner_id=str(owner), fencing_token=int(fence),
                    status="UNCERTAIN" if uncertain else "FAILED_RETRYABLE",
                    output=None, error=output.error_taxonomy, event=event,
                )
            except Exception:
                pass
            else:
                with suppress(Exception):
                    self.lens.dispatch_outbox()
                return output
        self.lens._deliver_event(event)
        return output

    def _start_lease_heartbeat(
        self, context: ToolCallContext
    ) -> _LeaseHeartbeat | None:
        if self.lens.repository is None:
            return None
        key = self._context_key(context)
        owner_id = context.metadata.get("actionlens_owner_id")
        fencing_token = context.metadata.get("actionlens_fencing_token")
        if key is None or owner_id is None or fencing_token is None:
            return None
        if self.spec.lease_seconds <= 0:
            return None
        return _LeaseHeartbeat(
            self.lens.repository,
            key=key,
            owner_id=str(owner_id),
            fencing_token=int(fencing_token),
            lease_seconds=self.spec.lease_seconds,
        )

    @property
    def _side_effect_risk(self) -> bool:
        return self.spec.risk in {RiskLevel.MUTATION, RiskLevel.DESTRUCTIVE}

    def _record_preflight_result(
        self, context: ToolCallContext, output: StructuredToolOutput
    ) -> None:
        self.lens._emit(
            context=context,
            event_type="tool_call.preflight_resolved",
            phase="PRE_FLIGHT",
            metadata={
                "status": output.status,
                "error_taxonomy": output.error_taxonomy,
            },
        )

    def _preflight(
        self, context: ToolCallContext, bound: inspect.BoundArguments
    ) -> StructuredToolOutput | None:
        resuming_sync_approval = bool(
            context.metadata.pop("actionlens_sync_approval_resuming", False)
        )
        safe_args = self._safe_arguments(bound)
        if resuming_sync_approval:
            decision = PolicyDecision(
                action="ALLOW",
                reason="Synchronous approval already resolved this invocation.",
            )
            modified_args = self._operation_arguments(bound)
        else:
            self.lens._emit(
                context=context,
                event_type="tool_call.started",
                phase="PRE_FLIGHT",
                metadata={"args": safe_args, "context_source": context.context_source},
            )
            try:
                decision, modified_args = self._decide_policy(context, bound)
            except (TypeError, ValueError):
                return StructuredToolOutput(status="DENIED", result_summary="Policy arguments failed validation.",
                                            error_taxonomy="ValidationError")
        safe_args = self._safe_arguments(bound)
        if decision.action == "DENY":
            taxonomy = decision.metadata.get("taxonomy", "PermissionDenied")
            return StructuredToolOutput(
                status="DENIED",
                result_summary="Tool invocation was denied by policy.",
                error_taxonomy=str(taxonomy),
                recovery_hint="Do not bypass the policy; request authorization from the user or summarize existing results.",
                governance={"policy_reason": self.redact(decision.reason, context)},
            )
        if decision.action == "MODIFY_ARGS":
            try:
                self._apply_modified_args(bound, modified_args)
            except (TypeError, ValueError) as exc:
                return StructuredToolOutput(
                    status="DENIED",
                    result_summary="Policy-modified tool arguments do not match the tool schema.",
                    error_taxonomy="ValidationError",
                    recovery_hint="Correct the policy-supplied arguments before invoking the tool.",
                    governance={"policy_argument_error": type(exc).__name__},
                )
            safe_args = self._safe_arguments(bound)
        if (
            inspect.isgeneratorfunction(self.func)
            or inspect.isasyncgenfunction(self.func)
        ) and self.lens.artifact_store.policy.raw_mode in {"deny", "reference_only"}:
            return StructuredToolOutput(
                status="DENIED",
                result_summary=(
                    "Streaming tools require artifact storage, but the active artifact "
                    "policy does not permit it."
                ),
                error_taxonomy="ArtifactPolicyDenied",
                recovery_hint=(
                    "Configure artifact storage for this streaming tool or return a "
                    "pre-persisted ArtifactRef instead."
                ),
            )
        try:
            key = self._idempotency_key(context, bound)
        except _IdempotencyKeyError as exc:
            error = ErrorRecord(
                taxonomy=exc.taxonomy,
                message=str(exc),
                type_name=type(exc).__name__,
                retryable=True,
            )
            self.lens._emit(
                context=context,
                event_type="tool_call.failed",
                phase="PRE_FLIGHT",
                error=error,
            )
            return StructuredToolOutput(
                status="FAILED",
                result_summary="A required idempotency key could not be established.",
                error_taxonomy=error.taxonomy,
                recovery_hint=(
                    "Provide a non-empty idempotency key or configure a valid "
                    "idempotency_key_fn before retrying this operation."
                ),
            )
        args_hash = self._args_hash(bound)
        tool_schema_hash = self._tool_schema_hash()
        context.metadata["actionlens_args_hash"] = args_hash
        context.metadata["actionlens_tool_schema_hash"] = tool_schema_hash
        existing_record = self.lens.ledger.get(key) if key is not None else None
        if existing_record is not None and (
            (existing_record.tool_name and existing_record.tool_name != self.spec.name)
            or (existing_record.project and (existing_record.project, existing_record.environment, existing_record.tenant_id)
                != (context.project, context.environment, context.tenant_id))
        ):
            return self._idempotency_conflict(key, existing_record)
        if (self.spec.approval_required or decision.action == "PENDING_APPROVAL"
                or resuming_sync_approval or (existing_record is not None and existing_record.ticket_id)):
            if key is None:
                key = self._auto_hash(context, bound)
            record = self.lens.ledger.get(key)
            approval_already_granted = False
            if record is not None and (
                record.tool_name != self.spec.name
                or (record.project, record.environment, record.tenant_id) != (context.project, context.environment, context.tenant_id)
                or
                getattr(record, "args_hash", "") not in {"", args_hash}
                or getattr(record, "tool_schema_hash", "") not in {"", tool_schema_hash}
            ):
                return self._idempotency_conflict(key, record)
            if record is not None and record.status in {"FAILED_TERMINAL", "DENIED", "EXPIRED", "UNCERTAIN", "EXECUTING", "PENDING"}:
                return self._ledger_hit_output(key, record)
            if (
                record is not None
                and record.status in {"APPROVED", "FAILED", "FAILED_RETRYABLE"}
                and record.ticket_id
            ):
                ticket = self.lens.ticket_store.get(record.ticket_id)
                approval_already_granted = (
                    ticket is not None and ticket.status == "APPROVED"
                )
                if approval_already_granted and ticket is not None and ticket.modified_args:
                    try:
                        self.validate_modified_args(ticket.modified_args)
                        self._apply_modified_args(bound, ticket.modified_args)
                        approved_hash = self._args_hash(bound)
                        approval_decision, _ = self._decide_policy(context, bound, charge_budget=False)
                        if approval_decision.action == "DENY" or self._args_hash(bound) != approved_hash:
                            return StructuredToolOutput(status="DENIED", result_summary="Approved arguments do not satisfy current policy.",
                                                        error_taxonomy="ApprovalPolicyDenied")
                    except (TypeError, ValueError):
                        event = self.lens._make_event(
                            context=context,
                            event_type="approval.invalid_modified_args",
                            phase="PRE_FLIGHT",
                            metadata={"ticket_id": ticket.ticket_id},
                        )
                        if self.lens.repository is not None:
                            self.lens.repository.set_ledger_status(
                                key, status="FAILED_TERMINAL",
                                error="approved modified_args failed tool schema validation",
                                event=event,
                            )
                            self.lens.dispatch_outbox()
                        else:
                            self.lens.ledger.fail(key)
                            self.lens._deliver_event(event)
                        return StructuredToolOutput(
                            status="DENIED",
                            result_summary="Approved modified arguments do not match the tool schema; tool was not executed.",
                            error_taxonomy="ApprovalArgsInvalid",
                            recovery_hint="Correct the approval parameters and retry with a new idempotency key.",
                            governance={"ticket_id": ticket.ticket_id},
                        )
                    safe_args = self._safe_arguments(bound)
            if record is None or (
                record.status not in {"APPROVED", "SUCCEEDED"}
                and not approval_already_granted
            ):
                if record is not None and record.ticket_id:
                    existing_ticket = self.lens.ticket_store.get(record.ticket_id)
                    if existing_ticket is not None and existing_ticket.status == "DENIED":
                        return StructuredToolOutput(
                            status="DENIED",
                            result_summary="Human approval was denied; tool was not executed.",
                            error_taxonomy="ApprovalDenied",
                            recovery_hint="Do not repeatedly initiate the same high-risk operation; report the denial to the user.",
                            governance={
                                "ticket_id": record.ticket_id,
                                "idempotency_key": key,
                            },
                        )
                    if existing_ticket is not None and existing_ticket.status == "EXPIRED":
                        event = self.lens._make_event(
                            context=context,
                            event_type="approval.expired",
                            phase="PRE_FLIGHT",
                            metadata={"ticket_id": record.ticket_id},
                        )
                        if self.lens.repository is not None:
                            self.lens.repository.expire_ticket(record.ticket_id, event=event)
                            self.lens.dispatch_outbox()
                        else:
                            self.lens.ledger.fail(key)
                            self.lens._deliver_event(event)
                        return StructuredToolOutput(
                            status="DENIED",
                            result_summary="Human approval ticket has expired; tool was not executed.",
                            error_taxonomy="ApprovalExpired",
                            recovery_hint="Submit a new approval request with a fresh idempotency key.",
                            governance={"ticket_id": record.ticket_id, "idempotency_key": key},
                        )
                    if existing_ticket is not None and existing_ticket.status == "PENDING":
                        return StructuredToolOutput(
                            status="PENDING_APPROVAL",
                            result_summary="The same operation is already pending human approval.",
                            result={"ticket_id": existing_ticket.ticket_id},
                            recovery_hint=(
                                "Operation is paused awaiting human approval. Stop calling other tools, "
                                "inform the user that approval has been submitted, and end the current session."
                            ),
                            governance={"ticket_id": existing_ticket.ticket_id, "idempotency_key": key},
                        )
                ticket = ApprovalTicket(
                    ticket_id=f"ticket-{uuid4().hex}",
                    idempotency_key=key,
                    call_id=context.call_id,
                    tool_name=self.spec.name,
                    safe_args=safe_args,
                    risk=self.spec.risk,
                    reason="Tool requires human approval.",
                    expires_at=(
                        datetime.now(timezone.utc) + timedelta(seconds=self.spec.approval_ttl_sec)
                        if self.spec.approval_ttl_sec is not None
                        else None
                    ),
                    metadata={
                        "allowed_args": (
                            sorted(safe_args)
                            if self.dynamic_arguments
                            else sorted(self.original_params - {"__al_ctx"})
                        ),
                        "context": self.redact(context.model_dump(mode="json"), context),
                        "requested_args_hash": args_hash,
                        "tool_schema_hash": tool_schema_hash,
                    },
                )
                approval_record = self.lens.ledger.mark_approval_pending(
                    key, call_id=context.call_id, ticket_id=ticket.ticket_id,
                    context=context, spec=self.spec,
                ) if self.lens.repository is None else None
                if self.lens.repository is not None:
                    decision = PolicyDecision(
                        action="PENDING_APPROVAL", reason=ticket.reason,
                        approval_ticket=ticket,
                    )
                    event = self.lens._make_event(
                        context=context, event_type="approval.pending", phase="PRE_FLIGHT",
                        decision=decision,
                    )
                    stored_ticket, approval_record, created = self.lens.repository.create_approval(
                        key, ticket=ticket, context=context, spec=self.spec,
                        args_hash=args_hash, tool_schema_hash=tool_schema_hash, event=event,
                    )
                    if not created:
                        if (
                            approval_record.args_hash != args_hash
                            or approval_record.tool_schema_hash != tool_schema_hash
                            or approval_record.tool_name != self.spec.name
                            or (approval_record.project, approval_record.environment, approval_record.tenant_id) != (context.project, context.environment, context.tenant_id)
                        ):
                            return self._idempotency_conflict(key, approval_record)
                        if approval_record.status == "SUCCEEDED" and approval_record.output is not None:
                            return self._finalize_stored_output(context, approval_record.output)
                        if approval_record.status != "APPROVAL_PENDING":
                            return self._ledger_hit_output(key, approval_record)
                        return StructuredToolOutput(
                            status="PENDING_APPROVAL",
                            result_summary="The same operation is already pending human approval.",
                            result={"ticket_id": stored_ticket.ticket_id},
                            recovery_hint=("Operation is paused awaiting human approval. Stop calling other tools, "
                                           "inform the user that approval has been submitted, and end the current session."),
                            governance={"ticket_id": stored_ticket.ticket_id, "idempotency_key": key},
                        )
                    self.lens._cache_ticket_key(ticket.ticket_id, key)
                    self.lens.dispatch_outbox()
                    resolution_handled, resolved = self._resolve_new_approval(
                        context, bound, stored_ticket, key
                    )
                    if resolution_handled:
                        return resolved
                    return StructuredToolOutput(
                        status="PENDING_APPROVAL",
                        result_summary="Operation has been submitted for human approval and has not yet been executed.",
                        result={"ticket_id": ticket.ticket_id},
                        recovery_hint=("Operation is paused awaiting human approval. Stop calling other tools, "
                                       "inform the user that approval has been submitted, and end the current session."),
                        governance={"ticket_id": ticket.ticket_id, "idempotency_key": key},
                    )
                assert approval_record is not None
                if approval_record.ticket_id != ticket.ticket_id:
                    existing_ticket = (
                        self.lens.ticket_store.get(approval_record.ticket_id)
                        if approval_record.ticket_id
                        else None
                    )
                    return StructuredToolOutput(
                        status="PENDING_APPROVAL",
                        result_summary="The same operation is already pending human approval.",
                        result={"ticket_id": approval_record.ticket_id},
                        recovery_hint=(
                            "Operation is paused awaiting human approval. Stop calling other tools, "
                            "inform the user that approval has been submitted, and end the current session."
                        ),
                        governance={
                            "ticket_id": approval_record.ticket_id,
                            "idempotency_key": key,
                            "ticket_status": existing_ticket.status if existing_ticket else "PENDING",
                        },
                    )
                self.lens.ticket_store.create(ticket)
                self.lens._cache_ticket_key(ticket.ticket_id, key)
                decision = PolicyDecision(
                    action="PENDING_APPROVAL",
                    reason=ticket.reason,
                    approval_ticket=ticket,
                )
                self.lens._emit(
                    context=context,
                    event_type="approval.pending",
                    phase="PRE_FLIGHT",
                    decision=decision,
                )
                resolution_handled, resolved = self._resolve_new_approval(
                    context, bound, ticket, key
                )
                if resolution_handled:
                    return resolved
                return StructuredToolOutput(
                    status="PENDING_APPROVAL",
                    result_summary="Operation has been submitted for human approval and has not yet been executed.",
                    result={"ticket_id": ticket.ticket_id},
                    recovery_hint=(
                        "Operation is paused awaiting human approval. Stop calling other tools, "
                        "inform the user that approval has been submitted, and end the current session."
                    ),
                    governance={"ticket_id": ticket.ticket_id, "idempotency_key": key},
                )
        context.metadata["actionlens_effective_args_hash"] = self._args_hash(bound)
        if key is None:
            return None
        if self.lens.repository is not None:
            owner_id = f"worker-{uuid4().hex}"
            hit_kind, record = self.lens.repository.begin(
                key, call_id=context.call_id, context=context, spec=self.spec,
                args_hash=args_hash, tool_schema_hash=tool_schema_hash,
                owner_id=owner_id, lease_seconds=self.spec.lease_seconds,
            )
            if hit_kind == "created":
                context.metadata["actionlens_owner_id"] = owner_id
                context.metadata["actionlens_fencing_token"] = record.fencing_token
            elif hit_kind == "conflict":
                return self._idempotency_conflict(key, record)
            elif hit_kind == "uncertain":
                return self._uncertain_output(key, record)
        else:
            # Keep third-party legacy ledgers callable while using the richer
            # conflict contract when their begin() method advertises it.
            try:
                begin_parameters = inspect.signature(self.lens.ledger.begin).parameters
            except (TypeError, ValueError):
                begin_parameters = {}
            accepts_hashes = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in begin_parameters.values()
            ) or {"args_hash", "tool_schema_hash"}.issubset(begin_parameters)
            begin_kwargs = {
                "call_id": context.call_id,
                "context": context,
                "spec": self.spec,
            }
            if accepts_hashes:
                begin_kwargs.update(
                    args_hash=args_hash,
                    tool_schema_hash=tool_schema_hash,
                )
            hit_kind, record = self.lens.ledger.begin(key, **begin_kwargs)
        if hit_kind == "conflict":
            return self._idempotency_conflict(key, record)
        if hit_kind == "hit":
            self.lens._emit(
                context=context,
                event_type="idempotency.hit",
                phase="PRE_FLIGHT",
                metadata={"status": record.status, "idempotency_key": key},
            )
            if record.status == "SUCCEEDED" and record.output is not None:
                return self._finalize_stored_output(context, record.output)
            if record.status == "APPROVAL_PENDING":
                if record.ticket_id:
                    ticket = self.lens.ticket_store.get(record.ticket_id)
                    if ticket is not None and ticket.status == "DENIED":
                        return StructuredToolOutput(
                            status="DENIED",
                            result_summary="Human approval was denied; tool was not executed.",
                            error_taxonomy="ApprovalDenied",
                            recovery_hint="Do not repeatedly initiate the same high-risk operation; report the denial to the user.",
                            governance={
                                "ticket_id": record.ticket_id,
                                "idempotency_key": key,
                            },
                        )
                    if ticket is not None and ticket.status == "EXPIRED":
                        if self.lens.repository is not None:
                            event = self.lens._make_event(
                                context=context, event_type="approval.expired", phase="PRE_FLIGHT",
                                metadata={"ticket_id": record.ticket_id},
                            )
                            self.lens.repository.expire_ticket(record.ticket_id, event=event)
                            self.lens.dispatch_outbox()
                        else:
                            self.lens.ledger.fail(key)
                        return StructuredToolOutput(
                            status="DENIED",
                            result_summary="Human approval ticket has expired; tool was not executed.",
                            error_taxonomy="ApprovalExpired",
                            recovery_hint="Submit a new approval request with a fresh idempotency key.",
                            governance={"ticket_id": record.ticket_id, "idempotency_key": key},
                        )
                return StructuredToolOutput(
                    status="PENDING_APPROVAL",
                    result_summary="The same operation is already pending human approval.",
                    result={"ticket_id": record.ticket_id},
                    recovery_hint=(
                        "Operation is paused awaiting human approval. Stop calling other tools, "
                        "inform the user that approval has been submitted, and end the current session."
                    ),
                    governance={"ticket_id": record.ticket_id, "idempotency_key": key},
                )
            if record.status == "PENDING":
                return StructuredToolOutput(
                    status="SKIPPED",
                    result_summary="Same operation is being executed or has been taken over; duplicate call skipped.",
                    governance={"idempotency_key": key, "ledger_status": record.status},
                )
            if record.status == "EXECUTING":
                return StructuredToolOutput(
                    status="SKIPPED",
                    result_summary="Same operation is being executed by another lease owner; duplicate call skipped.",
                    governance={"idempotency_key": key, "ledger_status": record.status,
                                "fencing_token": record.fencing_token},
                )
            if record.status == "UNCERTAIN":
                return self._uncertain_output(key, record)
            return self._ledger_hit_output(key, record)
        if hit_kind != "created":
            return self._ledger_hit_output(key, record)
        return None

    def _decide_policy(self, context, bound, *, charge_budget=True):
        def validate_patch(patch):
            self.validate_modified_args(patch)
            self._apply_modified_args(bound, patch)
            return self._operation_arguments(bound)

        return self.lens.policy_chain.decide(
            spec=self.spec, args=self._operation_arguments(bound), context=context,
            validate_patch=validate_patch, charge_budget=charge_budget,
            on_decision=lambda decision: self.lens._emit(
                context=context, event_type="policy.decision", phase="PRE_FLIGHT", decision=decision,
            ),
        )

    def _ledger_hit_output(self, key, record):
        if record.status == "UNCERTAIN":
            return self._uncertain_output(key, record)
        return StructuredToolOutput(
            status="SKIPPED" if record.status in {"EXECUTING", "PENDING"} else "DENIED",
            result_summary="Existing governance state does not grant execution permission.",
            error_taxonomy="ApprovalDenied" if record.status == "DENIED" else "ApprovalExpired" if record.status == "EXPIRED" else "LedgerExecutionBlocked",
            recovery_hint="Inspect the existing operation and its approval or reconciliation state.",
            governance={"idempotency_key": key, "ledger_status": record.status},
        )

    def _finalize_error(self, error: ErrorRecord, context: ToolCallContext) -> ErrorRecord:
        if self.spec.output.error_message_mode == "classification":
            return error.model_copy(update={"message": f"{error.taxonomy} ({error.type_name or 'tool error'})"})
        message = self.redact(error.message, context)
        return error.model_copy(update={"message": _truncate_utf8(str(message), 2048)})

    def _finalize_stored_output(self, context, payload):
        output = StructuredToolOutput.model_validate(self.redact(payload, context))
        result_bytes = len(output.result.encode("utf-8")) if isinstance(output.result, str) else len(_json_bytes(output.result))
        if (result_bytes > self.spec.output.max_inline_bytes
                or (_top_level_item_count(output.result) or 0) > self.spec.output.max_inline_items):
            return self._shape_delegated_output(context, output)[0]
        return output.model_copy(update={
            "result_summary": _truncate_utf8(output.result_summary, max(160, self.spec.output.max_inline_bytes)),
            "artifact_refs": [ref.model_copy(update={"preview": None}) for ref in output.artifact_refs],
        })

    def _resolve_new_approval(
        self,
        context: ToolCallContext,
        bound: inspect.BoundArguments,
        ticket: ApprovalTicket,
        key: str,
    ) -> tuple[bool, StructuredToolOutput | None]:
        resolution = self.lens._resolve_sync_approval(ticket, context)
        if resolution.action == "APPROVE":
            context.metadata["actionlens_sync_approval_resuming"] = True
            return True, self._preflight(context, bound)
        if resolution.action == "DENY":
            return True, StructuredToolOutput(
                status="DENIED",
                result_summary="Synchronous approval denied; tool was not executed.",
                error_taxonomy="ApprovalDenied",
                recovery_hint=(
                    "Do not repeat the high-risk operation without a new approval "
                    "decision."
                ),
                governance={"ticket_id": ticket.ticket_id, "idempotency_key": key},
            )
        return False, None

    def _handle_exception(
        self, context: ToolCallContext, exc: BaseException
    ) -> StructuredToolOutput:
        error = classify_exception(exc)
        uncertain = error.taxonomy == "SideEffectUncertain" or (
            self._side_effect_risk and not isinstance(exc, NoSideEffectError)
        )
        error = self._finalize_error(error, context)
        if isinstance(exc, NoSideEffectError):
            error = error.model_copy(update={"retryable": True})
        if uncertain:
            error = error.model_copy(update={"retryable": False})
        key = self._context_key(context)
        event = self.lens._make_event(
            context=context,
            event_type="tool_call.failed",
            phase="POST_FLIGHT",
            error=error,
        )
        if key is not None and self.lens.repository is not None:
            owner_id = context.metadata.get("actionlens_owner_id")
            fencing_token = context.metadata.get("actionlens_fencing_token")
            if owner_id is not None and fencing_token is not None:
                status = (
                    "UNCERTAIN" if uncertain
                    else "FAILED_RETRYABLE" if error.retryable
                    else "FAILED_TERMINAL"
                )
                self.lens.repository.finish(
                    key, owner_id=str(owner_id), fencing_token=int(fencing_token),
                    status=status, output=None, error=error.message, event=event,
                )
                self.lens.dispatch_outbox()
            else:
                self.lens._deliver_event(event)
        elif key is not None:
            if uncertain and callable(getattr(self.lens.ledger, "mark_uncertain", None)):
                self.lens.ledger.mark_uncertain(key)
            else:
                mark_failed = getattr(self.lens.ledger, "mark_failed", None)
                if callable(mark_failed):
                    mark_failed(key, retryable=error.retryable)
                else:
                    self.lens.ledger.fail(key)
            self.lens._deliver_event(event)
        else:
            self.lens._deliver_event(event)
        recovery = recovery_hint(error)
        if uncertain:
            recovery = (
                "The high-risk tool's side effect may have completed. "
                "Query the business system or request human confirmation; do not auto-retry."
            )
        governance: dict[str, Any] = {}
        if context.metadata.get("actionlens_background_execution_may_continue"):
            governance["background_execution_may_continue"] = True
        return StructuredToolOutput(
            status=("UNCERTAIN" if uncertain
                    else "FAILED" if error.taxonomy != "Timeout" else "TIMEOUT"),
            result_summary=f"Tool execution failed: {error.taxonomy}",
            error_taxonomy=error.taxonomy,
            recovery_hint=recovery,
            governance=governance,
        )

    def _handle_success(
        self,
        context: ToolCallContext,
        result: Any,
        started: datetime,
    ) -> StructuredToolOutput:
        output, output_ref = self._shape_output(context, result)
        key = self._context_key(context)
        latency_ms = (
            datetime.now(timezone.utc) - started
        ).total_seconds() * 1000.0
        governance_failed = output.status == "FAILED"
        if governance_failed and self._side_effect_risk:
            output = output.model_copy(
                update={
                    "status": "UNCERTAIN",
                    "result_summary": (
                        "The high-risk tool completed, but its result could not be "
                        "persisted under the output policy; outcome is uncertain."
                    ),
                    "recovery_hint": (
                        "Query the business system or request human confirmation; "
                        "do not auto-retry this side effect."
                    ),
                    "governance": {
                        **output.governance,
                        "result_persistence_failed_after_execution": True,
                    },
                }
            )
        governance_uncertain = output.status == "UNCERTAIN"
        governance_failed = governance_failed or governance_uncertain
        event = self.lens._make_event(
            context=context,
            event_type="tool_call.failed" if governance_failed else "tool_call.completed",
            phase="POST_FLIGHT",
            error=(
                ErrorRecord(
                    taxonomy=output.error_taxonomy or "ArtifactPolicyDenied",
                    message=output.result_summary,
                    retryable=False,
                )
                if governance_failed else None
            ),
            output_ref=output_ref,
            metrics={"latency_ms": latency_ms},
            metadata={"output": self._trajectory_output(output), "authorization": {
                "requested_args_hash": context.metadata.get("actionlens_args_hash"),
                "effective_args_hash": context.metadata.get("actionlens_effective_args_hash"),
                "tool_schema_hash": context.metadata.get("actionlens_tool_schema_hash"),
                "basis": "tool_spec_and_current_policy",
            }},
        )
        if key is not None and self.lens.repository is not None:
            owner_id = context.metadata.get("actionlens_owner_id")
            fencing_token = context.metadata.get("actionlens_fencing_token")
            if owner_id is None or fencing_token is None:
                self.lens._deliver_event(event)
            else:
                self.lens.repository.finish(
                    key, owner_id=str(owner_id), fencing_token=int(fencing_token),
                    status=(
                        "UNCERTAIN" if governance_uncertain
                        else "FAILED_TERMINAL" if governance_failed
                        else "SUCCEEDED"
                    ),
                    output=None if governance_failed else output.model_dump(mode="json"),
                    error=None, event=event,
                )
                self.lens.dispatch_outbox()
        elif key is not None:
            if governance_failed:
                if governance_uncertain and callable(
                    getattr(self.lens.ledger, "mark_uncertain", None)
                ):
                    self.lens.ledger.mark_uncertain(key)
                else:
                    mark_failed = getattr(self.lens.ledger, "mark_failed", None)
                    if callable(mark_failed):
                        mark_failed(key, retryable=True)
                    else:
                        self.lens.ledger.fail(key)
            else:
                self.lens.ledger.succeed(key, output.model_dump(mode="json"))
            self.lens._deliver_event(event)
        else:
            self.lens._deliver_event(event)
        return output

    def _shape_output(
        self, context: ToolCallContext, result: Any
    ) -> tuple[StructuredToolOutput, ArtifactRef | None]:
        if isinstance(result, StructuredToolOutput):
            return self._shape_delegated_output(context, result)
        if isinstance(result, _StreamedArtifactFailure):
            return (
                StructuredToolOutput(
                    status="FAILED",
                    result_summary="Streaming result could not be persisted as an artifact.",
                    error_taxonomy="ArtifactPolicyDenied",
                    recovery_hint=(
                        "Configure a compatible artifact policy before running this "
                        "streaming tool again."
                    ),
                    governance={"reason": str(result.error)},
                ),
                None,
            )
        if isinstance(result, _StreamedArtifact):
            return (
                StructuredToolOutput(
                    status="SUCCESS",
                    result_summary=(
                        "Streaming result stored as an artifact; returning the configured "
                        "tail of model-visible lines."
                    ),
                    result=_truncate_utf8(result.tail, self.spec.output.max_inline_bytes) or None,
                    artifact_refs=[result.artifact],
                    governance={
                        "streaming": True,
                        "chunks": result.chunks,
                        "tail_lines": self.spec.output.streaming_tail_lines,
                    },
                ),
                result.artifact,
            )
        if isinstance(result, ArtifactRef):
            try:
                self._validate_output_reference(result)
            except ArtifactPolicyError as exc:
                return (
                    StructuredToolOutput(status="FAILED", result_summary="Artifact reference does not comply with security policy.",
                                         error_taxonomy="ArtifactPolicyDenied", governance={"reason": str(exc)}),
                    None,
                )
            return (
                StructuredToolOutput(
                    status="SUCCESS",
                    result_summary="Tool returned an external artifact reference.",
                    artifact_refs=[result],
                    governance={"reference_only": True},
                ),
                result,
            )
        visible_result = self._redact_visible_result(result, context)
        raw_bytes = _json_bytes(visible_result)
        item_count = _top_level_item_count(visible_result)
        exceeds_bytes = len(raw_bytes) > self.spec.output.max_inline_bytes
        exceeds_items = (
            item_count is not None
            and item_count > self.spec.output.max_inline_items
        )
        if not exceeds_bytes and not exceeds_items:
            return (
                StructuredToolOutput(
                    status="SUCCESS",
                    result_summary=_summary(
                        visible_result,
                        output_policy=self.spec.output,
                        artifact_policy=self.lens.artifact_store.policy,
                    ),
                    result=visible_result,
                ),
                None,
            )
        policy = self.lens.artifact_store.policy
        value_to_store = visible_result if policy.raw_mode == "redact_then_store" else result
        model_preview = _preview_value(
            visible_result,
            limit=self.spec.output.max_inline_bytes,
            max_items=self.spec.output.max_inline_items,
        )
        try:
            artifact = self.lens.artifact_store.put(
                value_to_store,
                metadata=self._artifact_metadata(context),
                preview=model_preview,
                redacted=_json_bytes(visible_result) != _json_bytes(result),
            )
        except ArtifactPolicyError as exc:
            return (
                StructuredToolOutput(
                    status="FAILED",
                    result_summary="Result exceeds inline limit and artifact policy denies storage.",
                    error_taxonomy="ArtifactPolicyDenied",
                    recovery_hint="Reduce tool output or return a pre-persisted ArtifactRef from the business system.",
                    governance={"artifact_policy": policy.raw_mode, "reason": str(exc)},
                ),
                None,
            )
        inline_preview = _truncate_utf8(
            model_preview, self.spec.output.max_inline_bytes
        )
        limits = []
        if exceeds_bytes:
            limits.append(f"{self.spec.output.max_inline_bytes} UTF-8 bytes")
        if exceeds_items:
            limits.append(f"{self.spec.output.max_inline_items} top-level items")
        return (
            StructuredToolOutput(
                status="SUCCESS",
                result_summary=(
                    f"Result exceeds the inline limit ({', '.join(limits)}); "
                    "it has been truncated and stored as an artifact."
                ),
                result=inline_preview,
                artifact_refs=[artifact],
                governance={"truncated": True},
            ),
            artifact,
        )

    def _shape_delegated_output(
        self,
        context: ToolCallContext,
        delegated: StructuredToolOutput,
    ) -> tuple[StructuredToolOutput, ArtifactRef | None]:
        """Apply local output policy to a successful delegated tool response."""

        if delegated.status != "SUCCESS":
            return (
                StructuredToolOutput(
                    status="FAILED",
                    result_summary="A delegated tool returned a non-success result.",
                    error_taxonomy=delegated.error_taxonomy or "DelegatedToolFailure",
                    recovery_hint=(
                        "Inspect the delegated tool's durable status before retrying "
                        "a side-effecting operation."
                    ),
                    governance={"delegated_status": delegated.status},
                ),
                None,
            )

        shaped, local_ref = self._shape_output(context, delegated.result)
        if shaped.status != "SUCCESS":
            return shaped, local_ref
        # A remote runner's artifact preview is untrusted input. It may not
        # have been produced under this lens's redaction or confidentiality
        # policy, so never copy it into local model output or trajectories.
        try:
            for ref in delegated.artifact_refs:
                self._validate_output_reference(ref)
        except ArtifactPolicyError:
            return StructuredToolOutput(status="FAILED", result_summary="Delegated artifact reference violates local policy.",
                                        error_taxonomy="ArtifactPolicyDenied"), None
        delegated_refs = [
            ref.model_copy(update={"preview": None})
            for ref in delegated.artifact_refs
        ]
        artifact_refs: list[ArtifactRef] = []
        seen_refs: set[tuple[str, str]] = set()
        for ref in [*delegated_refs, *shaped.artifact_refs]:
            identity = (ref.uri, ref.sha256)
            if identity not in seen_refs:
                seen_refs.add(identity)
                artifact_refs.append(ref)
        summary = shaped.result_summary
        if delegated_refs and delegated.result is None:
            summary = "Delegated tool returned artifact references."
        return (
            StructuredToolOutput(
                status="SUCCESS",
                result_summary=summary,
                result=shaped.result,
                artifact_refs=artifact_refs,
                governance={
                    **shaped.governance,
                    "delegated": True,
                    "delegated_artifact_previews_withheld": bool(delegated_refs),
                },
            ),
            local_ref or (delegated_refs[0] if delegated_refs else None),
        )

    def _validate_output_reference(self, artifact: ArtifactRef) -> None:
        uri = artifact.uri
        scheme = urlsplit(uri).scheme
        if scheme in {"", "file"} or (len(uri) >= 3 and uri[1] == ":"):
            if self.lens.artifact_store.policy.raw_mode == "reference_only":
                raise ArtifactPolicyError("reference_only mode requires an approved external URI scheme")
            self.lens.artifact_store._local_path(artifact)
        else:
            self.lens.artifact_store.validate_reference(artifact)

    def _collect_stream_sync(
        self, context: ToolCallContext, stream: Generator[Any, None, None]
    ) -> _StreamedArtifact | _StreamedArtifactFailure:
        collector = _StreamCollector(self, context)
        with ExitStack() as resources:
            resources.callback(collector.release)
            buffer = resources.enter_context(tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b"))
            resources.callback(stream.close)
            try:
                for chunk in stream:
                    collector.write(buffer, chunk)
            except ArtifactPolicyError as exc:
                return _StreamedArtifactFailure(exc)
            collector.release()
            buffer.seek(0)
            try:
                artifact = self.lens.artifact_store.put_stream(
                    buffer,
                    media_type=collector.media_type,
                    metadata=self._artifact_metadata(context),
                    preview="Streaming output stored as an artifact.",
                    redacted=collector.redacted,
                )
            except ArtifactPolicyError as exc:
                return _StreamedArtifactFailure(exc)
        return _StreamedArtifact(
            artifact=artifact,
            tail=collector.tail,
            chunks=collector.chunks,
        )

    async def _collect_stream_async(
        self, context: ToolCallContext, stream: AsyncGenerator[Any, None]
    ) -> _StreamedArtifact | _StreamedArtifactFailure:
        collector = _StreamCollector(self, context)
        with ExitStack() as resources:
            resources.callback(collector.release)
            buffer = resources.enter_context(tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b"))
            try:
                async for chunk in stream:
                    await _await_thread(collector.write, buffer, chunk)
            except ArtifactPolicyError as exc:
                return _StreamedArtifactFailure(exc)
            finally:
                await stream.aclose()
            collector.release()
            buffer.seek(0)
            try:
                artifact = await _await_thread(
                    self.lens.artifact_store.put_stream,
                    buffer,
                    media_type=collector.media_type,
                    metadata=self._artifact_metadata(context),
                    preview="Streaming output stored as an artifact.",
                    redacted=collector.redacted,
                )
            except ArtifactPolicyError as exc:
                return _StreamedArtifactFailure(exc)
        return _StreamedArtifact(
            artifact=artifact,
            tail=collector.tail,
            chunks=collector.chunks,
        )

    @staticmethod
    def _artifact_metadata(context: ToolCallContext) -> dict[str, Any]:
        return {
            "project": context.project,
            "environment": context.environment,
            "tenant_id": context.tenant_id,
            "session_id": context.session_id,
            "run_id": context.run_id,
            "tool_name": context.tool_name,
        }

    def _call_sync_and_collect_stream(
        self,
        context: ToolCallContext,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        result = self.func(*args, **kwargs)
        if inspect.isgenerator(result):
            return self._collect_stream_sync(context, result)
        return result

    def _trajectory_output(self, output: StructuredToolOutput) -> dict[str, Any]:
        payload = output.model_dump(mode="json")
        if not self.spec.output.include_raw_in_trajectory:
            payload["result"] = None
        return payload

    def _safe_arguments(self, bound: inspect.BoundArguments) -> dict[str, Any]:
        arguments = self._operation_arguments(bound)
        safe = redact_value(
            arguments,
            keys=self.spec.output.redact_keys,
            patterns=self.spec.output.redact_patterns,
        )
        if self.lens.redactor is not None:
            safe = self.lens.redactor.redact(safe)
        return _json_safe_value(safe)

    def _redact_visible_result(self, value: Any, context: ToolCallContext) -> Any:
        redacted = redact_value(
            value,
            keys=self.spec.output.redact_keys,
            patterns=self.spec.output.redact_patterns,
        )
        if self.lens.redactor is not None:
            redacted = self.lens.redactor.redact(redacted, context)
        return _json_safe_value(redacted)

    def _idempotency_key(
        self, context: ToolCallContext, bound: inspect.BoundArguments
    ) -> str | None:
        if self.spec.idempotency == IdempotencyPolicy.OFF:
            return None
        if self.spec.idempotency in {
            IdempotencyPolicy.REQUIRED,
            IdempotencyPolicy.AUTO_HASH,
        }:
            key = bound.arguments.get(self.spec.idempotency_key_param)
            if key is not None and str(key).strip():
                return self._set_idempotency_key(context, str(key))
            key_fn = self.spec.idempotency_key_fn
            if key_fn is not None:
                try:
                    generated = key_fn(self._operation_arguments(bound), context)
                except Exception as exc:  # noqa: BLE001 - key generation is a pre-flight boundary.
                    raise _IdempotencyKeyError(
                        "IdempotencyKeyGenerationFailed",
                        "The configured idempotency_key_fn raised an exception.",
                    ) from exc
                if not isinstance(generated, str) or not generated.strip():
                    raise _IdempotencyKeyError(
                        "IdempotencyKeyInvalid",
                        "The configured idempotency_key_fn must return a non-empty string.",
                    )
                return self._set_idempotency_key(context, generated)
            if self.spec.idempotency == IdempotencyPolicy.REQUIRED:
                raise _IdempotencyKeyError(
                    "IdempotencyKeyRequired",
                    "A non-empty idempotency key is required for this tool.",
                )
            return self._auto_hash(context, bound)
        if self.spec.idempotency == IdempotencyPolicy.CACHE_READ:
            return self._cache_read_hash(context, bound)
        return None

    @staticmethod
    def _set_idempotency_key(context: ToolCallContext, key: str) -> str:
        context.metadata["actionlens_idempotency_key"] = key
        return key

    def _operation_arguments(self, bound: inspect.BoundArguments) -> dict[str, Any]:
        arguments = dict(bound.arguments)
        if self.dynamic_arguments:
            assert self.var_keyword_param is not None
            arguments = dict(arguments.get(self.var_keyword_param, {}))
        return _drop_keys(
            arguments,
            {self.spec.idempotency_key_param, "__al_ctx"} | ({"context"} if "context" not in self.original_params else set()),
        )

    def _auto_hash(self, context: ToolCallContext, bound: inspect.BoundArguments) -> str:
        args = _drop_keys(
            self._operation_arguments(bound), set(self.spec.hash_ignore_keys)
        )
        payload = {
            "project": context.project,
            "environment": context.environment,
            "tenant_id": context.tenant_id,
            "session_id": context.session_id,
            "tool_name": self.spec.name,
            "args": args,
        }
        key = sha256(_json_bytes(payload)).hexdigest()
        context.metadata["actionlens_idempotency_key"] = key
        return key

    def _cache_read_hash(self, context: ToolCallContext, bound: inspect.BoundArguments) -> str:
        """Create a scoped, time-bounded cache key for READ-risk tools.

        AUTO_HASH deduplicates a business operation within a session. CACHE_READ
        intentionally omits session/run identity and adds a TTL window so it is
        an actual shared read cache rather than an accidental idempotency alias.
        """

        args = _drop_keys(
            self._operation_arguments(bound), set(self.spec.hash_ignore_keys)
        )
        payload = {
            "project": context.project,
            "environment": context.environment,
            "tenant_id": context.tenant_id,
            "tool_name": self.spec.name,
            "cache_window": int(time.time() // self.spec.cache_ttl_sec),
            "args": args,
        }
        key = "cache:" + sha256(_json_bytes(payload)).hexdigest()
        context.metadata["actionlens_idempotency_key"] = key
        return key

    def _context_key(self, context: ToolCallContext) -> str | None:
        value = context.metadata.get("actionlens_idempotency_key")
        return str(value) if value else None

    def _args_hash(self, bound: inspect.BoundArguments) -> str:
        args = _drop_keys(
            self._operation_arguments(bound), set(self.spec.hash_ignore_keys)
        )
        return canonical_operation_hash(args)

    def _tool_schema_hash(self) -> str:
        spec_payload = self.spec.model_dump(
            mode="json",
            exclude={
                "name": True,
                "description": True,
                "output": {"include_raw_in_trajectory"},
            },
        )
        # Default validation keeps pre-fix schema fingerprints readable. Tool
        # identity is checked independently by the repository.
        if self.spec.validation_mode == "strict":
            spec_payload.pop("validation_mode", None)
        if self.spec.output.max_stream_bytes == 64 * 1024 * 1024:
            spec_payload["output"].pop("max_stream_bytes", None)
        if self.spec.output.max_stream_chunks == 100_000:
            spec_payload["output"].pop("max_stream_chunks", None)
        if self.spec.output.error_message_mode == "classification":
            spec_payload["output"].pop("error_message_mode", None)
        schema = {
            "spec": spec_payload,
            "signature": str(self.public_signature),
        }
        external_argument_schema = getattr(
            self.func, "actionlens_schema_fingerprint", None
        )
        if external_argument_schema is not None:
            schema["external_argument_schema"] = external_argument_schema
        return canonical_operation_hash(schema)

    def _idempotency_conflict(self, key: str, record: Any) -> StructuredToolOutput:
        return StructuredToolOutput(
            status="DENIED",
            result_summary="Idempotency key has been used with different arguments or tool schema; call rejected.",
            error_taxonomy="IdempotencyConflict",
            recovery_hint="Check the call arguments; if this is a new business operation, use a new idempotency key.",
            governance={
                "idempotency_key": key,
                "existing_args_hash": getattr(record, "args_hash", ""),
                "existing_tool_schema_hash": getattr(record, "tool_schema_hash", ""),
            },
        )

    def _uncertain_output(self, key: str, record: Any) -> StructuredToolOutput:
        return StructuredToolOutput(
            status="UNCERTAIN",
            result_summary="Outcome of a previous external side effect is uncertain; auto-retry has been blocked.",
            error_taxonomy="SideEffectUncertain",
            recovery_hint="Query the business system or request human confirmation before explicitly resolving this ledger record.",
            governance={"idempotency_key": key, "ledger_status": "UNCERTAIN",
                        "fencing_token": getattr(record, "fencing_token", 0)},
        )

    def _original_args(
        self, bound: inspect.BoundArguments, context: ToolCallContext
    ) -> tuple[Any, ...]:
        args: list[Any] = []
        for name, param in self.original_signature.parameters.items():
            if name == "__al_ctx":
                if param.kind in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                }:
                    args.append(context)
                continue
            if name not in bound.arguments:
                continue
            if param.kind in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }:
                args.append(bound.arguments[name])
            elif param.kind == inspect.Parameter.VAR_POSITIONAL:
                args.extend(bound.arguments[name])
        return tuple(args)

    def _original_kwargs(
        self, bound: inspect.BoundArguments, context: ToolCallContext
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        for name, param in self.original_signature.parameters.items():
            if name == "__al_ctx":
                if param.kind == inspect.Parameter.KEYWORD_ONLY:
                    kwargs[name] = context
                continue
            if name not in bound.arguments:
                continue
            if name == self.spec.idempotency_key_param and self.injected_idempotency:
                continue
            if param.kind == inspect.Parameter.KEYWORD_ONLY:
                kwargs[name] = bound.arguments[name]
            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                kwargs.update(bound.arguments[name])
        return kwargs


class _LeaseHeartbeat:
    """Refresh a durable execution lease while a tool body is running."""

    def __init__(
        self,
        repository: Any,
        *,
        key: str,
        owner_id: str,
        fencing_token: int,
        lease_seconds: float,
    ) -> None:
        self._repository = repository
        self._key = key
        self._owner_id = owner_id
        self._fencing_token = fencing_token
        self._lease_seconds = lease_seconds
        self._interval_seconds = max(0.001, min(5.0, lease_seconds / 3.0))
        self._stop = threading.Event()
        self._failure_lock = threading.Lock()
        self.failure: Exception | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="actionlens-lease-heartbeat",
            daemon=True,
        )
        self._thread.start()

    @property
    def failed(self) -> bool:
        with self._failure_lock:
            return self.failure is not None

    def stop(self) -> None:
        self._stop.set()
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=max(0.05, self._interval_seconds * 2))

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                renewed = self._repository.heartbeat(
                    self._key,
                    owner_id=self._owner_id,
                    fencing_token=self._fencing_token,
                    lease_seconds=self._lease_seconds,
                )
                if not renewed:
                    raise RuntimeError("lease heartbeat lost ownership")
            except Exception as exc:  # noqa: BLE001 - ownership is no longer provable.
                with self._failure_lock:
                    self.failure = exc
                self._stop.set()
                return


class _StreamedArtifact:
    def __init__(self, *, artifact: ArtifactRef, tail: str, chunks: int) -> None:
        self.artifact = artifact
        self.tail = tail
        self.chunks = chunks


class _StreamedArtifactFailure:
    def __init__(self, error: ArtifactPolicyError) -> None:
        self.error = error


class _StreamCollector:
    """Serialize yielded chunks to a bounded-memory artifact staging stream."""

    def __init__(self, runtime: ToolRuntime, context: ToolCallContext) -> None:
        self._runtime = runtime
        self._context = context
        self._store_redacted = (
            runtime.lens.artifact_store.policy.raw_mode == "redact_then_store"
        )
        self._tail: deque[str] = deque(maxlen=runtime.spec.output.streaming_tail_lines)
        self._saw_binary = False
        self._saw_structured = False
        self.redacted = False
        self.chunks = 0
        self._staged_bytes = 0
        self._reserved_bytes = 0
        self._deadline = (time.monotonic() + runtime.spec.timeout_sec
                          if runtime.spec.timeout_sec is not None else None)
        self._run_key = runtime.lens.artifact_store._run_id_from_metadata(runtime._artifact_metadata(context))

    @property
    def tail(self) -> str:
        return _truncate_utf8("\n".join(self._tail), self._runtime.spec.output.max_inline_bytes)

    @property
    def media_type(self) -> str:
        if self._saw_binary:
            return "application/octet-stream"
        if self._saw_structured:
            return "application/x-ndjson"
        return "text/plain; charset=utf-8"

    def write(self, destination: Any, chunk: Any) -> None:
        if self.chunks >= self._runtime.spec.output.max_stream_chunks:
            raise ArtifactPolicyError("stream staging chunk budget exceeded")
        if isinstance(chunk, (str, bytes, bytearray, memoryview)) and len(chunk) > self._runtime.spec.output.max_stream_bytes - self._staged_bytes:
            raise ArtifactPolicyError("stream staging byte budget exceeded")
        raw, visible, payload = self._serialize(chunk)
        if self._deadline is not None and time.monotonic() >= self._deadline:
            raise TimeoutError("stream collection deadline exceeded")
        if self._staged_bytes + len(payload) > self._runtime.spec.output.max_stream_bytes:
            raise ArtifactPolicyError("stream staging byte budget exceeded")
        self._runtime.lens.artifact_store._reserve_usage(self._run_key, len(payload))
        self._reserved_bytes += len(payload)
        self._staged_bytes += len(payload)
        destination.write(payload)
        self.redacted = self.redacted or raw != payload
        self.chunks += 1
        if self._tail.maxlen:
            limit = self._runtime.spec.output.max_inline_bytes
            visible = visible[-limit:] if limit else ""
            visible = visible.encode("utf-8")[-limit:].decode("utf-8", errors="ignore") if limit else ""
            lines = visible.splitlines() or ([visible] if visible else [])
            self._tail.extend(lines)
            while len("\n".join(self._tail).encode("utf-8")) > limit:
                self._tail.popleft()

    def release(self) -> None:
        self._runtime.lens.artifact_store._release_usage(self._run_key, self._reserved_bytes)
        self._reserved_bytes = 0

    def _serialize(self, chunk: Any) -> tuple[bytes, str, bytes]:
        if isinstance(chunk, str):
            raw = chunk.encode("utf-8")
            visible_value = self._runtime._redact_visible_result(chunk, self._context)
            visible = visible_value if isinstance(visible_value, str) else str(visible_value)
            payload = visible.encode("utf-8") if self._store_redacted else raw
            return raw, visible, payload
        if isinstance(chunk, (bytes, bytearray, memoryview)):
            self._saw_binary = True
            raw = bytes(chunk)
            visible_value = self._runtime._redact_visible_result(
                raw.decode("utf-8", errors="replace"), self._context
            )
            visible = visible_value if isinstance(visible_value, str) else str(visible_value)
            payload = visible.encode("utf-8") if self._store_redacted else raw
            return raw, visible, payload

        self._saw_structured = True
        raw_text = json.dumps(chunk, ensure_ascii=False, default=str, separators=(",", ":"))
        visible_value = self._runtime._redact_visible_result(chunk, self._context)
        visible = json.dumps(
            visible_value, ensure_ascii=False, default=str, separators=(",", ":")
        )
        raw = (raw_text + "\n").encode("utf-8")
        payload = (visible + "\n").encode("utf-8") if self._store_redacted else raw
        return raw, visible, payload


def _json_safe_value(value: Any) -> Any:
    """Return a JSON-native value for model output and durable trajectory data.

    Tool functions are ordinary Python callables, so a valid return value can
    include a Path, dataclass, SDK result, or another opaque object. The public
    ActionLens protocol is JSON-based; preserving an opaque object until the
    outbox serializes it turns a successful call into a misleading governance
    failure. Convert only at this boundary and leave raw artifact persistence
    unchanged.
    """

    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [_json_safe_value(item) for item in sorted(value, key=repr)]
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return _json_safe_value(model_dump(mode="json"))
        except Exception:  # noqa: BLE001 - arbitrary tool results may expose a broken serializer.
            pass
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        return str(value)
    return value


def _validate_argument_value(adapter: TypeAdapter, value: Any, *, strict: bool) -> Any:
    # JSON containers use JSON's date/UUID representations while preserving
    # strict number/bool rules. Native Python objects retain Python semantics.
    if isinstance(value, (dict, list)):
        try:
            payload = json.dumps(value, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError):
            pass
        else:
            return adapter.validate_json(payload, strict=strict)
    return adapter.validate_python(value, strict=strict)


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True).encode(
        "utf-8"
    )


def _safe_reference(value: str | None) -> str | None:
    if value is None:
        return None
    parts = urlsplit(value)
    if not parts.scheme:
        return value
    hostname = parts.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    netloc = hostname
    if parts.port is not None:
        netloc += f":{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _summary(
    value: Any,
    *,
    output_policy: OutputPolicy,
    artifact_policy: ArtifactPolicy,
) -> str:
    content_allowed = (
        output_policy.summary_includes_content
        and artifact_policy.raw_mode not in {"deny", "reference_only"}
        and artifact_policy.encryption != "provider"
    )
    summary_limit = min(160, output_policy.max_inline_bytes)
    if isinstance(value, str):
        if content_allowed:
            return _truncate_utf8(value, summary_limit, marker="...(truncated)")
        return (
            "Returned string "
            f"({len(value.encode('utf-8'))} UTF-8 bytes, sha256={sha256(value.encode('utf-8')).hexdigest()[:12]})"
        )
    if isinstance(value, dict):
        if not content_allowed:
            return f"Returned dict with {len(value)} keys"
        if output_policy.summary_fields is not None:
            selected = {
                name: value[name]
                for name in output_policy.summary_fields
                if name in value
            }
            if not selected:
                return "Returned dict with no configured summary fields present"
            rendered = json.dumps(selected, ensure_ascii=False, default=str, separators=(",", ":"))
            return "Returned dict summary: " + _truncate_utf8(
                rendered, summary_limit, marker="...(truncated)"
            )
        return f"Returned dict with keys: {', '.join(list(map(str, value.keys()))[:8])}"
    if isinstance(value, (list, tuple, set, frozenset)):
        return f"Returned {type(value).__name__} with {len(value)} items"
    return f"Returned {type(value).__name__}"


def _preview_value(value: Any, *, limit: int, max_items: int) -> str:
    value = _truncate_top_level_items(value, max_items)
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    return _truncate_utf8(text, limit, marker="...(artifact preview truncated)")


def _truncate_utf8(text: str, limit: int, *, marker: str = "") -> str:
    if limit <= 0:
        return ""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    marker_bytes = marker.encode("utf-8")
    prefix_limit = limit - len(marker_bytes)
    if prefix_limit < 0:
        marker = ""
        prefix_limit = limit
    prefix = encoded[:prefix_limit]
    while prefix:
        try:
            visible = prefix.decode("utf-8")
            return visible + marker
        except UnicodeDecodeError:
            prefix = prefix[:-1]
    return marker if len(marker.encode("utf-8")) <= limit else ""


def _top_level_item_count(value: Any) -> int | None:
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return len(value)
    return None


def _truncate_top_level_items(value: Any, max_items: int) -> Any:
    if max_items < 0:
        return value
    if isinstance(value, dict):
        return dict(list(value.items())[:max_items])
    if isinstance(value, list):
        return value[:max_items]
    if isinstance(value, tuple):
        return value[:max_items]
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=lambda item: repr(item))[:max_items]
    return value


def _drop_keys(value: Any, keys: set[str]) -> Any:
    if isinstance(value, dict):
        return {
            key: _drop_keys(item, keys)
            for key, item in value.items()
            if str(key) not in keys
        }
    if isinstance(value, list):
        return [_drop_keys(item, keys) for item in value]
    if isinstance(value, tuple):
        return tuple(_drop_keys(item, keys) for item in value)
    return value


def _make_artifact_navigation_tools(lens: ActionLens):
    @lens.tool(name="artifact_read", risk=RiskLevel.READ, max_bytes=8192)
    def artifact_read(
        artifact: ArtifactRef,
        offset: int = 0,
        limit: int = 4096,
        __al_ctx: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        return lens.read_artifact_page(
            artifact,
            offset=offset,
            limit=limit,
            context=__al_ctx,
        )

    @lens.tool(name="artifact_grep", risk=RiskLevel.READ, max_bytes=8192)
    def artifact_grep(
        artifact: ArtifactRef,
        needle: str,
        offset: int = 0,
        scan_bytes: int = 16 * 1024,
        max_matches: int = 20,
        __al_ctx: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        return lens.grep_artifact(
            artifact,
            needle,
            offset=offset,
            scan_bytes=scan_bytes,
            max_matches=max_matches,
            context=__al_ctx,
        )

    return {"artifact_read": artifact_read, "artifact_grep": artifact_grep}


async def _await_thread(function, *args, **kwargs):
    """Keep thread-owned resources alive until synchronous work finishes."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Repeated cancellation must not close a file underneath its writer.
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        try:
            result = task.result()
        except Exception:
            # The host's cancellation remains the outward result even when
            # the tracked worker fails while finishing its own operation.
            pass
        else:
            if getattr(function, "__name__", "") == "_preflight" and result is None:
                try:
                    await _await_thread(function.__self__._handle_exception, args[0], NoSideEffectError("Cancelled before business dispatch."))
                except Exception as exc:
                    function.__self__._governance_failure(args[0], exc, after_execution=False)
        raise
