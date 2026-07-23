from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import json
import threading
from urllib.parse import urlsplit, urlunsplit
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import TypeAdapter, ValidationError

from .artifacts import ArtifactPolicyError, FileArtifactStore, MediaMetadataExtractor
from .context import SessionContext, get_current_context, make_generated_context
from .errors import classify_exception, recovery_hint
from .ledger import (
    SQLiteApprovalTicketStore,
    SQLiteLedger,
)
from .models import (
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
from .policy import Policy, PolicyChain
from .outbox import OutboxDispatcher
from .repositories import SQLiteGovernanceRepository
from .repository import canonical_operation_hash
from .reconciliation import ReconciliationResult, SideEffectReconciler
from .redaction import Redactor, redact_value
from .sinks import JsonlSink


_SYNC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="actionlens-sync")


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
    ) -> None:
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
            self.ledger = self.repository.ledger
            self.ticket_store = self.repository.tickets
            self.outbox_dispatcher = OutboxDispatcher(self.repository, self.sink)
        else:
            self.ledger = ledger or SQLiteLedger(default_db)
            self.ticket_store = ticket_store or SQLiteApprovalTicketStore(default_db)
            self.outbox_dispatcher = None
        self.policy_chain = PolicyChain(policies)
        self.redactor = redactor
        self._sequence = 0
        self._sequence_lock = threading.Lock()
        self._sink_error_count = 0
        self._tickets: dict[str, str] = {}
        self._runtimes: dict[str, ToolRuntime] = {}

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

    def tool(
        self,
        func: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        description: str | None = None,
        risk: RiskLevel | str = RiskLevel.READ,
        idempotency: IdempotencyPolicy | str = IdempotencyPolicy.OFF,
        idempotency_key_param: str = "idempotency_key",
        hash_ignore_keys: list[str] | None = None,
        timeout_sec: float | None = None,
        run_sync_in_thread: bool = False,
        max_bytes: int | None = None,
        output: OutputPolicy | None = None,
        approval_required: bool = False,
        approval_ttl_sec: float | None = 86400.0,
        concurrency: ConcurrencyPolicy | str = ConcurrencyPolicy.UNKNOWN,
        lease_seconds: float = 30.0,
        fencing_supported: bool = False,
    ):
        def decorate(target: Callable[..., Any]):
            policy = output or OutputPolicy()
            if max_bytes is not None:
                policy = policy.model_copy(
                    update={
                        "max_inline_bytes": max_bytes,
                        "artifact_threshold_bytes": max_bytes,
                    }
                )
            spec = ToolSpec(
                name=name or target.__name__,
                description=description or inspect.getdoc(target),
                risk=RiskLevel(risk),
                idempotency=IdempotencyPolicy(idempotency),
                idempotency_key_param=idempotency_key_param,
                hash_ignore_keys=hash_ignore_keys or [],
                timeout_sec=timeout_sec,
                run_sync_in_thread=run_sync_in_thread,
                concurrency=ConcurrencyPolicy(concurrency),
                approval_required=approval_required,
                approval_ttl_sec=approval_ttl_sec,
                lease_seconds=lease_seconds,
                fencing_supported=fencing_supported,
                output=policy,
            )
            return self.wrap(target, spec=spec)

        if func is not None:
            return decorate(func)
        return decorate

    def wrap(self, func: Callable[..., Any], *, spec: ToolSpec) -> Callable[..., Any]:
        runtime = ToolRuntime(self, func, spec)
        self._runtimes[spec.name] = runtime
        return runtime.as_callable()

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
            key = ticket.idempotency_key if ticket is not None else self._tickets.get(ticket_id)
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

    def _validate_approval_args(
        self, ticket: ApprovalTicket, modified_args: dict[str, Any] | None
    ) -> None:
        if modified_args is None:
            return
        allowed = set(ticket.metadata.get("allowed_args", ticket.safe_args))
        unknown = set(modified_args) - allowed
        if unknown:
            raise ValueError(
                f"modified_args contains unknown parameters: {', '.join(sorted(unknown))}"
            )
        runtime = self._runtimes.get(ticket.tool_name)
        if runtime is not None:
            runtime.validate_modified_args(modified_args)

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
        results: list[StructuredToolOutput | None] = [None] * len(calls)
        parallel: list[tuple[int, Callable[..., Any], tuple[Any, ...], dict[str, Any]]] = []
        for index, (func, args, kwargs) in enumerate(calls):
            spec = getattr(func, "actionlens_spec", None)
            can_parallel = (
                spec is not None
                and spec.risk in {RiskLevel.READ, RiskLevel.EXTERNAL_IO}
                and spec.concurrency == ConcurrencyPolicy.SAFE
            )
            if can_parallel:
                parallel.append((index, func, args, kwargs))
            else:
                results[index] = func(*args, **kwargs)

        if parallel:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_to_index = {
                    pool.submit(func, *args, **kwargs): index
                    for index, func, args, kwargs in parallel
                }
                for future in concurrent.futures.as_completed(future_to_index):
                    results[future_to_index[future]] = future.result()
        return [result for result in results if result is not None]

    def inspect_artifact(
        self, artifact: ArtifactRef, *, context: ToolCallContext | None = None
    ) -> dict[str, Any]:
        result = self.artifact_store.inspect(artifact)
        event_context = context or get_current_context() or make_generated_context(
            self.project, "artifact.inspect"
        )
        parts = urlsplit(artifact.uri)
        safe_uri = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
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
            parts = urlsplit(artifact.uri)
            safe_uri = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
            self._emit(
                context=event_context,
                event_type="artifact.accessed",
                phase="POST_FLIGHT",
                metadata={"uri": safe_uri, "status": status, "sha256": artifact.sha256},
            )

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
            error=None if status == "SUCCEEDED" else result.summary,
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
            sequence = self._sequence
        event = TrajectoryEvent(
            event_id=f"evt-{uuid4().hex}",
            timestamp=datetime.now(timezone.utc),
            project=context.project,
            session_id=context.session_id,
            run_id=context.run_id,
            call_id=context.call_id,
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
        if self.outbox_dispatcher is not None:
            self.outbox_dispatcher.stop()
        self.sink.close()
        if self._owns_repository and self.repository is not None:
            close_repository = getattr(self.repository, "close", None)
            if close_repository is not None:
                close_repository()


class ToolRuntime:
    def __init__(self, lens: ActionLens, func: Callable[..., Any], spec: ToolSpec):
        self.lens = lens
        self.func = func
        self.spec = spec
        self.original_signature = inspect.signature(func)
        self.public_signature = self._build_public_signature()
        self.original_params = set(self.original_signature.parameters)
        self.injected_idempotency = (
            self.spec.idempotency == IdempotencyPolicy.REQUIRED
            and self.spec.idempotency_key_param not in self.original_params
        )

    def as_callable(self) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(self.func):

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
        unknown = set(modified_args) - self.original_params
        if unknown:
            raise ValueError(
                f"modified_args contains unknown parameters: {', '.join(sorted(unknown))}"
            )
        for name, value in modified_args.items():
            annotation = self.original_signature.parameters[name].annotation
            if annotation is inspect.Parameter.empty:
                continue
            try:
                TypeAdapter(annotation).validate_python(value, strict=True)
            except ValidationError as exc:
                raise ValueError(f"modified_args[{name!r}] does not match its annotation") from exc

    def redact(self, value: Any, context: ToolCallContext) -> Any:
        return self._redact_visible_result(value, context)

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
                result_summary="工具参数校验失败。",
                error_taxonomy=error.taxonomy,
                recovery_hint=recovery_hint(error),
            )
        context = self._resolve_context(explicit_context)
        return context, bound, None

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
            update={
                "project": base.project or self.lens.project,
                "call_id": call_id,
                "tool_name": self.spec.name,
                "context_source": source,
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
        try:
            original_args = self._original_args(bound, context)
            original_kwargs = self._original_kwargs(bound, context)
            if self.spec.timeout_sec is not None and self.spec.run_sync_in_thread:
                future = _SYNC_EXECUTOR.submit(self.func, *original_args, **original_kwargs)
                result = future.result(timeout=self.spec.timeout_sec)
            else:
                result = self.func(*original_args, **original_kwargs)
        except Exception as exc:  # noqa: BLE001 - mapped into tool protocol.
            try:
                return self._handle_exception(context, exc)
            except Exception as governance_exc:
                return self._governance_failure(context, governance_exc, after_execution=True)
        try:
            return self._handle_success(context, result, started)
        except Exception as exc:  # business result exists but durable commit did not complete
            return self._governance_failure(context, exc, after_execution=True)

    async def _invoke_async(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        # Governance may perform synchronous database, filesystem, and sink I/O.
        # Keep it off the host framework's event loop; the tool coroutine itself
        # still runs in the caller's event loop.
        context, bound, early = await asyncio.to_thread(self._prepare, args, kwargs)
        if early is not None or bound is None:
            return early  # type: ignore[return-value]
        try:
            preflight = await asyncio.to_thread(self._preflight, context, bound)
        except Exception as exc:  # governance failed before business execution
            return self._governance_failure(context, exc, after_execution=False)
        if preflight is not None:
            await asyncio.to_thread(self._record_preflight_result, context, preflight)
            return preflight
        started = datetime.now(timezone.utc)
        try:
            coro = self.func(
                *self._original_args(bound, context),
                **self._original_kwargs(bound, context),
            )
            if self.spec.timeout_sec is not None:
                result = await asyncio.wait_for(coro, timeout=self.spec.timeout_sec)
            else:
                result = await coro
        except Exception as exc:  # noqa: BLE001 - mapped into tool protocol.
            try:
                return await asyncio.to_thread(self._handle_exception, context, exc)
            except Exception as governance_exc:
                return self._governance_failure(context, governance_exc, after_execution=True)
        try:
            return await asyncio.to_thread(self._handle_success, context, result, started)
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
                "工具已执行，但治理状态未能持久化，结果不确定。"
                if uncertain else "治理存储暂时不可用，工具未执行。"
            ),
            error_taxonomy=(
                "GovernanceCommitUncertain" if uncertain else "GovernanceUnavailable"
            ),
            recovery_hint=(
                "请先核对业务系统和 ledger 状态，不要自动重试。"
                if uncertain else "可在治理存储恢复后重试；本次未执行工具业务逻辑。"
            ),
            governance={"failure_type": type(exc).__name__, "after_execution": after_execution},
        )
        self.lens._emit(
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
        return output

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
        safe_args = self._safe_arguments(bound)
        self.lens._emit(
            context=context,
            event_type="tool_call.started",
            phase="PRE_FLIGHT",
            metadata={"args": safe_args, "context_source": context.context_source},
        )
        decision, modified_args = self.lens.policy_chain.decide(
            spec=self.spec,
            args=safe_args,
            context=context,
        )
        if decision.action != "ALLOW":
            self.lens._emit(
                context=context,
                event_type="policy.decision",
                phase="PRE_FLIGHT",
                decision=decision,
            )
        if decision.action == "DENY":
            taxonomy = decision.metadata.get("taxonomy", "PermissionDenied")
            return StructuredToolOutput(
                status="DENIED",
                result_summary="工具调用被策略拒绝。",
                error_taxonomy=str(taxonomy),
                recovery_hint="请不要绕过策略，向用户请求授权或总结已有结果。",
                governance={"policy_reason": decision.reason},
            )
        if decision.action == "MODIFY_ARGS":
            for name, value in modified_args.items():
                if name in bound.arguments:
                    bound.arguments[name] = value
            safe_args = self._safe_arguments(bound)
        key = self._idempotency_key(context, bound)
        args_hash = self._args_hash(bound)
        tool_schema_hash = self._tool_schema_hash()
        context.metadata["actionlens_args_hash"] = args_hash
        context.metadata["actionlens_tool_schema_hash"] = tool_schema_hash
        if self.spec.approval_required:
            if key is None:
                key = self._auto_hash(context, bound)
            record = self.lens.ledger.get(key)
            if record is not None and (
                getattr(record, "args_hash", "") not in {"", args_hash}
                or getattr(record, "tool_schema_hash", "") not in {"", tool_schema_hash}
            ):
                return self._idempotency_conflict(key, record)
            if record is not None and record.status == "APPROVED" and record.ticket_id:
                ticket = self.lens.ticket_store.get(record.ticket_id)
                if ticket is not None and ticket.modified_args:
                    try:
                        self.validate_modified_args(ticket.modified_args)
                    except ValueError:
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
                            result_summary="审批修改后的参数不符合工具 schema，工具未执行。",
                            error_taxonomy="ApprovalArgsInvalid",
                            recovery_hint="请修正审批参数后使用新的幂等键重新发起操作。",
                            governance={"ticket_id": ticket.ticket_id},
                        )
                    for name, value in ticket.modified_args.items():
                        bound.arguments[name] = value
                    safe_args = self._safe_arguments(bound)
            if record is None or record.status not in {"APPROVED", "SUCCEEDED"}:
                if record is not None and record.ticket_id:
                    existing_ticket = self.lens.ticket_store.get(record.ticket_id)
                    if existing_ticket is not None and existing_ticket.status == "DENIED":
                        return StructuredToolOutput(
                            status="DENIED",
                            result_summary="人类审批已拒绝，工具未执行。",
                            error_taxonomy="ApprovalDenied",
                            recovery_hint="请不要重复发起同一高风险操作，向用户汇报审批拒绝结果。",
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
                            result_summary="人类审批票据已过期，工具未执行。",
                            error_taxonomy="ApprovalExpired",
                            recovery_hint="请使用新的幂等键重新发起审批。",
                            governance={"ticket_id": record.ticket_id, "idempotency_key": key},
                        )
                    if existing_ticket is not None and existing_ticket.status == "PENDING":
                        return StructuredToolOutput(
                            status="PENDING_APPROVAL",
                            result_summary="相同操作已在等待人类审批。",
                            result={"ticket_id": existing_ticket.ticket_id},
                            recovery_hint=(
                                "操作已挂起等待人类审批。请停止调用其他工具，"
                                "向用户汇报已提交审批，并结束当前会话。"
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
                        "allowed_args": sorted(self.original_params - {"__al_ctx"}),
                        "context": context.model_dump(mode="json"),
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
                        ):
                            return self._idempotency_conflict(key, approval_record)
                        return StructuredToolOutput(
                            status="PENDING_APPROVAL",
                            result_summary="相同操作已在等待人类审批。",
                            result={"ticket_id": stored_ticket.ticket_id},
                            recovery_hint=("操作已挂起等待人类审批。请停止调用其他工具，"
                                           "向用户汇报已提交审批，并结束当前会话。"),
                            governance={"ticket_id": stored_ticket.ticket_id, "idempotency_key": key},
                        )
                    self.lens._tickets[ticket.ticket_id] = key
                    self.lens.dispatch_outbox()
                    return StructuredToolOutput(
                        status="PENDING_APPROVAL",
                        result_summary="操作已提交人类审批，尚未执行。",
                        result={"ticket_id": ticket.ticket_id},
                        recovery_hint=("操作已挂起等待人类审批。请停止调用其他工具，"
                                       "向用户汇报已提交审批，并结束当前会话。"),
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
                        result_summary="相同操作已在等待人类审批。",
                        result={"ticket_id": approval_record.ticket_id},
                        recovery_hint=(
                            "操作已挂起等待人类审批。请停止调用其他工具，"
                            "向用户汇报已提交审批，并结束当前会话。"
                        ),
                        governance={
                            "ticket_id": approval_record.ticket_id,
                            "idempotency_key": key,
                            "ticket_status": existing_ticket.status if existing_ticket else "PENDING",
                        },
                    )
                self.lens.ticket_store.create(ticket)
                self.lens._tickets[ticket.ticket_id] = key
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
                return StructuredToolOutput(
                    status="PENDING_APPROVAL",
                    result_summary="操作已提交人类审批，尚未执行。",
                    result={"ticket_id": ticket.ticket_id},
                    recovery_hint=(
                        "操作已挂起等待人类审批。请停止调用其他工具，"
                        "向用户汇报已提交审批，并结束当前会话。"
                    ),
                    governance={"ticket_id": ticket.ticket_id, "idempotency_key": key},
                )
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
            hit_kind, record = self.lens.ledger.begin(
                key, call_id=context.call_id, context=context, spec=self.spec
            )
        if hit_kind == "hit":
            self.lens._emit(
                context=context,
                event_type="idempotency.hit",
                phase="PRE_FLIGHT",
                metadata={"status": record.status, "idempotency_key": key},
            )
            if record.status == "SUCCEEDED" and record.output is not None:
                return StructuredToolOutput.model_validate(record.output)
            if record.status == "APPROVAL_PENDING":
                if record.ticket_id:
                    ticket = self.lens.ticket_store.get(record.ticket_id)
                    if ticket is not None and ticket.status == "DENIED":
                        return StructuredToolOutput(
                            status="DENIED",
                            result_summary="人类审批已拒绝，工具未执行。",
                            error_taxonomy="ApprovalDenied",
                            recovery_hint="请不要重复发起同一高风险操作，向用户汇报审批拒绝结果。",
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
                            result_summary="人类审批票据已过期，工具未执行。",
                            error_taxonomy="ApprovalExpired",
                            recovery_hint="请使用新的幂等键重新发起审批。",
                            governance={"ticket_id": record.ticket_id, "idempotency_key": key},
                        )
                return StructuredToolOutput(
                    status="PENDING_APPROVAL",
                    result_summary="相同操作已在等待人类审批。",
                    result={"ticket_id": record.ticket_id},
                    recovery_hint=(
                        "操作已挂起等待人类审批。请停止调用其他工具，"
                        "向用户汇报已提交审批，并结束当前会话。"
                    ),
                    governance={"ticket_id": record.ticket_id, "idempotency_key": key},
                )
            if record.status == "PENDING":
                return StructuredToolOutput(
                    status="SKIPPED",
                    result_summary="相同操作正在执行或已被接管，已跳过重复调用。",
                    governance={"idempotency_key": key, "ledger_status": record.status},
                )
            if record.status == "EXECUTING":
                return StructuredToolOutput(
                    status="SKIPPED",
                    result_summary="相同操作正在由另一个 lease owner 执行，已跳过重复调用。",
                    governance={"idempotency_key": key, "ledger_status": record.status,
                                "fencing_token": record.fencing_token},
                )
            if record.status == "UNCERTAIN":
                return self._uncertain_output(key, record)
            if record.status in {"FAILED", "FAILED_RETRYABLE"}:
                return None
        return None

    def _handle_exception(
        self, context: ToolCallContext, exc: Exception
    ) -> StructuredToolOutput:
        error = classify_exception(exc)
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
                    "UNCERTAIN" if error.taxonomy == "SideEffectUncertain"
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
            self.lens.ledger.fail(key)
            self.lens._deliver_event(event)
        else:
            self.lens._deliver_event(event)
        return StructuredToolOutput(
            status=("UNCERTAIN" if error.taxonomy == "SideEffectUncertain"
                    else "FAILED" if error.taxonomy != "Timeout" else "TIMEOUT"),
            result_summary=f"工具执行失败：{error.taxonomy}",
            error_taxonomy=error.taxonomy,
            recovery_hint=recovery_hint(error),
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
            metadata={"output": output.model_dump(mode="json")},
        )
        if key is not None and self.lens.repository is not None:
            owner_id = context.metadata.get("actionlens_owner_id")
            fencing_token = context.metadata.get("actionlens_fencing_token")
            if owner_id is None or fencing_token is None:
                self.lens._deliver_event(event)
            else:
                self.lens.repository.finish(
                    key, owner_id=str(owner_id), fencing_token=int(fencing_token),
                    status="FAILED_TERMINAL" if governance_failed else "SUCCEEDED",
                    output=None if governance_failed else output.model_dump(mode="json"),
                    error=None, event=event,
                )
                self.lens.dispatch_outbox()
        elif key is not None:
            if governance_failed:
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
        if isinstance(result, ArtifactRef):
            if self.lens.artifact_store.policy.raw_mode == "reference_only":
                try:
                    self.lens.artifact_store.validate_reference(result)
                except ArtifactPolicyError as exc:
                    return (
                        StructuredToolOutput(
                            status="FAILED",
                            result_summary="外部 artifact 引用不符合安全策略。",
                            error_taxonomy="ArtifactPolicyDenied",
                            recovery_hint="请返回允许 scheme 且不含凭据、query 或 fragment 的 URI。",
                            governance={"reason": str(exc)},
                        ),
                        None,
                    )
            return (
                StructuredToolOutput(
                    status="SUCCESS",
                    result_summary="工具返回了外部 artifact 引用。",
                    artifact_refs=[result],
                    governance={"reference_only": True},
                ),
                result,
            )
        visible_result = self._redact_visible_result(result, context)
        raw_bytes = _json_bytes(visible_result)
        if len(raw_bytes) <= self.spec.output.max_inline_bytes:
            return (
                StructuredToolOutput(
                    status="SUCCESS",
                    result_summary=_summary(visible_result),
                    result=visible_result,
                ),
                None,
            )
        policy = self.lens.artifact_store.policy
        value_to_store = visible_result if policy.raw_mode == "redact_then_store" else result
        try:
            artifact = self.lens.artifact_store.put(
                value_to_store,
                metadata={
                    "project": context.project,
                    "session_id": context.session_id,
                    "run_id": context.run_id,
                    "tool_name": context.tool_name,
                },
                preview=_preview_value(visible_result),
                redacted=_json_bytes(visible_result) != _json_bytes(result),
            )
        except ArtifactPolicyError as exc:
            return (
                StructuredToolOutput(
                    status="FAILED",
                    result_summary="结果超过内联限制且 artifact 策略禁止存储。",
                    error_taxonomy="ArtifactPolicyDenied",
                    recovery_hint="请缩小工具输出，或返回业务方已持久化的 ArtifactRef。",
                    governance={"artifact_policy": policy.raw_mode, "reason": str(exc)},
                ),
                None,
            )
        preview = artifact.preview or ""
        preview = self._redact_visible_result(preview, context)
        inline_preview = preview[: self.spec.output.max_inline_bytes]
        return (
            StructuredToolOutput(
                status="SUCCESS",
                result_summary=(
                    f"结果超过 {self.spec.output.max_inline_bytes} bytes，"
                    "已截断并存入 artifact。"
                ),
                result=inline_preview,
                artifact_refs=[artifact],
                governance={"truncated": True},
            ),
            artifact,
        )

    def _safe_arguments(self, bound: inspect.BoundArguments) -> dict[str, Any]:
        safe = redact_value(
            dict(bound.arguments),
            keys=self.spec.output.redact_keys,
            patterns=self.spec.output.redact_patterns,
        )
        if self.lens.redactor is not None:
            return self.lens.redactor.redact(safe)
        return safe

    def _redact_visible_result(self, value: Any, context: ToolCallContext) -> Any:
        redacted = redact_value(
            value,
            keys=self.spec.output.redact_keys,
            patterns=self.spec.output.redact_patterns,
        )
        if self.lens.redactor is not None:
            return self.lens.redactor.redact(redacted, context)
        return redacted

    def _idempotency_key(
        self, context: ToolCallContext, bound: inspect.BoundArguments
    ) -> str | None:
        if self.spec.idempotency == IdempotencyPolicy.OFF:
            return None
        if self.spec.idempotency == IdempotencyPolicy.REQUIRED:
            key = bound.arguments.get(self.spec.idempotency_key_param)
            if key:
                context.metadata["actionlens_idempotency_key"] = str(key)
                return str(key)
            return self._auto_hash(context, bound)
        if self.spec.idempotency in {
            IdempotencyPolicy.AUTO_HASH,
            IdempotencyPolicy.CACHE_READ,
        }:
            return self._auto_hash(context, bound)
        return None

    def _auto_hash(self, context: ToolCallContext, bound: inspect.BoundArguments) -> str:
        args = _drop_keys(
            dict(bound.arguments),
            set(self.spec.hash_ignore_keys)
            | {self.spec.idempotency_key_param, "__al_ctx", "context"},
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

    def _context_key(self, context: ToolCallContext) -> str | None:
        value = context.metadata.get("actionlens_idempotency_key")
        return str(value) if value else None

    def _args_hash(self, bound: inspect.BoundArguments) -> str:
        args = _drop_keys(
            dict(bound.arguments),
            set(self.spec.hash_ignore_keys)
            | {self.spec.idempotency_key_param, "__al_ctx", "context"},
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
        schema = {
            "spec": spec_payload,
            "signature": str(self.public_signature),
        }
        return canonical_operation_hash(schema)

    def _idempotency_conflict(self, key: str, record: Any) -> StructuredToolOutput:
        return StructuredToolOutput(
            status="DENIED",
            result_summary="幂等键已用于不同的参数或工具 schema，调用已拒绝。",
            error_taxonomy="IdempotencyConflict",
            recovery_hint="请检查调用参数；若这是新的业务操作，请使用新的幂等键。",
            governance={
                "idempotency_key": key,
                "existing_args_hash": getattr(record, "args_hash", ""),
                "existing_tool_schema_hash": getattr(record, "tool_schema_hash", ""),
            },
        )

    def _uncertain_output(self, key: str, record: Any) -> StructuredToolOutput:
        return StructuredToolOutput(
            status="UNCERTAIN",
            result_summary="先前执行的外部副作用结果不确定，已阻止自动重试。",
            error_taxonomy="SideEffectUncertain",
            recovery_hint="请查询业务系统或请求人工确认，然后再显式处置该 ledger 记录。",
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


def _summary(value: Any) -> str:
    if isinstance(value, str):
        return value[:160] + ("...(truncated)" if len(value) > 160 else "")
    if isinstance(value, dict):
        return f"返回 dict，字段：{', '.join(list(map(str, value.keys()))[:8])}"
    if isinstance(value, list):
        return f"返回 list，共 {len(value)} 项"
    return f"返回 {type(value).__name__}"


def _preview_value(value: Any, limit: int = 240) -> str:
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    return text[:limit] + ("...(artifact preview truncated)" if len(text) > limit else "")


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
