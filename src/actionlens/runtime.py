from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import json
from collections.abc import Callable
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from .artifacts import FileArtifactStore
from .context import SessionContext, get_current_context, make_generated_context
from .errors import classify_exception, recovery_hint
from .ledger import (
    MemoryLedger,
    SQLiteApprovalTicketStore,
    SQLiteLedger,
)
from .models import (
    ApprovalTicket,
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
from .redaction import CompositeRedactor, KeyRedactor, Redactor, RegexRedactor, redact_value
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
        ledger: Any | None = None,
        ticket_store: Any | None = None,
        policies: list[Policy] | None = None,
        redactor: Redactor | None = None,
    ) -> None:
        self.project = project
        self.storage_dir = Path(storage_dir)
        self.artifact_store = artifact_store or FileArtifactStore(self.storage_dir)
        self.sink = sink or JsonlSink(self.storage_dir)
        default_db = self.storage_dir / "ledger" / "actionlens.sqlite3"
        self.ledger = ledger or SQLiteLedger(default_db)
        self.ticket_store = ticket_store or SQLiteApprovalTicketStore(default_db)
        self.policy_chain = PolicyChain(policies)
        self.redactor = redactor
        self._sequence = 0
        self._tickets: dict[str, str] = {}

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
        concurrency: ConcurrencyPolicy | str = ConcurrencyPolicy.UNKNOWN,
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
                output=policy,
            )
            return self.wrap(target, spec=spec)

        if func is not None:
            return decorate(func)
        return decorate

    def wrap(self, func: Callable[..., Any], *, spec: ToolSpec) -> Callable[..., Any]:
        runtime = ToolRuntime(self, func, spec)
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
        if ticket_id is not None:
            ticket = self.ticket_store.approve(
                ticket_id,
                approved_by=approved_by,
                decision_note=decision_note,
                modified_args=modified_args,
            )
            if ticket is not None and ticket.status != "APPROVED":
                return False
            key = ticket.idempotency_key if ticket is not None else self._tickets.get(ticket_id)
        if key is None:
            return False
        return self.ledger.approve(key) is not None

    def deny(
        self, *, ticket_id: str, decision_note: str | None = None
    ) -> bool:
        ticket = self.ticket_store.deny(ticket_id, decision_note=decision_note)
        if ticket is None:
            return False
        if ticket.idempotency_key:
            self.ledger.fail(ticket.idempotency_key)
        return True

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
    ) -> None:
        self._sequence += 1
        event = TrajectoryEvent(
            event_id=f"evt-{uuid4().hex}",
            timestamp=datetime.now(timezone.utc),
            project=context.project,
            session_id=context.session_id,
            run_id=context.run_id,
            call_id=context.call_id,
            sequence=self._sequence,
            event_type=event_type,
            phase=phase,  # type: ignore[arg-type]
            tool_name=context.tool_name,
            error=error,
            decision=decision,
            output_ref=output_ref,
            metrics=metrics or {},
            metadata=metadata or {},
        )
        self.sink.emit(event)

    def flush(self) -> None:
        self.sink.flush()


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
            async_wrapper.actionlens_spec = self.spec  # type: ignore[attr-defined]
            return async_wrapper

        @functools.wraps(self.func)
        def wrapper(*args: Any, **kwargs: Any) -> StructuredToolOutput:
            return self.invoke(*args, **kwargs)

        wrapper.__signature__ = self.public_signature  # type: ignore[attr-defined]
        wrapper.actionlens_spec = self.spec  # type: ignore[attr-defined]
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
        return base.model_copy(
            update={
                "project": base.project or self.lens.project,
                "call_id": f"call-{uuid4().hex}",
                "tool_name": self.spec.name,
                "context_source": source,
            }
        )

    def _invoke_sync(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        context, bound, early = self._prepare(args, kwargs)
        if early is not None or bound is None:
            return early  # type: ignore[return-value]
        preflight = self._preflight(context, bound)
        if preflight is not None:
            return preflight
        started = datetime.now(timezone.utc)
        try:
            original_args = self._original_args(bound)
            original_kwargs = self._original_kwargs(bound)
            if self.spec.timeout_sec is not None and self.spec.run_sync_in_thread:
                future = _SYNC_EXECUTOR.submit(self.func, *original_args, **original_kwargs)
                result = future.result(timeout=self.spec.timeout_sec)
            else:
                result = self.func(*original_args, **original_kwargs)
        except Exception as exc:  # noqa: BLE001 - mapped into tool protocol.
            return self._handle_exception(context, exc)
        return self._handle_success(context, result, started)

    async def _invoke_async(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        context, bound, early = self._prepare(args, kwargs)
        if early is not None or bound is None:
            return early  # type: ignore[return-value]
        preflight = self._preflight(context, bound)
        if preflight is not None:
            return preflight
        started = datetime.now(timezone.utc)
        try:
            coro = self.func(*self._original_args(bound), **self._original_kwargs(bound))
            if self.spec.timeout_sec is not None:
                result = await asyncio.wait_for(coro, timeout=self.spec.timeout_sec)
            else:
                result = await coro
        except Exception as exc:  # noqa: BLE001 - mapped into tool protocol.
            return self._handle_exception(context, exc)
        return self._handle_success(context, result, started)

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
        if self.spec.approval_required:
            if key is None:
                key = self._auto_hash(context, bound)
            record = self.lens.ledger.get(key)
            if record is not None and record.status == "APPROVED" and record.ticket_id:
                ticket = self.lens.ticket_store.get(record.ticket_id)
                if ticket is not None and ticket.modified_args:
                    for name, value in ticket.modified_args.items():
                        if name in bound.arguments:
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
                ticket = ApprovalTicket(
                    ticket_id=f"ticket-{uuid4().hex}",
                    idempotency_key=key,
                    call_id=context.call_id,
                    tool_name=self.spec.name,
                    safe_args=safe_args,
                    risk=self.spec.risk,
                    reason="Tool requires human approval.",
                )
                self.lens.ledger.mark_approval_pending(
                    key, call_id=context.call_id, ticket_id=ticket.ticket_id
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
        hit_kind, record = self.lens.ledger.begin(key, call_id=context.call_id)
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
            if record.status == "FAILED":
                return None
        return None

    def _handle_exception(
        self, context: ToolCallContext, exc: Exception
    ) -> StructuredToolOutput:
        error = classify_exception(exc)
        key = self._context_key(context)
        if key is not None:
            self.lens.ledger.fail(key)
        self.lens._emit(
            context=context,
            event_type="tool_call.failed",
            phase="POST_FLIGHT",
            error=error,
        )
        return StructuredToolOutput(
            status="FAILED" if error.taxonomy != "Timeout" else "TIMEOUT",
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
        if key is not None:
            self.lens.ledger.succeed(key, output.model_dump(mode="json"))
        latency_ms = (
            datetime.now(timezone.utc) - started
        ).total_seconds() * 1000.0
        self.lens._emit(
            context=context,
            event_type="tool_call.completed",
            phase="POST_FLIGHT",
            output_ref=output_ref,
            metrics={"latency_ms": latency_ms},
        )
        return output

    def _shape_output(
        self, context: ToolCallContext, result: Any
    ) -> tuple[StructuredToolOutput, ArtifactRef | None]:
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
        artifact = self.lens.artifact_store.put(
            result,
            metadata={
                "project": context.project,
                "session_id": context.session_id,
                "run_id": context.run_id,
                "tool_name": context.tool_name,
            },
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

    def _original_args(self, bound: inspect.BoundArguments) -> tuple[Any, ...]:
        args: list[Any] = []
        for name, param in self.original_signature.parameters.items():
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

    def _original_kwargs(self, bound: inspect.BoundArguments) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        for name, param in self.original_signature.parameters.items():
            if name not in bound.arguments:
                continue
            if name == self.spec.idempotency_key_param and self.injected_idempotency:
                continue
            if name == "__al_ctx":
                kwargs[name] = get_current_context()
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


def _summary(value: Any) -> str:
    if isinstance(value, str):
        return value[:160] + ("...(truncated)" if len(value) > 160 else "")
    if isinstance(value, dict):
        return f"返回 dict，字段：{', '.join(list(map(str, value.keys()))[:8])}"
    if isinstance(value, list):
        return f"返回 list，共 {len(value)} 项"
    return f"返回 {type(value).__name__}"


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
