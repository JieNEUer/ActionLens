from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from actionlens.errors import RemoteToolExecutionError, SideEffectUncertainError
from actionlens.models import (
    ConcurrencyPolicy,
    IdempotencyPolicy,
    OutputPolicy,
    RiskLevel,
    StructuredToolOutput,
    ToolCallContext,
)
from actionlens.remote import RemoteJobRef, RemoteJobStatus, RemoteToolRequest, RemoteToolRunner

if TYPE_CHECKING:
    from actionlens.runtime import ActionLens


class RemoteToolAdapter:
    """Run a ``RemoteToolRunner`` through ActionLens' governed tool boundary.

    The adapter deliberately owns only request submission, bounded polling, and
    cancellation. Scheduling, durable job storage, and remote worker execution
    remain the runner's responsibility. The wrapped local callable receives the
    same policies, approvals, idempotency ledger, output shaping, and trajectory
    events as a native Python tool.
    """

    def __init__(
        self,
        lens: "ActionLens",
        runner: RemoteToolRunner,
        *,
        name: str,
        description: str | None = None,
        risk: RiskLevel | str = RiskLevel.READ,
        idempotency: IdempotencyPolicy | str = IdempotencyPolicy.AUTO_HASH,
        idempotency_key_param: str = "idempotency_key",
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
        poll_interval_sec: float = 0.1,
        capability: str | None = None,
        argument_validator: Callable[[dict[str, Any]], None] | None = None,
        schema_fingerprint: Any | None = None,
    ) -> None:
        if not isinstance(runner, RemoteToolRunner):
            raise TypeError("runner must implement RemoteToolRunner")
        if poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be positive")
        effective_risk = RiskLevel(risk)
        effective_idempotency = IdempotencyPolicy(idempotency)
        if effective_idempotency == IdempotencyPolicy.OFF:
            raise ValueError("remote tools require an idempotency policy")

        self.lens = lens
        self.runner = runner
        self.name = name
        self.risk = effective_risk
        self.timeout_sec = timeout_sec
        self.poll_interval_sec = poll_interval_sec
        self.capability = capability

        remote_call = _make_remote_call(self)
        remote_call.__name__ = name
        remote_call.__doc__ = description or f"Governed RemoteToolRunner proxy for {name}."
        remote_call.actionlens_dynamic_arguments = True  # type: ignore[attr-defined]
        if argument_validator is not None:
            remote_call.actionlens_argument_validator = argument_validator  # type: ignore[attr-defined]
        if schema_fingerprint is not None:
            remote_call.actionlens_schema_fingerprint = schema_fingerprint  # type: ignore[attr-defined]

        self._callable = lens.tool(
            name=name,
            description=description,
            risk=effective_risk,
            idempotency=effective_idempotency,
            idempotency_key_param=idempotency_key_param,
            hash_ignore_keys=hash_ignore_keys,
            timeout_sec=timeout_sec,
            run_sync_in_thread=run_sync_in_thread,
            cache_ttl_sec=cache_ttl_sec,
            max_bytes=max_bytes,
            output=output,
            approval_required=approval_required,
            approval_ttl_sec=approval_ttl_sec,
            concurrency=concurrency,
            lease_seconds=lease_seconds,
            fencing_supported=fencing_supported,
        )(remote_call)

    def as_callable(self) -> Callable[..., StructuredToolOutput]:
        return self._callable

    def __call__(self, *args: Any, **kwargs: Any) -> StructuredToolOutput:
        return self._callable(*args, **kwargs)

    def _execute(
        self, arguments: dict[str, Any], context: ToolCallContext
    ) -> StructuredToolOutput:
        key = context.metadata.get("actionlens_idempotency_key")
        args_hash = context.metadata.get("actionlens_args_hash")
        tool_schema_hash = context.metadata.get("actionlens_tool_schema_hash")
        if not key or not args_hash or not tool_schema_hash:
            raise RuntimeError("ActionLens did not establish remote request identity")

        deadline = (
            datetime.now(timezone.utc) + timedelta(seconds=self.timeout_sec)
            if self.timeout_sec is not None
            else None
        )
        deadline_monotonic = (
            time.monotonic() + self.timeout_sec if self.timeout_sec is not None else None
        )
        request = RemoteToolRequest(
            call_id=context.call_id,
            idempotency_key=str(key),
            args=dict(arguments),
            args_hash=str(args_hash),
            tool_schema_hash=str(tool_schema_hash),
            context=context,
            deadline=deadline,
            capability=self.capability,
        )
        try:
            job = RemoteJobRef.model_validate(self.runner.submit(request))
            return self._wait_for_result(job, deadline_monotonic)
        except (
            PermissionError,
            RemoteToolExecutionError,
            SideEffectUncertainError,
            TimeoutError,
        ):
            raise
        except Exception as exc:
            if self.risk in {RiskLevel.MUTATION, RiskLevel.DESTRUCTIVE}:
                raise SideEffectUncertainError(
                    "Remote job submission or result retrieval could not be confirmed."
                ) from exc
            raise

    def _wait_for_result(
        self, job: RemoteJobRef, deadline_monotonic: float | None
    ) -> StructuredToolOutput:
        while True:
            status = RemoteJobStatus.model_validate(self.runner.status(job))
            if status.status == "COMPLETED":
                return self._resolve_completed_job(job)
            if status.status in {"FAILED", "CANCELLED", "UNKNOWN"}:
                if self.risk in {RiskLevel.MUTATION, RiskLevel.DESTRUCTIVE}:
                    raise SideEffectUncertainError(
                        "Remote job did not return a confirmed successful outcome."
                    )
                raise RemoteToolExecutionError(
                    f"Remote job ended with status {status.status}."
                )
            if deadline_monotonic is not None:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    self._cancel_after_timeout(job)
                    raise TimeoutError("Remote job did not finish before its deadline.")
                time.sleep(min(self.poll_interval_sec, remaining))
            else:
                time.sleep(self.poll_interval_sec)

    def _resolve_completed_job(self, job: RemoteJobRef) -> StructuredToolOutput:
        output = StructuredToolOutput.model_validate(self.runner.result(job))
        if output.status == "SUCCESS":
            return output
        if output.status == "UNCERTAIN":
            raise SideEffectUncertainError(
                "Remote runner reported an uncertain side-effect outcome."
            )
        if output.status == "TIMEOUT":
            raise TimeoutError("Remote runner reported a timeout.")
        if output.status == "DENIED":
            raise PermissionError("Remote runner denied the completed job result.")
        if self.risk in {RiskLevel.MUTATION, RiskLevel.DESTRUCTIVE}:
            raise SideEffectUncertainError(
                "Remote runner did not return a confirmed successful outcome."
            )
        raise RemoteToolExecutionError(
            f"Remote runner returned {output.status} for a completed job."
        )

    def _cancel_after_timeout(self, job: RemoteJobRef) -> None:
        try:
            self.runner.cancel(job)
        except Exception:
            # Timeout is already the governing outcome; cancellation is best effort.
            return


def _make_remote_call(adapter: RemoteToolAdapter) -> Callable[..., StructuredToolOutput]:
    # This factory intentionally sits at module scope. A ``__al_ctx`` parameter
    # declared inside a class body is name-mangled by Python and would leak into
    # the public tool signature instead of being consumed by ToolRuntime.
    def remote_call(
        *, __al_ctx: ToolCallContext, **arguments: Any
    ) -> StructuredToolOutput:
        return adapter._execute(arguments, __al_ctx)

    return remote_call
