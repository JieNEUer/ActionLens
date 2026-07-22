"""Temporal Activity governance with a stable ActionLens context.

Install Temporal separately (for example, ``pip install temporalio``). The
workflow owns retry and approval waiting; the Activity re-enters ActionLens
with the same business idempotency key after every retry or signal wake-up.
"""
from __future__ import annotations

from datetime import timedelta

import actionlens as al
from actionlens.integrations.temporal import (
    TemporalActivityRunner,
    context_from_temporal_workflow,
)

# The import is intentionally local to this example. ActionLens itself does
# not require Temporal or import it as part of its core package.
from temporalio import activity, workflow  # type: ignore[import-not-found]


lens = al.ActionLens(project="orders", storage_dir=".actionlens-temporal-demo")


@lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
async def send_confirmation(order_id: str, message: str) -> dict[str, str]:
    # Replace with a provider call that accepts the same business idempotency key.
    return {"order_id": order_id, "message": message, "provider_status": "accepted"}


runner = TemporalActivityRunner(send_confirmation, heartbeater=activity.heartbeat)


@activity.defn
async def send_confirmation_activity(payload: dict[str, str]) -> dict[str, object]:
    info = activity.info()
    context = context_from_temporal_workflow(
        info.workflow_id,
        info.workflow_run_id,
        tool_name="send_confirmation",
        activity_id=info.activity_id,
        attempt=info.attempt,
        project="orders",
    )
    output = await runner.arun(
        context,
        payload["order_id"],
        payload["message"],
        idempotency_key=payload["idempotency_key"],
    )
    return output.model_dump(mode="json")


@workflow.defn
class SendConfirmationWorkflow:
    def __init__(self) -> None:
        self._approval_signal_received = False

    @workflow.signal
    def approval_available(self, decision_id: str) -> None:
        # The signal is only a wake-up. The next Activity invocation re-reads
        # the ActionLens ticket/ledger, which remains the authorization truth.
        self._approval_signal_received = True

    @workflow.run
    async def run(self, payload: dict[str, str]) -> dict[str, object]:
        output = await workflow.execute_activity(
            send_confirmation_activity,
            payload,
            start_to_close_timeout=timedelta(minutes=2),
        )
        if output["status"] == "PENDING_APPROVAL":
            await workflow.wait_condition(lambda: self._approval_signal_received)
            output = await workflow.execute_activity(
                send_confirmation_activity,
                payload,
                start_to_close_timeout=timedelta(minutes=2),
            )
        return output


# An approval endpoint must call lens.approve(...) first, commit the decision,
# then signal this workflow via an outbox consumer. It must not treat a signal
# receipt as proof that approval is valid.
