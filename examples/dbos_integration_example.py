"""DBOS Step governance with a stable ActionLens context.

This file deliberately keeps DBOS registration in the host application: DBOS
workflow decorators and context access vary across supported releases, while
the ActionLens bridge has no DBOS dependency. Pass the workflow and stable Step
identity supplied by the pinned DBOS runtime into this function.
"""
from __future__ import annotations

import actionlens as al
from actionlens.integrations.dbos import DBOSStepRunner, context_from_dbos_workflow


lens = al.ActionLens(project="orders", storage_dir=".actionlens-dbos-demo")


@lens.tool(risk=al.RiskLevel.MUTATION, idempotency=al.IdempotencyPolicy.REQUIRED)
def send_confirmation(order_id: str, message: str) -> dict[str, str]:
    # Replace with the provider call. The business idempotency key below must
    # also be passed to any external provider that supports it.
    return {"order_id": order_id, "message": message, "provider_status": "accepted"}


runner = DBOSStepRunner(send_confirmation)


def governed_send_confirmation_step(
    *,
    workflow_id: str,
    step_id: str,
    order_id: str,
    message: str,
    idempotency_key: str,
    retry_attempt: int = 0,
) -> dict[str, object]:
    """Register this callable as a DBOS Step in the host application."""

    context = context_from_dbos_workflow(
        workflow_id,
        tool_name="send_confirmation",
        step_id=step_id,
        attempt=retry_attempt,
        project="orders",
    )
    output = runner.run(
        context,
        order_id,
        message,
        idempotency_key=idempotency_key,
    )
    return output.model_dump(mode="json")


# A DBOS workflow should use its durable send/recv mechanism only to wake a
# waiting approval branch. The external approval handler first calls
# lens.approve(...); after that transaction commits, an outbox consumer sends
# the ticket/decision identity. The resumed Step re-reads ActionLens state.
