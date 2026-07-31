"""DBOS Step governance with a stable ActionLens context.

This file deliberately keeps DBOS registration in the host application: DBOS
workflow decorators and context access vary across supported releases, while
the ActionLens bridge has no DBOS dependency. Pass the workflow and stable Step
identity supplied by the pinned DBOS runtime into this function.

v1.5 additions shown in this example:

- ``approval_resolver`` for synchronous approval without a durable round-trip
- ``lens.close()`` at the application lifecycle boundary
- ``child_context()`` for sub-agent delegation within a Step
- ``CACHE_READ`` for read-only tools that benefit from a shared TTL cache
"""
from __future__ import annotations

import actionlens as al
from actionlens.integrations.dbos import DBOSStepRunner, context_from_dbos_workflow


def auto_approve(
    ticket: al.ApprovalTicket, context: al.ToolCallContext
) -> dict[str, str]:
    """Synchronous approval resolver.

    In production this would prompt an operator or check an external
    authorization system. The resolver returns APPROVE/DENY/PENDING;
    ActionLens persists the decision before the business function runs.
    """
    print(f"Auto-approving {ticket.tool_name} for {ticket.idempotency_key}")
    return {"action": "APPROVE", "approved_by": "dbos-auto-approver"}


lens = al.ActionLens(
    project="orders",
    storage_dir=".actionlens-dbos-demo",
    approval_resolver=auto_approve,
)


@lens.tool(
    risk=al.RiskLevel.MUTATION,
    idempotency=al.IdempotencyPolicy.REQUIRED,
    approval_required=True,
)
def send_confirmation(order_id: str, message: str) -> dict[str, str]:
    # Replace with the provider call. The business idempotency key below must
    # also be passed to any external provider that supports it.
    return {"order_id": order_id, "message": message, "provider_status": "accepted"}


@lens.tool(idempotency=al.IdempotencyPolicy.CACHE_READ, cache_ttl_sec=60)
def lookup_order(order_id: str) -> dict[str, str]:
    return {"order_id": order_id, "status": "shipped"}


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


def governed_lookup_order_step(
    *,
    workflow_id: str,
    step_id: str,
    order_id: str,
    retry_attempt: int = 0,
) -> dict[str, object]:
    """A read-only Step that uses a child context and CACHE_READ."""

    parent = context_from_dbos_workflow(
        workflow_id,
        tool_name="lookup_order",
        step_id=step_id,
        attempt=retry_attempt,
        project="orders",
    )
    child = lens.child_context(parent=parent, tool_name="lookup_order")
    output = lookup_order(order_id, __al_ctx=child)
    return output.model_dump(mode="json")


# A DBOS workflow should use its durable send/recv mechanism only to wake a
# waiting approval branch. The external approval handler first calls
# lens.approve(...); after that transaction commits, an outbox consumer sends
# the ticket/decision identity. The resumed Step re-reads ActionLens state.
#
# When the synchronous approval_resolver is configured (as above), the first
# Step invocation can resolve the approval inline without a durable wake-up.
# The durable ticket is still persisted for audit.

# At the owning application lifecycle boundary:
# lens.close()
