# Provider Reconciliation Cookbook

ActionLens never resolves an `UNCERTAIN` mutation by retrying it blindly. A provider adapter must perform a read-only lookup and return a `ProviderReconciliationObservation`; `ProviderStatusReconciler` maps that observation to the stable ActionLens state machine.

## Standard Reconciliation

```python
import actionlens as al


class PaymentLookup:
    def __init__(self, provider):
        self.provider = provider

    def lookup(self, record):
        transaction = self.provider.find_by_idempotency_key(record.key)
        if transaction is None:
            # Return UNKNOWN until the provider's documented read-after-write
            # consistency window has elapsed.
            return al.ProviderReconciliationObservation(
                state="UNKNOWN",
                summary="transaction is not yet visible",
            )
        if transaction.status == "succeeded":
            return al.ProviderReconciliationObservation(
                state="APPLIED",
                summary="provider transaction succeeded",
                evidence_ref=f"https://audit.internal/payments/{transaction.id}",
                result={"provider_transaction_id": transaction.id},
            )
        if transaction.status in {"pending", "processing"}:
            return al.ProviderReconciliationObservation(
                state="PENDING",
                summary=f"provider transaction is {transaction.status}",
                evidence_ref=f"https://audit.internal/payments/{transaction.id}",
            )
        return al.ProviderReconciliationObservation(
            state="NOT_APPLIED",
            summary=f"provider transaction ended as {transaction.status}",
            evidence_ref=f"https://audit.internal/payments/{transaction.id}",
        )


result = lens.reconcile_uncertain(
    idempotency_key,
    al.ProviderStatusReconciler(PaymentLookup(payment_provider)),
)
```

The `evidence_ref` should point to an immutable or independently retained status record. Do not place provider credentials or signed query parameters in it; ActionLens removes URL credentials, query, and fragments from persisted reconciliation events.

## Manual Override

When automated reconciliation cannot reach a terminal conclusion, a human operator can force a resolution via `MANUAL_OVERRIDE`:

```python
class ManualReconciler:
    def inspect(self, record):
        return al.ReconciliationResult(
            outcome="MANUAL_OVERRIDE",
            summary="operator verified outcome via direct database inspection",
            evidence_ref="https://audit.internal/manual-reviews/rev-42",
            override_target="SUCCEEDED",  # or "NOT_APPLIED" or "STILL_UNCERTAIN"
        )


result = lens.reconcile_uncertain(
    idempotency_key,
    ManualReconciler(),
    actor_id="on-call-engineer",
    reason="provider API outage; verified via direct DB query",
    evidence_ref="https://audit.internal/manual-reviews/rev-42",
)
```

`MANUAL_OVERRIDE` requires `actor_id`, `reason`, and `evidence_ref`. The ledger transition and reconciliation event are committed atomically.

## Custom Reconciler

For providers that don't fit the `ProviderStatusLookup` pattern, implement `SideEffectReconciler` directly:

```python
class EmailReconciler:
    def inspect(self, record):
        # record.key is the ActionLens idempotency key
        # record.tool_name, record.session_id, etc. are available
        receipt = email_provider.find_receipt(record.key)
        if receipt is None:
            return al.ReconciliationResult(
                outcome="STILL_UNCERTAIN",
                summary="no delivery receipt found yet",
            )
        if receipt.delivered:
            return al.ReconciliationResult(
                outcome="CONFIRMED_SUCCEEDED",
                summary=f"email delivered to {receipt.recipient}",
                output={
                    "status": "SUCCESS",
                    "result_summary": f"email delivered to {receipt.recipient}",
                    "result": {"message_id": receipt.message_id},
                },
                evidence_ref=f"https://audit.internal/emails/{receipt.message_id}",
            )
        return al.ReconciliationResult(
            outcome="CONFIRMED_NOT_APPLIED",
            summary="email was rejected before send",
            evidence_ref=f"https://audit.internal/emails/{receipt.message_id}",
        )


result = lens.reconcile_uncertain(idempotency_key, EmailReconciler())
```

A terminal reconciliation (`CONFIRMED_SUCCEEDED` or `CONFIRMED_NOT_APPLIED`) requires `evidence_ref`. `CONFIRMED_SUCCEEDED` also requires a `StructuredToolOutput` payload with `status="SUCCESS"`.

## Domain Matrix

| Domain | Provider idempotency identity | Read-only reconciliation query | Safe `APPLIED` evidence | When `NOT_APPLIED` is allowed |
| --- | --- | --- | --- | --- |
| Payment | Provider idempotency key equal to the ActionLens ledger key | Get/search transaction by idempotency key or operation ID | Final provider transaction ID and terminal succeeded status | Only after the provider's creation visibility window and terminal absence rule |
| Email | Stable custom argument/message key persisted before send | Provider message activity API or a durable sender-owned receipt table | Provider message ID plus accepted/delivered status | Only when the provider or receipt store guarantees the key was never accepted |
| GitHub PR | Repository + base + head, with the ActionLens key in a marker or durable mapping | Query open/closed PRs by head/base, then verify marker and content hash | Repository, PR number, head SHA, and marker | Only after all PR states are queried and the head/marker cannot exist under another number |
| Object storage | Bucket + canonical key + expected content SHA-256 | `HEAD` the object and compare version/etag/checksum metadata | Bucket, key, immutable version ID, and expected checksum | Only after read-after-write semantics or the documented consistency window make absence conclusive |

## Required Adapter Decisions

Every provider adapter must document and test:

1. How the ActionLens idempotency key reaches the provider or a durable correlation table.
2. Which statuses are terminal success, terminal non-application, pending, and ambiguous.
3. The provider's read-after-write or eventual-consistency window. During that window, a negative lookup is `UNKNOWN`.
4. How long reconciliation is retried by the host and when it escalates to a human. ActionLens records facts but does not own the scheduler.
5. Which immutable evidence proves the conclusion and how long that evidence is retained.
6. How output is reconstructed after confirmed success without repeating the mutation.

`NOT_APPLIED` permits the existing operation to become retryable. It is therefore the most dangerous classification: absence from an eventually consistent search endpoint is not sufficient proof.