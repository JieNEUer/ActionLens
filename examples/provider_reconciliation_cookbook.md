# Provider Reconciliation Cookbook

ActionLens never resolves an `UNCERTAIN` mutation by retrying it blindly. A provider adapter must perform a read-only lookup and return a `ProviderReconciliationObservation`; `ProviderStatusReconciler` maps that observation to the stable ActionLens state machine.

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
