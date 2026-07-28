from __future__ import annotations

from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from .ledger.memory import LedgerRecord

ReconciliationOutcome = Literal[
    "CONFIRMED_SUCCEEDED",
    "CONFIRMED_NOT_APPLIED",
    "STILL_UNCERTAIN",
    "MANUAL_OVERRIDE",
]
ReconciliationTarget = Literal["SUCCEEDED", "NOT_APPLIED", "STILL_UNCERTAIN"]


class ReconciliationResult(BaseModel):
    """Evidence-backed conclusion about an uncertain external side effect."""

    model_config = ConfigDict(frozen=True)

    outcome: ReconciliationOutcome
    summary: str
    output: dict[str, Any] | None = None
    evidence_ref: str | None = None
    override_target: ReconciliationTarget | None = None


@runtime_checkable
class SideEffectReconciler(Protocol):
    def inspect(self, record: LedgerRecord) -> ReconciliationResult: ...


ProviderOperationState = Literal["APPLIED", "NOT_APPLIED", "PENDING", "UNKNOWN"]


class ProviderReconciliationObservation(BaseModel):
    """Normalized result of a provider-specific, read-only status lookup."""

    model_config = ConfigDict(frozen=True)

    state: ProviderOperationState
    summary: str
    evidence_ref: str | None = None
    result: Any | None = None


@runtime_checkable
class ProviderStatusLookup(Protocol):
    def lookup(self, record: LedgerRecord) -> ProviderReconciliationObservation: ...


class ProviderStatusReconciler:
    """Map a provider status lookup onto ActionLens reconciliation semantics.

    The lookup owns provider-specific consistency windows. It must return
    ``UNKNOWN`` rather than ``NOT_APPLIED`` while a negative lookup may still
    be stale.
    """

    def __init__(self, lookup: ProviderStatusLookup):
        self.lookup = lookup

    def inspect(self, record: LedgerRecord) -> ReconciliationResult:
        observation = self.lookup.lookup(record)
        if observation.state in {"APPLIED", "NOT_APPLIED"} and not observation.evidence_ref:
            raise ValueError("a terminal provider observation requires evidence_ref")
        if observation.state == "APPLIED":
            return ReconciliationResult(
                outcome="CONFIRMED_SUCCEEDED",
                summary=observation.summary,
                output={
                    "status": "SUCCESS",
                    "result_summary": observation.summary,
                    "result": observation.result,
                },
                evidence_ref=observation.evidence_ref,
            )
        if observation.state == "NOT_APPLIED":
            return ReconciliationResult(
                outcome="CONFIRMED_NOT_APPLIED",
                summary=observation.summary,
                evidence_ref=observation.evidence_ref,
            )
        return ReconciliationResult(
            outcome="STILL_UNCERTAIN",
            summary=observation.summary,
            evidence_ref=observation.evidence_ref,
        )
