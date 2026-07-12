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

