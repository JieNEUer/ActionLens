from __future__ import annotations

import copy
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any, Protocol

from .models import PolicyDecision, ToolCallContext, ToolSpec


class Policy(Protocol):
    def decide(
        self,
        *,
        spec: ToolSpec,
        args: dict[str, Any],
        context: ToolCallContext,
    ) -> PolicyDecision:
        ...


@dataclass(frozen=True)
class BudgetPolicy:
    max_calls_per_run: int | None = None
    max_calls_per_tool_per_run: int | None = None
    max_tracked_runs: int = 10_000

    def __post_init__(self) -> None:
        if self.max_tracked_runs <= 0:
            raise ValueError("max_tracked_runs must be positive")
        object.__setattr__(self, "_run_counts", OrderedDict())
        object.__setattr__(self, "_tool_counts", {})
        object.__setattr__(self, "_lock", RLock())

    def decide(
        self,
        *,
        spec: ToolSpec,
        args: dict[str, Any],
        context: ToolCallContext,
    ) -> PolicyDecision:
        run_key = (context.project, context.environment, context.tenant_id, context.run_id)
        tool_key = (*run_key, spec.name)
        with self._lock:  # type: ignore[attr-defined]
            run_counts = self._run_counts  # type: ignore[attr-defined]
            tool_counts = self._tool_counts  # type: ignore[attr-defined]
            if run_key not in run_counts and len(run_counts) >= self.max_tracked_runs:
                return PolicyDecision(action="DENY", reason="Active run accounting capacity exhausted.",
                                      metadata={"taxonomy": "BudgetExceeded", "scope": "local_instance"})
            run_counts[run_key] = run_counts.get(run_key, 0) + 1
            run_counts.move_to_end(run_key)
            tool_counts[tool_key] = tool_counts.get(tool_key, 0) + 1

            if (
                self.max_calls_per_run is not None
                and run_counts[run_key] > self.max_calls_per_run
            ):
                return PolicyDecision(
                    action="DENY",
                    reason="Run tool-call budget exceeded.",
                    metadata={
                        "taxonomy": "BudgetExceeded",
                        "limit": self.max_calls_per_run,
                        "scope": "run",
                    },
                )
            if (
                self.max_calls_per_tool_per_run is not None
                and tool_counts[tool_key] > self.max_calls_per_tool_per_run
            ):
                return PolicyDecision(
                    action="DENY",
                    reason="Per-tool run budget exceeded.",
                    metadata={
                        "taxonomy": "BudgetExceeded",
                        "limit": self.max_calls_per_tool_per_run,
                        "scope": "tool_run",
                    },
                )
        return PolicyDecision(action="ALLOW", reason="Budget available.")

    def reset_run(self, *, project: str, run_id: str, context: ToolCallContext | None = None) -> None:
        """Release budget state for a run once its host considers it complete."""

        with self._lock:  # type: ignore[attr-defined]
            run_counts = self._run_counts  # type: ignore[attr-defined]
            scopes = {key for key in run_counts if key[0] == project and key[-1] == run_id
                      and (context is None or key[1:3] == (context.environment, context.tenant_id))}
            for key in scopes:
                run_counts.pop(key, None)
            tool_counts = self._tool_counts  # type: ignore[attr-defined]
            for key in [key for key in tool_counts if key[:-1] in scopes]:
                tool_counts.pop(key, None)

    @property
    def tracked_run_count(self) -> int:
        with self._lock:  # type: ignore[attr-defined]
            return len(self._run_counts)  # type: ignore[attr-defined]



class PolicyChain:
    def __init__(self, policies: list[Policy] | None = None):
        self.policies = policies or []

    def decide(
        self,
        *,
        spec: ToolSpec,
        args: dict[str, Any],
        context: ToolCallContext,
        validate_patch: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
        on_decision: Callable[[PolicyDecision], None] | None = None,
        charge_budget: bool = True,
    ) -> tuple[PolicyDecision, dict[str, Any]]:
        current_args = dict(args)
        final = PolicyDecision(action="ALLOW", reason="No policy denied execution.")
        for policy in self.policies:
            if not charge_budget and isinstance(policy, BudgetPolicy):
                continue
            decision = policy.decide(spec=spec, args=copy.deepcopy(current_args), context=context)
            if on_decision is not None:
                on_decision(decision)
            if decision.action == "MODIFY_ARGS" and decision.modified_args is not None:
                effective = None
                if validate_patch is not None:
                    effective = validate_patch(decision.modified_args)
                if effective is None:
                    current_args.update(decision.modified_args)
                else:
                    current_args = effective
                final = decision
                continue
            if decision.action != "ALLOW":
                return decision, current_args
            if final.action != "MODIFY_ARGS":
                final = decision
        return final, current_args
