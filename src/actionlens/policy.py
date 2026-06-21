from __future__ import annotations

from dataclasses import dataclass
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

    def __post_init__(self) -> None:
        object.__setattr__(self, "_run_counts", {})
        object.__setattr__(self, "_tool_counts", {})

    def decide(
        self,
        *,
        spec: ToolSpec,
        args: dict[str, Any],
        context: ToolCallContext,
    ) -> PolicyDecision:
        run_key = (context.project, context.run_id)
        tool_key = (context.project, context.run_id, spec.name)
        run_counts = self._run_counts  # type: ignore[attr-defined]
        tool_counts = self._tool_counts  # type: ignore[attr-defined]
        run_counts[run_key] = run_counts.get(run_key, 0) + 1
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


class PolicyChain:
    def __init__(self, policies: list[Policy] | None = None):
        self.policies = policies or []

    def decide(
        self,
        *,
        spec: ToolSpec,
        args: dict[str, Any],
        context: ToolCallContext,
    ) -> tuple[PolicyDecision, dict[str, Any]]:
        current_args = dict(args)
        final = PolicyDecision(action="ALLOW", reason="No policy denied execution.")
        for policy in self.policies:
            decision = policy.decide(spec=spec, args=current_args, context=context)
            if decision.action == "MODIFY_ARGS" and decision.modified_args is not None:
                current_args.update(decision.modified_args)
                final = decision
                continue
            if decision.action != "ALLOW":
                return decision, current_args
            final = decision
        return final, current_args
