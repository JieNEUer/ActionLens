from __future__ import annotations

from collections import OrderedDict
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
        run_key = (context.project, context.run_id)
        tool_key = (context.project, context.run_id, spec.name)
        with self._lock:  # type: ignore[attr-defined]
            run_counts = self._run_counts  # type: ignore[attr-defined]
            tool_counts = self._tool_counts  # type: ignore[attr-defined]
            run_counts[run_key] = run_counts.get(run_key, 0) + 1
            run_counts.move_to_end(run_key)
            tool_counts[tool_key] = tool_counts.get(tool_key, 0) + 1
            self._evict_oldest_runs(run_counts, tool_counts)

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

    def reset_run(self, *, project: str, run_id: str) -> None:
        """Release budget state for a run once its host considers it complete."""

        run_key = (project, run_id)
        with self._lock:  # type: ignore[attr-defined]
            self._run_counts.pop(run_key, None)  # type: ignore[attr-defined]
            tool_counts = self._tool_counts  # type: ignore[attr-defined]
            for key in [key for key in tool_counts if key[:2] == run_key]:
                tool_counts.pop(key, None)

    @property
    def tracked_run_count(self) -> int:
        with self._lock:  # type: ignore[attr-defined]
            return len(self._run_counts)  # type: ignore[attr-defined]

    def _evict_oldest_runs(
        self,
        run_counts: OrderedDict[tuple[str, str], int],
        tool_counts: dict[tuple[str, str, str], int],
    ) -> None:
        while len(run_counts) > self.max_tracked_runs:
            evicted_key, _ = run_counts.popitem(last=False)
            for tool_key in [key for key in tool_counts if key[:2] == evicted_key]:
                tool_counts.pop(tool_key, None)


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
