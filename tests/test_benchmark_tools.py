from __future__ import annotations

import importlib.util
from pathlib import Path


def _postgres_benchmark_module() -> object:
    script = Path(__file__).parents[1] / "benchmarks" / "postgres.py"
    specification = importlib.util.spec_from_file_location(
        "actionlens_postgres_benchmark", script
    )
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_postgres_plan_index_discovery_handles_explain_document_root() -> None:
    module = _postgres_benchmark_module()
    plan = [
        {
            "Plan": {
                "Node Type": "ModifyTable",
                "Plans": [
                    {
                        "Node Type": "Index Scan",
                        "Index Name": "idx_al_outbox_claim_v11",
                    }
                ],
            }
        }
    ]
    assert module._index_nodes(plan) == ["idx_al_outbox_claim_v11"]
