from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import actionlens as al
from actionlens.evals import EvalCaseCandidate, EvalCaseProvenance
from actionlens.exporters import (
    export_eval_candidates,
    export_evidence_bundle,
    export_inspect_samples,
    map_eval_candidate_to_inspect,
)
from actionlens.models import PolicyDecision, TrajectoryEvent
from actionlens.ledger.memory import LedgerRecord
from actionlens.sinks import OTEL_GENAI_PROFILE_V1, OpenTelemetrySink
from actionlens.schema import read_eval_case_candidate


FIXTURES = Path(__file__).parent / "fixtures"


def _write_run(storage_dir: Path) -> None:
    lens = al.ActionLens(project="checkout", storage_dir=storage_dir)

    @lens.tool
    def lookup_payment(transaction_id: str) -> dict[str, str]:
        return {"status": "succeeded", "transaction_id": transaction_id}

    with lens.session(session_id="session-1", run_id="run-1"):
        lookup_payment("txn-1")
    lens.close()


def _ready_context() -> al.EvalCaseContext:
    return al.EvalCaseContext(
        case_id="case-payment-001",
        task_input={"question": "Was txn-1 applied?", "token": "sk-private"},
        environment_spec=al.EvalEnvironmentSpec(
            name="payments-sandbox", version="2026-07-01"
        ),
        target={"status": "SUCCESS"},
        rubric={"business_state_matches": True},
        outcome_evidence=[
            al.OutcomeEvidence(
                kind="provider_status", summary="provider reports succeeded", verified_by="payments-api"
            )
        ],
        scorer_version="payment-state.v1",
    )


def test_eval_candidate_declares_missing_host_facts_and_has_stable_id(tmp_path: Path) -> None:
    _write_run(tmp_path)
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    result = export_eval_candidates(tmp_path, first)
    export_eval_candidates(tmp_path, second)

    candidate = json.loads(first.read_text(encoding="utf-8"))
    repeated = json.loads(second.read_text(encoding="utf-8"))
    assert result == {"exported": 1, "ready": 0, "incomplete": 1, "skipped": 0}
    assert candidate["case_id"] == repeated["case_id"]
    assert candidate["case_id"].startswith("alcase_")
    assert candidate["state"] == "INCOMPLETE"
    assert candidate["missing_fields"] == [
        "task_input",
        "environment_spec",
        "target_or_rubric",
        "outcome_evidence",
        "scorer_version",
    ]
    assert candidate["provenance"]["artifact_bodies_read"] is False
    assert read_eval_case_candidate(candidate).case_id == candidate["case_id"]


def test_eval_candidate_ready_context_is_redacted_and_inspect_filter_is_explicit(
    tmp_path: Path,
) -> None:
    _write_run(tmp_path)
    output = tmp_path / "candidates.jsonl"
    result = export_eval_candidates(tmp_path, output, contexts={"run-1": _ready_context()})
    candidate = json.loads(output.read_text(encoding="utf-8"))
    assert result["ready"] == 1
    assert candidate["state"] == "READY"
    assert candidate["task_input"]["token"] == "[REDACTED]"

    inspect_output = tmp_path / "inspect.jsonl"
    filtered = export_inspect_samples(tmp_path, inspect_output, require_ready=True)
    assert filtered == {"exported": 0, "filtered": 1, "skipped": 0}


def test_inspect_mapper_matches_golden_contract() -> None:
    candidate = EvalCaseCandidate(
        case_id="case-payment-001",
        state="READY",
        task_input="Check whether payment txn-1 was applied",
        environment_spec=al.EvalEnvironmentSpec(
            name="payments-sandbox",
            version="2026-07-01",
            spec_ref="https://example.invalid/env/payments-v1",
        ),
        target={"status": "SUCCESS"},
        rubric={"business_state_matches": True},
        outcome_evidence=[
            al.OutcomeEvidence(
                kind="provider_status",
                summary="provider reports succeeded",
                ref="https://example.invalid/evidence/txn-1",
                verified_by="payments-api",
            )
        ],
        scorer_version="payment-state.v1",
        trajectory=[
            {
                "event_id": "event-1",
                "event_type": "tool_call.completed",
                "tool_name": "lookup_payment",
            }
        ],
        provenance=EvalCaseProvenance(
            project="checkout",
            session_id="session-1",
            run_id="run-1",
            source_event_ids=["event-1"],
            source_files=[],
        ),
    )
    expected = json.loads(
        (FIXTURES / "eval" / "inspect-sample-v1.json").read_text(encoding="utf-8")
    )
    assert map_eval_candidate_to_inspect(candidate) == expected


def test_evidence_bundle_hash_chain_and_scope_declarations(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    _write_run(storage)
    bundle = tmp_path / "bundle"
    result = export_evidence_bundle(
        storage,
        bundle,
        retention_policy_id="regulated-six-months.v1",
        retention_days=180,
        host_context_ref="https://audit.invalid/context/run-1",
    )
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    events = [json.loads(line) for line in (bundle / "events.jsonl").read_text(encoding="utf-8").splitlines()]
    integrity = [
        json.loads(line)
        for line in (bundle / "integrity.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    previous = "0" * 64
    for event, item in zip(events, integrity, strict=True):
        canonical = json.dumps(
            event, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        payload_hash = hashlib.sha256(canonical).hexdigest()
        previous = hashlib.sha256(bytes.fromhex(previous) + bytes.fromhex(payload_hash)).hexdigest()
        assert item["chain_sha256"] == previous

    assert result["exported"] == 2
    assert manifest["integrity"]["chain_root_sha256"] == previous
    assert manifest["integrity"]["signed"] is False
    assert manifest["redaction"]["artifact_bodies_read"] is False
    assert manifest["missing_evidence"] == [
        "signature_manifest",
        "worm_archive_attestation",
        "actor_authorization_snapshot",
    ]


def test_remote_artifact_reference_is_not_reported_as_dangling(tmp_path: Path) -> None:
    trajectory = tmp_path / "trajectories"
    trajectory.mkdir()
    event = _otel_event("tool_call.completed").model_dump(mode="json")
    event["output_ref"] = {
        "uri": "s3://bucket/result?signature=private",
        "media_type": "application/json",
        "size_bytes": 10,
        "sha256": "0" * 64,
    }
    (trajectory / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    output = tmp_path / "inspect.jsonl"
    from actionlens.exporters import export_inspect_ai

    export_inspect_ai(tmp_path, output)
    sample = json.loads(output.read_text(encoding="utf-8"))
    artifact = sample["transcript"][0]["artifact"]
    assert artifact["uri"] == "s3://bucket/result"
    assert artifact["dangling"] is False


class _FakeSpan:
    def __init__(self) -> None:
        self.attributes: dict[str, Any] = {}
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.ended = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: dict[str, Any]) -> None:
        self.events.append((name, attributes))

    def set_status(self, status: Any) -> None:
        self.status = status

    def end(self) -> None:
        self.ended = True


class _FakeTracer:
    def __init__(self) -> None:
        self.names: list[str] = []
        self.span = _FakeSpan()

    def start_span(self, name: str) -> _FakeSpan:
        self.names.append(name)
        return self.span


def _otel_event(event_type: str, *, metadata: dict[str, Any] | None = None) -> TrajectoryEvent:
    return TrajectoryEvent(
        event_id=f"event-{event_type}",
        timestamp=datetime.now(timezone.utc),
        project="checkout",
        session_id="session-1",
        run_id="run-1",
        sequence=1,
        event_type=event_type,
        phase="POST_FLIGHT" if event_type == "tool_call.completed" else "PRE_FLIGHT",
        call_id="call-1",
        tool_name="lookup_payment",
        metadata=metadata or {},
    )


def test_otel_genai_mapping_matches_profile_and_excludes_payloads() -> None:
    profile_fixture = json.loads(
        (FIXTURES / "otel" / "genai-mapping-v1.json").read_text(encoding="utf-8")
    )
    assert OTEL_GENAI_PROFILE_V1.profile_id == profile_fixture["profile_id"]
    assert OTEL_GENAI_PROFILE_V1.semconv_revision == profile_fixture["semconv_revision"]
    assert OTEL_GENAI_PROFILE_V1.operation_name_attribute == profile_fixture["attributes"]["operation_name"]
    assert OTEL_GENAI_PROFILE_V1.tool_name_attribute == profile_fixture["attributes"]["tool_name"]
    assert OTEL_GENAI_PROFILE_V1.tool_call_id_attribute == profile_fixture["attributes"]["tool_call_id"]

    tracer = _FakeTracer()
    sink = OpenTelemetrySink(tracer)
    sink.emit(_otel_event("tool_call.started", metadata={"args": {"token": "secret"}}))
    sink.emit(_otel_event("policy.decision", metadata={"prompt": "private"}))
    assert tracer.span.ended is False
    sink.emit(
        _otel_event(
            "tool_call.completed",
            metadata={"output": "private", "artifact_uri": "s3://private/result"},
        )
    )

    assert tracer.names == ["execute_tool lookup_payment"]
    assert tracer.span.ended is True
    assert tracer.span.attributes["gen_ai.operation.name"] == "execute_tool"
    assert tracer.span.attributes["gen_ai.tool.name"] == "lookup_payment"
    assert tracer.span.attributes["gen_ai.tool_call.id"] == "call-1"
    flattened = json.dumps(tracer.span.attributes, sort_keys=True)
    assert "secret" not in flattened
    assert "private" not in flattened
    assert set(tracer.span.attributes).issubset(
        set(profile_fixture["attributes"].values())
        | set(profile_fixture["allowed_actionlens_attributes"])
    )


def test_policy_denial_records_preflight_terminal_and_closes_otel_span(tmp_path: Path) -> None:
    class DenyPolicy:
        def decide(self, *, spec: Any, args: dict[str, Any], context: Any) -> PolicyDecision:
            return PolicyDecision(action="DENY", reason="blocked")

    tracer = _FakeTracer()
    sink = OpenTelemetrySink(tracer)
    lens = al.ActionLens(
        project="secure",
        storage_dir=tmp_path,
        sink=sink,
        policies=[DenyPolicy()],
    )

    @lens.tool
    def mutate() -> str:
        raise AssertionError("denied tool executed")

    output = mutate()
    assert output.status == "DENIED"
    assert tracer.span.ended is True
    assert [name for name, _ in tracer.span.events][-1] == "tool_call.preflight_resolved"
    assert tracer.span.attributes["actionlens.tool.terminal_event"] == (
        "tool_call.preflight_resolved"
    )


def test_provider_status_reconciler_requires_terminal_evidence() -> None:
    now = datetime.now(timezone.utc)
    record = LedgerRecord(
        key="payment-1", status="UNCERTAIN", call_id="call-1", created_at=now, updated_at=now
    )

    @dataclass
    class Lookup:
        observation: al.ProviderReconciliationObservation

        def lookup(self, lookup_record: LedgerRecord) -> al.ProviderReconciliationObservation:
            assert lookup_record.key == "payment-1"
            return self.observation

    pending = al.ProviderStatusReconciler(
        Lookup(al.ProviderReconciliationObservation(state="UNKNOWN", summary="not visible yet"))
    ).inspect(record)
    assert pending.outcome == "STILL_UNCERTAIN"

    applied = al.ProviderStatusReconciler(
        Lookup(
            al.ProviderReconciliationObservation(
                state="APPLIED",
                summary="provider succeeded",
                evidence_ref="https://audit.invalid/payments/txn-1?token=secret",
                result={"provider_id": "txn-1"},
            )
        )
    ).inspect(record)
    assert applied.outcome == "CONFIRMED_SUCCEEDED"
    assert applied.output == {
        "status": "SUCCESS",
        "result_summary": "provider succeeded",
        "result": {"provider_id": "txn-1"},
    }

    missing_evidence = al.ProviderStatusReconciler(
        Lookup(al.ProviderReconciliationObservation(state="NOT_APPLIED", summary="absent"))
    )
    try:
        missing_evidence.inspect(record)
    except ValueError as exc:
        assert "requires evidence_ref" in str(exc)
    else:
        raise AssertionError("terminal reconciliation without evidence was accepted")
