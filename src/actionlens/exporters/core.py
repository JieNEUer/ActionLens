from __future__ import annotations

import json
import hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

from actionlens.evals import (
    EVAL_CASE_SCHEMA,
    EVAL_CONTEXTS_SCHEMA,
    EVAL_MAPPER_VERSION,
    INSPECT_MAPPER_VERSION,
    EvalCaseCandidate,
    EvalCaseContext,
    EvalCaseProvenance,
    EvalEnvironmentSpec,
    OutcomeEvidence,
    candidate_missing_fields,
    resolve_case_context,
    stable_case_id,
)
from actionlens.trajectory import TrajectoryReadStats, iter_trajectory_events
from actionlens.redaction import redact_value


def load_events(
    storage_dir: str | Path,
    *,
    session_id: str | None = None,
    run_id: str | None = None,
    project: str | None = None,
) -> tuple[list[dict[str, Any]], TrajectoryReadStats]:
    stats = TrajectoryReadStats()
    events = [
        event
        for event in iter_trajectory_events(storage_dir, stats=stats)
        if (session_id is None or event.get("session_id") == session_id)
        and (run_id is None or event.get("run_id") == run_id)
        and (project is None or event.get("project") == project)
    ]
    events.sort(key=lambda event: (str(event.get("timestamp", "")), int(event.get("sequence", 0))))
    return events, stats


def summarize_events(
    storage_dir: str | Path,
    **filters: str | None,
) -> dict[str, Any]:
    events, stats = load_events(storage_dir, **filters)
    by_type: Counter[str] = Counter()
    by_status: Counter[str] = Counter()
    by_error: Counter[str] = Counter()
    by_tool: dict[str, Counter[str]] = defaultdict(Counter)
    sessions: set[str] = set()
    runs: set[str] = set()
    artifact_refs = 0
    dangling_refs = 0
    for event in events:
        event_type = str(event.get("event_type", "unknown"))
        by_type[event_type] += 1
        sessions.add(str(event.get("session_id", "unknown")))
        runs.add(str(event.get("run_id", "unknown")))
        tool = event.get("tool_name")
        if tool:
            by_tool[str(tool)][event_type] += 1
        status = _event_status(event)
        if status:
            by_status[status] += 1
        error = event.get("error") or {}
        if isinstance(error, dict) and error.get("taxonomy"):
            by_error[str(error["taxonomy"])] += 1
        ref = event.get("output_ref")
        if isinstance(ref, dict):
            artifact_refs += 1
            if _is_dangling(ref):
                dangling_refs += 1
    return {
        "events": len(events),
        "sessions": sorted(sessions),
        "runs": sorted(runs),
        "by_type": dict(by_type),
        "by_status": dict(by_status),
        "by_error_taxonomy": dict(by_error),
        "by_tool": {tool: dict(counts) for tool, counts in by_tool.items()},
        "artifact_refs": artifact_refs,
        "dangling_artifact_refs": dangling_refs,
        "skipped_lines": stats.skipped,
        "source_files": stats.files,
    }


def export_events(storage_dir: str | Path, output: str | Path, **filters: str | None) -> dict[str, int]:
    events, stats = load_events(storage_dir, **filters)
    _write_jsonl(Path(output), events)
    return {"exported": len(events), "skipped": stats.skipped}


def export_inspect_ai(
    storage_dir: str | Path, output: str | Path, **filters: str | None
) -> dict[str, int]:
    events, stats = load_events(storage_dir, **filters)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[(str(event.get("session_id", "unknown")), str(event.get("run_id", "unknown")))].append(event)
    samples: list[dict[str, Any]] = []
    for (session_id, run_id), run_events in grouped.items():
        transcript = []
        for event in run_events:
            item = {
                "timestamp": event.get("timestamp"),
                "event": event.get("event_type"),
                "tool": event.get("tool_name"),
                "call_id": event.get("call_id"),
                "phase": event.get("phase"),
                "error": event.get("error"),
                "decision": event.get("decision"),
                "metrics": event.get("metrics", {}),
                "metadata": event.get("metadata", {}),
            }
            if isinstance(event.get("output_ref"), dict):
                item["artifact"] = _safe_artifact(event["output_ref"])
            transcript.append(
                _sanitize({key: value for key, value in item.items() if value not in (None, {}, [])})
            )
        samples.append(
            {
                "id": run_id,
                "session_id": session_id,
                "run_id": run_id,
                "transcript": transcript,
                "metadata": {
                    "format": "actionlens.inspect-transcript.v1",
                    "scope": "tool-boundary transcript; no scorer or replay guarantee",
                },
            }
        )
    _write_jsonl(Path(output), samples)
    return {"exported": len(samples), "skipped": stats.skipped}


def load_eval_case_contexts(path: str | Path | None) -> dict[str, EvalCaseContext]:
    if path is None:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("eval case contexts must be a JSON object")
    if "schema_version" in payload:
        if payload.get("schema_version") != EVAL_CONTEXTS_SCHEMA:
            raise ValueError(f"unsupported eval contexts schema: {payload.get('schema_version')!r}")
        payload = payload.get("runs", {})
    if not isinstance(payload, dict):
        raise ValueError("eval case contexts 'runs' must be a JSON object")
    return {str(key): EvalCaseContext.model_validate(value) for key, value in payload.items()}


def build_eval_case_candidates(
    storage_dir: str | Path,
    *,
    contexts: Mapping[str, EvalCaseContext] | None = None,
    **filters: str | None,
) -> tuple[list[EvalCaseCandidate], TrajectoryReadStats]:
    events, stats = load_events(storage_dir, **filters)
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[
            (
                str(event.get("project", "unknown")),
                str(event.get("session_id", "unknown")),
                str(event.get("run_id", "unknown")),
            )
        ].append(event)
    source_files = _source_hashes(Path(storage_dir))
    case_contexts = contexts or {}
    candidates: list[EvalCaseCandidate] = []
    for (project, session_id, run_id), run_events in grouped.items():
        context = resolve_case_context(
            case_contexts, project=project, session_id=session_id, run_id=run_id
        )
        missing = candidate_missing_fields(context)
        candidates.append(
            EvalCaseCandidate(
                case_id=context.case_id or stable_case_id(project, session_id, run_id),
                state="INCOMPLETE" if missing else "READY",
                task_input=_sanitize(context.task_input),
                environment_spec=(
                    EvalEnvironmentSpec.model_validate(
                        _sanitize(context.environment_spec.model_dump(mode="json"))
                        | {"spec_ref": _safe_reference(context.environment_spec.spec_ref)}
                    )
                    if context.environment_spec is not None
                    else None
                ),
                target=_sanitize(context.target),
                rubric=_sanitize(context.rubric),
                outcome_evidence=[
                    OutcomeEvidence.model_validate(
                        _sanitize(item.model_dump(mode="json"))
                        | {"ref": _safe_reference(item.ref)}
                    )
                    for item in context.outcome_evidence
                ],
                scorer_version=context.scorer_version,
                trajectory=[_candidate_event(event) for event in run_events],
                provenance=EvalCaseProvenance(
                    project=project,
                    session_id=session_id,
                    run_id=run_id,
                    source_event_ids=[
                        str(event["event_id"]) for event in run_events if event.get("event_id")
                    ],
                    source_files=source_files,
                ),
                missing_fields=missing,
                metadata=_sanitize(context.metadata),
            )
        )
    return candidates, stats


def export_eval_candidates(
    storage_dir: str | Path,
    output: str | Path,
    *,
    contexts: Mapping[str, EvalCaseContext] | None = None,
    **filters: str | None,
) -> dict[str, int]:
    candidates, stats = build_eval_case_candidates(storage_dir, contexts=contexts, **filters)
    rows = [candidate.model_dump(mode="json", exclude_none=False) for candidate in candidates]
    _write_jsonl(Path(output), rows)
    ready = sum(candidate.state == "READY" for candidate in candidates)
    _write_export_manifest(
        Path(output),
        storage_dir=Path(storage_dir),
        format_name=EVAL_CASE_SCHEMA,
        mapper_version=EVAL_MAPPER_VERSION,
        exported=len(candidates),
        skipped=stats.skipped,
        extra={"ready": ready, "incomplete": len(candidates) - ready},
    )
    return {
        "exported": len(candidates),
        "ready": ready,
        "incomplete": len(candidates) - ready,
        "skipped": stats.skipped,
    }


def map_eval_candidate_to_inspect(candidate: EvalCaseCandidate) -> dict[str, Any]:
    """Map a candidate to an Inspect-oriented Sample, never to an EvalLog."""

    return _sanitize({
        "id": candidate.case_id,
        "input": candidate.task_input,
        "target": candidate.target,
        "metadata": {
            "schema": "actionlens.inspect-sample.v1",
            "mapper_version": INSPECT_MAPPER_VERSION,
            "candidate_schema": candidate.schema_version,
            "candidate_state": candidate.state,
            "environment_spec": (
                candidate.environment_spec.model_dump(mode="json")
                | {"spec_ref": _safe_reference(candidate.environment_spec.spec_ref)}
                if candidate.environment_spec is not None
                else None
            ),
            "rubric": candidate.rubric,
            "outcome_evidence": [
                item.model_dump(mode="json") | {"ref": _safe_reference(item.ref)}
                for item in candidate.outcome_evidence
            ],
            "scorer_version": candidate.scorer_version,
            "missing_fields": candidate.missing_fields,
            "provenance": candidate.provenance.model_dump(mode="json"),
            "actionlens_trajectory": candidate.trajectory,
            "scope": "dataset sample candidate; no scorer, sandbox, replay, or EvalLog guarantee",
        },
    })


def export_inspect_samples(
    storage_dir: str | Path,
    output: str | Path,
    *,
    contexts: Mapping[str, EvalCaseContext] | None = None,
    require_ready: bool = False,
    **filters: str | None,
) -> dict[str, int]:
    candidates, stats = build_eval_case_candidates(storage_dir, contexts=contexts, **filters)
    selected = [candidate for candidate in candidates if not require_ready or candidate.state == "READY"]
    _write_jsonl(Path(output), [map_eval_candidate_to_inspect(item) for item in selected])
    filtered = len(candidates) - len(selected)
    _write_export_manifest(
        Path(output),
        storage_dir=Path(storage_dir),
        format_name="actionlens.inspect-sample.v1",
        mapper_version=INSPECT_MAPPER_VERSION,
        exported=len(selected),
        skipped=stats.skipped,
        extra={"filtered_incomplete": filtered, "require_ready": require_ready},
    )
    return {"exported": len(selected), "filtered": filtered, "skipped": stats.skipped}


def export_evidence_bundle(
    storage_dir: str | Path,
    output: str | Path,
    *,
    retention_policy_id: str | None = None,
    retention_days: int | None = None,
    host_context_ref: str | None = None,
    actor_authorization_ref: str | None = None,
    **filters: str | None,
) -> dict[str, int]:
    if retention_days is not None and retention_days <= 0:
        raise ValueError("retention_days must be positive")
    output_dir = Path(output)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"evidence bundle output directory is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    events, stats = load_events(storage_dir, **filters)
    sanitized_events = [_evidence_event(event) for event in events]
    event_path = output_dir / "events.jsonl"
    _write_jsonl(event_path, sanitized_events)

    previous = "0" * 64
    integrity_rows: list[dict[str, Any]] = []
    for index, event in enumerate(sanitized_events):
        payload_hash = hashlib.sha256(_canonical_json(event)).hexdigest()
        chain_hash = hashlib.sha256(bytes.fromhex(previous) + bytes.fromhex(payload_hash)).hexdigest()
        integrity_rows.append(
            {
                "sequence": index + 1,
                "event_id": event.get("event_id"),
                "payload_sha256": payload_hash,
                "previous_chain_sha256": previous,
                "chain_sha256": chain_hash,
            }
        )
        previous = chain_hash
    integrity_path = output_dir / "integrity.jsonl"
    _write_jsonl(integrity_path, integrity_rows)

    missing_evidence = ["signature_manifest", "worm_archive_attestation"]
    if not host_context_ref:
        missing_evidence.append("model_prompt_and_context")
    if not actor_authorization_ref:
        missing_evidence.append("actor_authorization_snapshot")
    if not retention_policy_id or retention_days is None:
        missing_evidence.append("complete_retention_policy")
    source_files = _source_hashes(Path(storage_dir))
    bundle_material = {
        "chain_root_sha256": previous,
        "source_files": source_files,
        "events_sha256": _file_hash(event_path),
    }
    bundle_id = f"albundle_{hashlib.sha256(_canonical_json(bundle_material)).hexdigest()[:24]}"
    manifest = {
        "schema_version": "actionlens.evidence-bundle-manifest.v1",
        "bundle_id": bundle_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "scope": "ActionLens tool-boundary evidence; not a complete compliance attestation",
        "source_files": source_files,
        "events": {
            "path": event_path.name,
            "count": len(events),
            "sha256": _file_hash(event_path),
            "skipped_source_lines": stats.skipped,
        },
        "integrity": {
            "path": integrity_path.name,
            "algorithm": "sha256",
            "construction": "chain_n = sha256(chain_(n-1) || sha256(canonical_event_json))",
            "chain_root_sha256": previous,
            "sha256": _file_hash(integrity_path),
            "signed": False,
        },
        "redaction": {
            "policy_id": "actionlens.export.default.v1",
            "sensitive_key_and_token_patterns_applied": True,
            "artifact_bodies_read": False,
            "artifact_references_preserved": True,
        },
        "retention": {
            "policy_id": retention_policy_id,
            "minimum_days": retention_days,
            "enforcement": "host-owned",
        },
        "host_evidence_refs": {
            "model_prompt_and_context": _safe_reference(host_context_ref),
            "actor_authorization_snapshot": _safe_reference(actor_authorization_ref),
        },
        "missing_evidence": missing_evidence,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    return {"exported": len(events), "skipped": stats.skipped, "missing": len(missing_evidence)}


def export_sft(
    storage_dir: str | Path, output: str | Path, **filters: str | None
) -> dict[str, int]:
    events, stats = load_events(storage_dir, **filters)
    started = {
        event.get("call_id"): event
        for event in events
        if event.get("event_type") == "tool_call.started"
    }
    samples: list[dict[str, Any]] = []
    filtered = 0
    for event in events:
        if event.get("event_type") != "tool_call.completed":
            continue
        call_id = event.get("call_id")
        start = started.get(call_id, {})
        args = _sanitize((start.get("metadata") or {}).get("args", {}))
        observed_output = _sanitize((event.get("metadata") or {}).get("output"))
        if observed_output is None and isinstance(event.get("output_ref"), dict):
            observed_output = {"artifact_ref": _safe_artifact(event["output_ref"])}
        if observed_output is None:
            filtered += 1
            continue
        tool_name = str(event.get("tool_name") or start.get("tool_name") or "unknown")
        samples.append(
            {
                "messages": [
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": tool_name,
                                    "arguments": json.dumps(args, ensure_ascii=False, default=str),
                                },
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": json.dumps(observed_output, ensure_ascii=False, default=str),
                    },
                ],
                "metadata": {
                    "schema": "actionlens.sft.v1",
                    "session_id": event.get("session_id"),
                    "run_id": event.get("run_id"),
                    "tool_name": tool_name,
                    "source_event_ids": [
                        value for value in [start.get("event_id"), event.get("event_id")] if value
                    ],
                },
            }
        )
    _write_jsonl(Path(output), samples)
    manifest = {
        "schema": "actionlens.dataset-manifest.v1",
        "actionlens_version": "1.1.0",
        "format": "actionlens.sft.v1",
        "selection_policy": "successful tool_call.completed events with bounded trajectory output",
        "filters": {key: value for key, value in filters.items() if value is not None},
        "redaction_policy_id": "actionlens.export.default.v1",
        "source_files": _source_hashes(Path(storage_dir)),
        "output_sha256": _file_hash(Path(output)),
        "exported": len(samples),
        "filtered": filtered,
        "skipped": stats.skipped,
        "rejection_reasons": {"missing_safe_observation": filtered},
    }
    manifest_path = Path(str(output) + ".manifest.json")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {"exported": len(samples), "filtered": filtered, "skipped": stats.skipped}


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _candidate_event(event: dict[str, Any]) -> dict[str, Any]:
    item = {
        "timestamp": event.get("timestamp"),
        "event_type": event.get("event_type"),
        "phase": event.get("phase"),
        "event_id": event.get("event_id"),
        "call_id": event.get("call_id"),
        "tool_name": event.get("tool_name"),
        "decision": event.get("decision"),
        "error": event.get("error"),
        "metrics": event.get("metrics", {}),
        "metadata": event.get("metadata", {}),
    }
    if isinstance(event.get("input_ref"), dict):
        item["input_ref"] = _safe_artifact(event["input_ref"])
    if isinstance(event.get("output_ref"), dict):
        item["output_ref"] = _safe_artifact(event["output_ref"])
    return _sanitize({key: value for key, value in item.items() if value not in (None, {}, [])})


def _evidence_event(event: dict[str, Any]) -> dict[str, Any]:
    sanitized = _sanitize(event)
    if isinstance(event.get("input_ref"), dict):
        sanitized["input_ref"] = _safe_artifact(event["input_ref"])
    if isinstance(event.get("output_ref"), dict):
        sanitized["output_ref"] = _safe_artifact(event["output_ref"])
    return sanitized


def _event_status(event: dict[str, Any]) -> str | None:
    output = (event.get("metadata") or {}).get("output")
    if isinstance(output, dict) and output.get("status"):
        return str(output["status"])
    mapping = {
        "tool_call.completed": "SUCCESS",
        "tool_call.failed": "FAILED",
        "approval.pending": "PENDING_APPROVAL",
        "approval.denied": "DENIED",
        "approval.expired": "DENIED",
    }
    return mapping.get(str(event.get("event_type")))


def _safe_artifact(ref: dict[str, Any]) -> dict[str, Any]:
    safe = {
        key: value
        for key, value in ref.items()
        if key in {"uri", "media_type", "size_bytes", "sha256", "preview", "redacted", "expires_at"}
    }
    if safe.get("uri"):
        safe["uri"] = _safe_reference(str(safe["uri"]))
    return safe | {"dangling": _is_dangling(ref)}


def _is_dangling(ref: dict[str, Any]) -> bool:
    uri = ref.get("uri")
    if not uri:
        return False
    value = str(uri)
    if len(value) >= 3 and value[0].isalpha() and value[1] == ":" and value[2] in {"/", "\\"}:
        return not Path(value).exists()
    parts = urlsplit(value)
    if parts.scheme not in {"", "file"}:
        return False
    return not Path(parts.path if parts.scheme else value).exists()


def _safe_reference(value: str | None) -> str | None:
    if value is None:
        return None
    parts = urlsplit(value)
    if not parts.scheme:
        return value
    hostname = parts.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    netloc = hostname
    if parts.port is not None:
        netloc += f":{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _sanitize(value: Any) -> Any:
    return redact_value(
        value,
        keys=["password", "token", "secret", "authorization", "api_key"],
        patterns=[r"sk-[A-Za-z0-9_-]+", r"(?i)bearer\s+\S+"],
    )


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str
    ).encode("utf-8")


def _write_export_manifest(
    output: Path,
    *,
    storage_dir: Path,
    format_name: str,
    mapper_version: str,
    exported: int,
    skipped: int,
    extra: dict[str, Any],
) -> None:
    manifest = {
        "schema_version": "actionlens.export-manifest.v1",
        "format": format_name,
        "mapper_version": mapper_version,
        "source_files": _source_hashes(storage_dir),
        "output_sha256": _file_hash(output),
        "redaction_policy_id": "actionlens.export.default.v1",
        "artifact_bodies_read": False,
        "exported": exported,
        "skipped": skipped,
        **extra,
    }
    Path(str(output) + ".manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )


def _source_hashes(storage_dir: Path) -> list[dict[str, str]]:
    trajectory_dir = storage_dir / "trajectories"
    return [
        {"path": path.name, "sha256": _file_hash(path)}
        for path in sorted(trajectory_dir.glob("*.jsonl"))
    ] if trajectory_dir.exists() else []
