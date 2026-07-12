from __future__ import annotations

import json
import hashlib
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

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
    return {
        key: value
        for key, value in ref.items()
        if key in {"uri", "media_type", "size_bytes", "sha256", "preview", "redacted", "expires_at"}
    } | {"dangling": _is_dangling(ref)}


def _is_dangling(ref: dict[str, Any]) -> bool:
    uri = ref.get("uri")
    return bool(uri) and not Path(str(uri)).exists()


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


def _source_hashes(storage_dir: Path) -> list[dict[str, str]]:
    trajectory_dir = storage_dir / "trajectories"
    return [
        {"path": path.name, "sha256": _file_hash(path)}
        for path in sorted(trajectory_dir.glob("*.jsonl"))
    ] if trajectory_dir.exists() else []
