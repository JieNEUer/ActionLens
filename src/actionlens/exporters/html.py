from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

from .core import _event_status, _is_dangling, load_events, summarize_events


def render_html_report(
    storage_dir: str | Path,
    output: str | Path,
    **filters: str | None,
) -> dict[str, int]:
    events, stats = load_events(storage_dir, **filters)
    summary = summarize_events(storage_dir, **filters)
    rows = "".join(_event_row(event) for event in events)
    summary_json = html.escape(json.dumps(summary, ensure_ascii=False, indent=2))
    document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'">
<title>ActionLens Report</title><style>
body{{font:14px/1.45 system-ui,sans-serif;margin:0;color:#202124;background:#fff}}
header{{padding:20px 28px;background:#1f2933;color:#fff}}main{{padding:20px 28px;overflow:auto}}
h1{{font-size:22px;margin:0}}h2{{font-size:16px;margin:24px 0 10px}}pre{{background:#f4f6f8;padding:12px;overflow:auto}}
table{{border-collapse:collapse;width:100%;min-width:900px}}th,td{{border-bottom:1px solid #d8dde3;padding:8px;text-align:left;vertical-align:top}}
th{{background:#f4f6f8;position:sticky;top:0}}.bad{{color:#a61b1b;font-weight:600}}.ok{{color:#166534;font-weight:600}}
</style></head><body><header><h1>ActionLens trajectory report</h1></header><main>
<h2>Summary</h2><pre>{summary_json}</pre><h2>Timeline</h2>
<table><thead><tr><th>Time</th><th>Session / run</th><th>Tool</th><th>Event</th><th>Status</th><th>Latency</th><th>Details</th></tr></thead>
<tbody>{rows}</tbody></table></main></body></html>"""
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(document, encoding="utf-8")
    return {"events": len(events), "skipped": stats.skipped}


def _event_row(event: dict[str, Any]) -> str:
    status = _event_status(event) or ""
    css = "ok" if status == "SUCCESS" else "bad" if status in {"FAILED", "DENIED", "TIMEOUT"} else ""
    details: list[str] = []
    error = event.get("error")
    if isinstance(error, dict) and error.get("taxonomy"):
        details.append(f"error={error['taxonomy']}")
    decision = event.get("decision")
    if isinstance(decision, dict) and decision.get("reason"):
        details.append(f"decision={decision['reason']}")
    ref = event.get("output_ref")
    if isinstance(ref, dict):
        details.append(f"artifact={ref.get('uri', '')}")
        if _is_dangling(ref):
            details.append("dangling=true")
    latency = (event.get("metrics") or {}).get("latency_ms", "")
    values = [
        event.get("timestamp", ""),
        f"{event.get('session_id', '')} / {event.get('run_id', '')}",
        event.get("tool_name", ""),
        event.get("event_type", ""),
        status,
        f"{latency:.2f} ms" if isinstance(latency, (int, float)) else latency,
        "; ".join(details),
    ]
    cells = "".join(f'<td class="{css if index == 4 else ""}">{html.escape(str(value))}</td>' for index, value in enumerate(values))
    return f"<tr>{cells}</tr>"
