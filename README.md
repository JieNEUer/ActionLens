# ActionLens

ActionLens is a low-intrusion Python library for agent tool governance and trajectory capture.

It sits at the tool boundary instead of replacing your agent framework. Wrap an existing Python function, get structured tool outputs, bounded model-visible results, local artifacts for large payloads, idempotency protection, approval handoff, and JSONL trajectories that can later feed eval or monitoring workflows.

## Why

Modern agents often fail at the tool boundary:

- A crawler returns 20,000 words and blows up the model context.
- A model retries the same mutation with a slightly different timestamp.
- A Python exception is sent back to the model as raw traceback noise.
- A high-risk operation needs human approval, but the approval state is not replayable.
- Production runs cannot be converted into eval or debugging traces later.

ActionLens turns these into explicit runtime protocols while keeping the host framework in charge.

## Status

This repository currently contains a v0.2 implementation focused on tool-boundary governance:

- `@lens.tool(...)` decorator for sync and async functions
- `StructuredToolOutput` for model-visible results
- local artifact storage for large outputs
- JSONL trajectory events
- explicit `ToolCallContext` passing for resume/distributed workers
- public signature injection for required `idempotency_key`
- SQLite and in-memory idempotency ledgers
- persistent approval pending / approve / deny / resume flow
- pluggable policy chain and redactors
- artifact metadata plus dry-run / size-aware GC
- optional thread-mode timeout for sync tools
- ordered `invoke_many()` for concurrency-safe read tools
- `actionlens summary`, `actionlens export`, and `actionlens gc`

Planned next steps include framework adapters, streaming tool chunks, richer local reports, Inspect/SFT exporters, and Redis-backed multi-instance ledgers.

## Install For Local Development

```bash
python -m pip install -e .
python -m pytest -q
```

The runtime dependency is intentionally light:

- Python 3.10+
- Pydantic v2

## Quick Start

```python
import actionlens as al

lens = al.ActionLens(project="demo-agent", storage_dir=".actionlens")

@lens.tool(max_bytes=1000)
def fetch_page() -> str:
    return "very long page..." * 1000

with lens.session(session_id="chat-001"):
    output = fetch_page()

print(output.status)
print(output.result_summary)
print(output.artifact_refs)
```

If the result exceeds `max_bytes`, ActionLens stores the raw result under `.actionlens/artifacts/` and returns a bounded preview plus an `ArtifactRef`.

## Idempotency

For mutation tools, require an explicit idempotency key:

```python
@lens.tool(
    risk=al.RiskLevel.MUTATION,
    idempotency=al.IdempotencyPolicy.REQUIRED,
)
def send_message(user_id: str, text: str) -> dict:
    return {"sent": True, "user_id": user_id}

with lens.session(session_id="chat-001"):
    first = send_message("u1", "hello", idempotency_key="msg-u1-001")
    second = send_message("u1", "hello", idempotency_key="msg-u1-001")
```

The wrapper modifies the public function signature so schema extractors can see the required `idempotency_key`:

```python
import inspect
print(inspect.signature(send_message))
# (user_id: str, text: str, *, idempotency_key: str) -> dict
```

For auto-hash mode, ignore unstable fields that models may invent:

```python
@lens.tool(
    risk=al.RiskLevel.MUTATION,
    idempotency=al.IdempotencyPolicy.AUTO_HASH,
    hash_ignore_keys=["timestamp", "nonce", "uuid"],
)
def write_note(message: str, timestamp: int) -> dict:
    return {"ok": True}
```

## Explicit Context For Resume

`ContextVar` works for normal in-process request scopes, but distributed resume systems such as LangGraph checkpointing, Temporal, or background workers need explicit context passing.

```python
ctx = al.ToolCallContext(
    project="demo-agent",
    session_id="serialized-session",
    run_id="resume-run",
    call_id="old-call",
    tool_name="lookup",
)

result = lookup("query", __al_ctx=ctx)
```

`__al_ctx` is consumed by ActionLens and hidden from the public tool signature.

## Human Approval Flow

```python
@lens.tool(
    risk=al.RiskLevel.DESTRUCTIVE,
    idempotency=al.IdempotencyPolicy.REQUIRED,
    approval_required=True,
)
def drop_table(name: str) -> dict:
    return {"dropped": name}

with lens.session(session_id="ops-001"):
    pending = drop_table("users", idempotency_key="drop-users")

ticket_id = pending.result["ticket_id"]
lens.approve(ticket_id=ticket_id)

with lens.session(session_id="ops-001"):
    success = drop_table("users", idempotency_key="drop-users")
```

The first call returns `PENDING_APPROVAL` and does not execute the function. After approval, the same idempotency key is allowed to execute.

## CLI

Summarize local trajectory events:

```bash
actionlens summary --storage-dir .actionlens
```

Export native JSONL or a summary JSON:

```bash
actionlens export --storage-dir .actionlens --format actionlens-jsonl --output trajectories.jsonl
actionlens export --storage-dir .actionlens --format summary-json --output summary.json
```

Remove old local artifacts:

```bash
actionlens gc --storage-dir .actionlens --older-than 7d
```

## Design Notes

The full engineering design is in `docs/actionlens_engineering_design.md`.

Key boundaries:

- ActionLens is not an agent framework.
- It does not own graph control flow, model selection, scorer logic, or environment reset.
- Local JSONL trajectories are the source of truth; OpenTelemetry, Prometheus, dashboards, and eval adapters are optional outputs.
- Naive key redaction is only an MVP fallback. Production users should plug in a stronger redaction/DLP engine.

## Development

```bash
python -m pytest -q
python -m compileall -q actionlens
```
