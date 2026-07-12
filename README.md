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

This repository contains the v1.0 stable protocol focused on multi-instance-safe governance, evidence-backed recovery, durable audit delivery, and explicit artifact confidentiality:

- `@lens.tool(...)` decorator for sync and async functions
- `StructuredToolOutput` for model-visible results
- local artifact storage for large outputs
- JSONL trajectory events
- explicit `ToolCallContext` passing for resume/distributed workers
- public signature injection for required `idempotency_key`
- interchangeable governance repositories for PostgreSQL, SQLite, and ephemeral tests
- lease ownership, heartbeat, fencing tokens, args/schema conflict detection, and `UNCERTAIN`
- atomic approval ticket + ledger + transactional outbox transitions
- at-least-once outbox dispatch with retry, claim leases, and dead-letter handling
- persistent approval pending / approve / deny / resume flow
- pluggable policy chain and redactors
- artifact metadata plus dry-run / size-aware GC
- optional thread-mode timeout for sync tools
- ordered `invoke_many()` for concurrency-safe read tools
- `actionlens summary`, `actionlens export`, and `actionlens gc`
- thin PydanticAI, OpenAI Agents SDK, and LangChain/LangGraph adapters
- Inspect-oriented transcript and conservative SFT JSONL exporters
- static local HTML trajectory reports
- optional bounded-queue JSONL writing with explicit drop policies
- expiring approval tickets, schema-checked modified arguments, and ticket/ledger inspection
- `ArtifactPolicy` with write-before-redaction guarantees, reference-only/deny modes, quotas, and encryption provider SPI
- signed webhook, composite, OpenTelemetry, and low-cardinality metrics sinks
- versioned schema readers/golden fixtures and reproducible SFT dataset manifests
- framework-neutral `RemoteToolRunner` SPI
- evidence-backed `UNCERTAIN` reconciliation with atomic audit events
- background outbox lifecycle, health state, and controlled dead-letter replay/termination
- authorized artifact read/decrypt/checksum verification and reference URI policy
- webhook key rotation, replay-window verification, event deduplication hook, and SSRF controls
- directly runnable repository and sink contract checks for third-party implementations

PostgreSQL is the preferred multi-instance backend because ledger, approval, and outbox facts share one transaction. Redis is intentionally not implemented in v1.0; the repository protocol permits a future backend without changing `ToolRuntime`.

## Install For Local Development

```bash
python -m pip install -e .
python -m pytest -q
```

The runtime dependency is intentionally light:

- Python 3.10+
- Pydantic v2

Install production PostgreSQL support separately:

```bash
python -m pip install -e ".[postgres]"
```

## PostgreSQL Repository

```python
import actionlens as al

repository = al.PostgresGovernanceRepository(
    "postgresql://actionlens:secret@db.internal/actionlens"
)
lens = al.ActionLens(project="demo-agent", repository=repository)
```

Migrations are idempotent and recorded in `actionlens_schema_migrations`. PostgreSQL uses row locks and `SKIP LOCKED` outbox claims. External side effects are not advertised as exactly-once: an expired non-fenceable execution becomes `UNCERTAIN` and must be reconciled explicitly.

## Artifact Confidentiality

```python
policy = al.ArtifactPolicy(
    raw_mode="redact_then_store",  # store, redact_then_store, reference_only, deny
    encryption="provider",
    max_bytes_per_run=10_000_000,
    retention_days=30,
)
lens = al.ActionLens(
    artifact_policy=policy,
    encryption_provider=my_kms_provider,
)
```

An encryption provider supplies `provider_id` and `encrypt(payload, context=...)`. ActionLens never stores a master key. `reference_only` accepts an existing `ArtifactRef`; `deny` prevents artifact writes.

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

## Reconcile Uncertain Side Effects

An expired non-fenceable mutation remains blocked as `UNCERTAIN`. Resolve it only through a business-specific reconciler that returns evidence:

```python
class PaymentReconciler:
    def inspect(self, record):
        return al.ReconciliationResult(
            outcome="CONFIRMED_SUCCEEDED",
            summary="provider transaction exists",
            output={"status": "SUCCESS", "result_summary": "payment confirmed"},
            evidence_ref="https://audit.internal/payments/txn-123",
        )

lens.reconcile_uncertain(idempotency_key, PaymentReconciler())
```

`MANUAL_OVERRIDE` additionally requires `actor_id`, `reason`, `evidence_ref`, and an explicit override target. The ledger transition and reconciliation event are committed atomically.

## CLI

Summarize local trajectory events:

```bash
actionlens summary --storage-dir .actionlens
```

Export native JSONL or a summary JSON:

```bash
actionlens export --storage-dir .actionlens --format actionlens-jsonl --output trajectories.jsonl
actionlens export --storage-dir .actionlens --format summary-json --output summary.json
actionlens export --storage-dir .actionlens --format inspect-ai --output inspect.jsonl
actionlens export --storage-dir .actionlens --format sft-jsonl --output sft.jsonl
```

Exporters skip corrupt or partial JSONL lines and report the skipped count. SFT export only includes completed successful calls. It uses redacted, bounded trajectory output and never reads raw artifact bodies.

Create a static report without a server or frontend build chain:

```bash
actionlens report --storage-dir .actionlens --html --output report.html
```

Inspect governance state:

```bash
actionlens tickets --storage-dir .actionlens --status PENDING
actionlens inspect-ledger --storage-dir .actionlens
actionlens outbox --storage-dir .actionlens list
actionlens outbox --storage-dir .actionlens replay --delivery-id delivery-123
actionlens outbox --storage-dir .actionlens terminate --delivery-id delivery-123 --reason "invalid endpoint"
```

Remove old local artifacts:

```bash
actionlens gc --storage-dir .actionlens --older-than 7d
```

## Framework Adapters

Adapters keep framework dependencies optional and preserve the ActionLens-managed signature:

```python
from actionlens.integrations import (
    wrap_langchain_tool,
    wrap_openai_agent_tool,
    wrap_pydantic_ai_tool,
)

pydantic_tool = wrap_pydantic_ai_tool(send_message)
openai_tool = wrap_openai_agent_tool(send_message)
langchain_tool = wrap_langchain_tool(send_message)

assert "idempotency_key" in openai_tool.parameters_json_schema["required"]
```

Each adapter exposes `invoke()` / `ainvoke()` for explicit framework context mapping. Native framework object factories are lazy imports in the respective integration modules, so importing ActionLens never imports those frameworks.

For LangGraph resume, map serialized state explicitly:

```python
from actionlens.integrations.langchain import context_from_langgraph_state

ctx = context_from_langgraph_state(
    {"config": {"configurable": {"thread_id": "chat-1", "run_id": "run-1"}}},
    tool_name="send_message",
)
result = send_message("hello", idempotency_key="msg-1", __al_ctx=ctx)
```

## Bounded JSONL Queue

Synchronous writing remains the default. Enable a bounded single-writer queue explicitly:

```python
from actionlens.sinks import JsonlSink

sink = JsonlSink(
    ".actionlens",
    queue_maxsize=10_000,
    drop_policy="drop_oldest",  # block, drop_oldest, or drop_newest
    strict=False,
)
lens = al.ActionLens(project="demo", storage_dir=".actionlens", sink=sink)

# At process shutdown or an application lifecycle boundary:
lens.close()
print(sink.stats())
```

With `strict=False`, sink failures are isolated from business tools and counted. With `strict=True`, write failures propagate through `emit()`, `flush()`, or `close()`.

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
python -m compileall -q src tests
```
