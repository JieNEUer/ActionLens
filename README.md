<div align="center">
<pre style="font-family: 'Courier New', monospace; font-size: 10px; color: #111; margin: 0; padding: 0; line-height: 1.15; display: inline-block; text-align: left;">
 █████╗  ██████╗████████╗██╗ ██████╗ ███╗   ██╗██╗     ███████╗███╗   ██╗███████╗
██╔══██╗██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██║     ██╔════╝████╗  ██║██╔════╝
███████║██║        ██║   ██║██║   ██║██╔██╗ ██║██║     █████╗  ██╔██╗ ██║███████╗
██╔══██║██║        ██║   ██║██║   ██║██║╚██╗██║██║     ██╔══╝  ██║╚██╗██║╚════██║
██║  ██║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║███████╗███████╗██║ ╚████║███████║
╚═╝  ╚═╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝╚══════╝╚═╝  ╚═══╝╚══════╝
</pre>
</div>

# ActionLens

[![PyPI version](https://img.shields.io/pypi/v/actionlens.svg)](https://pypi.org/project/actionlens/)
[![Python versions](https://img.shields.io/pypi/pyversions/actionlens.svg)](https://pypi.org/project/actionlens/)
[![License](https://img.shields.io/github/license/JieNEUer/ActionLens.svg)](https://github.com/JieNEUer/ActionLens)

[English](README.md) | [简体中文](README.zh-CN.md)

```bash
pip install actionlens
```

ActionLens is a low-intrusion Python library for agent tool governance and trajectory capture.

It sits at the tool boundary instead of replacing your agent framework. Wrap an existing Python function, get structured tool outputs, bounded model-visible results, local artifacts for large payloads, idempotency protection, approval handoff, and JSONL trajectories that can later feed eval or monitoring workflows.

## Where ActionLens Fits

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"ui-sans-serif, system-ui, sans-serif","primaryColor":"#F7FBF7","primaryTextColor":"#1F2933","primaryBorderColor":"#3F474A","lineColor":"#4A5559","tertiaryColor":"#FFFFFF"}}}%%
flowchart LR
    host["Agent / host runtime<br/>LangChain, LangGraph, PydanticAI, OpenAI Agents SDK"]:::host
    durable["Optional durable control<br/>Temporal Activities / DBOS Steps"]:::durable
    provider["Business tools / providers<br/>SaaS APIs, databases, local systems"]:::provider
    consumer["Observability / eval / audit<br/>metrics, traces, evidence consumers"]:::consumer

    subgraph actionlens["ActionLens: governance + evidence plane"]
        direction TB
        runtime["Governed tool runtime"]:::core
        policy["Policy + approvals<br/>budgets + redaction"]:::governance
        ledger["Ledger + idempotency<br/>outbox + recovery"]:::evidence
        artifacts["Artifacts + provenance<br/>retention + access checks"]:::evidence
        trajectory["Trajectory + exporters<br/>events + evidence bundles"]:::evidence
        runtime --> policy
        runtime --> ledger
        ledger --> artifacts
        ledger --> trajectory
    end

    host -->|"tools + explicit context"| runtime
    durable -->|"stable workflow / step identity"| runtime
    policy -->|"authorized invocation"| provider
    provider -->|"result + provider evidence"| runtime
    ledger --> consumer
    artifacts --> consumer
    trajectory --> consumer

    classDef host fill:#FFFFFF,stroke:#3F474A,stroke-width:1.25px,color:#1F2933;
    classDef durable fill:#F4F8F4,stroke:#586661,stroke-width:1.25px,color:#1F2933;
    classDef core fill:#EAF6E6,stroke:#6DAE4A,stroke-width:1.6px,color:#1F2933;
    classDef governance fill:#F7FBF7,stroke:#6B7972,stroke-width:1.2px,color:#1F2933;
    classDef evidence fill:#FFFFFF,stroke:#6B7972,stroke-width:1.2px,color:#1F2933;
    classDef provider fill:#FFFFFF,stroke:#3F474A,stroke-width:1.25px,color:#1F2933;
    classDef consumer fill:#F1F8EF,stroke:#6DAE4A,stroke-width:1.25px,color:#1F2933;
    style actionlens fill:#FAFCFA,stroke:#3F474A,stroke-width:1.25px,stroke-dasharray:2 3
```

## Why

Modern agents often fail at the tool boundary:

- A crawler returns 20,000 words and blows up the model context.
- A model retries the same mutation with a slightly different timestamp.
- A Python exception is sent back to the model as raw traceback noise.
- A high-risk operation needs human approval, but the approval state is not replayable.
- Production runs cannot be converted into eval or debugging traces later.

ActionLens turns these into explicit runtime protocols while keeping the host framework in charge.

## Status

This repository contains the v1.5.0 stable protocol focused on multi-instance-safe governance, bounded production data paths, evidence-backed recovery, durable audit delivery, explicit artifact confidentiality, and low-intrusion durable-runtime bridges:

- `@lens.tool(...)` decorator for sync and async functions
- `StructuredToolOutput` for model-visible results
- local artifact storage for large outputs
- UTF-8 byte and top-level-item output budgets, safe trajectory defaults, and explicit summary controls
- JSONL trajectory events
- explicit `ToolCallContext` passing for resume/distributed workers
- child contexts that retain `parent_call_id` and share the parent run budget
- public signature injection for required `idempotency_key`
- interchangeable governance repositories for PostgreSQL, SQLite, and ephemeral tests
- lease ownership, heartbeat, fencing tokens, args/schema conflict detection, and `UNCERTAIN`
- atomic approval ticket + ledger + transactional outbox transitions
- at-least-once outbox dispatch with retry, claim leases, and dead-letter handling
- persistent approval pending / approve / deny / resume flow
- pluggable policy chain and redactors
- artifact metadata plus dry-run / size-aware GC
- generator and async-generator tools with bounded-memory artifact capture and configurable model-visible tails
- optional thread-mode timeout for sync tools; high-risk timeouts are `UNCERTAIN` because Python cannot stop a running thread
- real TTL-scoped `CACHE_READ` reuse for read-only tools
- synchronous approval resolver support alongside durable approval tickets
- governed, authorized artifact paging and literal grep tools for model navigation
- governed MCP `tools/call` proxy with JSON Schema validation, including approval-safe dynamic argument changes
- `RemoteToolAdapter` / `lens.remote_tool(...)` bridge that carries ActionLens request identity into a `RemoteToolRunner`
- ordered `invoke_many()` for concurrency-safe read tools
- `actionlens summary`, `actionlens export`, and `actionlens gc`
- thin PydanticAI, OpenAI Agents SDK, and LangChain/LangGraph adapters
- Inspect-oriented transcript and conservative SFT JSONL exporters
- `EvalCaseCandidate` with stable case IDs, host-supplied task/environment/rubric facts, explicit readiness, and versioned Inspect sample mapping
- evidence bundles with source digests, a per-event SHA-256 chain, redaction/retention metadata, and explicit missing-evidence declarations
- static local HTML trajectory reports
- optional bounded-queue JSONL writing with explicit drop policies
- expiring approval tickets, schema-checked modified arguments, and ticket/ledger inspection
- `ArtifactPolicy` with write-before-redaction guarantees, reference-only/deny modes, quotas, and encryption provider SPI
- optional `MediaMetadata` and compact `ArtifactProvenance` for metadata-first, derived media artifacts
- host-provided `MediaMetadataExtractor` SPI for local plaintext image/audio/video artifacts
- provenance-aware local GC that preserves a source while a retained derivative still references it, with explicit cascade mode
- signed webhook, composite, low-cardinality metrics, and a pinned OpenTelemetry GenAI mapping profile
- versioned schema readers/golden fixtures and reproducible SFT dataset manifests
- framework-neutral `RemoteToolRunner` SPI with a concrete governed adapter
- dependency-free Temporal Activity and DBOS Step context bridges that preserve stable workflow/step identity
- evidence-backed `UNCERTAIN` reconciliation with atomic audit events
- background outbox lifecycle, health state, and controlled dead-letter replay/termination
- authorized artifact read/decrypt/checksum verification and reference URI policy
- webhook key rotation, replay-window verification, event deduplication hook, and SSRF controls
- directly runnable repository and sink contract checks for third-party implementations
- pooled PostgreSQL connections with bounded acquire/statement/lock/transaction timeouts
- explicit advisory-lock migrations and startup schema compatibility checks
- event-loop isolation for synchronous governance I/O around async tools
- phase-aware governance failures before and after external side effects
- outbox backlog/lag health, bounded retention, and single-round-trip PostgreSQL claims
- cross-process artifact read/GC leases plus symlink/reparse-point rejection
- bounded streaming artifact upload, authenticated decryption, and atomic destination promotion
- reproducible benchmark and soak probes with percentile and memory evidence

PostgreSQL is the preferred multi-instance backend because ledger, approval, and outbox facts share one transaction. Redis is intentionally not implemented in v1.5; the repository protocol permits a future backend without changing `ToolRuntime`.

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

Install MCP JSON Schema validation when proxying MCP tools:

```bash
python -m pip install -e ".[mcp]"
```

## PostgreSQL Repository

Production startup does not run DDL. Apply migrations as a deployment step, preferably with the DSN in the environment rather than the process command line:

```bash
ACTIONLENS_POSTGRES_DSN=postgresql://actionlens:secret@db.internal/actionlens actionlens migrate
ACTIONLENS_POSTGRES_DSN=postgresql://actionlens:secret@db.internal/actionlens actionlens schema-status
```

```python
import actionlens as al

repository = al.PostgresGovernanceRepository(
    "postgresql://actionlens:secret@db.internal/actionlens",
    min_pool_size=2,
    max_pool_size=20,
    pool_timeout=5,
    statement_timeout_ms=30_000,
    lock_timeout_ms=5_000,
    transaction_timeout_ms=60_000,
)
lens = al.ActionLens(project="demo-agent", repository=repository)

# At the owning application lifecycle boundary:
lens.close()
repository.close()
```

Migrations are idempotent, serialized by a PostgreSQL advisory transaction lock, and recorded in `actionlens_schema_migrations`. `auto_migrate=True` remains available for isolated development only. PostgreSQL uses row locks and `SKIP LOCKED` outbox claims. External side effects are not advertised as exactly-once: an expired non-fenceable execution becomes `UNCERTAIN` and must be reconciled explicitly.

SQLite keeps `synchronous="FULL"` as the durability default. Latency-sensitive local deployments that accept SQLite WAL's `NORMAL` power-loss tradeoff may opt in explicitly:

```python
repository = al.SQLiteGovernanceRepository(".actionlens/ledger.sqlite3", synchronous="NORMAL")
```

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

`OutputPolicy(include_raw_in_trajectory=False)` is the default, so the full
`StructuredToolOutput.result` is not copied into trajectory events. Set it to
`True` only for an explicitly approved training or diagnostic sink. When an
encryption provider is configured, artifact previews are withheld from the
unencrypted sidecar; the current caller can still receive its independently
bounded, redacted inline preview.

## Media Metadata And Provenance

Media support is metadata-first and dependency-free. `ArtifactRef.media_metadata` and `ArtifactRef.provenance` are optional additive fields; ActionLens does not import FFmpeg, OCR, ASR, or vision-model SDKs.

```python
source = lens.artifact_store.put(video_bytes, media_type="video/mp4")
thumbnail = lens.artifact_store.put(
    thumbnail_bytes,
    media_type="image/jpeg",
    media_metadata=al.MediaMetadata(width=320, height=180, codec="jpeg"),
    provenance=al.ArtifactProvenance.from_source(
        source,
        operation="thumbnail",
        operation_version="ffmpeg-7.0",
        parameters={"time_sec": 12.5, "max_width": 320},
        created_by_tool="extract_thumbnail",
    ),
)
```

To populate metadata automatically for local plaintext image/audio/video writes, inject a small host adapter. The extractor runs only after ActionLens has atomically persisted the local plaintext file. It is best-effort so decoder failure cannot turn a completed artifact write into an orphan; failures are emitted through the `actionlens.artifacts.fs` logger so operators can detect a broken extractor. Encrypted artifacts are never handed to the extractor; provide trusted `media_metadata=` at write time when extraction happens before encryption.

When content-addressed local storage deduplicates identical derived bytes from multiple sources, its sidecar retains every observed local provenance record. Local GC treats all of those sources as parents, so deleting one source cannot leave a retained derivative without its evidence chain. The local sidecar is capped at 64 distinct source/transformation identities per content-addressed file and fails closed rather than silently dropping lineage; hosts needing a larger many-to-one index should provide their own artifact store.

```python
class MyMediaExtractor:
    def extract(self, path, media_type):
        return al.MediaMetadata(width=1920, height=1080, codec="h264")

lens = al.ActionLens(media_metadata_extractor=MyMediaExtractor())
```

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

## v1.5 Execution Semantics

`max_bytes` is a UTF-8 byte budget for the model-visible result. Collections
are also limited by `OutputPolicy.max_inline_items`; an oversized value is
stored as an artifact and the caller receives a preview bounded by the same
byte budget. `summary_fields` can select safe dictionary fields for an inline
summary, while `summary_includes_content=False` disables content snippets.

For a synchronous function, `run_sync_in_thread=True` only releases the caller
after a timeout. Python cannot force-stop that business thread. A timeout for a
`MUTATION` or `DESTRUCTIVE` tool is therefore returned and persisted as
`UNCERTAIN`, blocks automatic retry, and must be reconciled against the business
system. Read-only tool timeouts remain retryable `TIMEOUT` results.

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

Read-only tools can opt into a real shared TTL cache. Its identity includes
project, environment, tenant, tool name, and arguments, but intentionally not
session or run identity:

```python
@lens.tool(idempotency=al.IdempotencyPolicy.CACHE_READ, cache_ttl_sec=60)
def lookup_customer(customer_id: str) -> dict:
    return provider.lookup(customer_id)
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

Use `child_context()` for a sub-agent or delegated call that must retain a
durable parent link and consume the same run budget:

```python
with lens.session(session_id="chat-001", run_id="run-001") as parent:
    child = lens.child_context(parent=parent, tool_name="lookup_customer")
    output = lookup_customer("cust-7", __al_ctx=child)
```

## MCP, Remote Tools, And Artifact Navigation

`MCPGovernanceProxy` turns registered MCP `tools/call` methods into normal
governed ActionLens tools. The proxy validates the MCP input schema before the
transport call; approvals, leases, output shaping, idempotency, and trajectories
are applied locally.

```python
proxy = al.MCPGovernanceProxy(lens, mcp_transport)
proxy.register_tool(
    "search_docs",
    input_schema={"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
)
result = proxy.handle_request(request)
```

The proxy is dual-era: legacy requests can use `initialize` and classic MCP
results, while 2026-07-28 requests carry protocol version and client
capabilities in `params._meta`. It implements `server/discover`, emits
`resultType`, cache hints, `title`/`icons`/`outputSchema`, and validates
`structuredContent`. For approval round trips, configure
`input_required_factory` and `input_response_handler`; the host must integrity
protect and verify opaque `requestState` before persisting an approval.

For an existing asynchronous job provider, `lens.remote_tool(runner, name="...")`
builds a governed `RemoteToolRunner` bridge. The request carries the ActionLens
idempotency key, arguments hash, schema hash, context, and deadline. A timeout
attempts cancellation; high-risk outcomes that cannot be confirmed become
`UNCERTAIN`.

Large artifacts can be navigated through governed tools rather than copied back
into context:

```python
tools = lens.artifact_navigation_tools()
page = tools["artifact_read"](artifact_ref, offset=0, limit=4096)
matches = tools["artifact_grep"](artifact_ref, needle="invoice")
```

Both operations require the configured artifact authorizer, verify checksums,
emit access events, and cap page/search output. Sync and async generators are
persisted as artifacts automatically; `OutputPolicy.streaming_tail_lines`
controls the tail returned to the model.

## Durable Workflow Bridges

The `temporal` and `dbos` integration modules are dependency-free mapping layers: the durable runtime controls replay, scheduling, signals, and durable waits; ActionLens controls the governed tool call inside an Activity or Step. They do not manage workflow state or introduce a core Temporal/DBOS dependency.

```python
from actionlens.integrations.temporal import (
    TemporalActivityRunner,
    context_from_temporal_workflow,
)

# Inside a Temporal Activity, pass values from activity.info().
ctx = context_from_temporal_workflow(
    workflow_id,
    run_id,
    tool_name="send_message",
    activity_id=activity_id,  # preferred for call-level correlation
    attempt=attempt,
    project="messaging",
)
output = await TemporalActivityRunner(send_message).arun(
    ctx, "hello", idempotency_key="message-123"
)
```

`workflow_id` / DBOS `workflow_id` becomes the stable ActionLens `session_id`. Retry `attempt` is observability metadata only and is never part of the auto-hash idempotency identity. Use a stable business idempotency key for mutations. Approval signals/messages are wake-ups only: commit `lens.approve(...)` first, then let the resumed Activity/Step re-read the ActionLens ticket and ledger. See [Temporal](examples/temporal_integration_example.py) and [DBOS](examples/dbos_integration_example.py) examples.

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

Interactive hosts can resolve the same ticket synchronously without losing the
durable audit trail. The resolver returns `APPROVE`, `DENY`, or `PENDING`; an
approved decision is persisted before the business function runs:

```python
def prompt_operator(ticket: al.ApprovalTicket, context: al.ToolCallContext):
    return {"action": "APPROVE", "approved_by": "on-call"}

lens = al.ActionLens(approval_resolver=prompt_operator)
```

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

For provider adapters, `ProviderStatusReconciler` maps a read-only `APPLIED`, `NOT_APPLIED`, `PENDING`, or `UNKNOWN` observation onto these outcomes and requires evidence for terminal conclusions. The [provider reconciliation cookbook](examples/provider_reconciliation_cookbook.md) covers payment, email, GitHub PR, and object-storage identity, query, consistency-window, and evidence rules.

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
actionlens export --storage-dir .actionlens --format eval-candidates --output eval-candidates.jsonl
actionlens export --storage-dir .actionlens --format sft-jsonl --output sft.jsonl
```

Exporters skip corrupt or partial JSONL lines and report the skipped count. SFT export only includes completed successful calls. It uses redacted, bounded trajectory output and never reads raw artifact bodies.

## Eval Case Bridge

Tool-boundary events alone do not contain the original user task, a reproducible environment, a grading target, or proof of the business outcome. ActionLens therefore exports an explicitly incomplete `EvalCaseCandidate` by default instead of pretending a trajectory is an Inspect `EvalLog`.

Supply facts owned by the host in a versioned JSON file keyed by `project/session/run` (the shorter `session/run` and `run` keys are also accepted):

```json
{
  "schema_version": "actionlens.eval-case-contexts.v1",
  "runs": {
    "demo-agent/chat-001/run-001": {
      "task_input": "Send the approved invoice once",
      "environment_spec": {
        "name": "billing-sandbox",
        "version": "2026-07-01",
        "spec_ref": "https://eval.internal/environments/billing-v3"
      },
      "target": {"invoice_status": "sent"},
      "rubric": {"no_duplicate_send": true},
      "outcome_evidence": [
        {
          "kind": "provider_status",
          "summary": "provider accepted exactly one message",
          "ref": "https://audit.internal/messages/msg-123"
        }
      ],
      "scorer_version": "billing-state.v2"
    }
  }
}
```

```bash
actionlens export --storage-dir .actionlens --format eval-candidates \
  --case-contexts eval-contexts.json --output eval-candidates.jsonl

actionlens export --storage-dir .actionlens --format inspect-samples \
  --case-contexts eval-contexts.json --require-ready --output inspect-samples.jsonl
```

Each export writes a sidecar manifest containing source hashes, output hash, mapper version, redaction policy, and readiness/filter counts. `inspect-samples` is a dataset mapper only; the host still owns the Inspect task, sandbox, scorer, and replay lifecycle.

## Audit Evidence Bundle

Generate a bounded tool-boundary evidence package without reading artifact bodies:

```bash
actionlens export --storage-dir .actionlens --format evidence-bundle \
  --output evidence/run-001 \
  --retention-policy-id regulated-six-months.v1 --retention-days 180 \
  --host-context-ref https://audit.internal/context/run-001 \
  --actor-authorization-ref https://audit.internal/authz/run-001 \
  --signature-manifest-ref https://audit.internal/signatures/run-001 \
  --worm-archive-ref s3://audit-archive/run-001
```

The directory contains `events.jsonl`, `integrity.jsonl`, and `manifest.json`. The manifest records source and output hashes, the event-chain root, retention metadata, redaction behavior, host evidence references, and missing evidence. Signature manifests and WORM attestations remain host-owned: the optional reference flags record sanitized references and clear the corresponding gaps, but ActionLens does not sign data, manage keys, or claim that it controls the archive.

## OpenTelemetry GenAI Mapping

`OpenTelemetrySink` uses the pinned `actionlens.otel-genai.v1` profile. It maps `gen_ai.operation.name`, `gen_ai.tool.name`, and `gen_ai.tool_call.id`, while approval, run, and evidence facts remain in the `actionlens.*` namespace. The mapping contract records its upstream development snapshot because the standalone GenAI semantic-conventions repository is still evolving.

Prompt/context, tool arguments, model output, artifact URI, and raw evidence are never copied into span attributes. OpenTelemetry remains a sampled observability output; trajectory storage remains the evidence source of truth.

Create a static report without a server or frontend build chain:

```bash
actionlens report --storage-dir .actionlens --html --output report.html
```

Inspect governance state:

```bash
actionlens tickets --storage-dir .actionlens --status PENDING
actionlens inspect-ledger --storage-dir .actionlens
actionlens outbox --storage-dir .actionlens list
actionlens outbox --storage-dir .actionlens status
actionlens outbox --storage-dir .actionlens cleanup --retention 30d --limit 1000
actionlens outbox --storage-dir .actionlens replay --delivery-id delivery-123
actionlens outbox --storage-dir .actionlens terminate --delivery-id delivery-123 --reason "invalid endpoint"
```

Remove old local artifacts:

```bash
actionlens gc --storage-dir .actionlens --older-than 7d
actionlens gc --storage-dir .actionlens --older-than 30d --cascade-derived
```

Default GC protects a source artifact while any retained local derivative references it. `--cascade-derived` is an explicit operator choice to remove eligible derivatives with an eligible source; active read leases still win. A GC run with no deletion candidates skips provenance sidecar reads. When candidates exist, the local store must inspect lineage evidence to preserve retained ancestors; deployments that need a cross-store or continuously indexed lineage service should provide that at the host storage layer.

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

For native LangGraph `StateGraph` / `ToolNode` execution, install the independent optional extra:

```bash
python -m pip install -e ".[langgraph]"
```

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

Key boundaries:

- ActionLens is not an agent framework.
- It does not own graph control flow, model selection, scorer logic, or environment reset.
- Local JSONL trajectories are the source of truth; OpenTelemetry, Prometheus, dashboards, and eval adapters are optional outputs.
- Naive key redaction is only an MVP fallback. Production users should plug in a stronger redaction/DLP engine.

## Development

```bash
python -m pytest -q
python -m compileall -q src tests
python -m ruff check src tests benchmarks
python benchmarks/benchmark.py --iterations 1000 --output benchmark.json
python benchmarks/soak.py --duration 86400 --output soak-24h.json
```

For the PostgreSQL query-plan check, supply only an isolated local test cluster:

```bash
ACTIONLENS_POSTGRES_DSN=postgresql://postgres@127.0.0.1:55432/postgres \
  python benchmarks/postgres.py --output postgres-query-plan.json
```
