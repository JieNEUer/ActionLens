# Governance contracts and migration notes

ActionLens governs one tool invocation. The host continues to own orchestration,
user identity, business outcome verification, global quota storage and workflow
resumption. The following contracts close the 1.5.3 review findings without
introducing an agent loop or a workflow scheduler.

## Arguments, policies and approval

Native arguments use cached strict Pydantic validation before dispatch. JSON
containers retain JSON date/UUID representations while integers and booleans
remain strict. Use `validation_mode="coerce"` for explicit conversion or
`validation_mode="passthrough"` for legacy Python behavior. Unresolvable argument
annotations fail registration in validated modes. Policy and approval patches
use the same validation boundary; each later policy sees the preceding patch's
effective values. Policies are trusted host code and receive execution
arguments. Redacted audit copies never replace business credentials.

Both static approval and a dynamic `PENDING_APPROVAL` decision create a durable
pause. Approved arguments must satisfy current policy; human approval does not
override a denial. `expires_at` also bounds initial dispatch. Expiry checks and
execution acquisition share the repository transaction, and an expiry transition
creates exactly one outbox event. Expiry after an already completed operation
does not invalidate its confirmed result. Re-evaluating approved parameters does
not charge the built-in budget a second time.

Use the same stable `name=` on every worker. Explicit keys bind tool,
project/environment/tenant, arguments and schema. Default validation/output
options retain existing schema fingerprints; tool identity is checked separately
so existing records continue to block duplicate effects. Invocation key, owner,
fence and argument hashes are isolated from the host's context metadata.

## Execution and recovery

Only a `created` repository result grants execution. Hits, unknown states,
terminal failures, denials and expired tickets cannot fall through to the handler.
`FAILED_RETRYABLE` requires an atomic reacquisition with a new fence.

After a mutation/destructive handler starts, an exception or cancellation leaves
the effect `UNCERTAIN`. Parsing failures and permission errors do not prove that
the provider applied nothing. A trusted handler may raise `NoSideEffectError`
only after confirming no effect occurred; that attempt can be retried with the
same operation key. Read failures retain their normal retry classification.
Cancellation tracks synchronous governance I/O to completion, records the
resulting fact, then propagates `CancelledError`. Backends and custom synchronous
providers must supply bounded I/O completion times; arbitrary Python threads
cannot be forcibly stopped safely.

Successful reconciliation requires the matching registered tool/schema and
passes its output through the local output/redaction/artifact policy before
atomic persistence. Reused confirmed results are also redacted and bounded.
Default error evidence retains classification and type only. Opt into bounded
diagnostic text with
`OutputPolicy(error_message_mode="redacted", redact_patterns=[...])` and a
production redactor. Failed redaction withholds diagnostic evidence.

`invoke_many()` executes ordered segments. Independent concurrency-safe reads
may run together within a segment; writes execute in order. Pending approval or
an uncertain effect stops later segments and returns `SKIPPED` placeholders in
their original positions. Already started independent reads finish their segment.
The host persists and resumes the batch manifest.

For independent durable steps with identical arguments, opt into
`actionlens.integrations.durable_step_idempotency_key` as `idempotency_key_fn`.
Its versioned identity includes workflow run, stable activity/step ID, tool and
scope, excluding retry attempt. Changed arguments within the same step conflict.
Default `AUTO_HASH` still deduplicates within a session. Critical business
operations can instead use host-owned `REQUIRED` keys. Workflow reuse and
continue-as-new identity remain explicit host decisions.

## Budgets and storage capabilities

Built-in call and artifact budgets protect one local instance. They are not
durable quotas shared across workers or process restarts. Identity includes
project/environment/tenant/run. Active call budgets are never silently evicted:
full accounting capacity denies a new run until the host calls `reset_run()`.
Use `lens.reset_run(run_id, context=ctx)` to release only one tenant/environment
scope. The legacy call without a context releases all matching scopes, so call
it only after all of those runs have finished.
Artifact quota counts cumulative plaintext payload writes, including repeated
writes of deduplicated content; it is not a physical disk-usage meter.

Global quotas require atomic reservation in a host-owned trusted policy or
artifact store. `lens.capabilities()` exposes local budget scope and distinguishes
the atomic governance repository from legacy ledger/ticket integration. Legacy
stores lack distributed fencing and atomic ticket/ledger/outbox commits; stale
legacy SQLite executions now remain `UNCERTAIN` rather than being reacquired.

`GovernanceRepository` implementations need only its public methods. ActionLens
constructs compatibility ledger/ticket views itself. The regression suite runs
the approval and repository contract workflow against a method-only implementation.

## Artifacts

Local reads reject parent segments, outside paths, symlinks/reparse points and
nonregular files. Directory-anchored POSIX opens and Windows final-handle checks
protect the target at open time. Correct checksums do not bypass path checks.
Built-in navigation tools hide the internal context parameter from signatures
and schemas and use the trusted host identity for authorization.

Storage descriptors are authoritative for expiry, ownership and crypto metadata.
New references have their own `descriptor_id` even when content is deduplicated;
callers must preserve it. Existing references use their original sidecar. If a
write supplies project/environment/tenant metadata, the reader must provide that
same scope as well as pass the authorizer. Mutating portable reference fields
cannot broaden stored permissions. Descriptor sidecars are removed with their
content during GC.

The local store serializes reads, writes and GC through a process-shared OS lock
with a five-second acquire timeout. Process death releases the lock. A long read
cannot be deleted merely because a time-based lease expired. This favors
correctness; a host store may implement a more concurrent equivalent protocol.
Access expiry does not constitute an independently enforced minimum audit
retention promise: hosts choose GC/retention policy.

Generator staging checks quota/deadline from the first chunk and closes the
generator on failure. `OutputPolicy.max_stream_bytes` defaults to 64 MiB; configure
a larger finite limit for larger streams. `max_stream_chunks` defaults to
100,000 and also bounds empty chunks from an otherwise infinite generator.
Model-visible tails obey both line
and UTF-8 byte limits, and async serialization/disk staging runs outside the event
loop. Third-party media processing stays outside the core package.

## Delivery and evidence consumption

Reliable JSONL outbox delivery synchronously writes, flushes and fsyncs before
ACK, even when observation events use a lossy queue. Composite bindings are
required delivery destinations by default; use
`SinkBinding(..., required=False)` for optional monitoring. Third-party queued
sinks must implement synchronous `deliver(event)` confirmation. Webhook
400/401/403/413 and other non-retryable rejections become dead letters, preserving
repair/replay facts. 429 respects Retry-After; 5xx uses bounded backoff.
Delivery remains at least once, so receivers must deduplicate event IDs.

Canonical readers validate supported event schemas, deduplicate identical IDs
and quarantine conflicting IDs in their entirety using a temporary disk index.
Invalid UTF-8, invalid events, duplicates, conflicts and unsupported versions have
separate counters. Reader diagnostics retain at most 100 file/line references, and the default
event-line limit is 16 MiB. Original delivery files are retained. Evidence manifests bind
the exact fixed source byte ranges consumed and record filters and validation
counts; they do not claim the original producer's authenticity.

A signature reference means provided/unverified. Programmatic
`export_evidence_bundle(..., signature_verifier=...)` can invoke a trusted host
verifier with the reference, `bundle_id` and `chain_root_sha256`. Its receipt must
return `verified=True`, the matching ID/root, a `key_id` and `algorithm`. Only then
does the manifest declare `signed=true`. The CLI alone records references without
verifying them. Key management, external anchors and WORM retention remain
host-owned.

`MetricsSink.latency_ms` contains online count/sum/min/max and nine fixed
histogram buckets, not an ever-growing sample list. `max_tools` bounds labels;
overflow aggregates under `__other__`. OpenTelemetry span identity includes
project/session/run/call, preventing cross-run collisions.

## Compatibility and validation

The PydanticAI extra installs `pydantic-ai-slim`, which supplies the native Tool
interfaces; install the host's chosen provider extras separately. CI checks
optional imports explicitly so transitive import failures cannot masquerade as
missing optional dependencies. Its PydanticAI 1.0 environment pins OpenTelemetry
1.36 because that framework imports the older events API.

| Review findings | Corrective boundary | Regression coverage |
| --- | --- | --- |
| R01, R02 | Confined file opens and hidden navigation context | `test_review_evidence.py` |
| R03, R08, R09, R19 | Unified validation, policy patches and approval barrier | `test_review_governance.py` |
| R04–R07, R22 | Explicit execution permission, effect certainty and isolated identity | `test_review_governance.py` |
| R10, R13 | Classified/redacted errors and governed recovery outputs | `test_review_governance.py` |
| R11, R12 | Synchronous receipt and explicit permanent rejection | `test_review_evidence.py` |
| R14, R16 | Early bounded staging and declared local quota scope | Both review suites |
| R15 | Audited expiry at dispatch acquisition | `test_review_governance.py` |
| R17, R18 | Per-reference stored identity and OS exclusion with GC | `test_review_evidence.py` |
| R20, R21 | Context precedence, root schema definitions and opt-in step identity | Both review suites |
| R23 | Ordered segments and approval/uncertainty barrier | `test_review_governance.py` |
| R24, R25 | Canonical evidence and verified signature facts | `test_review_evidence.py` |
| R26, R27 | Bounded aggregation and public method-only repository integration | Both review suites |

The two review suites run alongside existing core, native framework, MCP,
artifact/media, observability and PostgreSQL tests. CI also compiles sources,
runs Ruff, builds distributions, checks a wheel outside the source tree and
executes benchmark/soak/query-plan smoke probes.
