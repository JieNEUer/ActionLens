from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from threading import Event
from uuid import uuid4

import pytest
from pydantic import BaseModel

import actionlens as al
from actionlens.integrations.common import (
    ActionLensToolAdapter,
    durable_step_idempotency_key,
)
from actionlens.integrations.dbos import context_from_dbos_workflow
from actionlens.integrations.temporal import context_from_temporal_workflow
from actionlens.models import PolicyDecision
from actionlens.sinks import MemorySink


class FixedPolicy:
    def __init__(self, action, patch=None):
        self.action, self.patch = action, patch

    def decide(self, **kwargs):
        return PolicyDecision(action=self.action, reason="synthetic-secret", modified_args=self.patch)


@pytest.fixture(params=["sqlite", "postgres"])
def make_lens(request, tmp_path):
    instances = []

    def make(**kwargs):
        storage = tmp_path / str(len(instances))
        if request.param == "postgres":
            dsn = request.getfixturevalue("isolated_postgres_dsn")
            repository = al.PostgresGovernanceRepository(dsn, auto_migrate=True)
        else:
            repository = al.SQLiteGovernanceRepository(storage / "governance.sqlite3")
        lens = al.ActionLens(project=uuid4().hex, storage_dir=storage, repository=repository, sink=MemorySink(), **kwargs)
        instances.append(lens)
        return lens

    yield make
    for lens in instances:
        lens.close()
        lens.repository.close()


@pytest.mark.parametrize("synchronous_resolver", [False, True])
def test_dynamic_policy_approval_is_a_durable_barrier(make_lens, synchronous_resolver):
    lens = make_lens(policies=[FixedPolicy("PENDING_APPROVAL")],
                     approval_resolver=(lambda ticket, context: {"action": "APPROVE"}) if synchronous_resolver else None)
    effects = []

    @lens.tool(risk="MUTATION", idempotency="REQUIRED")
    def mutate(value: int):
        effects.append(value)
        return value

    key = uuid4().hex
    first = mutate(1, idempotency_key=key)
    if synchronous_resolver:
        assert first.status == "SUCCESS" and effects == [1]
    else:
        assert first.status == "PENDING_APPROVAL" and effects == []
        assert lens.approve(ticket_id=first.result["ticket_id"])
        assert mutate(1, idempotency_key=key).status == "SUCCESS"
    assert effects == [1]
    assert mutate(1, idempotency_key=key).status == "SUCCESS"
    assert effects == [1]


@pytest.mark.parametrize("terminal", ["FAILED_TERMINAL", "DENIED", "EXPIRED", "UNCERTAIN", "UNKNOWN"])
def test_ledger_hit_never_grants_execution(make_lens, terminal):
    lens = make_lens()
    effects = []

    @lens.tool(idempotency="REQUIRED")
    def read(value: int):
        effects.append(value)
        return value

    runtime = read.actionlens_runtime
    context, bound, _ = runtime._prepare((1,), {"idempotency_key": uuid4().hex})
    key = bound.arguments["idempotency_key"]
    _, record = lens.repository.begin(key, call_id=context.call_id, context=context, spec=runtime.spec,
                                      args_hash=runtime._args_hash(bound), tool_schema_hash=runtime._tool_schema_hash(),
                                      owner_id="seed", lease_seconds=30)
    connection = getattr(lens.repository, "_connect", None) or lens.repository._connection
    marker = "?" if isinstance(lens.repository, al.SQLiteGovernanceRepository) else "%s"
    with connection() as conn:
        conn.execute(f"UPDATE actionlens_governance_ledger SET status={marker} WHERE key={marker}", (terminal, record.key))
    output = read(1, idempotency_key=key)
    assert output.status in {"DENIED", "UNCERTAIN"}
    assert effects == []


@pytest.mark.parametrize("exception", [ValueError, PermissionError, ConnectionError])
def test_exception_after_mutation_requires_reconciliation(make_lens, exception):
    lens = make_lens()
    effects = []

    @lens.tool(risk="MUTATION", idempotency="REQUIRED")
    def mutate():
        effects.append(1)
        raise exception("provider effect applied, response lost")

    key = uuid4().hex
    assert mutate(idempotency_key=key).status == "UNCERTAIN"
    assert mutate(idempotency_key=key).status == "UNCERTAIN"
    assert effects == [1]
    assert lens.ledger.get(key).status == "UNCERTAIN"


def test_no_effect_declaration_allows_fenced_retry(make_lens):
    lens = make_lens()
    attempts = []

    @lens.tool(risk="MUTATION", idempotency="REQUIRED")
    def mutate():
        attempts.append(1)
        if len(attempts) == 1:
            raise al.NoSideEffectError("provider query confirmed no effect")
        return "ok"

    key = uuid4().hex
    assert mutate(idempotency_key=key).status == "FAILED"
    fence = lens.ledger.get(key).fencing_token
    assert mutate(idempotency_key=key).status == "SUCCESS"
    assert lens.ledger.get(key).fencing_token > fence


def test_invocation_metadata_is_isolated_from_parent_and_parallel_calls(make_lens):
    lens = make_lens()

    @lens.tool(idempotency="REQUIRED")
    def keyed(value: int, __al_ctx=None):
        __al_ctx.metadata["nested"]["changed"] = True
        return value

    @lens.tool
    def unkeyed():
        return "plain"

    keys = [uuid4().hex, uuid4().hex]
    with lens.session(session_id="s", run_id="r", metadata={"nested": {"changed": False}, "actionlens_owner_id": "forged"}) as parent:
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda key: keyed(1, idempotency_key=key, __al_ctx=parent), keys))
        assert all(result.status == "SUCCESS" for result in results)
        assert unkeyed().status == "SUCCESS"
        assert parent.metadata == {"nested": {"changed": False}, "actionlens_owner_id": "forged"}
    assert all(lens.ledger.get(key).status == "SUCCEEDED" for key in keys)
    assert lens.ledger.get(keys[0]).owner_id != lens.ledger.get(keys[1]).owner_id


def test_explicit_key_binds_tool_and_scope(make_lens):
    lens = make_lens()
    effects = []

    def business(value: int):
        effects.append(value)
        return value

    first = lens.tool(name="first", idempotency="REQUIRED")(business)
    second = lens.tool(name="second", idempotency="REQUIRED")(business)
    key = uuid4().hex
    assert first(1, idempotency_key=key).status == "SUCCESS"
    assert second(1, idempotency_key=key).error_taxonomy == "IdempotencyConflict"
    assert effects == [1]
    with lens.session(session_id="s", tenant_id="other"):
        assert first(1, idempotency_key=key).error_taxonomy == "IdempotencyConflict"


def test_policy_patches_compose_without_redacting_execution_credentials(make_lens):
    class CheckPatch:
        def decide(self, *, args, **kwargs):
            assert args["value"] == 7
            assert args["token"] == "synthetic-secret"
            return PolicyDecision(action="ALLOW", reason="synthetic-secret")

    lens = make_lens(policies=[FixedPolicy("MODIFY_ARGS", {"value": 7}), CheckPatch()],
                     redactor=al.RegexRedactor(["synthetic-secret"]))
    observed = []

    @lens.tool
    def tool(value: int, token: str):
        observed.append((value, token))
        return value

    assert tool(1, "synthetic-secret").result == 7
    assert observed == [(7, "synthetic-secret")]
    assert all("synthetic-secret" not in event.model_dump_json() for event in lens.sink.events)


@pytest.mark.parametrize("patch", [{"value": "wrong"}, {"unknown": 1}])
def test_invalid_policy_patch_is_rejected_before_dispatch(make_lens, patch):
    lens = make_lens(policies=[FixedPolicy("MODIFY_ARGS", patch)])
    effects = []

    @lens.tool
    def tool(value: int):
        effects.append(value)

    assert tool(1).error_taxonomy == "ValidationError"
    assert effects == []


def test_approved_patch_is_checked_against_current_policy_without_double_budget(make_lens):
    class AmountLimit:
        def decide(self, *, args, **kwargs):
            return PolicyDecision(action="DENY" if args["amount"] > 100 else "ALLOW", reason="amount limit")

    budget = al.BudgetPolicy(max_calls_per_run=2)
    lens = make_lens(policies=[budget, AmountLimit()])
    effects = []

    @lens.tool(risk="MUTATION", idempotency="REQUIRED", approval_required=True)
    def pay(amount: int):
        effects.append(amount)
        return amount

    key = uuid4().hex
    with lens.session(session_id="s", run_id="r"):
        pending = pay(50, idempotency_key=key)
        assert lens.approve(ticket_id=pending.result["ticket_id"], modified_args={"amount": 500})
        assert pay(50, idempotency_key=key).error_taxonomy == "ApprovalPolicyDenied"
    assert effects == []


def test_exception_messages_are_bounded_and_redacted_in_ledger_and_outbox(make_lens):
    lens = make_lens()

    @lens.tool(idempotency="REQUIRED", output=al.OutputPolicy(redact_patterns=["synthetic-secret"], error_message_mode="redacted"))
    def tool():
        raise ValueError("synthetic-secret " + "x" * 5000)

    key = uuid4().hex
    tool(idempotency_key=key)
    error = lens.ledger.get(key).last_error
    assert "synthetic-secret" not in error and len(error.encode()) <= 2048
    events = [record.event for record in lens.repository.list_outbox(limit=10000)]
    assert all("synthetic-secret" not in event.model_dump_json() for event in events)


def test_reconciled_success_passes_the_original_output_policy(make_lens):
    lens = make_lens()

    @lens.tool(risk="MUTATION", idempotency="REQUIRED", max_bytes=10)
    def mutate():
        raise ValueError("response lost")

    key = uuid4().hex
    assert mutate(idempotency_key=key).status == "UNCERTAIN"

    class Reconciler:
        def inspect(self, record):
            return al.ReconciliationResult(outcome="CONFIRMED_SUCCEEDED", summary="provider confirmed effect",
                                           evidence_ref="https://audit.invalid/effect",
                                           output={"status": "SUCCESS", "result_summary": "provider response",
                                                   "result": {"token": "synthetic-secret", "body": "x" * 1000}})

    lens.reconcile_uncertain(key, Reconciler())
    result = mutate(idempotency_key=key)
    assert result.status == "SUCCESS" and len(result.result.encode()) <= 10
    assert "synthetic-secret" not in result.model_dump_json()
    assert result.artifact_refs


@pytest.mark.parametrize("approved", [False, True])
def test_ticket_expiry_is_atomic_audited_and_blocks_initial_dispatch(make_lens, approved):
    lens = make_lens()
    effects = []

    @lens.tool(risk="MUTATION", idempotency="REQUIRED", approval_required=True)
    def mutate():
        effects.append(1)

    key = uuid4().hex
    pending = mutate(idempotency_key=key)
    ticket_id = pending.result["ticket_id"]
    if approved:
        assert lens.approve(ticket_id=ticket_id)
    connection = getattr(lens.repository, "_connect", None) or lens.repository._connection
    marker = "?" if isinstance(lens.repository, al.SQLiteGovernanceRepository) else "%s"
    expiry = datetime.now(timezone.utc) - timedelta(seconds=1)
    with connection() as conn:
        conn.execute(f"UPDATE actionlens_approval_tickets SET expires_at={marker} WHERE ticket_id={marker}",
                     (expiry.isoformat() if marker == "?" else expiry, ticket_id))
    assert lens.ticket_store.get(ticket_id).status == "EXPIRED"
    assert lens.ticket_store.get(ticket_id).status == "EXPIRED"
    assert mutate(idempotency_key=key).status == "DENIED" and effects == []
    expired = [record.event for record in lens.repository.list_outbox(limit=10000)
               if record.event.event_type == "approval.expired" and record.event.metadata["ticket_id"] == ticket_id]
    assert len(expired) == 1


def test_native_validation_supports_strict_coerce_and_variadic_arguments(make_lens):
    lens = make_lens()
    observed = []

    @lens.tool
    def strict(value: int, /, *items: int, enabled: bool = False, **extras: int):
        observed.append((value, items, enabled, extras))
        return value

    assert strict("wrong").error_taxonomy == "ValidationError"
    assert strict(True).error_taxonomy == "ValidationError"
    assert strict(1, "wrong").error_taxonomy == "ValidationError"
    assert strict(1, other="wrong").error_taxonomy == "ValidationError"
    assert strict(1, 2, enabled=True, other=3).status == "SUCCESS"
    assert len(observed) == 1

    @lens.tool(validation_mode="coerce")
    def coerce(value: int):
        return value

    assert coerce("7").result == 7

    @lens.tool(validation_mode="passthrough")
    def legacy(value: int):
        return value

    assert legacy("legacy").result == "legacy"


def test_adapter_preserves_ambient_identity_and_explicit_precedence(make_lens):
    lens = make_lens()

    @lens.tool
    def read():
        return "ok"

    adapter = ActionLensToolAdapter(read, framework="test")
    with lens.session(session_id="s", run_id="r", tenant_id="tenant", actor_id="actor"):
        adapter.invoke({})
        asyncio.run(adapter.ainvoke({}))
    started = [event for event in lens.sink.events if event.event_type == "tool_call.started"]
    assert all((event.project, event.session_id, event.run_id) == (lens.project, "s", "r") for event in started)
    with pytest.raises(ValueError, match="internal context"):
        adapter.invoke({"__al_ctx": {"actor_id": "forged"}})


@pytest.mark.parametrize("framework", ["temporal", "dbos"])
def test_opt_in_durable_step_identity_separates_steps_and_reuses_attempts(make_lens, framework):
    lens = make_lens()
    effects = []

    @lens.tool(risk="MUTATION", idempotency="AUTO_HASH", idempotency_key_fn=durable_step_idempotency_key)
    def mutate(value: int):
        effects.append(value)
        return value

    if framework == "temporal":
        def context(step, attempt):
            return context_from_temporal_workflow("workflow", "run", tool_name="mutate", activity_id=step, attempt=attempt, project=lens.project)
    else:
        def context(step, attempt):
            return context_from_dbos_workflow("workflow", tool_name="mutate", step_id=step, attempt=attempt, project=lens.project)
    assert mutate(1, __al_ctx=context("one", 1)).status == "SUCCESS"
    assert mutate(1, __al_ctx=context("one", 2)).status == "SUCCESS"
    assert mutate(1, __al_ctx=context("two", 1)).status == "SUCCESS"
    assert effects == [1, 1]


def test_async_cancellation_records_unknown_effect_and_propagates(make_lens):
    lens = make_lens()
    entered = Event()
    effects = []

    @lens.tool(risk="MUTATION", idempotency="REQUIRED")
    async def mutate():
        effects.append(1)
        entered.set()
        await asyncio.Event().wait()

    key = uuid4().hex

    async def scenario():
        task = asyncio.create_task(mutate(idempotency_key=key))
        while not entered.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert lens.ledger.get(key).status == "UNCERTAIN"
        assert (await mutate(idempotency_key=key)).status == "UNCERTAIN"

    asyncio.run(scenario())
    assert effects == [1]
    assert any(event.event_type == "tool_call.failed" and event.error.taxonomy == "Cancelled" for event in lens.sink.events)


def test_ordered_batch_stops_before_later_mutation(make_lens):
    lens = make_lens()
    effects = []

    @lens.tool(risk="MUTATION", approval_required=True)
    def pending():
        effects.append("pending")

    @lens.tool(risk="MUTATION")
    def later():
        effects.append("later")

    results = lens.invoke_many([(pending, (), {}), (later, (), {})])
    assert [result.status for result in results] == ["PENDING_APPROVAL", "SKIPPED"]
    assert results[1].error_taxonomy == "BatchPaused" and effects == []


def test_method_only_repository_can_run_approval_and_contract_workflow(make_lens, tmp_path):
    original = make_lens().repository

    class MethodOnlyRepository:
        pass

    for method in al.GovernanceRepository.__dict__:
        if not method.startswith("_"):
            def delegate(self, *args, _method=method, **kwargs):
                return getattr(original, _method)(*args, **kwargs)
            setattr(MethodOnlyRepository, method, delegate)

    repository = MethodOnlyRepository()
    assert isinstance(repository, al.GovernanceRepository)
    with closing(al.ActionLens(repository=repository, storage_dir=tmp_path / "protocol", sink=MemorySink())) as lens:
        @lens.tool(approval_required=True, idempotency="REQUIRED")
        def read():
            return "ok"

        key = uuid4().hex
        pending = read(idempotency_key=key)
        assert lens.approve(ticket_id=pending.result["ticket_id"])
        assert read(idempotency_key=key).status == "SUCCESS"
    assert all(al.verify_repository_contract(repository).values())


class NestedInput(BaseModel):
    value: int


def test_native_nested_model_validation_precedes_execution(make_lens):
    lens = make_lens()

    @lens.tool
    def read(value: NestedInput):
        return value.value

    assert read({"value": "wrong"}).error_taxonomy == "ValidationError"
    assert read({"value": 7}).result == 7


def test_default_error_evidence_does_not_persist_arbitrary_exception_text(make_lens):
    lens = make_lens()

    @lens.tool(idempotency="REQUIRED")
    def read():
        raise ValueError("a credential with no recognizable format")

    key = uuid4().hex
    read(idempotency_key=key)
    assert lens.ledger.get(key).last_error == "FormatError (ValueError)"


def test_business_parameter_named_context_participates_in_operation_identity(make_lens):
    lens = make_lens()

    @lens.tool(idempotency="REQUIRED")
    def read(context: str):
        return context

    key = uuid4().hex
    assert read("first", idempotency_key=key).result == "first"
    assert read("second", idempotency_key=key).error_taxonomy == "IdempotencyConflict"


@pytest.mark.parametrize("phase", ["preflight", "commit"])
def test_cancellation_tracks_synchronous_governance_work_to_completion(make_lens, monkeypatch, phase):
    lens = make_lens()
    entered, release = Event(), Event()
    effects = []
    method = "begin" if phase == "preflight" else "finish"
    original = getattr(lens.repository, method)

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(lens.repository, method, slow)

    @lens.tool(risk="MUTATION", idempotency="REQUIRED")
    async def mutate():
        effects.append(1)
        return "ok"

    key = uuid4().hex

    async def scenario():
        task = asyncio.create_task(mutate(idempotency_key=key))
        while not entered.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        asyncio.run(scenario())
    finally:
        release.set()
    assert effects == ([] if phase == "preflight" else [1])
    assert lens.ledger.get(key).status == ("FAILED_RETRYABLE" if phase == "preflight" else "SUCCEEDED")


def test_mcp_output_validation_cannot_retry_an_applied_mutation(make_lens):
    pytest.importorskip("jsonschema")
    lens = make_lens()
    effects = []

    def transport(name, arguments):
        effects.append(1)
        return {"count": "invalid provider response"}

    proxy = al.MCPGovernanceProxy(lens, transport)
    mutate = proxy.register_tool("mutate", risk="MUTATION", idempotency="REQUIRED",
                                 output_schema={"type": "object", "properties": {"count": {"type": "integer"}}, "required": ["count"]})
    key = uuid4().hex
    assert mutate(idempotency_key=key).status == "UNCERTAIN"
    assert mutate(idempotency_key=key).status == "UNCERTAIN"
    assert effects == [1]
