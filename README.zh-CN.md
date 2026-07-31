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

[English](README.md) | 简体中文

[![PyPI version](https://img.shields.io/pypi/v/actionlens.svg)](https://pypi.org/project/actionlens/)
[![Python versions](https://img.shields.io/pypi/pyversions/actionlens.svg)](https://pypi.org/project/actionlens/)
[![License](https://img.shields.io/github/license/JieNEUer/ActionLens.svg)](https://github.com/JieNEUer/ActionLens)


```bash
pip install actionlens
```

ActionLens 是一个低侵入的 Python 库，用于 agent 工具治理与运行轨迹采集。

它位于工具边界，不替代现有 agent 框架。包装一个已有 Python 函数，即可获得结构化工具输出、有界的模型可见结果、大载荷本地 artifact、幂等保护、人工审批交接，以及可供后续评测和监控使用的 JSONL 轨迹。

## ActionLens 的位置

```mermaid
%%{init: {"theme":"base","themeVariables":{"fontFamily":"ui-sans-serif, system-ui, sans-serif","primaryColor":"#F7FBF7","primaryTextColor":"#1F2933","primaryBorderColor":"#3F474A","lineColor":"#4A5559","tertiaryColor":"#FFFFFF"}}}%%
flowchart LR
    host["Agent / 宿主运行时<br/>LangChain、LangGraph、PydanticAI、OpenAI Agents SDK"]:::host
    durable["可选的持久控制层<br/>Temporal Activity / DBOS Step"]:::durable
    provider["业务工具 / provider<br/>SaaS API、数据库、本地系统"]:::provider
    consumer["可观测 / 评测 / 审计<br/>指标、trace、证据消费者"]:::consumer

    subgraph actionlens["ActionLens：治理与证据平面"]
        direction TB
        runtime["受治理的工具运行时"]:::core
        policy["策略与审批<br/>预算与脱敏"]:::governance
        ledger["Ledger 与幂等<br/>Outbox 与恢复"]:::evidence
        artifacts["Artifact 与 provenance<br/>保留策略与访问检查"]:::evidence
        trajectory["轨迹与 exporter<br/>事件与证据包"]:::evidence
        runtime --> policy
        runtime --> ledger
        ledger --> artifacts
        ledger --> trajectory
    end

    host -->|"工具与显式 context"| runtime
    durable -->|"稳定的 workflow / step 标识"| runtime
    policy -->|"已授权调用"| provider
    provider -->|"结果与 provider 证据"| runtime
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

## 为什么需要它

现代 agent 经常在工具边界失控：

- 爬虫返回两万字，直接挤爆模型上下文。
- 模型只改了时间戳，又重试同一个写操作。
- Python 原始 traceback 被作为噪声返回给模型。
- 高风险操作需要人工审批，但审批状态无法持久恢复。
- 生产运行事后无法转换成评测样本或调试轨迹。

ActionLens 将这些问题变成明确的运行时协议，同时让宿主框架继续掌控编排。

## 当前状态

当前仓库提供 v1.5.0 稳定协议，重点是多实例安全治理、有界生产数据路径、基于证据的恢复、可靠审计投递、明确的 artifact 机密性，以及低侵入的持久运行时桥接：

- 支持同步和异步函数的 `@lens.tool(...)` 装饰器
- 面向模型可见结果的 `StructuredToolOutput`
- 大结果本地 artifact 存储
- UTF-8 字节与顶层项数双重结果预算、安全的轨迹默认值和显式摘要控制
- JSONL 轨迹事件
- 用于恢复和分布式 worker 的显式 `ToolCallContext`
- 保留 `parent_call_id` 且共享父 run 预算的 child context
- required `idempotency_key` 的公开签名注入
- 可替换的 PostgreSQL、SQLite 和临时测试治理 repository
- lease 所有权、heartbeat、fencing token、参数/schema 冲突检测和 `UNCERTAIN`
- 原子审批 ticket、ledger 与事务 outbox 状态转换
- 带重试、claim lease 和 dead-letter 的至少一次 outbox 投递
- 可持久恢复的待审批、批准、拒绝与继续执行流程
- 可插拔策略链和脱敏器
- artifact 元数据，以及 dry-run / 容量感知 GC
- 同步/异步 generator 工具的有界内存 artifact 捕获和可配置模型可见尾部
- 同步工具可选的线程模式超时；Python 无法停止仍在运行的线程，因此高风险超时会落为 `UNCERTAIN`
- 只读工具真实、带 TTL 的 `CACHE_READ` 复用
- 同步审批 resolver，与持久化审批 ticket 并存
- 供模型导航的受治理、已授权 artifact 分页与字面 grep 工具
- 具有 JSON Schema 校验且支持审批改参的 MCP `tools/call` 治理代理
- 将 ActionLens 请求身份带入 `RemoteToolRunner` 的 `RemoteToolAdapter` / `lens.remote_tool(...)` 桥接
- 面向并发安全读取工具的有序 `invoke_many()`
- `actionlens summary`、`actionlens export` 和 `actionlens gc`
- 轻量 PydanticAI、OpenAI Agents SDK 和 LangChain/LangGraph adapter
- 面向 Inspect 的 transcript 和保守的 SFT JSONL exporter
- 具有稳定 case ID、宿主提供的任务/环境/rubric、显式 readiness 和版本化 Inspect 映射的 `EvalCaseCandidate`
- 包含源文件摘要、逐事件 SHA-256 链、脱敏/保留元数据和显式缺失项的证据包
- 静态本地 HTML 轨迹报告
- 带明确丢弃策略的可选有界 JSONL 队列
- 可过期审批 ticket、schema 校验后的修改参数，以及 ticket/ledger 检查
- 支持写前脱敏、仅引用/拒绝模式、配额和加密 provider SPI 的 `ArtifactPolicy`
- 用于 metadata-first 派生媒体 artifact 的可选 `MediaMetadata` 和紧凑 `ArtifactProvenance`
- 面向本地明文图片、音频和视频的宿主 `MediaMetadataExtractor` SPI
- 保留仍被派生 artifact 引用的源文件、并支持显式级联模式的 provenance-aware 本地 GC
- 签名 webhook、composite sink、低基数指标和固定版本的 OpenTelemetry GenAI 映射
- 版本化 schema reader / golden fixture，以及可复现的 SFT dataset manifest
- 带具体受治理适配器的框架无关 `RemoteToolRunner` SPI
- 保留稳定 workflow/step 标识且无额外依赖的 Temporal Activity 与 DBOS Step context bridge
- 带原子审计事件、基于证据的 `UNCERTAIN` 对账
- 后台 outbox 生命周期、健康状态和受控 dead-letter replay/terminate
- 经授权的 artifact 读取、解密、checksum 校验和引用 URI 策略
- webhook key rotation、replay window 校验、事件去重 hook 和 SSRF 控制
- 可直接执行的第三方 repository 与 sink 契约检查
- 带有界 acquire/statement/lock/transaction timeout 的 PostgreSQL 连接池
- 显式 advisory-lock migration 和启动时 schema 兼容检查
- 异步工具周围同步治理 I/O 的 event-loop 隔离
- 外部副作用执行前后的阶段化治理失败语义
- outbox backlog/lag 健康指标、有界保留和单往返 PostgreSQL claim
- 跨进程 artifact read/GC lease，以及 symlink/reparse-point 拒绝
- 有界流式 artifact 上传、认证解密和原子目标提升
- 可复现的 benchmark 与 soak 探针，记录百分位和内存证据

PostgreSQL 是多实例部署的首选后端，因为 ledger、审批和 outbox 事实共享同一事务。v1.5 有意不实现 Redis；repository 协议允许未来增加后端，而不需要改变 `ToolRuntime`。

## 本地开发安装

```bash
python -m pip install -e .
python -m pytest -q
```

运行时依赖刻意保持轻量：

- Python 3.10+
- Pydantic v2

生产 PostgreSQL 支持需单独安装：

```bash
python -m pip install -e ".[postgres]"
```

代理 MCP 工具时，安装 MCP JSON Schema 校验依赖：

```bash
python -m pip install -e ".[mcp]"
```

## PostgreSQL Repository

生产启动时不会执行 DDL。请把 migration 作为部署步骤执行，并优先通过环境变量而不是进程命令行提供 DSN：

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

# 在所属应用的生命周期边界关闭：
lens.close()
repository.close()
```

Migration 幂等执行，通过 PostgreSQL advisory transaction lock 串行化，并记录在 `actionlens_schema_migrations`。`auto_migrate=True` 仅保留给隔离的开发环境。PostgreSQL 使用行锁和 `SKIP LOCKED` claim outbox。ActionLens 不把外部副作用宣传成 exactly-once：过期且不可 fencing 的执行会变成 `UNCERTAIN`，必须显式对账。

SQLite 默认保持 `synchronous="FULL"`。能接受 SQLite WAL `NORMAL` 断电权衡、且对延迟敏感的本地部署可显式选择：

```python
repository = al.SQLiteGovernanceRepository(".actionlens/ledger.sqlite3", synchronous="NORMAL")
```

## Artifact 机密性

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

加密 provider 提供 `provider_id` 和 `encrypt(payload, context=...)`。ActionLens 从不存储主密钥。`reference_only` 接受已有 `ArtifactRef`；`deny` 禁止 artifact 写入。

`OutputPolicy(include_raw_in_trajectory=False)` 是默认值，因此完整的
`StructuredToolOutput.result` 不会复制到轨迹事件。只有经过明确批准的训练或诊断
sink 才应打开它。配置加密 provider 后，artifact preview 不会写进未加密的
sidecar；当前调用仍可获得独立受限、已经脱敏的 inline preview。

## 媒体元数据与 Provenance

媒体支持坚持 metadata-first 且不增加编解码依赖。`ArtifactRef.media_metadata` 和 `ArtifactRef.provenance` 是可选的增量字段；ActionLens 不导入 FFmpeg、OCR、ASR 或视觉模型 SDK。

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

如需自动填充本地明文图片、音频或视频的元数据，请注入小型宿主 adapter。Extractor 仅在 ActionLens 原子持久化明文文件后运行，并保持 best-effort，decoder 失败不会把已完成的 artifact 写入变成孤儿；失败会通过 `actionlens.artifacts.fs` logger 输出，便于运维发现持续故障。加密 artifact 永远不会传给 extractor；如果宿主在加密前完成提取，请显式传入可信的 `media_metadata=`。

内容寻址存储可能让来自多个源的相同派生字节去重到同一个文件。此时 sidecar 会保留观察到的所有本地 provenance 记录，GC 把全部源文件视为父节点，不会删除仍有派生证据链的源文件。每个内容寻址文件最多保留 64 个不同的源/转换身份，超限时 fail closed；需要更大 many-to-one 索引的宿主应提供自己的 artifact store。

```python
class MyMediaExtractor:
    def extract(self, path, media_type):
        return al.MediaMetadata(width=1920, height=1080, codec="h264")

lens = al.ActionLens(media_metadata_extractor=MyMediaExtractor())
```

## 快速开始

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

结果超过 `max_bytes` 时，ActionLens 将原始结果存入 `.actionlens/artifacts/`，只返回有界 preview 和 `ArtifactRef`。

## v1.5 执行语义

`max_bytes` 是模型可见结果的 UTF-8 字节预算。集合还会受
`OutputPolicy.max_inline_items` 约束；超限值进入 artifact，调用方获得同一字节
预算约束下的 preview。`summary_fields` 可选择字典中安全的 inline 摘要字段，
`summary_includes_content=False` 可完全禁止摘要包含内容片段。

对同步函数而言，`run_sync_in_thread=True` 只会在超时后解除调用方阻塞，Python
无法强制终止业务线程。`MUTATION` 或 `DESTRUCTIVE` 工具超时因此会返回并持久化为
`UNCERTAIN`，阻断自动重试，必须到业务系统中对账；只读工具的超时仍是可重试的
`TIMEOUT`。

## 幂等

写工具应要求显式幂等键：

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

包装器会修改公开函数签名，使 schema extractor 能看到 required `idempotency_key`：

```python
import inspect
print(inspect.signature(send_message))
# (user_id: str, text: str, *, idempotency_key: str) -> dict
```

在 auto-hash 模式下，应忽略模型可能随意生成的不稳定字段：

```python
@lens.tool(
    risk=al.RiskLevel.MUTATION,
    idempotency=al.IdempotencyPolicy.AUTO_HASH,
    hash_ignore_keys=["timestamp", "nonce", "uuid"],
)
def write_note(message: str, timestamp: int) -> dict:
    return {"ok": True}
```

只读工具可启用真实的共享 TTL 缓存。它的身份包括 project、environment、tenant、
工具名和参数，但刻意不包含 session 或 run：

```python
@lens.tool(idempotency=al.IdempotencyPolicy.CACHE_READ, cache_ttl_sec=60)
def lookup_customer(customer_id: str) -> dict:
    return provider.lookup(customer_id)
```

## 恢复时显式传递 Context

`ContextVar` 适合普通的进程内 request scope；LangGraph checkpoint、Temporal 或后台 worker 等分布式恢复场景必须显式传递 context。

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

`__al_ctx` 由 ActionLens 消费，不会出现在公开工具签名中。

需要保留持久化父调用链接并共享同一 run 预算的 sub-agent 或委派调用，使用
`child_context()`：

```python
with lens.session(session_id="chat-001", run_id="run-001") as parent:
    child = lens.child_context(parent=parent, tool_name="lookup_customer")
    output = lookup_customer("cust-7", __al_ctx=child)
```

## MCP、远程工具与 Artifact 导航

`MCPGovernanceProxy` 会把已注册的 MCP `tools/call` 方法变成普通的 ActionLens
受治理工具。代理会在传输调用前校验 MCP 输入 schema；审批、lease、结果整形、
幂等与轨迹记录均在本地统一执行。

```python
proxy = al.MCPGovernanceProxy(lens, mcp_transport)
proxy.register_tool(
    "search_docs",
    input_schema={"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
)
result = proxy.handle_request(request)
```

该代理同时兼容两个协议时代：旧请求可使用 `initialize` 和经典结果结构；
2026-07-28 请求则在 `params._meta` 中逐请求携带协议版本与客户端能力。代理实现
`server/discover`，输出 `resultType`、缓存提示、`title`/`icons`/`outputSchema`，
并校验 `structuredContent`。审批多轮交互可配置 `input_required_factory` 与
`input_response_handler`；宿主必须先对不透明的 `requestState` 做完整性与重放校验，
再持久化审批决定。

对于已有异步作业 provider，`lens.remote_tool(runner, name="...")` 会建立受治理的
`RemoteToolRunner` 桥接。请求携带 ActionLens 幂等键、参数 hash、schema hash、
context 和 deadline；超时会尝试取消，无法确认的高风险结果会进入 `UNCERTAIN`。

大 artifact 不必复制回上下文，可通过受治理工具导航：

```python
tools = lens.artifact_navigation_tools()
page = tools["artifact_read"](artifact_ref, offset=0, limit=4096)
matches = tools["artifact_grep"](artifact_ref, needle="invoice")
```

两种操作均要求已配置的 artifact authorizer、校验 checksum、产生访问事件并限制
分页/搜索输出。同步和异步 generator 会自动持久化为 artifact；
`OutputPolicy.streaming_tail_lines` 控制返回给模型的尾部。

## 持久 Workflow Bridge

`temporal` 和 `dbos` integration 是无 SDK 依赖的映射层：持久运行时负责 replay、调度、signal 和 durable wait；ActionLens 负责 Activity 或 Step 内部的工具治理。它们不接管 workflow 状态，也不向核心包引入 Temporal/DBOS 依赖。

```python
from actionlens.integrations.temporal import (
    TemporalActivityRunner,
    context_from_temporal_workflow,
)

# 在 Temporal Activity 内传入 activity.info() 的字段。
ctx = context_from_temporal_workflow(
    workflow_id,
    run_id,
    tool_name="send_message",
    activity_id=activity_id,  # 推荐用作调用级关联标识
    attempt=attempt,
    project="messaging",
)
output = await TemporalActivityRunner(send_message).arun(
    ctx, "hello", idempotency_key="message-123"
)
```

Temporal / DBOS 的 `workflow_id` 成为稳定的 ActionLens `session_id`。重试 `attempt` 只用于可观测性，绝不参与 auto-hash 幂等身份。写操作应使用稳定的业务幂等键。审批 signal/message 只是唤醒通知：先提交 `lens.approve(...)`，再让恢复后的 Activity/Step 重新读取 ActionLens ticket 和 ledger。参见 [Temporal 示例](examples/temporal_integration_example.py) 和 [DBOS 示例](examples/dbos_integration_example.py)。

## 人工审批流程

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

第一次调用返回 `PENDING_APPROVAL`，不会执行函数。批准后，相同幂等键可以继续执行。

交互式宿主可通过同步 resolver 处理同一 ticket，同时保留持久化审计链。resolver 返回
`APPROVE`、`DENY` 或 `PENDING`；批准决定会先持久化，再运行实际业务函数：

```python
def prompt_operator(ticket: al.ApprovalTicket, context: al.ToolCallContext):
    return {"action": "APPROVE", "approved_by": "on-call"}

lens = al.ActionLens(approval_resolver=prompt_operator)
```

## 对账不确定的副作用

过期且不可 fencing 的写操作会保持 `UNCERTAIN` 阻塞状态。只能通过返回证据的业务 reconciler 解决：

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

`MANUAL_OVERRIDE` 还要求 `actor_id`、`reason`、`evidence_ref` 和明确的 override target。Ledger 转换与 reconciliation 事件会原子提交。

对于 provider adapter，`ProviderStatusReconciler` 将只读的 `APPLIED`、`NOT_APPLIED`、`PENDING` 或 `UNKNOWN` 观察映射到对账结果，并要求终态结论必须带证据。[Provider 对账手册](examples/provider_reconciliation_cookbook.md) 覆盖支付、邮件、GitHub PR 和对象存储的身份、查询、一致性窗口及证据规则。

## CLI

汇总本地轨迹事件：

```bash
actionlens summary --storage-dir .actionlens
```

导出原生 JSONL、汇总或数据集：

```bash
actionlens export --storage-dir .actionlens --format actionlens-jsonl --output trajectories.jsonl
actionlens export --storage-dir .actionlens --format summary-json --output summary.json
actionlens export --storage-dir .actionlens --format inspect-ai --output inspect.jsonl
actionlens export --storage-dir .actionlens --format eval-candidates --output eval-candidates.jsonl
actionlens export --storage-dir .actionlens --format sft-jsonl --output sft.jsonl
```

Exporter 会跳过损坏或未写完的 JSONL 行，并报告 skipped 数量。SFT 只导出成功完成的调用，使用已脱敏、有界的轨迹输出，从不读取原始 artifact 正文。

## Eval Case Bridge

只有工具边界事件时，并不知道原始用户任务、可复现环境、评分目标或业务结果证明。因此 ActionLens 默认导出明确标记为不完整的 `EvalCaseCandidate`，不会把轨迹伪装成 Inspect `EvalLog`。

由宿主提供版本化 JSON，按 `project/session/run` 索引事实；也接受较短的 `session/run` 和 `run` key：

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

每次导出都会写 sidecar manifest，包含源 hash、输出 hash、mapper 版本、脱敏策略和 readiness/filter 统计。`inspect-samples` 只是 dataset mapper；Inspect task、sandbox、scorer 和 replay 生命周期仍由宿主负责。

## 审计证据包

生成不读取 artifact 正文的有界工具边界证据包：

```bash
actionlens export --storage-dir .actionlens --format evidence-bundle \
  --output evidence/run-001 \
  --retention-policy-id regulated-six-months.v1 --retention-days 180 \
  --host-context-ref https://audit.internal/context/run-001 \
  --actor-authorization-ref https://audit.internal/authz/run-001 \
  --signature-manifest-ref https://audit.internal/signatures/run-001 \
  --worm-archive-ref s3://audit-archive/run-001
```

目录包含 `events.jsonl`、`integrity.jsonl` 和 `manifest.json`。Manifest 记录源与输出 hash、事件链根、保留元数据、脱敏行为、宿主证据引用和缺失项。签名 manifest 与 WORM attestation 仍由宿主管理：可选参数只记录经过清理的引用并消除对应缺失项，ActionLens 不签名数据、不管理密钥，也不声称控制存档系统。

## OpenTelemetry GenAI 映射

`OpenTelemetrySink` 使用固定的 `actionlens.otel-genai.v1` profile，映射 `gen_ai.operation.name`、`gen_ai.tool.name` 和 `gen_ai.tool_call.id`；审批、run 和证据事实保留在 `actionlens.*` 命名空间。由于独立 GenAI semantic conventions 仍在演进，映射契约会记录其上游开发快照。

Prompt/context、工具参数、模型输出、artifact URI 和原始证据不会复制到 span attribute。OpenTelemetry 是可采样的观测输出；轨迹存储仍是证据 source of truth。

生成无需服务器或前端构建链的静态报告：

```bash
actionlens report --storage-dir .actionlens --html --output report.html
```

检查治理状态：

```bash
actionlens tickets --storage-dir .actionlens --status PENDING
actionlens inspect-ledger --storage-dir .actionlens
actionlens outbox --storage-dir .actionlens list
actionlens outbox --storage-dir .actionlens status
actionlens outbox --storage-dir .actionlens cleanup --retention 30d --limit 1000
actionlens outbox --storage-dir .actionlens replay --delivery-id delivery-123
actionlens outbox --storage-dir .actionlens terminate --delivery-id delivery-123 --reason "invalid endpoint"
```

清理旧的本地 artifact：

```bash
actionlens gc --storage-dir .actionlens --older-than 7d
actionlens gc --storage-dir .actionlens --older-than 30d --cascade-derived
```

默认 GC 会保护仍被任何 retained 本地派生文件引用的源文件。`--cascade-derived` 是显式的运维选择，用于把派生文件随符合条件的源文件一起删除；活动 read lease 始终优先。没有删除候选时，GC 会跳过 provenance sidecar 读取；存在候选时，本地 store 必须检查血缘证据才能保护 retained ancestor。需要跨 store 或持续索引血缘服务的部署应在宿主存储层提供该能力。

## 框架 Adapter

Adapter 保持框架依赖可选，并保留 ActionLens 管理后的签名：

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

每个 adapter 都提供 `invoke()` / `ainvoke()`，用于显式映射框架 context。原生框架对象 factory 在对应 integration 模块中延迟导入，因此导入 ActionLens 不会加载这些框架。

原生 LangGraph `StateGraph` / `ToolNode` 执行需要独立 optional extra：

```bash
python -m pip install -e ".[langgraph]"
```

LangGraph 恢复时应显式映射序列化状态：

```python
from actionlens.integrations.langchain import context_from_langgraph_state

ctx = context_from_langgraph_state(
    {"config": {"configurable": {"thread_id": "chat-1", "run_id": "run-1"}}},
    tool_name="send_message",
)
result = send_message("hello", idempotency_key="msg-1", __al_ctx=ctx)
```

## 有界 JSONL 队列

同步写入仍是默认行为。需要时显式启用有界单 writer 队列：

```python
from actionlens.sinks import JsonlSink

sink = JsonlSink(
    ".actionlens",
    queue_maxsize=10_000,
    drop_policy="drop_oldest",  # block, drop_oldest, or drop_newest
    strict=False,
)
lens = al.ActionLens(project="demo", storage_dir=".actionlens", sink=sink)

# 在进程退出或应用生命周期边界关闭：
lens.close()
print(sink.stats())
```

`strict=False` 时，sink 故障与业务工具隔离并计数；`strict=True` 时，写入失败会通过 `emit()`、`flush()` 或 `close()` 传播。

## 设计边界

- ActionLens 不是 agent 框架。
- 它不负责图控制流、模型选择、scorer 逻辑或环境重置。
- 本地 JSONL 轨迹是 source of truth；OpenTelemetry、Prometheus、dashboard 和 eval adapter 都是可选输出。
- 简单 key 脱敏只是 MVP fallback；生产环境应接入更强的脱敏/DLP 引擎。

## 开发与验证

```bash
python -m pytest -q
python -m compileall -q src tests
python -m ruff check src tests benchmarks
python benchmarks/benchmark.py --iterations 1000 --output benchmark.json
python benchmarks/soak.py --duration 86400 --output soak-24h.json
```

PostgreSQL query-plan 检查只能连接隔离的本地测试集群：

```bash
ACTIONLENS_POSTGRES_DSN=postgresql://postgres@127.0.0.1:55432/postgres \
  python benchmarks/postgres.py --output postgres-query-plan.json
```
