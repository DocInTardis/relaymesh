# RelayMesh 最终架构文档（审视版）

版本: `v1.0-final`  
日期: `2026-02-24`  
状态: `可作为项目实施基线`  
项目名: `RelayMesh`

---

## 1. 文档目标

本文件给出一份可落地的最终方案，目标是构建一个支持多异构 Agent（如 Codex、Claude Code、GPT 系列 API 代理）协作的运行环境，并满足以下现实约束：

- 轻量化优先，不强制依赖 Redis。
- v1 先保证单机稳定、可恢复、可观测。
- v2 再演进到多节点去中心化 Mesh。
- 明确系统语义边界，避免“伪 exactly-once”。
- 把已识别的关键风险点固化为必须实现项。

---

## 2. 最终结论（先说结果）

- `Redis 不是必须`。RelayMesh v1 采用 `SQLite + 文件队列` 即可落地，且更符合轻量化目标。
- 系统语义采用 `at-least-once + 幂等`，不承诺 exactly-once。
- Agent 之间通信采用 `总线转发`（而非点对点直连），通过标准消息信封实现跨语言协作。
- 低内存模式可行，单机目标为 `JVM < 512MB`。
- v2 才引入多节点 membership、ownership 迁移和去中心化调度。

---

## 3. 范围与非目标

### 3.1 v1 范围（必须做）

- 单节点 Runtime，支持同时拉起多个 Agent 进程。
- Agent 注册、发现、消息投递、重试、回收、死信。
- DAG/链式工作流编排。
- 幂等去重、失败补偿、崩溃恢复。
- 基础安全（Token、权限边界）与可观测（日志+指标+追踪）。

### 3.2 v1 非目标（明确不做）

- 跨机房高可用一致性。
- 强一致共识存储（Raft/Paxos）。
- 千万级吞吐和超大规模集群调度。

---

## 4. 语义边界（必须写清楚）

### 4.1 投递语义

- 投递语义: `at-least-once`
- 防重机制: `idempotency_key + result_hash + 状态机检查`
- 不提供: `exactly-once`

### 4.2 任务状态机

`PENDING -> RUNNING -> SUCCESS | FAILED | RETRYING | TIMEOUT | ABANDONED | DEAD_LETTER`

说明：

- `ABANDONED`: lease 过期或 worker 崩溃后被回收。
- `DEAD_LETTER`: 超过最大重试次数后进入死信。

### 4.3 幂等定义

- 请求级幂等键: `idempotency_key = SHA-256(canonical_request)`
- 结果哈希: `result_hash_algo = "sha256"`，并持久化算法字段。
- 同幂等键重复提交时，返回已完成结果或当前执行状态。

---

## 5. v1 总体架构（轻量中心化）

```text
Client/CLI
   |
   v
RelayMesh Runtime (single JVM)
  - API Layer
  - Agent Supervisor
  - Dispatcher/Scheduler
  - Workflow Engine
  - Idempotency Manager
  - Lease & Recovery Manager
  - Retry Scheduler
  - State Store (SQLite)
  - File Bus (meta/payload + inbox tiers)
   |
   +--> Agent Adapter: Codex CLI
   +--> Agent Adapter: Claude Code CLI
   +--> Agent Adapter: OpenAI/HTTP Agent
   +--> Agent Adapter: Custom Script Agent
```

架构原则：

- Runtime 只做调度与状态控制，不持有超长上下文。
- 各 Agent 通过统一 Adapter 接口接入，避免绑定单厂商协议。
- 消息体与元数据分离，降低状态查询 IO 开销。

---

## 6. Agent 间通信模型（核心）

### 6.1 为什么不用 Agent 直连

- 直连会导致连接关系爆炸（N*N）。
- 难做统一幂等、追踪、审计、重试与回收。

### 6.2 RelayMesh 采用“总线中转”

流程：

1. 发送 Agent 写入消息信封（meta）和消息体（payload）。
2. Dispatcher 按策略取消息并投递给目标 Agent。
3. 目标 Agent 回写结果消息（reply_to）。
4. Workflow Engine 推进下一步或结束任务。

### 6.3 消息文件布局（v1）

```text
data/
  inbox/high/
  inbox/normal/
  inbox/low/
  payload/
  meta/
  processing/<worker_id>/
  done/YYYY-MM-DD/
  dead/
```

约束：

- 元数据: `meta/<msg_id>.meta.json`
- 消息体: `payload/<msg_id>.payload.json`
- 单文件 payload 上限: `10MB`（默认）

---

## 7. 关键实现细节（吸收你提出的问题）

### 7.1 时间与 lease（P0 必做）

问题：绝对时间可能受时钟跳变影响。  
方案：

- 运行时超时判断使用 `monotonic clock`（`System.nanoTime()`）。
- 持久化仅用于审计与恢复，不直接作为超时判定唯一依据。
- 增加“时钟跳变守护”：若 wall time 与 monotonic 漂移异常，暂缓 reclaim 一轮并打告警。

### 7.2 SQLite PRAGMA 校验（P0 必做）

问题：PRAGMA 拼写错误会被静默忽略。  
方案：

- 启动后执行并回读校验，校验不通过直接 fail-fast。
- 强制项示例：
  - `journal_mode=WAL`
  - `synchronous=NORMAL`（或更高）
  - `busy_timeout`
  - `foreign_keys=ON`

### 7.3 Windows 退出语义（P0 必做）

问题：`CTRL_CLOSE_EVENT` 常见只有约 5-10 秒处理窗口。  
方案：

- Windows 默认 `graceful_timeout=5s`。
- Linux/macOS 默认 `graceful_timeout=15s`。
- 退出流程仅做必要动作：停止接单、flush 状态、释放 lease、记录恢复点。

### 7.4 优先级与公平性（P1 必做）

问题：仅靠文件名优先级在无序遍历下可能失效。  
方案：

- 采用多 inbox 目录：`high/normal/low`。
- 配比调度默认：`8:3:1`。
- 增加抗饥饿规则：`max_consecutive_high=100`，达到阈值后强制处理低优先级至少 1 条。
- 增加指标：`low_starvation_count`。

### 7.5 幂等表 TTL 清理（P1 必做）

问题：只做每日后台清理不够，长期离线重启可能膨胀。  
方案：

- 启动时立即执行一次 TTL 清理。
- 运行时每日清理一次。
- `processed_at` 建索引，清理使用范围删除。

### 7.6 processing 扫描开销（P1 必做）

问题：处理中目录堆积会导致 reclaim 扫描变慢。  
方案：

- reclaim 以 SQLite 索引驱动，不全目录遍历。
- 目录扫描只做校验兜底，低频执行。

### 7.7 写热点与吞吐边界（P2 规划）

风险：高压下 lease 高频写可能压到 SQLite。  
v1 处理：

- 通过 heartbeat 合并、批量提交、WAL 降低写放大。
- 明确 v1 吞吐目标，不承诺 10k+/s。

v2 处理：

- lease 子系统解耦（本地日志/内存索引 + 异步快照）。

---

## 8. 数据模型（v1）

### 8.1 SQLite 表

`tasks`

- `task_id` TEXT PK
- `status` TEXT
- `idempotency_key` TEXT UNIQUE
- `trace_id` TEXT
- `created_at_ms` INTEGER
- `updated_at_ms` INTEGER

`steps`

- `step_id` TEXT PK
- `task_id` TEXT
- `agent_id` TEXT
- `status` TEXT
- `attempt` INTEGER
- `next_retry_at_ms` INTEGER
- `lease_owner` TEXT
- `lease_token` TEXT
- `updated_at_ms` INTEGER

`messages`

- `msg_id` TEXT PK
- `task_id` TEXT
- `from_agent` TEXT
- `to_agent` TEXT
- `priority` TEXT
- `state` TEXT
- `meta_path` TEXT
- `payload_path` TEXT
- `created_at_ms` INTEGER

`idempotency`

- `idempotency_key` TEXT PK
- `status` TEXT
- `result_ref` TEXT
- `result_hash` TEXT
- `result_hash_algo` TEXT
- `processed_at_ms` INTEGER

### 8.2 索引建议

- `idx_steps_status_next_retry`
- `idx_steps_lease_owner_updated`
- `idx_messages_state_priority_created`
- `idx_idempotency_processed_at`

---

## 9. 调度与重试策略（默认值）

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `max_attempts` | `3` | 超过后进入死信 |
| `base_backoff_ms` | `1000` | 初始退避 |
| `max_backoff_ms` | `60000` | 退避上限 |
| `reclaim_scan_interval_ms` | `5000` | reclaim 主循环 |
| `lease_heartbeat_ms` | `2000` | 续租频率 |
| `lease_timeout_ms` | `15000` | 判定超时 |
| `suspect_recover_min_ms` | `5000` | SUSPECT 恢复最小驻留窗口 |
| `dead_recover_min_ms` | `30000` | DEAD 恢复最小驻留窗口 |
| `pause_worker_when_local_not_alive` | `true` | 本地节点为 SUSPECT/DEAD 时暂停 worker 拉取执行 |
| `mesh_prune_older_than_ms` | `604800000` | stale DEAD 成员清理年龄窗口（7d） |
| `mesh_prune_limit` | `256` | 单次清理上限 |
| `mesh_prune_interval_ms` | `3600000` | maintenance 自动清理周期（1h） |
| `payload_max_bytes` | `10485760` | 10MB |
| `done_retention_days` | `7` | done 保留天数 |
| `idempotency_ttl_days` | `7` | 幂等保留天数 |
| `queue_capacity` | `100` | 内存待调度队列上限 |
| `thread_pool_core/max` | `4/8` | 低内存默认线程池 |
| `llm_parallel_limit` | `4` | LLM 并发信号量 |
| `gossip_sync_sample_size` | `32` | 反熵同步每包携带的成员样本上限 |

以上参数可通过 `<root>/relaymesh-settings.json` 覆盖（v1 已落地），用于环境级调优。
运行中可通过 `reload-settings` 或 worker 周期热加载生效。
配置变更审计应输出 `changed_fields`（字段名列表），避免记录敏感值明文。
可通过 `settings-history --limit N` 回放最近配置加载摘要。

重试算法：

`next = min(max_backoff_ms, base_backoff_ms * 2^(attempt-1)) + jitter`

---

## 10. 低内存模式（推荐默认）

### 10.1 目标

- 单 JVM 进程运行。
- 峰值内存控制在 `300MB ~ 500MB`。

### 10.2 策略

- 有界线程池 + 有界队列。
- LLM 调用并发信号量。
- Agent 尽量插件化，外部脚本 Agent 采用短生命周期子进程。
- 不常驻大对象缓存，优先流式处理。
- 大 payload 文件化，不常驻堆内存。

### 10.3 JVM 建议参数（示例）

`-Xms256m -Xmx512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200`

---

## 11. 协议与互操作策略

RelayMesh 不绑定单一协议，采用“内核语义统一 + 外围协议适配”：

- 内核统一语义：任务、消息、lease、幂等、重试。
- 协议适配层：
  - `A2A Adapter` 用于跨 Agent Runtime 互通。
  - `MCP Adapter` 用于工具/资源上下文接入。
  - `gRPC/HTTP Adapter` 用于内部服务化边界。

这样可以同时支持：

- 本地 CLI Agent（Codex/Claude Code）
- 远端 API Agent（OpenAI/Anthropic 等）
- 社区自定义 Agent

---

## 12. 安全与隔离

最小安全基线：

- Agent 注册 token 校验。
- Web 控制台 token 鉴权（读写角色分离：`ro-token` / `rw-token`）。
- Web token 轮换（双 token 过渡）。
- Web 写接口默认 `POST-only`，可通过显式开关临时兼容 GET。
- 兼容 GET 写时必须输出弃用提示，并记录 `web.write.get_compat` 审计事件。
- Web 写接口应支持速率限制，并在触发时返回 `429` 与 `Retry-After`。
- Web 写操作审计必须带来源字段（method/route/remote_addr/user_agent/x_forwarded_for/origin/referer）。
- namespace 隔离（团队/项目级）。
- 执行动作审计日志（谁在何时执行了什么）。
- 对高危工具调用启用显式确认策略。

多节点场景（v2）补充：

- 节点间 mTLS。
- 节点身份证书轮换。

---

## 13. 可观测性（必须落地）

### 13.1 指标

- `relaymesh_queue_depth`
- `relaymesh_active_tasks`
- `relaymesh_retry_total`
- `relaymesh_dead_total`
- `relaymesh_abandoned_total`
- `relaymesh_low_starvation_count`
- `relaymesh_workflow_latency_p95`
- `relaymesh_anti_entropy_merge_total`
- `relaymesh_web_write_get_compat_total`
- `relaymesh_web_write_rate_limited_total`
- `relaymesh_dead_owner_reclaimed_total`
- `relaymesh_membership_recovery_suppressed_total`
- `relaymesh_mesh_nodes_pruned_total`
- `relaymesh_worker_paused_total`
- `relaymesh_mesh_nodes_total{status}`
- `relaymesh_running_steps_by_owner_status{owner_status}`
- `relaymesh_oldest_dead_node_age_ms`
- `relaymesh_gossip_convergence_seconds`

### 13.2 追踪

- 每任务一个 `trace_id`，每 step 一个 `span_id`。
- 跨 Agent 传递 trace 上下文。

### 13.3 日志

- 任务提交
- 调度决策
- Agent 调用结果
- 重试与回收事件
- 死信入队原因

---

## 14. v2 演进（去中心化 Mesh）

### 14.1 目标

- 多节点对等运行。
- 成员发现与故障探测。
- task ownership 可迁移。

### 14.2 关键机制

- membership（SWIM 类机制）
- 节点状态：`ALIVE | SUSPECT | DEAD`
- `lease_owner=node_id` + `lease_token` + `lease_epoch` fencing
- reclaim 后重新分配任务
- dead owner 快速回收（维护周期自动执行，手动命令 `mesh-recover`）
- stale DEAD 成员清理（手动命令 `mesh-prune` + maintenance 周期自动清理）
- mesh 风险汇总（命令 `mesh-summary`，输出成员状态与 RUNNING ownership 风险）
- ownership 事件时间线导出（`ownership-events`）
- 节点抖动保护（`SUSPECT/DEAD` 最小驻留窗口 + 恢复抑制计数）
- worker 节点安全模式（本地状态为 `SUSPECT/DEAD` 时暂停拉取，避免在不稳定成员态继续执行）

### 14.3 状态策略

- v1: `SQLite local-first` + 单节点闭环
- v2: `local-first + eventual sync`（插件化外部存储可选）

---

## 15. 开发里程碑与 DoD

### 15.1 M1（2 周）基础可跑

- Agent Supervisor + File Bus + SQLite 表结构
- 单步任务可提交、可执行、可查询
- DoD: 1000 条任务无崩溃，状态一致

### 15.2 M2（2 周）可靠性闭环

- 幂等、重试、死信、lease reclaim
- Windows/Linux 优雅退出
- DoD: 故障注入后可恢复，重复提交不重复执行

### 15.3 M3（2 周）工作流与观测

- DAG/链式工作流
- 指标/日志/trace 打通
- `relaymesh purge` 清理命令
- DoD: 能演示完整多 Agent 协作链路 + 观测面板

### 15.4 M4（进阶）v2 试验

- membership 原型
- ownership 迁移
- DoD: 两节点失效转移成功

---

## 16. 与同类项目的差异化定位

RelayMesh 的差异化不在“再造一个 Agent SDK”，而在：

- 面向异构 Agent 的统一运行语义（幂等、lease、回收、审计）。
- 轻量单机可落地，不依赖重中间件即可跑通。
- 从 v1 到 v2 的演进路径明确，可用于工程化展示和面试系统设计讲解。

---

## 17. 风险清单（最终版）

`P0（v1 必须完成）`

- 单调时钟驱动 lease 超时判定。
- PRAGMA 回读校验 fail-fast。
- Windows `graceful_timeout=5s` 与事件处理落地。

`P1（v1 强烈建议完成）`

- 多 inbox 优先级 + 抗饥饿规则。
- payload 限流（10MB）与入队前校验。
- `relaymesh purge`（done/dead 清理策略化）。
- 启动即执行幂等 TTL 清理。

`P2（v2 规划）`

- SQLite 写热点优化（lease 子系统解耦）。
- 更完整的消息分层与同步机制。
- 多节点 membership 与 ownership 稳定化。

---

## 18. 实施建议（技术栈）

推荐基线：

- 语言: `Java 17`
- RPC: `gRPC`（仅在服务化边界启用）
- 存储: `SQLite (WAL)`
- 可观测: `OpenTelemetry + Prometheus`
- Agent 适配: `CLI Adapter + HTTP Adapter + MCP/A2A Adapter`

可选增强（非必需）：

- Redis 作为高吞吐缓存/队列插件（不是 v1 前置）
- MySQL/PostgreSQL 作为长期审计归档

---

## 19. 参考资料（官方/一手）

以下资料于 `2026-02-24` 核验可访问：

- MCP 发布（Anthropic）  
  https://www.anthropic.com/news/model-context-protocol
- MCP 规范  
  https://modelcontextprotocol.io/specification/2024-11-05  
  https://modelcontextprotocol.io/specification/2025-06-18  
  https://github.com/modelcontextprotocol/modelcontextprotocol
- A2A（Google）  
  https://developers.googleblog.com/en/a2a-a-new-era-of-agent-interoperability/  
  https://github.com/google-a2a/A2A
- gRPC 官方文档  
  https://grpc.io/docs/
- SQLite WAL / PRAGMA  
  https://www.sqlite.org/wal.html  
  https://www.sqlite.org/pragma.html
- Redis SET 语义（可选组件参考）  
  https://redis.io/docs/latest/commands/set/
- OpenTelemetry 概念  
  https://opentelemetry.io/docs/concepts/
- SWIM 论文（Cornell）  
  https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
- Java 并发（ThreadPoolExecutor / Semaphore）  
  https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ThreadPoolExecutor.html  
  https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/Semaphore.html
- OpenAI 官方文档（工具调用/限流）  
  https://platform.openai.com/docs/guides/function-calling  
  https://platform.openai.com/docs/guides/rate-limits
- Claude Code 官方文档  
  https://docs.anthropic.com/en/docs/claude-code/overview

---

## 20. 一句话版本（用于简历/答辩）

RelayMesh 是一个面向异构 AI Agent 的轻量运行时：在 v1 以 `SQLite + 文件总线` 落地多 Agent 协作、幂等与故障恢复，在 v2 演进到去中心化 Mesh，实现节点级发现、ownership 迁移与故障回收。
