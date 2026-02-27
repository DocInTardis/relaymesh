# RelayMesh 系统使用手册

更新时间：`2026-02-27`

本文档面向“第一次上手 + 线上日常运维 + 故障调试”三个场景，给出可直接执行的命令和操作路径。

## 1. 系统简介

RelayMesh 是一个基于 Java 的轻量多 Agent 运行时，核心由以下组件构成：

- 任务与状态存储：SQLite（`tasks` / `steps` / `messages` 等）
- 队列与交付：文件总线（`inbox/processing/done/dead`）
- 执行器：Worker（按优先级取任务、执行、重试、落死信）
- 运维接口：CLI + Web 控制台 + Prometheus 指标
- 多节点能力：membership、gossip、replication、ownership recovery


## 2. 环境准备

### 2.1 依赖

- JDK 17+
- Maven 3.9+
- Windows / Linux / macOS（本文示例以 PowerShell 为主）

### 2.2 编译与测试

```powershell
mvn test
```

如果测试全绿，说明环境可用。


## 3. 快速启动（5 分钟）

### 3.1 初始化运行目录

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root init"
```

### 3.2 提交一条任务

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root submit --agent echo --input hello-manual --priority normal"
```

### 3.3 启动 worker（长循环）

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root worker --worker-id worker-a --node-id node-a --interval-ms 1000 --maintenance"
```

### 3.4 查询任务

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root tasks --limit 20"
```

### 3.5 启动可视化与指标

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root serve-web --port 18080 --ro-token relay_ro --rw-token relay_rw"
mvn -q exec:java "-Dexec.args=--root tmp/manual-root serve-metrics --port 19090"
```

访问：

- 控制台：`http://127.0.0.1:18080/?token=relay_ro`
- 指标：`http://127.0.0.1:19090/metrics`


## 4. 运行目录说明

当你使用 `--root <dir>` 时，主要会生成：

- `<root>/relaymesh.db`：主数据库
- `<root>/inbox/`：待处理消息（按优先级）
- `<root>/processing/`：处理中消息
- `<root>/done/`：已完成消息元数据
- `<root>/dead/`：失败消息元数据
- `<root>/payload/`：消息负载
- `<root>/audit/audit.log`：审计日志
- `<root>/reports/`：冲突报告/收敛报告等
- `<root>/relaymesh-settings.json`：运行参数（可热重载）


## 5. 命令体系总览

查看总命令：

```powershell
mvn -q exec:java "-Dexec.args=--help"
```

建议按以下分组理解：

### 5.1 任务与工作流

- `submit`：提交单步任务
- `submit-workflow --file <json>`：提交 DAG 工作流
- `worker`：执行任务（支持单次或循环）
- `task <taskId>`：查看任务详情
- `tasks --status --limit --offset`：列表查询
- `workflow <taskId>`：查看工作流 step/依赖
- `cancel <taskId> --mode hard|soft`：取消任务
- `task-export <taskId> --out <file>`：导出任务快照

### 5.2 故障与恢复

- `replay <taskId>`：重放单个死信
- `replay-batch --status DEAD_LETTER --limit N`：批量重放
- `dead-export --out <file>`：导出死信元数据
- `mesh-recover --limit N`：回收 DEAD owner 的运行中 step
- `mesh-prune --older-than-ms --limit`：清理历史 DEAD 节点

### 5.3 多节点 / Mesh

- `members`：成员列表
- `maintenance --node-id <id>`：一次维护 tick
- `gossip`：单轮/循环 gossip
- `gossip-sync`：反熵同步
- `mesh-summary`：Mesh 风险汇总
- `ownership-events`：ownership 事件时间线
- `lease-conflicts` / `lease-report`：冲突明细与聚合报告

### 5.4 复制与快照

- `replication-export --out <file> [--since-ms ...]`
- `replication-import --in <file>`
- `replication-controller`：持续 pull/push 控制器
- `snapshot-export --out <dir>`
- `snapshot-import --in <dir>`（破坏性导入）
- `convergence-report`：复制收敛报告

### 5.5 观测与治理

- `stats`：运行统计
- `metrics` / `serve-metrics`：指标输出
- `health`：健康检查
- `audit-tail` / `audit-query`：审计查看与过滤
- `audit-verify`：审计完整性校验
- `audit-siem-export --out <ndjson>`：导出 SIEM 格式
- `slo-evaluate`：SLO 评估与告警状态文件

### 5.6 安全与密钥

- `serve-web`：Web 控制台（支持 token 鉴权和限流）
- `serve-node-rpc`：HTTPS 节点 RPC（支持可选 mTLS）
- `node-rpc-revocation-template --out <file>`：吊销模板
- `payload-key-status` / `payload-key-rotate`：payload 密钥状态与轮换

### 5.7 Schema 与迁移

- `schema-migrations`：查看已应用迁移
- `schema-rollback --version <version>`：安全子集回滚


## 6. Web 控制台与 API

### 6.1 启动示例

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root serve-web --port 18080 --ro-token relay_ro --rw-token relay_rw --write-rate-limit-per-min 120"
```

### 6.2 主要 API

- `GET /api/tasks`
- `GET /api/dead`
- `GET /api/workflow?taskId=...`
- `GET /api/stats`
- `GET /api/members`
- `GET /api/lease-conflicts`
- `POST /api/cancel`
- `POST /api/replay`
- `POST /api/replay-batch`
- `GET /api/audit-query`
- `GET /api/metrics`
- `GET /events`（SSE）

### 6.3 鉴权传递方式

- Query 参数：`?token=relay_ro`
- Header：`Authorization: Bearer <token>`

默认写接口是 `POST-only`；如需兼容可启用 `--allow-get-writes`（不建议长期开启）。


## 7. 指标与可观测

### 7.1 拉取方式

```powershell
Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:19090/metrics" | Select-Object -Expand Content
```

### 7.2 常看指标

- `relaymesh_tasks_total{status=...}`
- `relaymesh_steps_total{status=...}`
- `relaymesh_messages_total{state=...}`
- `relaymesh_queue_lag_ms`
- `relaymesh_step_latency_ms{quantile="0.95|0.99"}`
- `relaymesh_dead_letter_growth_1h`
- `relaymesh_dead_owner_reclaimed_total`
- `relaymesh_mesh_nodes_pruned_total`
- `relaymesh_worker_paused_total`
- `relaymesh_web_write_rate_limited_total`


## 8. 调试手册（可见调试）

### 8.1 推荐调试拓扑

开 3 个终端：

1. `worker` 长循环
2. `serve-web`
3. `serve-metrics`

再开第 4 个终端执行 submit / query / replay。

### 8.2 实时观察命令

```powershell
# 审计日志
Get-Content "tmp/manual-root/audit/audit.log" -Wait

# worker 输出（如果你用重定向写到日志）
Get-Content "tmp/manual-root/debug-logs/worker.out.log" -Wait

# tasks 状态轮询
while ($true) { mvn -q exec:java "-Dexec.args=--root tmp/manual-root tasks --limit 10"; Start-Sleep 2 }
```

### 8.3 一条完整调试链路

1. 提交 `echo` 任务，确认 `SUCCESS`
2. 提交 `fail` 任务，观察 `RETRYING` / `DEAD_LETTER`
3. 在 `audit.log` 搜 `task.submit`、`step.execute`
4. 在 `/metrics` 查看任务状态和重试/死信相关指标
5. 用 `replay` 或 `replay-batch` 验证恢复路径


## 9. 运行参数（relaymesh-settings.json）

配置文件路径：`<root>/relaymesh-settings.json`

支持热重载：

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root reload-settings"
mvn -q exec:java "-Dexec.args=--root tmp/manual-root settings-history --limit 20"
```

常见字段（节选）：

- `leaseTimeoutMs`
- `suspectAfterMs` / `deadAfterMs`
- `suspectRecoverMinMs` / `deadRecoverMinMs`
- `pauseWorkerWhenLocalNotAlive`
- `maxAttempts` / `baseBackoffMs` / `maxBackoffMs`
- `gossipFanout` / `gossipPacketTtl`
- `meshPruneOlderThanMs` / `meshPruneLimit` / `meshPruneIntervalMs`
- `minFreeDiskBytes`
- `maxIngressQueueDepth` / `maxRunningSteps` / `submitRateLimitPerMin`
- `sloQueueLagTargetMs` / `sloStepP95TargetMs` / `sloStepP99TargetMs`


## 10. 生产建议

- 所有命令都固定 `--root`，避免写入到错误目录
- Web 端必须开启鉴权，不要裸奔
- 持续采集 `metrics` + `audit.log`
- 定期执行：
  - `audit-verify`
  - `mesh-prune`
  - `lease-report`
  - `snapshot-export`
- 变更运行参数后，做一次 `reload-settings` 并检查 `settings-history`


## 11. 常见故障与处理

### 11.1 任务长期 RETRYING

排查顺序：

1. `task <taskId>` 看 `lastError`
2. `audit-query --task-id <taskId>` 看失败轨迹
3. 查看 Agent 配置和输入
4. 评估是否 `replay` 或人工取消

### 11.2 Web 写接口返回 401/403

- 检查 token 是否正确
- 检查 token 角色（reader/writer/admin）
- 检查是否命中 namespace 限制

### 11.3 Web 写接口返回 429

- 命中 `--write-rate-limit-per-min`
- 降低请求频率或调高限流参数

### 11.4 节点状态误判抖动

- 调整 `suspectRecoverMinMs` / `deadRecoverMinMs`
- 用 `membership_recovery_suppressed_total` 指标验证抖动抑制效果

### 11.5 复制/恢复前后状态不一致

- 先做 `snapshot-export`
- 用 `convergence-report` + 审计日志对账
- 必要时执行 `snapshot-import` 回滚


## 12. 参考文档

- `README.md`
- `docs/README.md`
- `docs/guides/READING_GUIDE.md`
- `docs/RELAYMESH_FINAL_ARCHITECTURE.md`
- `docs/PRODUCTION_READINESS.md`
- `docs/NODE_RPC_SECURITY.md`
- `docs/ENDGAME_BACKLOG.md`
