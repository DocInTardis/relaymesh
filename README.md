# RelayMesh

RelayMesh 是一个轻量级多 Agent 运行时。当前主线已覆盖 `v1 M1-M4 + P8`，并落地了一批 `v2` Mesh 稳定化能力。

它的核心目标是：

- 用本地文件队列 + SQLite 提供可恢复的任务执行闭环
- 在单机可用的基础上，逐步演进到多节点 Mesh 协作
- 通过审计日志、指标和脚本化回归，保证可观测与可运维

## 文档导航

- 架构总览：`docs/RELAYMESH_FINAL_ARCHITECTURE.md`
- 生产化清单：`docs/PRODUCTION_READINESS.md`
- Node RPC 安全：`docs/NODE_RPC_SECURITY.md`
- 阅读指南：`docs/guides/READING_GUIDE.md`
- 实施与路线：`docs/IMPLEMENTATION_BACKLOG.md`、`docs/ENDGAME_BACKLOG.md`、`docs/NEXT_FEATURES.md`

## 当前能力（摘要）

### 核心执行链路

- 任务提交：`submit`
- 工作流提交：`submit-workflow --file`
- Worker 执行：`worker`
- 结果查询：`task`、`tasks`、`workflow`
- 取消任务：`cancel --mode hard|soft`
- 失败回放：`replay`、`replay-batch`
- 历史清理：`purge`

### 可靠性与治理

- 幂等键去重（`idempotency_key`）
- Retry（指数退避 + jitter）
- lease fencing（`lease_token` + `lease_epoch`）
- ownership 冲突记录与可视化（`lease-conflicts`、`lease-report`）
- 审计日志（JSONL）和结构化查询（`audit-query`）

### Mesh / Gossip（v2）

- 节点成员状态：`ALIVE/SUSPECT/DEAD`
- Gossip 传播：`gossip`
- 反熵同步：`gossip-sync`
- dead owner 回收：`mesh-recover`
- stale DEAD 清理：`mesh-prune`
- ownership 时间线：`ownership-events`

### 可观测性

- 运行统计：`stats`
- Prometheus 文本：`metrics`
- 指标服务：`serve-metrics`
- 汇总视图：`mesh-summary`、`health`

### Web 控制台与安全

- 控制台：`serve-web`
- 读写鉴权：`--ro-token` / `--rw-token`
- token 轮换：`--ro-token-next` / `--rw-token-next`
- 写接口安全基线：默认 `POST`，兼容开关 `--allow-get-writes`
- 写接口限流：`--write-rate-limit-per-min`

## 快速开始

### 1) 构建与测试

```bash
mvn test
```

### 2) 初始化运行目录

```bash
mvn -q exec:java -Dexec.args="init --root tmp/demo-data"
```

### 3) 查看 Agent

```bash
mvn -q exec:java -Dexec.args="agents --root tmp/demo-data"
```

### 4) 提交任务

```bash
mvn -q exec:java -Dexec.args="submit --root tmp/demo-data --agent echo --payload '{\"text\":\"hello\"}'"
```

### 5) 启动 Worker

```bash
mvn -q exec:java -Dexec.args="worker --root tmp/demo-data --once"
```

### 6) 查询任务

```bash
mvn -q exec:java -Dexec.args="tasks --root tmp/demo-data --limit 20"
```

## 常用命令速查

### 工作流与任务

```bash
# 提交工作流
mvn -q exec:java -Dexec.args="submit-workflow --root tmp/demo-data --file examples/workflows/simple.json"

# 查看某个任务
mvn -q exec:java -Dexec.args="task --root tmp/demo-data <taskId>"

# 查看工作流图
mvn -q exec:java -Dexec.args="workflow --root tmp/demo-data <taskId>"

# 取消任务
mvn -q exec:java -Dexec.args="cancel --root tmp/demo-data <taskId> --mode soft --reason 'manual stop'"
```

### 维护与恢复

```bash
# 单次维护 tick
mvn -q exec:java -Dexec.args="maintenance --root tmp/demo-data --node-id node-a"

# dead owner 回收
mvn -q exec:java -Dexec.args="mesh-recover --root tmp/demo-data --limit 200"

# 清理 stale DEAD 节点
mvn -q exec:java -Dexec.args="mesh-prune --root tmp/demo-data --older-than-ms 604800000 --limit 256"

# ownership 事件
mvn -q exec:java -Dexec.args="ownership-events --root tmp/demo-data --since-hours 24 --limit 200"
```

### Gossip / Mesh

```bash
# gossip tick
mvn -q exec:java -Dexec.args="gossip --root tmp/demo-data --node-id node-a --bind-port 18080 --seeds 127.0.0.1:18081"

# gossip 反熵同步
mvn -q exec:java -Dexec.args="gossip-sync --root tmp/demo-data --node-id node-a --peers http://127.0.0.1:8081"

# mesh 风险汇总
mvn -q exec:java -Dexec.args="mesh-summary --root tmp/demo-data"
```

### 可观测性

```bash
# 文本指标
mvn -q exec:java -Dexec.args="metrics --root tmp/demo-data"

# 指标服务
mvn -q exec:java -Dexec.args="serve-metrics --root tmp/demo-data --port 19090"

# 审计查询
mvn -q exec:java -Dexec.args="audit-query --root tmp/demo-data --action task.cancel --limit 20"
```

### Web 控制台

```bash
# 最小控制台（无鉴权）
mvn -q exec:java -Dexec.args="serve-web --root tmp/demo-data --port 8080"

# 带鉴权
mvn -q exec:java -Dexec.args="serve-web --root tmp/demo-data --port 8080 --ro-token relay_ro --rw-token relay_rw"

# 写接口限流
mvn -q exec:java -Dexec.args="serve-web --root tmp/demo-data --port 8080 --write-rate-limit-per-min 60"
```

## 示例与脚本

- 工作流示例：`examples/workflows/`
- P8 冒烟：`scripts/p8_smoke.ps1`
- v2 Mesh 回收：`scripts/v2_mesh_smoke.ps1`
- v2 抖动保护：`scripts/v2_jitter_smoke.ps1`
- v2 stale DEAD 清理：`scripts/v2_prune_smoke.ps1`
- v2 worker 节点安全：`scripts/v2_worker_pause_smoke.ps1`
- v2 lease epoch：`scripts/v2_lease_epoch_smoke.ps1`

## 关键输出与目录

- 审计日志：`<root>/audit/audit.log`
- 报告目录：`<root>/reports/`
- payload：`<root>/payload/`
- 队列目录：`<root>/inbox|processing|done|dead`
- SQLite：`<root>/relaymesh.db`

## 项目结构

- 核心代码：`src/main/java/io/relaymesh`
- 测试代码：`src/test/java/io/relaymesh`
- 文档：`docs/`
- 脚本：`scripts/`
- 样例：`examples/`

更细结构请看 `PROJECT_STRUCTURE.md`。

## 开发建议

- 开发前先跑：`mvn test`
- 变更调度、租约或复制逻辑时，至少执行：
  - `scripts/p8_smoke.ps1`
  - `scripts/v2_mesh_smoke.ps1`
  - `scripts/v2_lease_epoch_smoke.ps1`
- 提交前关注以下指标是否异常：
  - `relaymesh_dead_owner_reclaimed_total`
  - `relaymesh_mesh_nodes_pruned_total`
  - `relaymesh_worker_paused_total`
  - `relaymesh_web_write_rate_limited_total`