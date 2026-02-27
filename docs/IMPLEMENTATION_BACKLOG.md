# RelayMesh 实施清单（持续推进）

更新日期：`2026-02-27`

## M1 基线（已完成）

- [x] Java 17 + Maven 项目骨架
- [x] SQLite 初始化与核心表结构（`tasks/steps/messages/idempotency`）
- [x] 文件总线目录（`inbox/high|normal|low`、`processing`、`done`、`dead`）
- [x] 提交 -> 拉取 -> 执行 -> 落库 -> 归档闭环
- [x] 内置 Agent（`echo`、`fail`）与 `AgentRegistry`
- [x] CLI 基础命令（`init` / `agents` / `submit` / `worker` / `task`）

## M2 可靠性（已完成）

- [x] Retry 调度（指数退避 + jitter + `max_attempts`）
- [x] 死信回放（`replay`）与导出（`dead-export`）
- [x] lease 心跳与 reclaim（单调时钟 + wall fallback）
- [x] `purge` 历史清理能力
- [x] payload 大小限制（默认 10MB）
- [x] Windows 优雅退出超时策略

## M3 工作流与观测（已完成）

- [x] DAG/链式工作流（`submit-workflow --file`）
- [x] step 依赖解锁与自动入队
- [x] Trace 传播（`trace_id` / `span_id` / `traceparent`）
- [x] 审计日志标准化（JSONL）
- [x] Prometheus 指标（`metrics` / `serve-metrics`）

## M4 去中心化实验（已完成）

- [x] membership 原型（`ALIVE/SUSPECT/DEAD`）
- [x] ownership CAS 冲突控制
- [x] 双节点故障回收演示

## P8 与治理增强（已完成）

- [x] 批量重放（`replay-batch`）
- [x] Script Agent（`agents/scripts.json`）
- [x] 按 Agent 覆盖重试策略（`agents/retry-policies.json`）
- [x] 审计查询（`audit-query`）
- [x] Web 控制台（`serve-web`）与 SSE 推送（`/events`）
- [x] Web 鉴权（`--ro-token` / `--rw-token`）与 token 轮换
- [x] Web 写接口限流（`--write-rate-limit-per-min`）
- [x] 配置热重载（`reload-settings` / `settings-history`）
- [x] CI 工作流与 `mvn test` 基线

## v2 Mesh 稳定化（已完成）

- [x] dead owner 快速回收（`mesh-recover`）
- [x] stale DEAD 清理（`mesh-prune`）
- [x] ownership 事件时间线（`ownership-events`）
- [x] 抖动保护（`suspectRecoverMinMs` / `deadRecoverMinMs`）
- [x] worker 节点安全模式（`pauseWorkerWhenLocalNotAlive`）
- [x] lease epoch fencing（`lease_epoch`）
- [x] gossip 反熵同步（`gossip-sync`）
- [x] mesh 风险汇总（`mesh-summary`）

## 最新补充（2026-02-25）

- [x] 调度防饥饿保护（高优先级连续命中阈值后强制尝试低优先级）
- [x] 防饥饿指标（`relaymesh_low_starvation_count`）
- [x] ownership epoch 回归测试
- [x] 公平性冒烟脚本（`scripts/v2_fairness_smoke.ps1`）

## 后续追踪

- 长期路线见：`docs/ENDGAME_BACKLOG.md`
- 下一批功能见：`docs/NEXT_FEATURES.md`