# RelayMesh v1 生产化就绪清单

更新时间: `2026-02-25`

## 1. 可靠性

- [x] 幂等去重（`idempotency_key` + 状态落库）
- [x] lease 心跳 + reclaim（单调时钟）
- [x] 重试调度（指数退避 + jitter）与死信
- [x] 死信重放（单条 + 批量）
- [x] 配置热重载（`reload-settings` + worker 周期检查）
- [x] 配置变更历史回放（`settings-history --limit N`）

## 2. 安全

- [x] Web 鉴权（`ro-token` / `rw-token`）
- [x] token 轮换（`--ro-token-next` / `--rw-token-next`）
- [x] 写接口默认 `POST-only`
- [x] GET 写兼容开关弃用提示（`--allow-get-writes`）
- [x] 写接口限流（`--write-rate-limit-per-min`，429 + `Retry-After`）

## 3. 可观测性

- [x] Prometheus 指标（`metrics` / `serve-metrics`）
- [x] SLA 指标（queue lag / step latency p95,p99 / dead growth）
- [x] Gossip 收敛指标（`relaymesh_anti_entropy_merge_total` / `relaymesh_gossip_convergence_seconds`）
- [x] Web 写兼容指标（`relaymesh_web_write_get_compat_total`）
- [x] Web 写限流指标（`relaymesh_web_write_rate_limited_total`）
- [x] 审计日志（JSONL）
- [x] 审计查询（`audit-query`）

## 4. 质量保障

- [x] JUnit 测试（`mvn test`）
- [x] 端到端冒烟脚本（`scripts/p8_smoke.ps1`）
- [x] CI 流水线（`.github/workflows/ci.yml`）

## 5. 运维可执行命令

- 初始化：`mvn -q exec:java "-Dexec.args=--root data init"`
- 启动 worker：`mvn -q exec:java "-Dexec.args=--root data worker --node-id node-a"`
- 启动控制台：`mvn -q exec:java "-Dexec.args=--root=data serve-web --port 8080 --ro-token relay_ro --rw-token relay_rw"`
- 查看指标：`mvn -q exec:java "-Dexec.args=--root=data metrics"`
- 健康检查：`mvn -q exec:java "-Dexec.args=--root data health"`

## 6. 结论

RelayMesh v1 已满足单节点生产可运行形态（调度、重试、回收、观测、安全、自动化验证闭环）。

边界说明：

- v2 去中心化 Mesh（强一致 ownership、跨节点高可用）属于后续演进，不属于 v1 单节点生产就绪范围。
