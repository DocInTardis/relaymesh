# RelayMesh 下一批可落地功能（建议顺序）

更新日期：`2026-02-27`

> 说明：P0-P8 的既定主线已落地完成。这里给出下一阶段（P9+）建议，优先级按“收益 / 风险 / 实施成本”综合排序。

## P9（优先）

1. [ ] 复制链路一致性校验工具
- 目标：为 `replication-export/import` 增加哈希对账与差异报告
- 价值：快速定位跨节点状态漂移
- 输出：`replication-verify` 命令 + 报告文件

2. [ ] Worker 并发执行池（可控上限）
- 目标：在单节点内支持 `N` 并发 step 执行
- 价值：提高吞吐，保持 lease/fencing 语义不变
- 输出：`worker --parallelism` + 指标

3. [ ] 审计日志压缩与归档策略
- 目标：自动滚动 `audit.log`，支持按天压缩
- 价值：降低磁盘压力，便于长期保留
- 输出：归档目录 + 查询兼容层

## P10（高价值）

1. [ ] Node RPC 端到端证书轮换演练自动化
- 目标：把当前手工轮换步骤固化为一键脚本
- 价值：降低运维门槛，避免证书过期风险
- 输出：`scripts/v2_node_rpc_rotation_drill.ps1`

2. [ ] 多租户配额与隔离策略
- 目标：按 namespace 配置吞吐、并发、存储配额
- 价值：减少租户间资源争抢
- 输出：`relaymesh-settings.json` 新增 namespace 配额项

3. [ ] Dead Letter 智能分流
- 目标：按失败原因自动分层（可重试 / 人工介入 / 安全风险）
- 价值：提升回放效率与处理优先级
- 输出：`dead-letter-classifier` + 报表

## P11（中期）

1. [ ] Web 控制台任务操作审计增强
- 目标：前端操作关联到审计事件链路（含 trace）
- 价值：线上问题可追责、可回放

2. [ ] 指标看板模板与告警规则基线
- 目标：提供 Grafana Dashboard + Prometheus Alert Rules
- 价值：开箱即用的可观测体系

3. [ ] 端到端基准压测回归（Nightly）
- 目标：将 Phase A/B/C 的关键场景接入定时 CI
- 价值：提前暴露性能与稳定性回退

## P12（长期）

1. [ ] 跨地域 Mesh 实验
- 目标：验证高延迟网络下 gossip 与复制策略
- 风险：时钟漂移与带宽波动对收敛时间影响较大

2. [ ] 存储分层（冷热数据）
- 目标：降低主库压力，提升历史查询能力

3. [ ] Operator 化部署（K8s）
- 目标：标准化集群部署、升级、回滚和证书管理

## 与现有路线的关系

- 已完成历史：`docs/IMPLEMENTATION_BACKLOG.md`
- Endgame 总路线：`docs/ENDGAME_BACKLOG.md`
- 架构约束参考：`docs/RELAYMESH_FINAL_ARCHITECTURE.md`

## 建议先做的 3 件事

1. 先做 `replication-verify`，因为它能直接提升多节点可维护性。
2. 再做 `worker --parallelism`，用真实吞吐收益换取复杂度。
3. 最后做审计归档，控制长期运行成本。