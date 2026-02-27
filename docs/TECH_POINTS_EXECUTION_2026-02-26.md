# RelayMesh Top10 技术点执行对齐（2026-02-26）

来源文档：`relaymesh_可引入技术点_实用性优先_2026-02-26.md`

## 1) 企业级身份与授权（OIDC + RBAC + Service Account）
- 已落地最小可用 RBAC 与 Service Account 模型（token -> principal -> role）。
- `serve-web` 新增 `--auth-file`，支持 principal 级角色与可访问 namespace。
- 兼容旧 `--ro-token/--rw-token`，自动映射为 legacy service account。
- Web 写接口审计统一包含 `principal_id/principal_role/principal_service_account`。

## 2) 真正多租户隔离（namespace 落库 + 查询隔离 + 配额）
- CLI 根参数新增 `--namespace`，默认 `default`。
- 运行目录按 namespace 隔离：非 default 使用 `<root>/namespaces/<namespace>`。
- `tasks/steps/messages/idempotency/step_dependencies` 已扩展 `namespace` 列并自动补齐。
- 复制导入/导出 envelope 新增 `namespace`，导入时强校验 namespace 一致。

## 3) 持续复制控制器（自动 pull/push + peer 管理）
- 新增命令 `replication-controller`。
- 支持 `--peers/--peers-file`，按 state 文件维护 peer pull/push cursor。
- 支持持续运行或 `--once` 单次，支持 push 开关，输出每轮结果与错误明细。

## 4) 灾备自动化（定时快照、异地对象存储、恢复演练）
- 新增命令 `dr-automation`。
- 自动执行：快照导出 -> 异地副本目录复制 -> 可选恢复演练（`--drill`）。
- 默认 DR 根目录在 `<root>/dr/<namespace>/`，确保快照路径不落在 runtime 根内。
- 每轮写出最新演练报告 JSON。

## 5) SLO 告警体系（Burn Rate + Alertmanager 路由）
- 新增命令 `slo-evaluate`。
- 基于当前 stats 执行 burn-rate 判定（queue lag / p95 / p99 / dead growth / replication lag）。
- 输出最新告警快照到 `<root>/alerts/slo-alerts-latest.json`（可自定义 `--out`）。
- 增加指标：`relaymesh_slo_alert_fire_total`。

## 6) OpenTelemetry 分布式追踪（trace exporter）
- 审计事件同步导出 OTel 结构化 span 到 `trace/otel-spans.jsonl`。
- Span 带 `service.name/service.namespace/relaymesh.namespace` 资源属性。

## 7) 数据安全治理（payload 加密落盘 + 密钥轮换 + 脱敏）
- payload 采用 AES-GCM 加密落盘（默认启用，自动初始化 keyring）。
- 新增命令：
  - `payload-key-status`
  - `payload-key-rotate`
- 审计 details 引入敏感字段掩码（token/secret/password/api key 等）。

## 8) 资源准入与背压（配额、并发门限、动态限流）
- 提交路径增加 admission gate（submit / submit-workflow）：
  - 队列深度上限
  - 运行中 step 上限
  - 每分钟 submit 速率上限
- 拒绝会记审计 `admission.reject` 并累计指标 `relaymesh_submit_rejected_total`。

## 9) 数据库迁移治理（Flyway/Liquibase 类能力）
- 增加 `schema_migrations` 版本记录表与自动 migration apply（init 时执行）。
- 新增命令：
  - `schema-migrations`（查看版本历史）
  - `schema-rollback --version`（已支持安全子集回滚）

## 10) 防篡改审计（哈希链/签名 + SIEM 出口）
- 审计行新增 `prev_hash/hash/signature`（HMAC 签名），形成链式防篡改。
- 新增命令：
  - `audit-verify`（完整性验证）
  - `audit-siem-export`（导出 SIEM NDJSON）

## 新增核心命令索引
- `replication-controller`
- `dr-automation`
- `slo-evaluate`
- `payload-key-status`
- `payload-key-rotate`
- `audit-verify`
- `audit-siem-export`
- `schema-migrations`
- `schema-rollback`
