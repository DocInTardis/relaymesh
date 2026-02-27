# RelayMesh 系统使用手册（重写版）

更新时间：2026-02-27

本文是面向实际落地的操作手册，覆盖从首次启动、单终端并行多 Agent、多项目隔离、调试排障到运维巡检的完整流程。

## 1. 目标与核心概念

RelayMesh 是一个可本地运行的多 Agent 任务编排与执行系统，核心能力：

- 任务提交与状态追踪（SQLite 持久化）
- Worker 并行消费任务队列
- 多项目隔离（通过 `namespace`）
- Web 控制台与指标暴露（Prometheus）
- Control Room 多窗口联动视图（共享快照 + 快捷键切屏）
- 失败重试、死信、回放
- Mesh 成员状态、复制与恢复能力

你最关心的目标场景：

- 一键启动后，在同一个终端里统一调度
- 随时拉起多个 agent/worker
- 多个项目并行处理，或同一个项目内并行处理
- 在浏览器里像“监控室”一样观察多个窗口，并用快捷键切换焦点

## 2. 环境准备

## 2.1 依赖

- JDK 17+
- Maven 3.9+
- Windows PowerShell（本文命令以 PowerShell 为主）

## 2.2 快速自检

```powershell
mvn test
```

如果测试通过，说明本机环境可运行。

## 3. 一键进入单终端调度模式（推荐）

仓库根目录执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/agent_hub.ps1 -AutoWorkers 0 -AutoTopology dual
```

这条命令会：

- 初始化运行目录（默认在 `tmp/agent-hub-root-<timestamp>`）
- 启动 `dual` 拓扑（`project-a` + `project-b`）
- 每个项目拉起 `echo`/`fail` 两类 worker
- 进入交互终端 `hub[default]>`

你也可以不指定拓扑，进入后手动调度：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/agent_hub.ps1
```

## 4. Hub 终端常用命令

## 4.1 项目（namespace）管理

```text
project create <namespace>
project use <namespace>
project list
```

## 4.2 Worker/Agent 并行管理

```text
worker start <name> [namespace] [agent-hint]
worker stop <name|all>
worker list
```

说明：

- `name` 是 worker 实例名
- `namespace` 用于项目隔离
- `agent-hint` 是路由标签，可配合 `submith` 快速投递

## 4.3 拓扑预设（最省事）

```text
topology up <team-a|team-b|one-project|dual>
topology down <preset|all>
topology list
```

预设含义：

- `team-a`：`project-a` 双 worker
- `team-b`：`project-b` 双 worker
- `one-project`：`project-main` 多 worker 并行
- `dual`：`team-a + team-b`

## 4.4 任务提交与查询

```text
submit <agent> <priority> <text>
submitns <namespace> <agent> <priority> <text>
submith <agent-hint> <priority> <text>
tasks [status] [limit]
tasksns <namespace> [status] [limit]
task <taskId>
taskns <namespace> <taskId>
replay <taskId>
replayns <namespace> <taskId>
```

优先级可选：`high` / `normal` / `low`

## 4.5 服务与日志

```text
service start web [port]
service start metrics [port]
service stop <web|metrics|all>
service list
open <web|metrics>
tail <worker:<name>|service:web|service:metrics> [out|err] [lines]
```

## 5. 你要的两类并行场景

## 5.1 一个终端管理多个项目

```text
topology up dual
submitns project-a echo high build-a
submitns project-b echo normal build-b
tasksns project-a
tasksns project-b
```

如果想按标签快速投递：

```text
submith echo normal quick-job
```

`submith` 会找一个带 `agent-hint=echo` 的 worker，对应 namespace 投递任务。

## 5.2 一个项目内并行多个 Agent/Worker

```text
topology down all
topology up one-project
submitns project-main echo normal job-1
submitns project-main echo high job-2
submitns project-main fail low job-3
tasksns project-main
```

核心点：

- 同 namespace 多 worker 提升并发吞吐
- `agent` 字段决定任务由哪类逻辑执行

## 6. 纯 CLI 方式（不进 Hub 也能跑）

## 6.1 初始化运行目录

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root init"
```

## 6.2 提交任务

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root --namespace project-a submit --agent echo --priority normal --input hello"
```

## 6.3 启动 worker（长循环）

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root --namespace project-a worker --worker-id worker-a1 --node-id node-a1 --interval-ms 1000 --maintenance"
```

## 6.4 查询任务

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root --namespace project-a tasks --limit 20"
```

## 7. 可视化与指标

## 7.1 启动 Web

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root serve-web --port 18080 --ro-token relay_ro --rw-token relay_rw"
```

经典控制台：`http://127.0.0.1:18080/?token=relay_ro`

## 7.2 Control Room（多屏控制室）

入口：`http://127.0.0.1:18080/control-room?token=relay_ro`

Control Room 默认提供 4 个面板，并且由同一次后端快照统一驱动。你可以：

- 给每个面板独立选择 `namespace`
- 切换视图类型：`tasks` / `dead` / `conflicts` / `members` / `stats`
- 对任务面板设置 `status` 与 `limit`
- 修改自动刷新间隔（1-60 秒）

快捷键：

- `Alt+1..9`：聚焦指定面板
- `Tab` / `Shift+Tab`：按顺序切换面板焦点
- `Ctrl+R`：立即刷新快照

相关 API（调试时可直接调用）：

- `GET /api/namespaces`
- `GET /api/control-room/snapshot?namespaces=all&taskLimit=30&deadLimit=30&conflictLimit=20`

## 7.3 启动 Metrics

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/manual-root serve-metrics --port 19090"
```

访问：`http://127.0.0.1:19090/metrics`

## 8. 运行目录说明

当你指定 `--root <dir>` 时，主要目录如下：

- `<root>/relaymesh.db`：主数据库
- `<root>/inbox`：待处理队列
- `<root>/processing`：处理中
- `<root>/done`：完成元数据
- `<root>/dead`：死信元数据
- `<root>/payload`：任务负载
- `<root>/audit/audit.log`：审计日志
- `<root>/reports`：报告输出
- `<root>/relaymesh-settings.json`：可热更新配置

## 9. 调试与排障

## 9.1 先看状态

在 Hub 终端执行：

```text
status
worker list
service list
```

## 9.2 追日志

```text
tail worker:a-echo-1 out 100
tail worker:a-fail-1 err 100
tail service:web out 80
```

## 9.3 任务异常排查顺序

1. `taskns <namespace> <taskId>` 看 `status`、`lastError`
2. `tasksns <namespace> RETRYING 20` 看是否持续重试
3. 检查 `audit.log` 与 worker 错误日志
4. 需要时执行 `replayns <namespace> <taskId>`

## 10. 关键运维命令（CLI）

```powershell
mvn -q exec:java "-Dexec.args=--root <root> --namespace <ns> stats"
mvn -q exec:java "-Dexec.args=--root <root> --namespace <ns> health"
mvn -q exec:java "-Dexec.args=--root <root> --namespace <ns> lease-conflicts --since-hours 24 --limit 50"
mvn -q exec:java "-Dexec.args=--root <root> --namespace <ns> mesh-summary"
mvn -q exec:java "-Dexec.args=--root <root> --namespace <ns> replay-batch --status DEAD_LETTER --limit 20"
```

## 11. 常见问题

## 11.1 为什么任务卡在 `RETRYING`？

常见原因：

- Agent 逻辑持续失败
- Payload 解密失败（密钥不匹配）
- 外部依赖不可用

先看 `taskns` 的 `lastError`，再看 worker `err` 日志。

## 11.2 为什么 `submith` 找不到 worker？

因为当前没有带对应 `agent-hint` 的 worker。先启动：

```text
worker start my-worker project-a echo
submith echo normal hello
```

## 11.3 如何彻底清空 Hub 拉起的 worker？

```text
topology down all
worker stop all
```

## 12. 推荐阅读顺序

1. `README.md`
2. `docs/guides/AGENT_HUB_QUICKSTART.md`
3. `docs/guides/READING_GUIDE.md`
4. `docs/RELAYMESH_FINAL_ARCHITECTURE.md`
5. `docs/PRODUCTION_READINESS.md`

如果你主要目标是“单终端并行调度”，先只看第 3、4、5 章即可上手。

## 13. RelayMesh Studio 增强模式（终端套终端）

如果你通过桌面快捷方式启动（Windows Terminal Profile: `RelayMesh Studio`），建议按下面路径使用：

1. 启动后先看 `panel`（紧凑操作面板）
2. 用 `palette <keyword>` 像命令面板一样搜索命令
3. 用 `alias set` 定义高频命令别名
4. 用 `template add/run` 快速提交标准任务
5. 用 `workspace save/launch` 打开新的“并行终端会话”
6. 用 `monitor watch all 2 10` 做实时状态巡检

常用命令：

```text
palette [query]
alias set ls status
template add smoke echo normal smoke-check
template run smoke project-a
workspace save dev dual
workspace launch dev
monitor watch all 2 10
session show
```

说明：

- `workspace launch` 会打开新的 Windows Terminal，会话参数从保存的 profile 读取。
- `session` 会持久化 root/namespace/topology/alias/template，便于下次恢复。
- `session clear` 可清掉自动恢复状态，重新从干净会话启动。
- 建议把 Hub 作为“操作终端”，把 `/control-room` 作为“监控终端”并排使用。
