# RelayMesh Control Room 全自动执行文档

更新时间：2026-02-27

本文档用于约束“全自动模式”执行顺序。目标是把当前系统从单页控制台升级为更接近软件形态的“监控室/多屏控制室”，并且保持 CLI 与 Hub 兼容。

## 1. 目标定义

一键启动后，用户可以进入一个控制室界面，满足以下能力：

- 同屏显示多个面板（可理解为多窗口）
- 多面板共享同一份后端快照数据源
- 快捷键快速切换焦点窗口（`Alt+1..9`、`Tab`、`Shift+Tab`）
- 每个窗口可独立查看不同 namespace 或不同视图
- 仍可回落到原有 Hub 与 CLI 工作流

## 2. 执行原则

- 不破坏现有命令兼容性（`serve-web`、`agent_hub.ps1`、核心 CLI）
- 优先增量改造，避免一次性大规模重写
- 每一步均可编译通过，先可用再增强
- 改动必须配套文档，保证可交接

## 3. 分阶段任务清单

## 3.1 Phase A：后端能力扩展（serve-web）

- 新增路由：
  - `GET /control-room`：控制室 UI 页面
  - `GET /api/namespaces`：返回可见 namespace 列表
  - `GET /api/control-room/snapshot`：返回多 namespace 聚合快照
- 聚合快照包含：
  - `stats`
  - `members`
  - `tasks`
  - `dead`
  - `conflicts`
- 鉴权约束：
  - 复用现有 token 模型
  - 对每个请求的目标 namespace 做权限过滤
  - 未授权 namespace 返回 403，不隐式放行

验收标准：

- `/` 原控制台可继续访问
- `/control-room` 能访问
- `/api/control-room/snapshot` 能返回结构化 JSON

## 3.2 Phase B：控制室前端 MVP

- 提供 4 面板以上布局
- 面板可独立设置：
  - namespace
  - 视图类型（tasks/dead/conflicts/members/stats）
  - limit / status（按需）
- 快捷键：
  - `Alt+1..9` 聚焦面板
  - `Tab` 与 `Shift+Tab` 循环切换
  - `Ctrl+R` 手动刷新
- 单次请求获取共享快照，再由前端分发到各面板渲染

验收标准：

- 不打开开发者工具时也可通过键盘完成窗口切换
- 多面板切换后渲染稳定，不出现整页崩溃

## 3.3 Phase C：文档与阅读路线

- 重写并覆盖系统手册中的 Web 章节，加入控制室操作流
- 更新阅读指南：增加“从控制室入口反向追代码”的阅读路线
- 明确与 `agent_hub.ps1` 的协同关系（Hub 管理进程，Control Room 做可视化观察与轻运维）

验收标准：

- 新用户按手册可在 10 分钟内跑通最小闭环

## 3.4 Phase D：验证与交付

- 编译验证：
  - `mvn -q -DskipTests compile`
- 运行验证：
  - `serve-web` 启动成功
  - `/` 与 `/control-room` 都可访问
  - 聚合快照 API 返回正常
- 版本交付：
  - 提交 commit
  - 推送到 `origin/main`

验收标准：

- 无编译错误
- 关键路径可手工验证

## 4. 后续迭代（下一轮自动化方向）

以下属于下一阶段，不阻塞本轮交付：

- 控制室布局持久化（每个操作者自定义保存）
- WebSocket/SSE 全量事件驱动，降低轮询成本
- 窗口预设模板（排障视图、生产巡检视图、压测视图）
- 与 Hub 深度互通（点击面板直接触发 Hub 命令）
- 桌面壳（JavaFX/Tauri/Electron）封装为完整桌面软件
