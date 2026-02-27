# RelayMesh M4 双节点故障回收演示

本演示验证两件事：

- membership 心跳与节点死亡标记
- 节点故障后任务 ownership 的回收与重调度

## 1. 初始化

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-m4 init"
```

## 2. 让两个节点上线（node-a / node-b）

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-m4 worker --once=true --worker-id wa --node-id node-a"
mvn -q exec:java "-Dexec.args=--root=demo-m4 worker --once=true --worker-id wb --node-id node-b"
mvn -q exec:java "-Dexec.args=--root=demo-m4 members"
```

预期：`node-a`、`node-b` 状态为 `ALIVE`。

## 3. 提交任务并模拟 node-a 崩溃中断

先提交一个任务：

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-m4 submit --agent echo --input failover-demo --priority normal"
```

记下返回的 `taskId`，然后用脚本把该任务 step 强制改成过期的 `RUNNING`（模拟 node-a 崩溃）：

```powershell
@'
import sqlite3, time
db = r'd:/codes/relaymesh/demo-m4/relaymesh.db'
task_id = '替换成上一步 taskId'
old = int(time.time()*1000) - 120000
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("update tasks set status='RUNNING', updated_at_ms=? where task_id=?", (old, task_id))
cur.execute("update steps set status='RUNNING', attempt=1, lease_owner='node-a', lease_token='lease-crashed', updated_at_ms=? where task_id=?", (old, task_id))
conn.commit()
print('simulated crash for', task_id)
'@ | python -
```

## 4. 让 node-b 执行维护 tick 与回收

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-m4 worker --once=true --worker-id wb --node-id node-b"
mvn -q exec:java "-Dexec.args=--root=demo-m4 worker --once=true --worker-id wb --node-id node-b"
```

也可以直接触发 dead owner 快速回收：

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-m4 mesh-recover --limit 128"
```

预期：

- 第一次会回收过期 RUNNING 并安排重试
- 第二次会执行重试消息并将任务推进到 `SUCCESS`

## 5. 查询任务状态

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-m4 task <taskId>"
```

预期：任务最终 `SUCCESS`。

可用 ownership 时间线查看冲突与恢复记录：

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-m4 ownership-events --since-hours 24 --limit 50"
```

## 6. 模拟 node-a 死亡并查看成员状态

可通过把 `node-a.last_heartbeat_ms` 回拨到很久以前，再触发 node-b 一次维护：

```powershell
@'
import sqlite3, time
db = r'd:/codes/relaymesh/demo-m4/relaymesh.db'
old = int(time.time()*1000) - 120000
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("update mesh_nodes set last_heartbeat_ms=? where node_id='node-a'", (old,))
conn.commit()
print('node-a heartbeat moved back')
'@ | python -

mvn -q exec:java "-Dexec.args=--root=demo-m4 worker --once=true --worker-id wb --node-id node-b"
mvn -q exec:java "-Dexec.args=--root=demo-m4 members"
```

预期：`node-a` 会被标记为 `DEAD`，`node-b` 保持 `ALIVE`。
