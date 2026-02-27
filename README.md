# RelayMesh

RelayMesh 鏄竴涓交閲忓 Agent 杩愯鏃讹紝褰撳墠瀹炵幇宸茶鐩?`v1 M1-M4 + P8` 鍙繍琛岄棴鐜細

- 鎻愪氦浠诲姟锛坄submit`锛?- 鏂囦欢鎬荤嚎鎶曢€掞紙`inbox/high|normal|low`锛?- Worker 鎷夊彇鎵ц锛坄worker`锛?- SQLite 鐘舵€佽惤搴擄紙`tasks/steps/messages/idempotency`锛?- 鏌ヨ缁撴灉锛坄task`锛?
## 鏂囨。

- 鏈€缁堟灦鏋勬枃妗ｏ細`docs/RELAYMESH_FINAL_ARCHITECTURE.md`
- 鐢熶骇鍖栧氨缁竻鍗曪細`docs/PRODUCTION_READINESS.md`

## 褰撳墠瀹炵幇锛圡1-M4 + P8 澧炲己锛?
- Java 17 + Maven
- SQLite锛圵AL锛? PRAGMA 鍥炶鏍￠獙锛坒ail-fast锛?- 澶氫紭鍏堢骇鐩綍闃熷垪 + processing/done/dead
- 骞傜瓑閿幓閲嶏紙`idempotency_key`锛?- payload 澶у皬闄愬埗锛堥粯璁?10MB锛?- 閲嶈瘯璋冨害锛堟寚鏁伴€€閬?+ jitter锛変笌姝讳俊钀藉湴
- lease 蹇冭烦涓庤秴鏃跺洖鏀讹紙鍗曡皟鏃堕挓 + wall fallback锛?- DAG/閾惧紡宸ヤ綔娴佹彁浜わ紙`submit-workflow --file`锛?- step 渚濊禆瑙ｉ攣鍚庤嚜鍔ㄥ叆闃熸墽琛?- 鍐呯疆绀轰緥 Agent锛歚echo`銆乣fail`
- Script Agent Adapter锛坄agents/scripts.json`锛岀煭鐢熷懡鍛ㄦ湡鑴氭湰杩涚▼锛?- 鎸?Agent 瑕嗙洊閲嶈瘯绛栫暐锛坄agents/retry-policies.json`锛?- 鍘嗗彶娓呯悊鍛戒护锛歚purge`
- 姝讳俊閲嶆斁鍛戒护锛歚replay`
- 鎵归噺姝讳俊閲嶆斁鍛戒护锛歚replay-batch`
- 姝讳俊瀵煎嚭鍛戒护锛歚dead-export`
- 鍩虹瑙傛祴鍛戒护锛歚stats`
- Prometheus 鎸囨爣杈撳嚭锛歚metrics` / `serve-metrics`
- 瀹¤鏃ュ織锛圝SONL锛夛細`audit/audit.log`
- Mesh 鎴愬憳鍒楄〃锛歚members`
- Mesh 椋庨櫓姹囨€伙細`mesh-summary`锛堣妭鐐圭姸鎬?+ RUNNING ownership 椋庨櫓锛?- 鐙珛缁存姢鍛戒护锛歚maintenance --node-id`锛堟墽琛?membership/lease/retry 缁存姢 tick锛?- UDP Gossip membership 鍘熷瀷锛歚gossip --node-id --bind-port --seeds`
- Gossip 鏀寔 seed 鏂囦欢锛歚gossip --seeds-file seeds.txt`
- Gossip 澶氳烦浼犳挱浼樺寲锛氶殢鏈?`fanout` + `ttl` + 娑堟伅鍘婚噸缂撳瓨
- Gossip 鍙嶇喌鍚屾锛歚gossip-sync`锛堣妭鐐瑰揩鐓т氦鎹紝淇涓㈠寘婕傜Щ锛?- 鎴愬憳鐘舵€佹満锛歚ALIVE -> SUSPECT -> DEAD`锛堣鍒ょ紦鍐诧級
- ownership 绔炰簤鍙鍖栵細`lease-conflicts` + `relaymesh_lease_conflict_*`
- lease fencing 浠ｉ檯锛歚lease_epoch`锛坈laim/heartbeat/commit 鍏ㄩ摼璺牎楠岋級
- dead owner 蹇€熷洖鏀讹細`mesh-recover`锛堟寜 `DEAD` 鑺傜偣 ownership 鐩存帴鍥炴敹 RUNNING step锛?- stale DEAD 鎴愬憳娓呯悊锛歚mesh-prune`锛堟寜骞撮緞绐楀彛鎵归噺娓呯悊鍘嗗彶鑺傜偣锛?- ownership 浜嬩欢鏃堕棿绾匡細`ownership-events`锛堝悎骞?lease 鍐茬獊涓庢仮澶嶄簨浠讹級
- 鑺傜偣鎶栧姩淇濇姢锛歚SUSPECT/DEAD` 鎭㈠鏈€灏忛┗鐣欑獥鍙ｏ紙閬垮厤鐘舵€佹姈鍔級
- Worker 鑺傜偣瀹夊叏妯″紡锛氭湰鍦拌妭鐐逛负 `SUSPECT/DEAD` 鏃舵殏鍋滄帴鍗曪紙鍙厤缃級
- 鍐茬獊鑱氬悎鎶ュ憡锛歚lease-report` + `reports/lease-conflicts-*.json`
- Web 鎺у埗鍙版渶灏忕増锛歚serve-web`锛坱asks/workflow/dead/metrics锛?- Web 鎺у埗鍙版敮鎸佹搷浣滐細cancel / replay / replay-batch / audit-query
- Web 瀹炴椂鎺ㄩ€侊細`/events`锛圫SE snapshot锛?- Web 閴存潈鏈€灏忛棴鐜細`serve-web --ro-token --rw-token`锛堣鍐欒鑹插垎绂伙級
- Web 閴存潈澧炲己锛氭敮鎸?`--ro-token-next/--rw-token-next` 鍙?token 杞崲
- Web 鍐欐帴鍙ｅ畨鍏ㄥ熀绾匡細榛樿 `POST-only`锛屽彲鐢?`--allow-get-writes` 鍏煎 GET
- Web 鍐欐帴鍙ｉ檺娴侊細`--write-rate-limit-per-min`锛堥粯璁?120 娆?鍒嗛挓/鏉ユ簮鍦板潃锛?- Web GET 鍐欏吋瀹瑰懡涓鏁帮細`relaymesh_web_write_get_compat_total`
- Web 鍐欓檺娴佸懡涓鏁帮細`relaymesh_web_write_rate_limited_total`
- Web GET 鍐欏吋瀹瑰純鐢ㄥ璁★細`web.write.get_compat`
- Web 鍐欓檺娴佹嫆缁濆璁★細`web.write.rate_limited`
- Web 鍐欐搷浣滃璁★細璁板綍鏉ユ簮瀛楁锛坢ethod/route/remote_addr/user_agent/x_forwarded_for/origin/referer锛?- 杩愯鍙傛暟閰嶇疆鏂囦欢锛歚<root>/relaymesh-settings.json`锛坙ease/retry/gossip/mesh-prune/worker-safety锛?- 閰嶇疆鐑噸杞斤細`reload-settings` + `worker --settings-reload-ms`
- 閰嶇疆鍙樻洿瀹¤锛歚reload-settings` 杈撳嚭 `changedFields`锛堜粎瀛楁鍚嶏紝涓嶅洖鏄炬晱鎰熷€硷級
- 閰嶇疆鍙樻洿鍘嗗彶鍥炴斁锛歚settings-history --limit N`
- Worker 缁存姢鍛ㄦ湡浼氳嚜鍔ㄦ寜灏忔椂杈撳嚭鍐茬獊鎶ュ憡鍒?`reports/lease-conflicts-YYYYMMDDHH.json`
- 浠诲姟鍒楄〃涓庡伐浣滄祦璇︽儏锛歚tasks` / `workflow`
- 浠诲姟鍙栨秷锛歚cancel --mode hard|soft`
- 浠诲姟蹇収瀵煎嚭锛歚task-export`
- 瀹¤鏃ュ織鏌ョ湅锛歚audit-tail`
- 瀹¤鏃ュ織缁撴瀯鍖栨煡璇細`audit-query`
- SLA 鎸囨爣锛歚queueLagMs`銆乣stepLatencyP95Ms`銆乣stepLatencyP99Ms`銆乣deadLetterGrowth1h`
- Gossip 鏀舵暃鎸囨爣锛歚relaymesh_anti_entropy_merge_total`銆乣relaymesh_gossip_convergence_seconds`
- ownership 鍥炴敹鎸囨爣锛歚relaymesh_dead_owner_reclaimed_total`
- 鍋ュ悍妫€鏌ワ細`health`

## 蹇€熷紑濮?
鍦ㄩ」鐩牴鐩綍鎵ц锛?
```powershell
mvn -q -DskipTests package
```

杩愯鑷姩鍖栨祴璇曪細

```powershell
mvn -q test
```

鍒濆鍖栵細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data init"
```

鏌ョ湅鍙敤 Agent锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data agents"
```

娉ㄥ唽鑴氭湰 Agent锛堝彲閫夛紝鏀惧湪 `<root>/agents/scripts.json`锛夛細

```json
{
  "agents": [
    {
      "id": "py-upper",
      "command": ["python", "agents/upper.py"],
      "timeoutMs": 15000
    }
  ]
}
```

鎸?Agent 閰嶇疆閲嶈瘯绛栫暐锛堝彲閫夛紝鏀惧湪 `<root>/agents/retry-policies.json`锛夛細

```json
{
  "agents": [
    {
      "id": "fail",
      "maxAttempts": 1,
      "baseBackoffMs": 1000,
      "maxBackoffMs": 5000
    }
  ]
}
```

杩愯鍙傛暟閰嶇疆锛堝彲閫夛紝鏀惧湪 `<root>/relaymesh-settings.json`锛夛細

```json
{
  "leaseTimeoutMs": 15000,
  "suspectAfterMs": 30000,
  "deadAfterMs": 60000,
  "suspectRecoverMinMs": 5000,
  "deadRecoverMinMs": 30000,
  "pauseWorkerWhenLocalNotAlive": true,
  "maxAttempts": 3,
  "baseBackoffMs": 1000,
  "maxBackoffMs": 60000,
  "retryDispatchLimit": 32,
  "gossipFanout": 3,
  "gossipPacketTtl": 2,
  "gossipSyncSampleSize": 32,
  "gossipDedupWindowMs": 60000,
  "gossipDedupMaxEntries": 4096,
  "meshPruneOlderThanMs": 604800000,
  "meshPruneLimit": 256,
  "meshPruneIntervalMs": 3600000
}
```

鎻愪氦浠诲姟锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data submit --agent echo --input hello-relaymesh --priority normal"
```

鎻愪氦宸ヤ綔娴侊紙DAG锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data submit-workflow --file examples/workflows/simple.json"
```

`examples/workflows/simple.json` 绀轰緥锛?
```json
{
  "steps": [
    { "id": "s1", "agent": "echo", "input": "first", "priority": "normal", "dependsOn": [] },
    { "id": "s2", "agent": "echo", "input": "second", "priority": "normal", "dependsOn": ["s1"] }
  ]
}
```

鎵ц涓€杞?Worker锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data worker --once=true --worker-id worker-a --node-id node-a"
```

璇存槑锛?
- `worker` 榛樿浼氬厛璺戜竴娆＄淮鎶や换鍔★紙lease heartbeat / reclaim / retry dispatch锛夊啀鎷夊彇闃熷垪銆?- 闀垮惊鐜ā寮忎笅锛學indows 榛樿浼橀泤閫€鍑鸿秴鏃朵负 `5s`锛屽叾浠栧钩鍙伴粯璁?`15s`銆?- `--node-id` 浼氬弬涓?membership 蹇冭烦涓庤妭鐐圭姸鎬佺淮鎶わ紝骞朵綔涓?step `lease_owner` 鍐欏叆锛坴2 ownership 杩佺Щ璇箟锛夈€?- `--settings-reload-ms` 鍙湪闀垮惊鐜?worker 涓儹鍔犺浇 `<root>/relaymesh-settings.json`銆?- 褰?`pauseWorkerWhenLocalNotAlive=true` 涓旀湰鍦?`node-id` 涓?`SUSPECT/DEAD` 鏃讹紝worker 浼氭殏鍋滄媺鍙栨墽琛屻€?
鏌ヨ浠诲姟锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data task <task_id>"
```

鍙栨秷浠诲姟锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data cancel <task_id> --reason manual-stop"
```

杞彇娑堬紙浠呴樆姝㈡湭寮€濮?step锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data cancel <task_id> --mode soft --reason graceful-stop"
```

瀵煎嚭浠诲姟蹇収锛堜换鍔?宸ヤ綔娴?瀹¤锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data task-export <task_id> --out tmp/demo-data/task-export.json"
```

鍒楀嚭浠诲姟锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data tasks --status SUCCESS --limit 20 --offset 0"
```

鏌ョ湅宸ヤ綔娴佽鎯咃細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data workflow <task_id>"
```

鏌ョ湅杩愯缁熻锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data stats"
```

鏌ョ湅 mesh 鎴愬憳锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data members"
```

鏌ョ湅 mesh 姹囨€伙紙鐘舵€佽鏁?+ RUNNING step owner 椋庨櫓锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data mesh-summary"
```

鎵ц涓€娆＄淮鎶?tick锛坢embership + lease + retry锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data maintenance --node-id node-a"
```

璇存槑锛?- 鑻?`meshPruneIntervalMs > 0`锛岀淮鎶?tick 浼氭寜鍛ㄦ湡鑷姩娓呯悊瓒呴緞 `DEAD` 鎴愬憳锛坄meshPruneOlderThanMs` / `meshPruneLimit`锛夈€?
鎵ц涓€娆?Gossip membership tick锛堝弻鑺傜偣浜掗厤 seed锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data gossip --node-id node-a --bind-port 17901 --seeds 127.0.0.1:17902 --window-ms 1200"
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data gossip --node-id node-b --bind-port 17902 --seeds 127.0.0.1:17901 --window-ms 1200"
```

鑷畾涔?gossip 澶氳烦浼犳挱鍙傛暟锛堣鐩栭厤缃枃浠堕粯璁ゅ€硷級锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data gossip --node-id node-a --bind-port 17901 --seeds 127.0.0.1:17902,127.0.0.1:17903 --window-ms 1200 --fanout 1 --ttl 3"
```

鎵ц gossip 鍙嶇喌鍚屾锛堝杞揩鐓т氦鎹級锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data gossip-sync --node-id node-a --bind-port 17901 --seeds 127.0.0.1:17902 --window-ms 1200 --rounds 3 --interval-ms 250"
```

鎸?`DEAD` 鑺傜偣 ownership 鎵ц蹇€熷洖鏀讹紙v2 杩佺Щ璺緞锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data mesh-recover --limit 128"
```

娓呯悊瓒呰繃 7 澶╂湭鏇存柊鐨?`DEAD` 鎴愬憳璁板綍锛堥檺鍒跺崟娆?256 鏉★級锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data mesh-prune --older-than-ms 604800000 --limit 256"
```

鏌ョ湅 ownership 鏃堕棿绾夸簨浠讹紙鍐茬獊 + 鍥炴敹锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data ownership-events --since-hours 24 --limit 100"
```

寮哄埗閲嶈浇杩愯鍙傛暟閰嶇疆锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data reload-settings"
```

`reload-settings` 杈撳嚭鍖呭惈 `changedFields`锛屼粎灞曠ず鍙戠敓鍙樺寲鐨勫瓧娈靛悕銆?
鏌ョ湅鏈€杩?N 娆￠厤缃姞杞藉璁℃憳瑕侊細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data settings-history --limit 20"
```

浣跨敤 seed 鏂囦欢鎵ц gossip锛堟瘡琛屼竴涓?`host:port`锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data gossip --node-id node-a --bind-port 17901 --seeds-file tmp/demo-data/seeds.txt --window-ms 1200"
```

鏌ョ湅 ownership / lease CAS 鍐茬獊锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data lease-conflicts --limit 100 --since-hours 24"
```

璇存槑锛歚lease-conflicts` 宸插寘鍚?`expectedLeaseEpoch` / `actualLeaseEpoch` 瀛楁锛屽彲鐢ㄤ簬瀹氫綅 fencing 鎷掔粷鍘熷洜銆?
鐢熸垚鍐茬獊鑱氬悎鎶ュ憡锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data lease-report --hours 24 --limit 5000 --out tmp/demo-data/reports/lease-conflicts-manual.json"
```

杈撳嚭 Prometheus 鏂囨湰鎸囨爣锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data metrics"
```

璇存槑锛?- `relaymesh_web_write_get_compat_total`锛欸ET 鍐欏吋瀹瑰懡涓疮璁★紙鐢ㄤ簬鍘诲吋瀹硅繃娓℃湡瑙傛祴锛?- `relaymesh_web_write_rate_limited_total`锛氬啓鎺ュ彛琚檺娴佹嫆缁濈疮璁?- `relaymesh_dead_owner_reclaimed_total`锛氫粠 `DEAD` owner 鍥炴敹鐨?RUNNING step 绱
- `relaymesh_membership_recovery_suppressed_total`锛氬洜鎶栧姩淇濇姢绐楀彛琚姂鍒剁殑鑺傜偣鎭㈠娆℃暟
- `relaymesh_mesh_nodes_pruned_total`锛氭竻鐞嗗巻鍙?`DEAD` membership 琛岀疮璁?- `relaymesh_mesh_nodes_total{status=...}`锛氭寜鐘舵€佺粺璁?mesh 鎴愬憳鏁?- `relaymesh_running_steps_by_owner_status{owner_status=...}`锛氭寜 owner 鐘舵€佺粺璁?RUNNING steps
- `relaymesh_oldest_dead_node_age_ms`锛氭渶鑰?DEAD 鑺傜偣璁板綍骞撮緞
- `relaymesh_worker_paused_total`锛氬洜鏈湴鑺傜偣闈?ALIVE 琚殏鍋滅殑 worker 杞绱
- `relaymesh_low_starvation_count`: cumulative forced LOW picks by anti-starvation guard
- `relaymesh_cluster_epoch`: cluster fencing epoch for ownership-changing writes

鍚姩 Prometheus 鎶撳彇绔偣锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-metrics --port 9464 --path /metrics"
```

鍚姩鏈€灏?Web 鎺у埗鍙帮細

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-web --port 8080"
```

鍚敤 Web 璇诲啓閴存潈锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-web --port 8080 --ro-token relay_ro --rw-token relay_rw"
```

閰嶇疆鍐欐帴鍙ｉ檺娴侊紙姣忓垎閽?60 娆?鏉ユ簮鍦板潃锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-web --port 8080 --ro-token relay_ro --rw-token relay_rw --write-rate-limit-per-min 60"
```

鍐欐帴鍙ｉ粯璁や粎鍏佽 `POST`锛涘鏋滈渶瑕佸吋瀹规棫鐗?GET 璋冪敤锛?
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-web --port 8080 --ro-token relay_ro --rw-token relay_rw --allow-get-writes"
```

璇存槑锛歚--allow-get-writes` 涓哄吋瀹瑰紑鍏筹紝宸叉爣璁板純鐢紝鍚庣画鐗堟湰璁″垝绉婚櫎 GET 鍐欒兘鍔涳紱鍏煎鍛戒腑浼氳緭鍑烘寚鏍?`relaymesh_web_write_get_compat_total` 涓庡璁′簨浠?`web.write.get_compat`銆?
閴存潈 token 杞崲锛堟柊鏃?token 骞跺瓨杩囨浮锛夛細

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-web --port 8080 --ro-token relay_ro_old --ro-token-next relay_ro_new --rw-token relay_rw_old --rw-token-next relay_rw_new"
```

SSE 瀹炴椂娴侊紙鎺у埗鍙板唴閮ㄤ娇鐢級锛?
```powershell
curl.exe -N http://127.0.0.1:8080/events
```

璇存槑锛?- 涔熷彲鐢?`Authorization: Bearer <token>` 浼犻€?token銆?- 鎺у埗鍙伴〉闈㈡敮鎸?URL 甯?token锛歚http://127.0.0.1:8080/?token=relay_ro`銆?- 鍐欐帴鍙ｈ姹備細鍐欏叆鏉ユ簮瀹¤瀛楁锛坢ethod/route/ip/ua/forwarded/origin/referer锛夈€?- 鍐欐帴鍙ｈ秴鍑洪€熺巼浼氳繑鍥?`429`锛屽苟璁板綍 `web.write.rate_limited` 瀹¤浜嬩欢銆?
Web API 绀轰緥锛?
```powershell
# soft cancel (POST)
Invoke-WebRequest "http://127.0.0.1:8080/api/cancel" `
  -Method Post `
  -ContentType "application/x-www-form-urlencoded" `
  -Body "taskId=<task_id>&mode=soft&reason=web-api&token=relay_rw"

# replay one dead task (POST)
Invoke-WebRequest "http://127.0.0.1:8080/api/replay" `
  -Method Post `
  -ContentType "application/x-www-form-urlencoded" `
  -Body "taskId=<task_id>&token=relay_rw"

# replay batch (POST)
Invoke-WebRequest "http://127.0.0.1:8080/api/replay-batch" `
  -Method Post `
  -ContentType "application/x-www-form-urlencoded" `
  -Body "status=DEAD_LETTER&limit=50&token=relay_rw"

# audit query (read API)
Invoke-WebRequest "http://127.0.0.1:8080/api/audit-query?action=task.cancel&limit=20&token=relay_ro"
```

娓呯悊鍘嗗彶鏂囦欢涓庤繃鏈熷箓绛夎褰曪細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data purge --done-retention-days 7 --dead-retention-days 7 --idempotency-ttl-days 7"
```

閲嶆斁姝讳俊浠诲姟锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data replay <task_id>"
```

鎸夌姸鎬佹壒閲忛噸鏀炬淇′换鍔★細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data replay-batch --status DEAD_LETTER --limit 100"
```

瀵煎嚭姝讳俊鍏冩暟鎹細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data dead-export --out tmp/demo-data/dead-export.json --limit 1000"
```

鏌ョ湅瀹¤鏃ュ織灏鹃儴锛?
```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data audit-tail --lines 50"
```

缁撴瀯鍖栬繃婊ゅ璁℃棩蹇楋細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data audit-query --task-id <task_id> --action task.cancel --from 2026-02-24T00:00:00Z --to 2026-02-25T00:00:00Z --limit 100"
```

杩愯 P8 鍐掔儫鑴氭湰锛?
```powershell
powershell -ExecutionPolicy Bypass -File scripts/p8_smoke.ps1
```

杩愯 v2 ownership 杩佺Щ鍐掔儫鑴氭湰锛?
```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_mesh_smoke.ps1
```

杩愯 v2 鑺傜偣鎶栧姩淇濇姢鍐掔儫鑴氭湰锛?
```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_jitter_smoke.ps1
```

杩愯 v2 stale DEAD 娓呯悊鍐掔儫鑴氭湰锛?
```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_prune_smoke.ps1
```

杩愯 v2 mesh 椋庨櫓姹囨€诲啋鐑熻剼鏈細

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_mesh_summary_smoke.ps1
```

杩愯 v2 worker 鑺傜偣瀹夊叏妯″紡鍐掔儫鑴氭湰锛?
```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_worker_pause_smoke.ps1
```

杩愯 v2 lease epoch fencing 鍐掔儫鑴氭湰锛?
```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_lease_epoch_smoke.ps1
```

鍋ュ悍妫€鏌ワ細

```powershell
mvn -q exec:java "-Dexec.args=--root tmp/demo-data health"
```

## Trace 涓庡璁?
- step 鎵ц鏃朵細鐢熸垚骞朵紶鎾細
  - `traceId`
  - `spanId`
  - `traceParent`锛圵3C `traceparent` 鏍煎紡锛?- 瀹¤鏃ュ織浣嶇疆锛歚<root>/audit/audit.log`
- 瀹¤鏃ュ織瀛楁锛圝SONL锛夛細
  - `timestamp`
  - `action`
  - `actor`
  - `resource`
  - `result`
  - `trace_id`
  - `span_id`
  - `task_id`
  - `step_id`
  - `details`

## 鐩綍缁撴瀯

- `src/main/java/io/relaymesh`锛氭牳蹇冧唬鐮?- `docs/RELAYMESH_FINAL_ARCHITECTURE.md`锛氭渶缁堟灦鏋勮鑼?- `docs/M4_FAILOVER_DEMO.md`锛氬弻鑺傜偣鏁呴殰鍥炴敹婕旂ず姝ラ
- `docs/NEXT_FEATURES.md`锛氫笅涓€鎵瑰彲钀藉湴鍔熻兘娓呭崟
- `scripts/p8_smoke.ps1`锛歅8 绔埌绔啋鐑熼獙璇佽剼鏈?- `scripts/v2_mesh_smoke.ps1`锛歷2 dead owner 鍥炴敹鍐掔儫鑴氭湰
- `scripts/v2_jitter_smoke.ps1`锛歷2 鑺傜偣鎶栧姩淇濇姢鍐掔儫鑴氭湰
- `scripts/v2_prune_smoke.ps1`锛歷2 stale DEAD 娓呯悊鍐掔儫鑴氭湰
- `scripts/v2_mesh_summary_smoke.ps1`锛歷2 mesh 椋庨櫓姹囨€诲啋鐑熻剼鏈?- `scripts/v2_worker_pause_smoke.ps1`锛歷2 worker 鑺傜偣瀹夊叏妯″紡鍐掔儫鑴氭湰
- `scripts/v2_lease_epoch_smoke.ps1`锛歷2 lease epoch fencing 鍐掔儫鑴氭湰
- `.github/workflows/ci.yml`锛欳I锛圠inux build+test + Windows p8/v2/v2-jitter/v2-prune/v2-mesh-summary/v2-worker-pause/v2-lease-epoch smoke锛?- `tmp/demo-data/`锛氳繍琛屾暟鎹洰褰曪紙鍙浛鎹級

## V2 Additional Smoke
```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_fairness_smoke.ps1
```

## Endgame E1 Bootstrap (Cluster State)
```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data cluster-state"
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data cluster-epoch-bump --reason manual-fence --actor ops"
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_cluster_state_smoke.ps1
```

## Endgame E2 Phase A (Replication Envelope)
Export incremental task/step/message deltas to a versioned envelope:

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data replication-export --since-ms 0 --limit 1000 --source-node-id node-a --out tmp/demo-data/replication/envelope.json"
```

Run smoke verification:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_replication_envelope_smoke.ps1
```

Apply exported deltas on another node root:

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data-2 replication-import --in tmp/demo-data/replication/envelope.json --actor sync-loop"
```

Run end-to-end export+import smoke:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_replication_sync_smoke.ps1
```

Reconciliation rule (v2 baseline): rows are applied only when incoming `updatedAtMs` is newer; equal/older rows are skipped deterministically.
When two `RUNNING` step rows have equal timestamp, tie-break uses:
`cluster_epoch` (higher wins) -> `lease_epoch` (higher wins) -> node id lexical order (lower wins).

Partition simulation harness (split + heal loop):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_partition_sim_smoke.ps1
```

Convergence report (from replication import audit):

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data-2 convergence-report --since-hours 24 --limit 200 --out tmp/demo-data-2/reports/convergence.json"
powershell -ExecutionPolicy Bypass -File scripts/v2_convergence_report_smoke.ps1
```

Signed gossip packets (set shared secret in `relaymesh-settings.json`):

```json
{
  "gossipSharedSecret": "replace-with-strong-shared-secret"
}
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_gossip_signing_smoke.ps1
```

Node RPC over TLS / mTLS:

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-node-rpc --bind 127.0.0.1 --port 18443 --keystore tmp/demo-data/tls/node.p12 --keystore-pass changeit --keystore-type PKCS12"
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_node_rpc_tls_smoke.ps1
```

Node RPC certificate rotation + revocation list reload (Phase C):

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data node-rpc-revocation-template --out tmp/demo-data/tls/revocations.txt"
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data serve-node-rpc --bind 127.0.0.1 --port 18443 --keystore tmp/demo-data/tls/node.p12 --keystore-pass changeit --keystore-type PKCS12 --revocation-file tmp/demo-data/tls/revocations.txt --reload-interval-ms 1000"
```

Observe current TLS material:

```powershell
curl.exe -k https://127.0.0.1:18443/node/tls/status
```

Run rotation smoke verification:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_node_rpc_rotation_smoke.ps1
```

Sustained benchmark (E5 Phase A):

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data benchmark-run --tasks 2000 --agent echo --priority normal --input-size 64 --max-seconds 180 --report-out tmp/demo-data/reports/benchmark-phase-a.json"
powershell -ExecutionPolicy Bypass -File scripts/v2_benchmark_phase_a.ps1 -Root tmp/v2-benchmark-phase-a -Tasks 2000 -InputSize 64 -MaxSeconds 180
```

Chaos matrix (E5 Phase B):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_chaos_matrix_phase_b.ps1 -RootPrefix tmp/v2-chaos-matrix
```

Upgrade compatibility + rollback validation (E5 Phase C):

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data snapshot-export --out tmp/snapshots/before-upgrade"
mvn -q exec:java "-Dexec.args=--root=tmp/demo-data snapshot-import --in tmp/snapshots/before-upgrade"
powershell -ExecutionPolicy Bypass -File scripts/v2_upgrade_rollback_phase_c.ps1 -Root tmp/v2-upgrade-rollback-phase-c
```

Repository layout reference:

- `PROJECT_STRUCTURE.md`: repository layout and structure rules.
- `CONTRIBUTING.md`: local development and validation workflow.
- `SECURITY.md`: security reporting process and protected surfaces.
- `CODE_OF_CONDUCT.md`: contribution behavior baseline.
- `SUPPORT.md`: support and issue triage guidance.
- `LICENSE`: project license (MIT).
- `.github/ISSUE_TEMPLATE/`: bug/feature/question templates.
- `.github/pull_request_template.md`: PR checklist.
- `.github/CODEOWNERS`: review ownership map.
- `.github/dependabot.yml`: dependency update policy.
- `examples/workflows/`: workflow JSON examples.
- `tmp/`: local runtime artifacts and smoke outputs (ignored by git).



