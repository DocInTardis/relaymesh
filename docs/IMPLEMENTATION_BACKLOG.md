# RelayMesh 瀹炴柦娓呭崟锛堟寔缁帹杩涳級

鏇存柊鏃堕棿: `2026-02-24`

## 宸插畬鎴愶紙M1 鍩虹嚎锛?
- [x] Java 17 + Maven 椤圭洰楠ㄦ灦
- [x] SQLite 鍒濆鍖栦笌琛ㄧ粨鏋勶紙`tasks/steps/messages/idempotency`锛?- [x] SQLite PRAGMA 璁剧疆涓庡洖璇绘牎楠岋紙fail-fast锛?- [x] 鏂囦欢鎬荤嚎鐩綍缁撴瀯锛坄inbox/high|normal|low`銆乣processing`銆乣done`銆乣dead`锛?- [x] 浠诲姟鎻愪氦娴佺▼锛堝惈骞傜瓑閿幓閲嶏級
- [x] Worker 鍗曡疆鎵ц娴佺▼锛坈laim -> execute -> 鎸佷箙鍖?-> done/dead锛?- [x] 鍐呯疆 `echo` Agent 涓?Agent Registry
- [x] CLI 鍛戒护锛坄init` / `agents` / `submit` / `worker` / `task`锛?- [x] 鍚姩鏃跺箓绛夎褰?TTL 娓呯悊锛? 澶╋級

## 寰呭畬鎴愶紙M2 鍙潬鎬э級

- [x] 閲嶈瘯璋冨害鍣紙鎸囨暟閫€閬?+ jitter + `max_attempts`锛?- [x] 姝讳俊閲嶆斁鍛戒护锛坄replay <taskId>`锛?- [x] 姝讳俊杞偍鍛戒护锛坄dead-export --out` 瀵煎嚭 dead 鍏冩暟鎹級
- [x] lease 蹇冭烦涓?reclaim锛堝崟璋冩椂閽熼┍鍔?+ wall fallback锛?- [x] Windows 涓撶敤浼橀泤閫€鍑洪€昏緫锛坄graceful_timeout` 榛樿 5s锛?- [x] `relaymesh purge`锛坉one/dead 鎸変繚鐣欑瓥鐣ユ竻鐞嗭級
- [x] payload 澶у皬闄愬埗鎷︽埅锛堥粯璁?10MB锛?
## 寰呭畬鎴愶紙M3 宸ヤ綔娴佷笌瑙傛祴锛?
- [x] DAG/閾惧紡宸ヤ綔娴佺紪鎺掞紙`submit-workflow --file`锛?- [x] step 绾т緷璧栦笌鐘舵€佹帹杩涳紙鍚庣户 step 鑷姩瑙ｉ攣鍏ラ槦锛?- [x] 鍩虹杩愯缁熻锛坄stats` 鍛戒护锛?- [x] Trace 浼犳挱锛坄trace_id/span_id/traceparent` 璐┛ step 鎵ц锛?- [x] Prometheus 鎸囨爣鏆撮湶锛坄metrics` + `serve-metrics`锛?- [x] 瀹¤鏃ュ織鏍囧噯鍖栵紙JSONL: `action/actor/resource/result/...`锛?
## 寰呭畬鎴愶紙M4 鍘讳腑蹇冨寲瀹為獙锛?
- [x] membership 鍘熷瀷锛堣妭鐐瑰績璺?+ ALIVE/DEAD 鐘舵€侊級
- [x] ownership 杩佺Щ涓?CAS 鍐茬獊鎺у埗锛坙ease_token + status 鏉′欢鏇存柊锛?- [x] 鑺傜偣鏁呴殰鍥炴敹楠岃瘉锛堝弻鑺傜偣婕旂ず鏂囨。锛歚docs/M4_FAILOVER_DEMO.md`锛?
## 澧為噺鍔熻兘锛堝凡钀藉湴锛?
- [x] 浠诲姟鍒楄〃鏌ヨ锛坄tasks --status --limit --offset`锛?- [x] 宸ヤ綔娴佽鎯呮煡璇紙`workflow <taskId>`锛屽惈 step 渚濊禆锛?- [x] 浠诲姟鍙栨秷锛坄cancel <taskId> --reason`锛?- [x] 浠诲姟鍙栨秷妯″紡锛坄cancel <taskId> --mode hard|soft --reason`锛?- [x] 浠诲姟蹇収瀵煎嚭锛坄task-export <taskId> --out`锛?- [x] 瀹¤鏃ュ織灏鹃儴鏌ョ湅锛坄audit-tail --lines`锛?- [x] 瀹¤鏃ュ織缁撴瀯鍖栨煡璇紙`audit-query --task-id --action --from --to --limit`锛?- [x] 鎵归噺姝讳俊閲嶆斁锛坄replay-batch --status DEAD_LETTER --limit 100`锛?- [x] Script Agent Adapter锛坄agents/scripts.json` 鐭敓鍛藉懆鏈熻剼鏈?Agent锛?- [x] 鎸?Agent 瑕嗙洊閲嶈瘯绛栫暐锛坄agents/retry-policies.json`锛?- [x] SLA 鎸囨爣锛坬ueue lag銆乻tep latency p95/p99銆乨ead letter growth锛?- [x] UDP Gossip membership 鍘熷瀷锛坄gossip --node-id --bind-port --seeds`锛?- [x] Gossip seed 鏂囦欢杈撳叆锛坄gossip --seeds-file <file>`锛?- [x] ownership 绔炰簤鍙鍖栵紙`lease_conflicts` 鎸佷箙鍖?+ `lease-conflicts` 鍛戒护 + 鎸囨爣锛?- [x] 鏈€灏?Web 鎺у埗鍙帮紙`serve-web`锛屽惈 tasks/workflow/dead/metrics API锛?- [x] Web 鎺у埗鍙版搷浣?API锛坈ancel / replay / replay-batch锛?- [x] Web 鎺у埗鍙板璁℃煡璇?API锛坄/api/audit-query`锛?- [x] Gossip 鐘舵€佹満澧炲己锛坄ALIVE/SUSPECT/DEAD`锛?- [x] Web SSE 瀹炴椂鎺ㄩ€侊紙`/events`锛?- [x] 鑷姩鍐茬獊鑱氬悎鎶ュ憡锛堟瘡灏忔椂 `reports/lease-conflicts-*.json` + `lease-report`锛?- [x] 鍋ュ悍妫€鏌ワ紙`health`锛?- [x] Gossip 澶氳烦浼犳挱浼樺寲锛堥殢鏈?fanout + ttl + 鍘婚噸缂撳瓨锛?- [x] Web 閴存潈闂幆锛坄serve-web --ro-token --rw-token`锛岃鍐欒鑹插垎绂伙級
- [x] 鍥炴敹绛栫暐鍙厤缃寲锛坄relaymesh-settings.json` 瑕嗙洊 lease/retry/suspect/dead 闃堝€硷級
- [x] Gossip 鍙嶇喌鍚屾锛坄gossip-sync` + packet 蹇収鑺傜偣浜ゆ崲锛?- [x] Web 閴存潈澧炲己锛堝啓鎺ュ彛鏀寔 POST锛宼oken 杞崲 `--ro-token-next/--rw-token-next`锛?- [x] 閰嶇疆鐑噸杞斤紙`reload-settings` + worker `--settings-reload-ms` 鍛ㄦ湡妫€鏌ワ級
- [x] Gossip 鏀舵暃鎸囨爣锛坄relaymesh_anti_entropy_merge_total` + `relaymesh_gossip_convergence_seconds`锛?- [x] Web 鍐欐帴鍙ｅ畨鍏ㄥ熀绾匡紙榛樿 POST-only锛宍--allow-get-writes` 鍙€夊吋瀹癸級
- [x] Web 鍐欐搷浣滄潵婧愬璁★紙method/route/remote_addr/user_agent/x_forwarded_for/origin/referer锛?- [x] 閰嶇疆鍙樻洿瀛楁绾у璁★紙`SettingsReloadOutcome.changedFields` + `runtime.settings.load.changed_fields`锛?- [x] GET 鍐欏吋瀹瑰純鐢ㄨ娴嬶紙`web.write.get_compat` 瀹¤ + `relaymesh_web_write_get_compat_total` 鎸囨爣锛?- [x] Web 鍐欐帴鍙ｉ€熺巼闄愬埗锛坄serve-web --write-rate-limit-per-min` + `429/Retry-After`锛?- [x] Web 鍐欓檺娴佽娴嬶紙`web.write.rate_limited` 瀹¤ + `relaymesh_web_write_rate_limited_total` 鎸囨爣锛?- [x] 閰嶇疆鍙樻洿鍘嗗彶鍥炴斁锛坄settings-history --limit N`锛?- [x] P8 绔埌绔啋鐑熻剼鏈紙`scripts/p8_smoke.ps1`锛?- [x] JUnit 鑷姩鍖栨祴璇曞熀绾匡紙`mvn test`锛?- [x] CI 宸ヤ綔娴侊紙`.github/workflows/ci.yml`锛歀inux build+test + Windows smoke锛?- [x] v2 dead owner 蹇€熷洖鏀讹紙`mesh-recover` + maintenance 鑷姩鍥炴敹锛?- [x] v2 ownership 鍥炴敹鎸囨爣锛坄relaymesh_dead_owner_reclaimed_total`锛?- [x] v2 ownership 鍥炴敹鍐掔儫鑴氭湰锛坄scripts/v2_mesh_smoke.ps1`锛?- [x] v2 ownership 浜嬩欢鏃堕棿绾匡紙`ownership-events --since-hours --limit`锛?- [x] v2 鑺傜偣鎶栧姩淇濇姢锛坄suspectRecoverMinMs` / `deadRecoverMinMs` + 鎭㈠鎶戝埗璁℃暟锛?- [x] v2 鑺傜偣鎶栧姩鍥炲綊鑴氭湰锛坄scripts/v2_jitter_smoke.ps1`锛?- [x] 鐙珛缁存姢鍛戒护锛坄maintenance --node-id`锛屼究浜庤繍缁村拰鍥炲綊锛?- [x] stale DEAD 鎴愬憳娓呯悊锛坄mesh-prune --older-than-ms --limit`锛?- [x] stale DEAD 娓呯悊瑙傛祴鎸囨爣锛坄relaymesh_mesh_nodes_pruned_total`锛?- [x] maintenance 鑷姩娓呯悊 stale DEAD锛坄meshPruneOlderThanMs/meshPruneLimit/meshPruneIntervalMs`锛?- [x] v2 prune 鍐掔儫鑴氭湰锛坄scripts/v2_prune_smoke.ps1`锛?- [x] mesh 椋庨櫓姹囨€诲懡浠わ紙`mesh-summary`锛?- [x] mesh 椋庨櫓鎸囨爣锛坄relaymesh_mesh_nodes_total`銆乣relaymesh_running_steps_by_owner_status`銆乣relaymesh_oldest_dead_node_age_ms`锛?- [x] worker 鑺傜偣瀹夊叏妯″紡锛坄pauseWorkerWhenLocalNotAlive`锛屾湰鍦?`SUSPECT/DEAD` 鑷姩鏆傚仠鎷夊彇锛?- [x] worker 鏆傚仠瑙傛祴鎸囨爣锛坄relaymesh_worker_paused_total`锛?- [x] v2 worker pause 鍐掔儫鑴氭湰锛坄scripts/v2_worker_pause_smoke.ps1`锛?- [x] lease epoch fencing锛坄lease_epoch` + token/epoch 鍙岄噸鏍￠獙锛?- [x] lease 鍐茬獊 epoch 瀛楁锛坄expected_lease_epoch` / `actual_lease_epoch`锛?- [x] v2 lease epoch 鍐掔儫鑴氭湰锛坄scripts/v2_lease_epoch_smoke.ps1`锛?
## Latest Additions (2026-02-25)
- [x] Scheduler anti-starvation guard (max_consecutive_high fallback to forced LOW claim)
- [x] Starvation observability metric (relaymesh_low_starvation_count)
- [x] Ownership events epoch regression test (expected_lease_epoch / `actual_lease_epoch`)


- [x] Fairness smoke script + CI hook (`scripts/v2_fairness_smoke.ps1`)

## Endgame Tracking
- Active roadmap moved to `docs/ENDGAME_BACKLOG.md` (E1-E5).
