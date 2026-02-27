# RelayMesh 涓嬩竴鎵瑰彲钀藉湴鍔熻兘锛堝缓璁『搴忥級

鏇存柊鏃堕棿: `2026-02-24`

## P0锛堥珮浠峰€笺€佷綆椋庨櫓锛屼紭鍏堬級

1. [x] 鎵归噺姝讳俊閲嶆斁
- 鍛戒护: `replay-batch --status DEAD_LETTER --limit 100`
- 浠峰€? 闄嶄綆浜哄伐閫愭潯閲嶆斁鎴愭湰
- 褰卞搷闈? `TaskStore` + `runtime` + CLI

2. [x] 宸ヤ綔娴佸彇娑堜紶鎾瓥鐣?- 褰撳墠: 浠诲姟鍙栨秷浼氭妸鍙繍琛?step 缃?`CANCELLED`
- 澧炲己: 鏀寔 `--mode`锛?  - `hard`锛堝叏閲忓彇娑堬級
  - `soft`锛堜粎闃绘鏈紑濮?step锛屼繚鐣欏凡杩愯 step 鏀舵暃锛?- 浠峰€? 鏇磋创杩戠敓浜х瓥鐣?
3. [x] 瀹¤鏌ヨ杩囨护
- 鍛戒护: `audit-query --task-id --action --from --to --limit`
- 浠峰€? 涓嶅啀渚濊禆鏂囨湰 grep锛岄€傚悎闂杩借釜

## P1锛堜腑鏈熷寮猴級

1. [x] Script Agent Adapter
- 鏀寔閫氳繃閰嶇疆娉ㄥ唽鐭敓鍛藉懆鏈熻剼鏈?Agent锛圥ython/Node锛?- 浠峰€? 鐪熸寮傛瀯鎺ュ叆锛屼笉鏀?Java 鏍稿績

2. [x] Retry policy per-agent
- 鏀寔鎸?agent 瑕嗙洊 `max_attempts/base_backoff/max_backoff`
- 浠峰€? 鎻愬崌璋冨害绮剧粏搴?
3. [x] 闃熷垪涓庢墽琛?SLA 鎸囨爣
- 鏂板: queue lag銆乻tep latency p95/p99銆乨ead letter growth
- 浠峰€? 瑙傛祴鑳藉姏浠庘€滅姸鎬佲€濆崌绾у埌鈥滄€ц兘鈥?
## P2锛堣繘闃讹級

1. [x] Gossip membership 鍘熷瀷
- 褰撳墠: SQLite 鏈湴 membership
- 鐩爣: 澧炲姞 UDP gossip 鑺傜偣浜ゆ崲

2. [x] ownership 绔炰簤鍙鍖?- 璁板綍 lease CAS 鍐茬獊娆℃暟涓庡啿绐佸璞?- 杈撳嚭鍐茬獊瀹¤鍜屾寚鏍?
3. [x] Web 鎺у埗鍙版渶灏忕増
- task 鍒楄〃銆亀orkflow 鍥俱€乨ead 鍒楄〃銆乵etrics 蹇

## P3锛堝彲鐢ㄦ€у己鍖栵級

1. [x] Gossip seed 鏂囦欢杈撳叆
- 鍛戒护: `gossip --seeds-file <file>`
- 浠峰€? 澶氳妭鐐瑰惎鍔ㄦ椂涓嶅繀鍦ㄥ懡浠よ纭紪鐮侀暱 seed 鍒楄〃

2. [x] Web 鎺у埗鍙版搷浣?API
- 鏂板: `/api/cancel`銆乣/api/replay`銆乣/api/replay-batch`
- 浠峰€? 浠庡彧璇荤洃鎺у崌绾т负鍙搷浣滄帶鍒跺彴

3. [x] Web 鎺у埗鍙板璁℃煡璇?- 鏂板: `/api/audit-query` + 椤甸潰杩囨护鍏ュ彛
- 浠峰€? 绾夸笂鎺掗殰浠?CLI grep 鍓嶇Щ鍒伴〉闈?
## P4锛堜笅涓€鎵瑰彲缁х画钀藉湴锛?
1. [x] Gossip 鐘舵€佹満澧炲己锛圓LIVE/SUSPECT/DEAD锛?- 澧炲姞 `SUSPECT` 杩囨浮鎬佸拰鎺㈡祴澶辫触闃堝€硷紝闄嶄綆璇垽 DEAD

2. [x] Web 瀹炴椂鍒锋柊閫氶亾
- `serve-web` 澧炲姞 SSE锛圫erver-Sent Events锛夋帹閫?stats/members/conflicts

3. [x] 鑷姩鍖栧啿绐佸洖鏀炬姤鍛?- 姣忓皬鏃剁敓鎴?`lease_conflicts` 鑱氬悎鎶ュ憡锛坱op step/top worker/top event锛?
## P5锛堜笅涓€鎵瑰彲缁х画钀藉湴锛?
1. [x] Gossip 澶氳烦浼犳挱浼樺寲
- 姣忚疆闅忔満 fanout + 鍘婚噸缂撳瓨锛岄檷浣庡箍鎾鏆?
2. [x] Web 閴存潈鏈€灏忛棴鐜?- `serve-web` 澧炲姞 token 鏍￠獙涓庡彧璇?鍙啓瑙掕壊

3. [x] 鍥炴敹绛栫暐鍙厤缃寲
- `suspect/dead` 闃堝€间笌 lease/retry 鍙傛暟鏀寔閰嶇疆鏂囦欢瑕嗙洊

## P6锛堜笅涓€鎵瑰彲缁х画钀藉湴锛?
1. [x] Gossip 鍙嶇喌鍚屾
- 鏂板 `gossip-sync`锛氬懆鏈熸媺鍙?鎺ㄩ€?`mesh_nodes` 蹇収锛屼慨澶嶄涪鍖呭鑷寸殑鎴愬憳瑙嗗浘婕傜Щ

2. [x] Web 閴存潈澧炲己
- 澧炲姞 `POST` 鍐欐帴鍙ｏ紙淇濈暀鐜版湁 GET 鍏煎锛夛紝骞舵敮鎸?API token 杞崲锛堝弻 token 杩囨浮锛?
3. [x] 閰嶇疆鐑噸杞?- `reload-settings` 鍛戒护 + worker 鍛ㄦ湡鐑姞杞斤紝閬垮厤鏀归厤缃悗蹇呴』閲嶅惎杩涚▼

## P7锛堜笅涓€鎵瑰彲缁х画钀藉湴锛?
1. [x] Gossip 鏀舵暃鍙娴?- 澧炲姞 `gossip_convergence_seconds` 涓?`anti_entropy_merge_total` 鎸囨爣锛岄噺鍖栧悓姝ユ晥鏋?
2. [x] Web 瀹夊叏鍩虹嚎寮哄寲
- 鍐欐帴鍙ｉ粯璁よ姹?`POST`锛圙ET 鍐欐搷浣滃彲閰嶇疆鍏抽棴锛夛紝骞跺鍔犺姹傛潵婧愬璁″瓧娈?
3. [x] 閰嶇疆鍙樻洿瀹¤
- `reload-settings` 璁板綍鍓嶅悗 diff锛堝彧璁板綍瀛楁鍚嶏紝涓嶅洖鏄?token 绛夋晱鎰熷€硷級

## P8锛堜笅涓€鎵瑰彲缁х画钀藉湴锛?
1. [x] Web 鍐欐帴鍙ｅ幓鍏煎鍖?- 鎻愪緵 `--allow-get-writes` 寮冪敤鎻愮ず涓庢寚鏍囷紝璁″垝鍦ㄥ悗缁増鏈粯璁ょЩ闄?GET 鍐欒兘鍔?
2. [x] 閰嶇疆鍙樻洿鍥炴斁
- 鏂板 `settings-history` 鏌ヨ鍛戒护锛岃緭鍑烘渶杩?N 娆?`runtime.settings.load` 璁板綍鎽樿

3. [x] 绔埌绔洖褰掕剼鏈?- 澧炲姞 `scripts/p8_smoke.ps1`锛屼竴閿獙璇?web 閴存潈銆乬ossip-sync銆佺儹閲嶈浇涓庢寚鏍囪緭鍑?
## 鐘舵€?
褰撳墠 P0-P8 娓呭崟宸插叏閮ㄨ惤鍦板畬鎴愶紝鍚庣画鍙寜 v2 Mesh 婕旇繘璺嚎缁х画鎵╁睍銆?
## V2锛圡esh 绋冲畾鍖栵紝鎸佺画鎺ㄨ繘锛?
1. [x] dead owner 蹇€熷洖鏀?- 鏂板 `mesh-recover --limit N`锛屽苟鍦ㄧ淮鎶ゅ懆鏈熻嚜鍔ㄦ墽琛?DEAD 鑺傜偣 ownership 鍥炴敹
- 鎸囨爣: `relaymesh_dead_owner_reclaimed_total`

2. [x] ownership 浜嬩欢娴佸鍑?- 鏂板 `ownership-events --since --limit`锛岀敤浜庡洖鏀捐縼绉昏建杩?
3. [x] 鑺傜偣鎶栧姩淇濇姢
- 瀵?`SUSPECT/DEAD` 澧炲姞鏈€灏忛┗鐣欑獥鍙ｄ笌鎭㈠鎶栧姩鎶戝埗
- 閰嶇疆椤? `suspectRecoverMinMs` / `deadRecoverMinMs`
- 鎸囨爣: `relaymesh_membership_recovery_suppressed_total`
- 鍥炲綊鑴氭湰: `scripts/v2_jitter_smoke.ps1`

4. [x] 鐙珛缁存姢鍛戒护
- 鏂板 `maintenance --node-id`锛屽彲鍗曟瑙﹀彂 membership/lease/retry 缁存姢锛屼笉渚濊禆 worker 寰幆

5. [x] v2 鎶栧姩鍥炲綊鑷姩鍖?- 鏂板 `scripts/v2_jitter_smoke.ps1` 骞剁撼鍏?CI Windows smoke

6. [x] stale DEAD 鎴愬憳娓呯悊
- 鏂板 `mesh-prune --older-than-ms --limit`锛屾敮鎸佹寜骞撮緞绐楀彛娓呯悊鍘嗗彶鑺傜偣
- 鎸囨爣: `relaymesh_mesh_nodes_pruned_total`

7. [x] maintenance 鑷姩 prune
- 鏀寔閫氳繃 `meshPruneOlderThanMs/meshPruneLimit/meshPruneIntervalMs` 鍦ㄧ淮鎶ゅ懆鏈熻嚜鍔ㄦ竻鐞嗗巻鍙?DEAD 鑺傜偣

8. [x] v2 prune 鍥炲綊鑷姩鍖?- 鏂板 `scripts/v2_prune_smoke.ps1` 骞剁撼鍏?CI Windows smoke

9. [x] mesh-summary 椋庨櫓瑙嗗浘
- 鏂板 `mesh-summary` 鍛戒护锛堣妭鐐圭姸鎬佽鏁?+ RUNNING ownership 椋庨櫓缁熻锛?- 鎸囨爣: `relaymesh_mesh_nodes_total` / `relaymesh_running_steps_by_owner_status` / `relaymesh_oldest_dead_node_age_ms`

10. [x] worker 鑺傜偣瀹夊叏妯″紡
- 鏂板 `pauseWorkerWhenLocalNotAlive`锛堥粯璁?`true`锛夛紝鏈湴鑺傜偣涓?`SUSPECT/DEAD` 鏃舵殏鍋?worker 鎷夊彇
- 鎸囨爣: `relaymesh_worker_paused_total`
- 鍥炲綊鑴氭湰: `scripts/v2_worker_pause_smoke.ps1`

11. [x] lease epoch fencing
- `steps` 澧炲姞 `lease_epoch`锛宑laim/heartbeat/success/failure commit 鍏ㄩ摼璺牎楠?token+epoch
- `lease_conflicts` 澧炲姞 `expected_lease_epoch/actual_lease_epoch` 瀛楁
- 鍥炲綊鑴氭湰: `scripts/v2_lease_epoch_smoke.ps1`

## V2 Latest Delivered (2026-02-25)
1. [x] Scheduler anti-starvation guard for LOW priority
- FileBus adds a hard guard: when consecutive HIGH claims reach threshold, one LOW claim is forced.
- Metric: relaymesh_low_starvation_count.
- Tests: FileBusFairnessTest and runtime metrics assertion.

2. [x] Ownership events epoch regression guard
- Added runtime regression test to ensure ownership-events exposes expected_lease_epoch and `actual_lease_epoch` consistently.


- Smoke: `scripts/v2_fairness_smoke.ps1` (included in CI windows smoke).

## Endgame Tracking
- See `docs/ENDGAME_BACKLOG.md` for endgame-phase remaining items and current execution status.
