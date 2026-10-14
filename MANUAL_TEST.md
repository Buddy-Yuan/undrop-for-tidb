# todu 手动测试手册

三个测试路径，按需选择：

- **推荐：`./demo.sh`（真实集群，全自动，约 15 分钟）** —— 一键完成：
  起独立 playground 集群（端口 4100，不碰你已有的集群）→ 建库建表写入 3 行
  → DELETE → 等 GC 真正跑过（实时探测闪回何时失效）→ 停集群 → todu 从磁盘
  恢复 → 与删除前预期 diff。跑完打印 PASS/FAIL。
- **Part A：无集群手动测试（5 分钟）** —— 用 `todu fixture` 生成一个模拟
  "GC 之后、compaction 之前"事故现场的 TiKV 数据目录，手动走完整恢复流程。
  只需要编译好的 todu，什么都不用装。
- **Part B：真实集群分步手动测试** —— 想亲手敲每一条命令时看这部分。

二进制：`target/release/todu`（`cargo build --release`）。

---

## Part A：无集群测试（fixture 模拟现场）

### A0. 生成事故现场

```bash
cd /Users/yuanweixiang/ZCodeProject/todu
./target/release/todu fixture --out /tmp/todu-demo
```

它生成了一个"事故后"的 TiKV 数据目录，模拟的剧情：

| 行 | 剧情设定 |
|---|---|
| alice (id=1) | 一直存在；v1 的行值被 GC 物理删了 tombstone 标记（数据还在旧 SST 里） |
| bob (id=2) | +1s 被 UPDATE 成 bob-updated；旧版本被 GC 遮蔽 |
| carol (id=3) | v1 行值被 GC 删除 + +2s 被 DELETE（写了删除标记） |

元数据里有一张表 `test.t1`（tableID=90，列：id/name/amount/created/extra）。

### A1. 查元数据（表名 → tableID）

```bash
./target/release/todu meta --db /tmp/todu-demo
```

**预期输出：**

```
database `test`
  table `t1` id=90 pk_is_handle=true common_handle=false handle_col=Some(0) cols=[id(tp=3), name(tp=253), amount(tp=5), created(tp=12), extra(tp=252)]
```

### A2. 整库模式恢复（看"现在"还剩什么）

```bash
./target/release/todu recover --db /tmp/todu-demo --table test.t1 --format sql
```

**预期输出：** 只有 1 行 —— bob 的最新版本。alice 的行值被 GC 遮蔽、carol
已被 DELETE，所以整库模式看不见它们（这正是要取证模式的原因）：

```sql
-- todu recover: `test.t1` (table_id=90)
INSERT INTO `test.t1` (`id`, `name`, `amount`, `created`, `extra`) VALUES
(2, 'bob-updated', 99.5, '2026-10-12 10:30:00.500000', 'v2-h2')
;
```

### A3. 时间旅行（误操作之前的时点）

```bash
./target/release/todu recover --db /tmp/todu-demo --table test.t1 \
    --as-of "2026-01-01 00:00:00" --format csv
```

**预期输出：** 空的（只有表头）。因为 GC 把 v1 的行值也标记删除了，整库模式
被 tombstone 遮住，看不见任何历史 —— 这一步是为了让你直观看到"GC 过后整库
模式的极限"。

### A4. ★ 逐 SST 取证模式（核心）

```bash
SST=$(ls /tmp/todu-demo/*.sst | paste -sd, -)
./target/release/todu recover --sst "$SST" --table test.t1 --format csv
```

**预期输出：** 2 行。alice 从 SST 残留里被挖回来了（整库模式看不见）；
carol 因为被真实 DELETE，"最新时点"依然是删除态：

```
id,name,amount,created,extra
1,alice,10.5,'2026-10-12 10:30:00.500000',v1-h1
2,bob-updated,99.5,'2026-10-12 10:30:00.500000',v2-h2
```

### A5. ★ 取证 + 时间旅行 = 全部找回

```bash
./target/release/todu recover --sst "$SST" --table test.t1 \
    --as-of "2026-01-01 00:00:00" --format csv
```

**预期输出：** 3 行原始数据全回来了，包括被 GC 删掉行值 + 被 DELETE 的 carol：

```
id,name,amount,created,extra
1,alice,10.5,'2026-10-12 10:30:00.500000',v1-h1
2,bob,20,'2026-10-12 10:30:00.500000',v1-h2
3,carol,30.25,'2026-10-12 10:30:00.500000',v1-h3
```

### A6. BBED 式浏览版本链

```bash
./target/release/todu browse --sst "$SST" --table-id 90
```

**预期输出（节选）：** 每个 key 的所有 MVCC 版本、commit_ts（带人类可读时间）、
write 类型（Put/Delete）、行值大小：

```
=== key=74000000000000005a720000000000000001 handle=1
  commit_ts=4632... (2026-01-01 00:00:00.000 UTC) type=Put start_ts=4632... default(38B)
=== key=74000000000000005a720000000000000002 handle=2
  commit_ts=4632... (2026-01-01 00:00:00.000 UTC) type=Put start_ts=4632... default(36B)
  commit_ts=4632... (2026-01-01 00:00:01.000 UTC) type=Put start_ts=4632... default(44B)
=== key=...0003 handle=3
  ... type=Put ...
  ... type=Delete ...
```

加 `--values` 可以看行值的十六进制（BBED 式 dump）。

### A7. （可选）手工 schema 验证

不依赖 meta、纯手工提供列结构：

```bash
./target/release/todu recover --sst "$SST" --table-id 90 \
    --schema <(echo '[{"db":"test","name":"t1","table_id":90,"pk_is_handle":true,"handle_col":0,
      "columns":[{"name":"id","tp":3,"flag":3,"flen":11,"decimal":0},
                 {"name":"name","tp":253,"flag":0,"flen":64,"decimal":0},
                 {"name":"amount","tp":5,"flag":0,"flen":22,"decimal":0},
                 {"name":"created","tp":12,"flag":0,"flen":19,"decimal":0},
                 {"name":"extra","tp":252,"flag":0,"flen":0,"decimal":0}]}]') \
    --format csv
```

预期与 A4 输出一致。

**Part A 通过标准：A2=1 行、A3=空、A4=2 行、A5=3 行、A6 能看到 Delete 标记。**

---

## Part B：真实 TiDB 集群测试

### 方式一：一键脚本（已在 v6.5.11 真实集群验证 PASS）

```bash
./demo.sh
```

> 实测：v6.5.11 集群上整库模式可按列完整恢复（含 created_at 微秒精度），
> 与删除前预期逐行 diff 一致。注：v6.5 的 SST 索引值编码与行内 start_ts
> varint 仍在完善（该模式下部分文件跳过），整库模式(--db)不受影响；
> meta 键解析亦在进行中，表名解析走 information_schema + schema.json 路径。

预期输出（节选）：

```
STEP 2  DELETE 全部数据 -> 确认查不到东西
行数: 0   <-- 已经查不到东西（MVCC 删除标记）

STEP 3  等待 GC ...
  闪回已失效: ERROR 9006 (HY000): GC life time is shorter than transaction duration ...
  ==> GC 已越过删除时刻，数据在 SQL 侧彻底没了 ✓

STEP 6  对账：恢复结果 vs 删除前预期
[PASS] MySQL 里已经查不到、闪回也救不回来的 3 行数据，
       被 todu 从 TiKV 磁盘上完整恢复
```

### 方式二：分步手动（原 manual_cluster_test.sh 交互脚本）

```
1. tiup playground 起一个 1:1:1 最小集群（显式指定 TiKV 数据目录）
2. 建表 test.t1，插入 alice/bob/carol 三行，SELECT 一份"预期答案"
3. 模拟事故：UPDATE bob、DELETE carol
4. 调短 GC 间隔（tidb_gc_life_time='30s'），等 GC 真正跑过去
   （观察 mysql.tidb 里 tikv_gc_safe_point 前移）
5. 停集群（这一步很关键：停了就不会再 compaction）
6. 拷贝 TiKV 数据目录
7. todu meta / recover / --sst 取证 / --as-of，与第 2 步的预期 diff
```

```bash
cd /Users/yuanweixiang/ZCodeProject/todu
./manual_cluster_test.sh          # 需要 tiup 和 mysql 客户端
```

想手动一步步来（不用脚本）的话，关键命令：

```bash
# 起集群（显式 TiKV 数据目录，停掉后数据不会被 playground 清掉）
tiup playground v8.5.0 --db 1 --kv 1 --pd 1 --tag todu-manual --without-monitor \
    --kv.config /tmp/todu-cluster/tikv.toml   # 内容见脚本生成

mysql -h 127.0.0.1 -P 4000 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE test.t1 (id INT PRIMARY KEY, name VARCHAR(64), amount DOUBLE,
                      created DATETIME(3), extra VARCHAR(64));
INSERT INTO test.t1 VALUES (1,'alice',10.5,'2026-10-12 10:30:00.500','v1-h1'),
                           (2,'bob',20,'2026-10-12 10:30:00.500','v1-h2'),
                           (3,'carol',30.25,'2026-10-12 10:30:00.500','v1-h3');
SELECT * FROM test.t1;                      -- 预期答案存一份
UPDATE test.t1 SET name='bob-updated', amount=99.5 WHERE id=2;
DELETE FROM test.t1 WHERE id=3;
SET GLOBAL tidb_gc_life_time = '30s';       -- 逼 GC 快点跑
SET GLOBAL tidb_gc_run_interval = '30s';
SQL
# ...等 1~2 分钟，确认 safe point 前移：
mysql -h 127.0.0.1 -P 4000 -u root -e \
  "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tikv_gc_safe_point';"
# 停集群：到 playground 的终端按 Ctrl-C（或 kill 掉进程）
# 拷贝数据目录：
cp -r /tmp/todu-cluster/tikv-data /tmp/todu-cluster/tikv-data-copy
# 之后同 Part A 的 A1~A6（--db 换成 tikv-data-copy/db，表名用真实的）
```

**Part B 通过标准：**

1. `todu meta` 能列出真实的 `test.t1` 和它的 tableID；
2. `recover --db`（整库模式）能查出事故后还活着的行；
3. `recover --sst` + `--as-of "UPDATE 之前的时刻"` 能找回 alice 和旧版 bob，
   以及被 DELETE 的 carol —— 和第 2 步存的预期答案 diff 一致；
4. `browse` 能看到 bob 的两个版本和 carol 的 Delete 标记。

## 常见问题

- **`recover --db` 报 "open read-only" 失败**：目录不干净（WAL/锁残留）时改用
  `--sst` 模式，它完全不依赖 MANIFEST/WAL；
- **真实集群里解出来的行值是 hex**：说明某个列类型没被识别（比如 enum/set/json），
  把 `browse --values` 的 payload 发我，对着 TiDB 的类型码补即可；
- **卡在 GC 没跑**：看 `mysql.tidb` 里 `tikv_gc_state`/`tikv_gc_safe_point`；
  leader 所在 TiDB 节点的日志里 grep `gc`。
