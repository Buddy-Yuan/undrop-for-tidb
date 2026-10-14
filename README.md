<p align="center">
  <img src="docs/poster.png" width="460" alt="Undrop for TiDB">
</p>

# Undrop for TiDB

![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)

**在 TiDB 数据被误删且 GC 之后，直接解析 TiKV/RocksDB 底层文件，按列恢复数据。**
绕过 SQL 层 · 直读 TiKV 磁盘 · 抓住 compaction 擦除前的恢复窗口。

> The "undrop" tool for TiDB — recover GC'd data by parsing TiKV/RocksDB files directly.
> 内部二进制名：`todu`（TiDB Offline Data Unloader）｜已在与生产兼容的 TiDB **v6.5.11** 上端到端实测。

一个类似 Oracle **ODU / BBED** 的 TiDB 数据抢救工具：绕过 TiDB SQL 层和 TiKV 的
MVCC 可见性判断，直接读取 TiKV 的 RocksDB 文件，按表名把数据恢复出来。

适用场景：**数据已被 DELETE/UPDATE/DROP，且 TiDB GC 已经跑过**，但 RocksDB
compaction 还没有把旧版本物理擦除——GC 只是"标记删除"，数据还在磁盘上，这是
唯一的时间窗口。

```
误删数据 → GC 打删除标记（数据仍在 SST 里，被 tombstone 遮蔽）→ compaction 物理擦除（彻底没了）
                                    ▲
                          todu 的作业窗口
```

## 原理

1. TiDB 的删除只是 MVCC delete 标记；GC 清理 safe point 以下的旧版本（写删除
   记录 / compaction filter），真正的物理擦除发生在 RocksDB compaction。
2. todu 用两种方式读数据：
   - **整库模式**（`--db`）：以只读方式打开 `<tikv-data>/db`，扫描 write CF
     （提交记录）和 default CF（行数据），按 MVCC 时间戳重组每个 row key 的
     版本链；
   - **逐 SST 取证模式**（`--sst`）：用内置的纯 Rust SST 解析器直接解析每个
     `.sst` 文件（footer → index block → data block，支持 snappy/lz4/zstd），
     **不应用其他文件里的 tombstone**——被 GC 标记删除的旧版本因此可见。
3. 表名 → tableID + 列结构：直接解析 TiKV 里 `mDB:` 前缀的 TiDB 元数据
   （JSON），不需要 TiDB 进程活着；meta 也没了时可用 `--schema` 手工提供。
4. 行解码：TiDB row format v2（v4.0+ 默认）和 v1 datum 编码；int 主键
   （pk_is_handle）的值存在 key 里，输出时自动合并回行。

## 快速上手（无集群，1 分钟）

```bash
cargo build --release
./target/release/todu fixture --out /tmp/todu-demo   # 生成模拟事故现场
./target/release/todu meta --db /tmp/todu-demo       # 表名 -> tableID
./target/release/todu recover --db /tmp/todu-demo --table test.t1
SST=$(ls /tmp/todu-demo/*.sst | paste -sd, -)
./target/release/todu recover --sst "$SST" --table test.t1     --as-of "2026-01-01 00:00:00"                    # 取证 + 时间旅行
```

完整的手动测试步骤和逐步预期输出见 **[MANUAL_TEST.md](MANUAL_TEST.md)**
（含真实 tiup 集群测试脚本 `manual_cluster_test.sh`）。

## 编译

```bash
cargo build --release
# 产物: target/release/todu
```

## 应急手册（数据刚被误删）

```bash
# 0. 立即停止业务写入，防止磁盘块被覆盖

# 1. 冻结 GC（TiDB 侧）
SET GLOBAL tidb_gc_enable = OFF;
SET GLOBAL tidb_gc_life_time = '720h';

# 2. 冻结 compaction（TiKV 侧，这是真正擦数据的一步）:
#    tikv.toml 加上 rocksdb.defaultcf.disable-auto-compactions = true，重启 TiKV

# 3. 把 <tikv-data>/db 整个目录拷贝到安全位置，之后对副本操作
cp -r /path/to/tikv-data/db /safe/place/db

# 4. 看看有哪些表（表名 → tableID）
todu meta --db /safe/place/db --schema-out schema.json

# 5a. 整库模式恢复（数据未被 GC 时最简单）
todu recover --db /safe/place/db --table test.t1 --out recovered.sql

# 5b. 已被 GC：逐 SST 取证模式（绕过 tombstone 遮蔽）
todu recover --sst "$(ls /safe/place/db/*.sst | tr '\n' ',')" \
    --table-id 90 --schema schema.json --out recovered.sql

# 5c. 时间旅行：恢复到误操作之前的时点（所有模式通用）
todu recover --sst "$(ls /safe/place/db/*.sst | tr '\n' ',')" \
    --table-id 90 --schema schema.json \
    --as-of "2026-10-12 10:00:00" --out before_mistake.sql

# 6. BBED 式浏览某个 key 的全部 MVCC 版本
todu browse --sst "$(ls /safe/place/db/*.sst | tr '\n' ',')" --table-id 90 --values
```

## 子命令

### `meta` — 列出元数据里的库表

```bash
todu meta --db DIR [--as-of TIME] [--schema-out FILE]
```

### `recover` — 恢复一张表

```bash
todu recover (--db DIR | --sst FILE[,FILE...]) [--cf auto|write|default]
             (--table DB.TABLE | --table-id N [--schema FILE])
             [--as-of "YYYY-MM-DD HH:MM:SS" | --as-of-tso TSO]
             [--format sql|csv|json|raw] [--out FILE] [--limit N] [--verbose]
```

- `--table test.t1`：从 TiDB 元数据解析表名 → tableID + 列结构；
- `--sst`：取证模式。TiKV 的 SST 属于 default 或 write CF，但裸文件不自报
  家门，`--cf auto` 按值形态自动分类（也可强制指定）；
- `--as-of`：只恢复该时刻可见的版本（人类可读时间自动转 TSO）；
- `--format sql`：生成可直接执行的 INSERT（分批）；`csv`/`json` 便于导入其他
  系统；`raw` 输出 key/ts/payload 十六进制。

### `browse` — 逐版本检查（BBED 式）

```bash
todu browse (--db DIR | --sst FILES) --table-id N [--limit N] [--values]
```

### `decode-row` — 手工解码一个行 payload

```bash
todu decode-row --hex <HEX> --schema FILE
```

## Schema 文件

`meta --schema-out` 生成；meta 丢失时也可手写：

```json
[{
  "db": "test", "name": "t1", "table_id": 90,
  "pk_is_handle": true, "handle_col": 0,
  "columns": [
    {"name": "id", "tp": 3, "flag": 3, "flen": 11, "decimal": 0},
    {"name": "name", "tp": 253, "flag": 0, "flen": 64, "decimal": 0}
  ]
}]
```

`tp` 是 MySQL 类型码（3=LONG, 5=DOUBLE, 12=DATETIME, 246=NEWDECIMAL,
253=VAR_STRING, 254=STRING, 252=BLOB…）；`flag` 是 TiDB 字段标志位
（1=NOT NULL, 2=PRI KEY, 32=UNSIGNED）。

## 支持范围与限制

**支持**
- TiKV txnkv（TiDB 表数据），MVCC 版本重组、`--as-of` 时间旅行；
- RocksDB SST format_version 1–5，索引块 value-delta 编码；
  压缩 none/snappy/lz4/lz4hc/zstd；
- 行格式 v2（rowcodec，TiDB v4.0+ 真实行格式：colID+offset 布局，
  int/uint/float/double/varchar/blob/datetime(微秒)/decimal）+ 旧版 v1
  datum 格式；v6.5 真实集群实测按列完整恢复；解不开的列保留原始 hex，
  绝不静默丢数据。
- TiKV v6.x 键包装（z + memcomparable + 外置 ts）与 v6.x write CF 值格式
  （P/D/R + varint start_ts + short_value）自动识别，未知格式走
  short_value 直通恢复。

**限制 / 注意**
- **快照隔离**：todu 是离线工具，看见的是"拷贝那一刻磁盘上还剩什么"。如果
  compaction 已经物理擦除，任何工具都无能为力——所以第 2 步（停 compaction）
  必须第一时间做。
- DROP/TRUNCATE 的数据走 `unsafe_destroy_range`（整文件删除），GC 过后从 SST
  残留恢复的机会远小于普通 DELETE/UPDATE。
- SST 校验和有意跳过（损坏文件也要读），解析失败的块会跳过并报错；
- 不支持：partitioned index（TiKV 默认不用）、zlib/bzip2 压缩块、legacy
  footer（version 0）、TiKV RawKV 模式；
- common handle（聚簇索引表）行解码做了尽力而为：整行消费不上时按错误提示
  用 `--schema` 调整列；
- decimal 采用 MySQL bin 格式解析，边界场景可能退化为 hex；
- 对生产目录操作前**务必先拷贝**；只读打开也不能与运行中的 TiKV 共享目录。

## 已验证

`cargo test` 集成测试覆盖（合成 TiKV 格式数据）：
- MVCC 版本组装、GC tombstone 语义、`--as-of` 时间旅行；
- 逐 SST 模式绕过 tombstone 恢复被 GC 删除的行与旧版本；
- `mDB:` 元数据解析（表名→tableID、pk_is_handle、handle 列合并）；
- row v2 全列解码（varchar/double/datetime(微秒)/blob）。

> 行编码细节以 TiDB `pkg/types/row`、`pkg/types/time.go` 为准；若你的表里有
> 本工具未能正确解码的类型，先看 `--format raw` / `decode-row` 的原始
> payload，欢迎带着样本提 issue。


## License

[MIT](LICENSE) © 2026 Buddy-Yuan
