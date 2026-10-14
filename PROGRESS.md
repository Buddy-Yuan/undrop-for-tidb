# todu 开发状态与上下文（供后续会话接续）

> 最后更新: 2026-10-12。本文件是开发进度快照，接手前先通读。

## 项目定位

todu = TiDB/TiKV 版的 Oracle ODU/BBED：绕过 SQL 层，直接解析 TiKV 的
RocksDB 文件，按表名恢复已被删除且被 GC 的数据。真实 v6.5.11 集群
端到端验证通过（见"里程碑"）。

## 目录结构与关键文件

- `src/main.rs` — CLI：`meta` / `recover` / `browse` / `decode-row` / `fixture`
- `src/keycodec.rs` — TiDB 键编解码：`t + flipBE(tableID) + "_r" + flipBE(handle)`
  记录键（注意：整型是**首字节符号翻转**的 BE，分隔符是 `_r` 带下划线）；
  meta 键 `mDB:<十进制>`；z 包装键解码 `decode_z_parts`
- `src/mvcc.rs` — write CF 值解析（**两种格式**：
  旧 `[type u8][ts BE8]`；v6.x 新字符格式 `[b'P'|b'D'|b'R'][flags u8][varint ts][b'v'+varint len+sv]`）；
  TSO/时间互转（本地时区）
- `src/rowcodec.rs` — 行解码：
  - `decode_rowv2` — **TiDB v4.0+ 真实行格式**（util/rowcodec）：
    `[0x80][flags][numNotNull u16][numNull u16][colIDs u8×N][offsets u16×NotNull][data]`，
    值编码：int=flipBE8、double=符号掩码 BE8、datetime=ToPackedUint 位域
    **小端存储**、string=raw、decimal=[prec][frac][bin]
  - `decode_rowv2_aligned` — 按 schema 全列对齐（handle 列从 key 合并）
  - `decode_row_value` — 旧 datum 格式（v3.x/老 format_version）尽力解码
  - `extract_char_short_value` — 从 v6.x char 格式 write 值中按 `'v'+varint len`
    切出行 payload
- `src/sst.rs` — 纯 Rust SST 解析器（footer 变长+验证驱动、索引块
  delta/classic 双格式、snappy/lz4/zstd、校验和故意跳过）
- `src/reader.rs` — 整库只读 + 逐 SST 取证两种模式，z 键混合迭代
- `src/meta.rs` — `mDB:` 元数据 JSON 解析（表名→tableID+列）
- `src/fixture.rs` — `todu fixture` 生成模拟事故数据目录
- `demo.sh` — **真实集群一键演示（v6.5.11 实测 PASS，约 15 分钟）**
- `manual_cluster_test.sh` / `MANUAL_TEST.md` / `README.md`

## 已验证能力（测试 + 真实集群）

1. 真实 v6.5.11 集群 demo 全流程 PASS：建 3 行 → DELETE → GC 越过
   （闪回报 GC 错误）→ 停集群拷数据 → `todu recover --db --table-id N
   --schema schema.json` → **3 行按列完整恢复（含微秒时间）**，与删除前
   预期 diff 一致。bob 的历史版本也在。
2. 整库模式/逐 SST 取证模式的时间旅行、GC tombstone 语义（集成测试 4 个）。
3. TiKV v6.5 键存储格式（实测破解）：
   `存储键 = 'z' + EncodeBytes(user_key) + ts(8B raw) + rocksdb内部后缀(8B)`，
   EncodeBytes = MyRocks memcomparable（8 字节组 + 0xFF-pad 标记）。
   即 mvcc ts 在编码块**外**。解码入口 `decode_z_parts`。
4. v6.x write CF 值 = `[b'P'|b'D'][flags][varint ts][b'v'+varint len+short_value]`
   （'P'=0x50 'D'=0x44，flags bit3=有 short_value；start_ts LEB128 后接
   `[flags u8]` 的顺序是 [type][flags][ts]）。无法解析时走 short_value
   **直通恢复**（`Collector.direct` + `assemble_direct`，主流程自动兜底）。
5. 行 payload = rowcodec v2，列按 colID 升序，colID = 列序号(1-based)，
   pk_is_handle 表的主键列不在行内（值在 key 里）。
   真实样本已做单元测试：`decode_real_v65_payload`（alice 行，
   varchar/double/datetime(微秒) 全对）。

## 已知未完成（按优先级）

1. **v6.5 SST 纯模式部分可用**（2026-10-13 更新）：已实现两级回退——
   ① 索引块 delta/classic 双格式解析 + size=0 时按块边界推导
   （next_boundary - 5 - offset）；② 索引不可用时**链式暴力扫描**
   （数据块在文件内首尾相接，DFS 候选 size + 结构校验 + z 键位置剪枝，
   卡住 = 数据区结束后面是 filter/meta 块，属正常）。
   实测 v6.5 拷贝 28 个 SST：3 个文件链扫成功，其余跳过——**首块可能是
   0x01 前缀的 TiKV 内部键块**（结构未破解，条目格式与标准块不同），
   且 v6.5 索引块条目 2 的 shared=44 超过条目 1 键长 30（delta 基准疑为
   完整键而非截断键）——这两个点需要再对照 TiKV v6.5 block.rs 源码。
   **整库模式 `--db` 完全不受影响（推荐路径）**。
2. **meta 键解析**：v6.5 的 `mDB:` 键带扩展（观察到
   `mDB:1\0…\0hTID:16\0\0\0` 形态，疑似 MDL/infoschema 相关），plain
   `mDB:1` 没找到。当前表名解析走 information_schema（集群活着时）+
   `--schema`（手工）。若要纯离线表名解析，需 dump v6.5 元数据键样本
   分析（`todu browse` / examples/dbg_sst.rs 可用）。
3. **decimal 值解码**：rowcodec v2 的 decimal 走 [prec][frac][bin]，bin
   细节未对照真实样本验证（当前 hex 兜底）。
4. **SST large row**（flags bit0）不支持。

## v6.5 实测破解记录（格式结论，可直接复用）

- kvdb 存储键 = `z + EncodeBytes(user_key) + ts(8B, BE, 在编码块外)`
  + RocksDB 内部后缀(8B)；EncodeBytes = 8 字节组 + `0xFF-pad` 标记
- 用户记录键 = `t + flipBE(tableID) + "_r" + flipBE(handle)`
- 行 payload = rowcodec v2：`[0x80][flags][numNotNull u16][numNull u16]
  [colIDs u8×N][offsets u16LE×NotNull][data]`，colID = 列序号(1-based)，
  pk_is_handle 表的主键列不在行内
- write CF 值（v6.x）= `[b'P'|b'D'|b'R'][flags u8][varint(start_ts)]
  [b'v' + varint(len) + short_value]`；DOUBLE 值 = EncodeFloat
  （符号位掩码 BE 8B）；DATETIME = ToPackedUint 位域**小端存储**
- 真实样本：alice 行 payload
  `80 00 04 00 00 02 03 04 05 05 00 0d 00 15 00 22 00 alice [decimal8B]
  [datetime8B] beijing-order`（已做单元测试基准 decode_real_v65_payload）

## 手动验证方式

```bash
cargo build --release
./demo.sh                    # 真实集群全流程（自动起/停 playground，约 15 分钟）
./target/release/todu fixture --out /tmp/todu-demo   # 无集群 5 分钟路径
# 手册: MANUAL_TEST.md
```

## 测试

`cargo test --release` — 6 个测试全绿：
- `decode_real_v65_payload`：v6.5 真实行 payload 列解码（关键回归基准，
  改行解码逻辑后必须保持通过）
- `test_whole_db_mode` / `test_forensic_sst_mode_recovers_gced`：
  GC tombstone 语义 + 取证模式
- `test_meta_and_row_decode` / `test_z_encoded_store`

## 排障记录（避免重复踩坑）

- playground `--db.port` 被占用时会静默换随机端口 → 从 playground.log
  第一行 "TiDB: 127.0.0.1:PORT" 取真实端口
- TiKV 停机**不 flush** memtable → 演示集群的 tikv.toml 配了
  write-buffer-size=16KB（[rocksdb.defaultcf/writecf/lockcf] 各自都要配，
  [rocksdb] 层会被 CF 层覆盖）+ disable-auto-compactions=true 保住旧版本
- `SET GLOBAL tidb_gc_life_time='30s'` 在 v6.5 静默不生效（最小 10m）；
  demo 用闪回失效探测作为 GC 越过判据
- z 键混合迭代：裸 key 与 z key 共存时 seek 用 min(裸前缀, ENC(前缀))，
  解码后前缀比较，越过时 break
- bash 3.2 (macOS) 对多字节字符紧贴 `$var` 的解析有坑 → demo.sh 已规避
