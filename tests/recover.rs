//! Integration test: build a synthetic TiKV-like RocksDB store, simulate
//! UPDATE/DELETE + GC (delete markers written, compaction NOT run), then
//! verify recovery via both whole-DB mode and per-SST forensic mode.

use rocksdb::{DB, Options};
use todu::keycodec::{append_ts, table_record_prefix, Handle, META_PREFIX};
use todu::meta::{parse_db_info, row_columns, TableDef};
use todu::mvcc::WRITE_TYPE_PUT;
use todu::reader::{open_read_only, read_sst_file, scan_cf_prefix, Collector};
use todu::rowcodec::{decode_row_value, Column, Datum};

// ---- realistic TSOs (physical ms since epoch << 18) ----
fn tso(base_ms: u64) -> u64 {
    base_ms << 18
}
fn ts_base() -> u64 {
    // 2026-01-01T00:00:00Z
    1_767_225_600_000
}

fn record_key(table_id: i64, handle: i64) -> Vec<u8> {
    let mut k = table_record_prefix(table_id);
    k.extend_from_slice(&todu::keycodec::flip_int(handle));
    k
}

// ---- row format v2 encoder (mirrors what TiDB writes) ----
fn compact_uint(v: u64) -> Vec<u8> {
    let bytes = v.to_le_bytes();
    let n = ((8 - (v | 1).leading_zeros() as usize / 8).max(1)).min(8);
    let mut out = vec![(n - 1) as u8];
    out.extend_from_slice(&bytes[..n]);
    out
}

fn packed_datetime(y: u64, mo: u64, d: u64, h: u64, mi: u64, s: u64, us: u64) -> u64 {
    // TiDB ToPackedUint bit fields (pkg/types/time.go)
    let ymd = ((y * 13 + mo) << 5) | d;
    let hms = (h << 12) | (mi << 6) | s;
    ((ymd << 17) | hms) << 24 | us
}

/// columns in the VALUE (pk_is_handle: id is in the key, not here)
fn encode_row_v2(name: &str, amount: f64, created: u64, extra: &[u8]) -> Vec<u8> {
    let mut p = vec![0x80u8, 0x00, 0x00]; // ver, flag, null bitmap (4 cols, none null)
    p.push(3); // F2_BYTES
    p.extend(compact_uint(name.len() as u64));
    p.extend_from_slice(name.as_bytes());
    p.push(8); // F2_DOUBLE
    p.extend_from_slice(&amount.to_le_bytes());
    p.push(10); // F2_DATETIME
    p.extend(compact_uint(created));
    p.push(3); // F2_BYTES
    p.extend(compact_uint(extra.len() as u64));
    p.extend_from_slice(extra);
    p
}

fn write_record_value(write_type: u8, start_ts: u64) -> Vec<u8> {
    let mut v = vec![write_type];
    v.extend_from_slice(&start_ts.to_be_bytes());
    v
}

fn put_mvcc(db: &DB, def: &rocksdb::ColumnFamily, wr: &rocksdb::ColumnFamily, uk: &[u8], start: u64, commit: u64, payload: &[u8]) {
    db.put_cf(def, append_ts(uk, start), payload).unwrap();
    db.put_cf(wr, append_ts(uk, commit), &write_record_value(WRITE_TYPE_PUT, start)).unwrap();
}

fn delete_mvcc(db: &DB, wr: &rocksdb::ColumnFamily, uk: &[u8], commit: u64) {
    // TiDB DELETE: only a write record with type=Delete, no default payload
    db.put_cf(wr, append_ts(uk, commit), &write_record_value(2, commit)).unwrap();
}

const TABLE_ID: i64 = 90;

fn test_schema() -> TableDef {
    TableDef {
        db: "test".into(),
        name: "t1".into(),
        id: TABLE_ID,
        pk_is_handle: true,
        is_common_handle: false,
        handle_col: Some(0),
        cols: vec![
            Column { name: "id".into(), tp: 3, flag: 3, flen: 11, decimal: 0 }, // NOT NULL PRI int
            Column { name: "name".into(), tp: 253, flag: 0, flen: 64, decimal: 0 },
            Column { name: "amount".into(), tp: 5, flag: 0, flen: 22, decimal: 0 },
            Column { name: "created".into(), tp: 12, flag: 0, flen: 19, decimal: 0 },
            Column { name: "extra".into(), tp: 252, flag: 0, flen: 0, decimal: 0 },
        ],
    }
}

fn meta_json() -> Vec<u8> {
    let schema = test_schema();
    let cols: Vec<String> = schema
        .cols
        .iter()
        .map(|c| {
            format!(
                r#"{{"id":{},"name":{{"O":"{}","L":"{}"}},"type":{{"Tp":{},"Flag":{},"Flen":{},"Decimal":{}}}}}"#,
                1, c.name, c.name, c.tp, c.flag, c.flen, c.decimal
            )
        })
        .collect();
    format!(
        r#"{{"id":1,"db_name":{{"O":"test","L":"test"}},"tables":[{{"id":{},"name":{{"O":"t1","L":"t1"}},"pk_is_handle":true,"is_common_handle":false,"cols":[{}]}}]}}"#,
        TABLE_ID,
        cols.join(",")
    )
    .into_bytes()
}

/// Build the fixture. Returns (dir, sst_files).
fn build_fixture(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, dir, vec!["default", "write", "lock"]).unwrap();
    let def = db.cf_handle("default").unwrap();
    let wr = db.cf_handle("write").unwrap();

    let t1 = tso(ts_base()); // +0ms
    let t2 = tso(ts_base() + 1_000);
    let t3 = tso(ts_base() + 2_000);

    let created = packed_datetime(2026, 10, 12, 10, 30, 0, 500_000);

    // ---- phase 1: initial rows (handles 1..3), flushed to SST A ----
    for (h, name, amt) in [(1i64, "alice", 10.5), (2, "bob", 20.0), (3, "carol", 30.25)] {
        let uk = record_key(TABLE_ID, h);
        let payload = encode_row_v2(name, amt, created, format!("v1-h{h}").as_bytes());
        put_mvcc(&db, def, wr, &uk, t1, t1, &payload);
    }
    // meta: mDB:1 -> json
    let meta_key = b"mDB:1".to_vec();
    put_mvcc(&db, def, wr, &meta_key, t1, t1, &meta_json());
    db.flush_cf(def).unwrap();
    db.flush_cf(wr).unwrap();

    // ---- phase 2: UPDATE handle 2, DELETE handle 3, flushed to SST B ----
    let uk2 = record_key(TABLE_ID, 2);
    let payload2 = encode_row_v2("bob-updated", 99.5, created, b"v2-h2");
    put_mvcc(&db, def, wr, &uk2, t2, t2, &payload2);
    let uk3 = record_key(TABLE_ID, 3);
    delete_mvcc(&db, wr, &uk3, t3);
    db.flush_cf(def).unwrap();
    db.flush_cf(wr).unwrap();

    // ---- phase 3: GC runs — physically deletes handle3's v1 (both CFs) and
    // handle2's v1 default payload. These become RocksDB tombstones in SST C.
    // Compaction has NOT run: v1 entries still exist inside SST A/B. ----
    let uk1 = record_key(TABLE_ID, 1);
    db.delete_cf(def, append_ts(&uk3, t1)).unwrap();
    db.delete_cf(wr, append_ts(&uk3, t1)).unwrap();
    db.delete_cf(def, append_ts(&uk2, t1)).unwrap();
    db.delete_cf(def, append_ts(&uk1, t1)).unwrap();
    db.flush_cf(def).unwrap();
    db.flush_cf(wr).unwrap();

    let mut ssts: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "sst").unwrap_or(false))
        .collect();
    ssts.sort();
    assert!(ssts.len() >= 4, "expect multiple sst files, got {:?}", ssts);
    ssts
}

fn scan_whole_db(dir: &std::path::Path, prefix: &[u8]) -> Collector {
    let db = open_read_only(dir).unwrap();
    let mut c = Collector::new();
    for cf_name in ["default", "write"] {
        let cf = db.cf_handle(cf_name).unwrap();
        scan_cf_prefix(&db, cf, prefix, |k, v| {
            if cf_name == "write" {
                c.add_write_physical(k, v);
            } else {
                c.add_default_physical(k, v);
            }
        })
        .unwrap();
    }
    c
}

fn scan_ssts(ssts: &[std::path::PathBuf], prefix: &[u8]) -> Collector {
    let mut c = Collector::new();
    for f in ssts {
        read_sst_file(f, None, &mut c).unwrap();
    }
    // prefix only applied at assemble time (sst scan is whole-file)
    let _ = prefix;
    c
}

fn collect_payloads(c: &Collector, prefix: &[u8], as_of: Option<u64>) -> Vec<(i64, Vec<u8>)> {
    c.assemble(Some(prefix), as_of, false)
        .into_iter()
        .filter_map(|r| match parse_handle(&r.user_key) {
            Handle::Int(h) => Some((h, r.payload)),
            _ => None,
        })
        .collect()
}

fn parse_handle(uk: &[u8]) -> Handle {
    todu::keycodec::parse_record_key(uk).unwrap().1
}

#[test]
fn test_whole_db_mode() {
    let dir = std::env::temp_dir().join(format!("todu-test-db-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let _ssts = build_fixture(&dir);
    let prefix = table_record_prefix(TABLE_ID);
    let t1 = tso(ts_base());
    let t2 = tso(ts_base() + 1_000);
    let t3 = tso(ts_base() + 2_000);

    // Latest visible, whole-db mode (tombstone shadowing applies):
    // h1's only payload was GC-deleted -> invisible here; h2 v2 alive;
    // h3 deleted by a real transaction at t3 -> gone.
    let c = scan_whole_db(&dir, &prefix);
    let rows = collect_payloads(&c, &prefix, None);
    assert_eq!(rows.len(), 1, "rows: {rows:?}");
    assert_eq!(rows[0].0, 2);
    assert!(String::from_utf8_lossy(&rows[0].1).contains("bob-updated"));

    // Time travel before the UPDATE: h2's v1 payload was GC-deleted, so
    // whole-db mode has nothing left for the past — this is WHY per-SST
    // forensic mode exists.
    let rows = collect_payloads(&c, &prefix, Some(t1 + 100));
    assert_eq!(rows.len(), 0, "rows: {rows:?}");

    // Between t2 and t3: h2 updated payload alive; h3's v1 write record was
    // GC-deleted so it cannot be reassembled in this mode.
    let rows = collect_payloads(&c, &prefix, Some(t2 + 100));
    assert_eq!(rows.len(), 1, "rows: {rows:?}");
    assert!(String::from_utf8_lossy(&rows[0].1).contains("bob-updated"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_forensic_sst_mode_recovers_gced() {
    let dir = std::env::temp_dir().join(format!("todu-test-sst-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ssts = build_fixture(&dir);
    let prefix = table_record_prefix(TABLE_ID);
    let t1 = tso(ts_base());
    let t2 = tso(ts_base() + 1_000);

    // Per-SST mode bypasses tombstone shadowing: everything physically still
    // on disk in SST A/B is visible again.
    let c = scan_ssts(&ssts, &prefix);

    // At "now": h1 v1 (payload was GC-marked but still in SST A) and h2 v2.
    // h3 was removed by a real DELETE at t3, so its latest state is deleted.
    let rows = collect_payloads(&c, &prefix, None);
    assert_eq!(rows.len(), 2, "rows: {rows:?}");
    let h1 = rows.iter().find(|r| r.0 == 1).expect("h1 recovered despite GC");
    assert!(String::from_utf8_lossy(&h1.1).contains("alice"));

    // as-of BEFORE the GC'd state: all three v1 rows come back, including
    // handle3 whose write record + payload were both GC-deleted.
    let rows = collect_payloads(&c, &prefix, Some(t1 + 100));
    assert_eq!(rows.len(), 3, "rows: {rows:?}");
    let h3 = rows.iter().find(|r| r.0 == 3).expect("GC'd row recovered from SST A");
    assert!(String::from_utf8_lossy(&h3.1).contains("carol"));

    // as-of between t2 and t3: h2 shows the UPDATED version, h3 still alive at v1
    let rows = collect_payloads(&c, &prefix, Some(t2 + 100));
    assert_eq!(rows.len(), 3, "rows: {rows:?}");
    let h2 = rows.iter().find(|r| r.0 == 2).unwrap();
    assert!(String::from_utf8_lossy(&h2.1).contains("bob-updated"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_meta_and_row_decode() {
    let dir = std::env::temp_dir().join(format!("todu-test-meta-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let ssts = build_fixture(&dir);
    let t1 = tso(ts_base());

    // meta table resolution via mDB: keys
    let c = scan_ssts(&ssts, &[META_PREFIX]);
    let versions = c.assemble(Some(&[META_PREFIX]), None, false);
    assert_eq!(versions.len(), 1);
    let (_, tables) = parse_db_info(&versions[0].payload).expect("meta json");
    let t = tables.iter().find(|t| t.name == "t1").unwrap();
    assert_eq!(t.id, TABLE_ID);
    assert!(t.pk_is_handle);
    assert_eq!(t.handle_col, Some(0));

    // row decode: schema cols minus handle col; then merge handle back
    let row_cols = row_columns(t);
    assert_eq!(row_cols.len(), 4);
    let payload = encode_row_v2("alice", 10.5, packed_datetime(2026, 10, 12, 10, 30, 0, 500_000), b"xyz");
    eprintln!("payload({}B) = {}", payload.len(), hex::encode(&payload));
    let datums = match decode_row_value(&payload, &row_cols) {
        Ok(d) => d,
        Err(e) => { eprintln!("decode err: {e}"); panic!("{e}") }
    };
    eprintln!("datums: {datums:?}");
    assert_eq!(datums.len(), 4);
    match &datums[0] {
        Datum::Text(b) => assert_eq!(b, b"alice"),
        other => panic!("name: {other:?}"),
    }
    match &datums[1] {
        Datum::F64(v) => assert!((v - 10.5).abs() < 1e-9),
        other => panic!("amount: {other:?}"),
    }
    match &datums[2] {
        Datum::Time(s) => assert_eq!(s, "2026-10-12 10:30:00.500000", "created: {s}"),
        other => panic!("created: {other:?}"),
    }
    match &datums[3] {
        Datum::Text(b) => assert_eq!(b, b"xyz"),
        other => panic!("extra: {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// silence unused warnings for helpers only used above
#[allow(dead_code)]
fn _refs() {
    let _ = test_schema();
    let _ = tso_to_string();
}
fn tso_to_string() -> String {
    String::new()
}

#[test]
fn test_z_encoded_store() {
    // 真实 TiKV：kvdb key = 'z' + memcomparable(user_key + mvcc ts)。
    // 用 z 包装 key 建一个 fixture，验证两种模式都能恢复。
    use rocksdb::{DB, Options};
    use todu::keycodec::encode_z_key;

    let dir = std::env::temp_dir().join(format!("todu-test-z-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, &dir, vec!["default", "write", "lock"]).unwrap();
    let def = db.cf_handle("default").unwrap();
    let wr = db.cf_handle("write").unwrap();

    let t1 = tso(ts_base());
    let created = packed_datetime(2026, 10, 12, 10, 30, 0, 500_000);
    let payload = encode_row_v2("alice", 10.5, created, b"v1");
    for h in [1i64, 2] {
        let uk = record_key(TABLE_ID, h);
        // 正确分层：z + ENC(user_key) + ts(8B raw)
        db.put_cf(def, [&encode_z_key(&uk)[..], &t1.to_be_bytes()].concat(), &payload).unwrap();
        let mut wv = vec![1u8];
        wv.extend_from_slice(&t1.to_be_bytes());
        db.put_cf(wr, [&encode_z_key(&uk)[..], &t1.to_be_bytes()].concat(), &wv).unwrap();
    }
    db.flush_cf(def).unwrap();
    db.flush_cf(wr).unwrap();

    // 整库模式（混合裸 key/z key 迭代）
    let prefix = table_record_prefix(TABLE_ID);
    let c = scan_whole_db(&dir, &prefix);
    let rows = collect_payloads(&c, &prefix, None);
    assert_eq!(rows.len(), 2, "z-encoded rows: {rows:?}");

    // 逐 SST 取证模式
    let ssts: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "sst").unwrap_or(false)).collect();
    let c = scan_ssts(&ssts, &prefix);
    let rows = collect_payloads(&c, &prefix, None);
    assert_eq!(rows.len(), 2, "z-encoded sst rows: {rows:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
