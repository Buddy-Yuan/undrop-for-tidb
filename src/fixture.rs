//! `todu fixture` — generate a self-contained TiKV-like data directory that
//! simulates an accident scene: a table with 3 rows, one row UPDATEd, one row
//! DELETEd, and GC already run (old versions tombstone-shadowed). Lets anyone
//! manually exercise the whole recovery flow without a cluster.

use std::path::PathBuf;

use rocksdb::{DB, Options};

use crate::mvcc::WRITE_TYPE_PUT;

fn record_key(table_id: i64, handle: i64) -> Vec<u8> {
    let mut k = b"t".to_vec();
    k.extend_from_slice(&crate::keycodec::flip_int(table_id));
    k.extend_from_slice(b"_r");
    k.extend_from_slice(&crate::keycodec::flip_int(handle));
    k
}

fn append_ts(uk: &[u8], ts: u64) -> Vec<u8> {
    let mut k = uk.to_vec();
    k.extend_from_slice(&ts.to_be_bytes());
    k
}

fn compact_uint(v: u64) -> Vec<u8> {
    let bytes = v.to_le_bytes();
    let n = ((8 - (v | 1).leading_zeros() as usize / 8).max(1)).min(8);
    let mut out = vec![(n - 1) as u8];
    out.extend_from_slice(&bytes[..n]);
    out
}

/// TiDB ToPackedUint bit fields (pkg/types/time.go)
fn packed_datetime(y: u64, mo: u64, d: u64, h: u64, mi: u64, s: u64, us: u64) -> u64 {
    let ymd = ((y * 13 + mo) << 5) | d;
    let hms = (h << 12) | (mi << 6) | s;
    ((ymd << 17) | hms) << 24 | us
}

/// row format v2; columns in the VALUE (pk_is_handle: id lives in the key)
fn encode_row_v2(name: &str, amount: f64, created: u64, extra: &[u8]) -> Vec<u8> {
    let mut p = vec![0x80u8, 0x00, 0x00];
    p.push(3);
    p.extend(compact_uint(name.len() as u64));
    p.extend_from_slice(name.as_bytes());
    p.push(8);
    p.extend_from_slice(&amount.to_le_bytes());
    p.push(10);
    p.extend(compact_uint(created));
    p.push(3);
    p.extend(compact_uint(extra.len() as u64));
    p.extend_from_slice(extra);
    p
}

fn wr_value(write_type: u8, start_ts: u64) -> Vec<u8> {
    let mut v = vec![write_type];
    v.extend_from_slice(&start_ts.to_be_bytes());
    v
}

const TABLE_ID: i64 = 90;

pub fn run(out: Option<&str>) -> Result<String, String> {
    let dir = match out {
        Some(p) => PathBuf::from(p),
        None => PathBuf::from("/tmp/todu-fixture"),
    };
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| format!("clean {}: {e}", dir.display()))?;
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, &dir, vec!["default", "write", "lock"])
        .map_err(|e| format!("create fixture db: {e}"))?;
    let def = db.cf_handle("default").unwrap();
    let wr = db.cf_handle("write").unwrap();

    // commit timestamps: 2026-01-01 00:00:00 / +1s / +2s (TSO = ms << 18)
    let t1 = 1_767_225_600_000u64 << 18;
    let t2 = (1_767_225_600_000u64 + 1_000) << 18;
    let t3 = (1_767_225_600_000u64 + 2_000) << 18;
    let created = packed_datetime(2026, 10, 12, 10, 30, 0, 500_000);

    // ---- phase 1: 3 rows, flushed to SST A ----
    for (h, name, amt) in [(1i64, "alice", 10.5), (2, "bob", 20.0), (3, "carol", 30.25)] {
        let uk = record_key(TABLE_ID, h);
        let payload =
            encode_row_v2(name, amt, created, format!("v1-h{h}").as_bytes());
        db.put_cf(def, append_ts(&uk, t1), &payload)
            .map_err(|e| e.to_string())?;
        db.put_cf(wr, append_ts(&uk, t1), &wr_value(WRITE_TYPE_PUT, t1))
            .map_err(|e| e.to_string())?;
    }
    // TiDB meta: db id 1 "test", table t1 id 90 (pk_is_handle)
    let meta_key = b"mDB:1".to_vec();
    let schema = r#"{"id":1,"db_name":{"O":"test","L":"test"},"tables":[{"id":90,"name":{"O":"t1","L":"t1"},"pk_is_handle":true,"is_common_handle":false,"cols":[
        {"id":1,"name":{"O":"id","L":"id"},"type":{"Tp":3,"Flag":3,"Flen":11,"Decimal":0}},
        {"id":2,"name":{"O":"name","L":"name"},"type":{"Tp":253,"Flag":0,"Flen":64,"Decimal":0}},
        {"id":3,"name":{"O":"amount","L":"amount"},"type":{"Tp":5,"Flag":0,"Flen":22,"Decimal":0}},
        {"id":4,"name":{"O":"created","L":"created"},"type":{"Tp":12,"Flag":0,"Flen":19,"Decimal":0}},
        {"id":5,"name":{"O":"extra","L":"extra"},"type":{"Tp":252,"Flag":0,"Flen":0,"Decimal":0}}]}]}"#;
    db.put_cf(def, append_ts(&meta_key, t1), schema.as_bytes())
        .map_err(|e| e.to_string())?;
    db.put_cf(wr, append_ts(&meta_key, t1), &wr_value(WRITE_TYPE_PUT, t1))
        .map_err(|e| e.to_string())?;
    db.flush_cf(def).map_err(|e| e.to_string())?;
    db.flush_cf(wr).map_err(|e| e.to_string())?;

    // ---- phase 2: UPDATE bob, DELETE carol -> SST B ----
    let uk2 = record_key(TABLE_ID, 2);
    let payload2 = encode_row_v2("bob-updated", 99.5, created, b"v2-h2");
    db.put_cf(def, append_ts(&uk2, t2), &payload2).map_err(|e| e.to_string())?;
    db.put_cf(wr, append_ts(&uk2, t2), &wr_value(WRITE_TYPE_PUT, t2))
        .map_err(|e| e.to_string())?;
    let uk3 = record_key(TABLE_ID, 3);
    db.put_cf(wr, append_ts(&uk3, t3), &wr_value(2, t3)) // type=Delete
        .map_err(|e| e.to_string())?;
    db.flush_cf(def).map_err(|e| e.to_string())?;
    db.flush_cf(wr).map_err(|e| e.to_string())?;

    // ---- phase 3: GC runs — old versions tombstoned (compaction NOT run,
    // so the data is still physically inside SST A/B) ----
    let uk1 = record_key(TABLE_ID, 1);
    for k in [
        append_ts(&uk3, t1),
        append_ts(&uk2, t1),
        append_ts(&uk1, t1),
    ] {
        db.delete_cf(def, &k).map_err(|e| e.to_string())?;
    }
    db.delete_cf(wr, append_ts(&uk3, t1)).map_err(|e| e.to_string())?;
    db.flush_cf(def).map_err(|e| e.to_string())?;
    db.flush_cf(wr).map_err(|e| e.to_string())?;

    Ok(dir.to_string_lossy().to_string())
}
