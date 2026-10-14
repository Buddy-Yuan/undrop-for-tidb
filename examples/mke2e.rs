// Build a full E2E fixture: meta + table data + UPDATE/DELETE + GC simulation.
use rocksdb::{DB, Options};
fn record_key(table_id: i64, handle: i64) -> Vec<u8> {
    let mut k = b"t".to_vec();
    k.extend_from_slice(&table_id.to_be_bytes());
    k.push(b'r');
    k.extend_from_slice(&handle.to_be_bytes());
    k
}
fn append_ts(uk: &[u8], ts: u64) -> Vec<u8> { let mut k = uk.to_vec(); k.extend_from_slice(&ts.to_be_bytes()); k }
fn compact_uint(v: u64) -> Vec<u8> {
    let bytes = v.to_le_bytes();
    let n = ((8 - (v | 1).leading_zeros() as usize / 8).max(1)).min(8);
    let mut out = vec![(n - 1) as u8];
    out.extend_from_slice(&bytes[..n]);
    out
}
fn packed_datetime(y: u64, mo: u64, d: u64, h: u64, mi: u64, s: u64, us: u64) -> u64 {
    let ymd = ((y * 13 + mo) << 5) | d;
    let hms = (h << 12) | (mi << 6) | s;
    ((ymd << 17) | hms) << 24 | us
}
fn encode_row_v2(name: &str, amount: f64, created: u64, extra: &[u8]) -> Vec<u8> {
    let mut p = vec![0x80u8, 0x00, 0x00];
    p.push(3); p.extend(compact_uint(name.len() as u64)); p.extend_from_slice(name.as_bytes());
    p.push(8); p.extend_from_slice(&amount.to_le_bytes());
    p.push(10); p.extend(compact_uint(created));
    p.push(3); p.extend(compact_uint(extra.len() as u64)); p.extend_from_slice(extra);
    p
}
fn wr_value(t: u8, start: u64) -> Vec<u8> { let mut v = vec![t]; v.extend_from_slice(&start.to_be_bytes()); v }
fn main() {
    let dir = std::path::Path::new("/tmp/todu-e2e");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = DB::open_cf(&opts, dir, vec!["default", "write", "lock"]).unwrap();
    let def = db.cf_handle("default").unwrap();
    let wr = db.cf_handle("write").unwrap();
    let t1 = 1_767_225_600_000u64 << 18;
    let t2 = (1_767_225_600_000u64 + 1000) << 18;
    let t3 = (1_767_225_600_000u64 + 2000) << 18;
    let created = packed_datetime(2026, 10, 12, 10, 30, 0, 500_000);
    for (h, name, amt) in [(1i64, "alice", 10.5), (2, "bob", 20.0), (3, "carol", 30.25)] {
        let uk = record_key(90, h);
        db.put_cf(def, append_ts(&uk, t1), &encode_row_v2(name, amt, created, format!("v1-h{h}").as_bytes())).unwrap();
        db.put_cf(wr, append_ts(&uk, t1), &wr_value(1, t1)).unwrap();
    }
    // meta: db id 1 "test" with table t1 (id 90)
    let meta_key = { let mut k = b"m".to_vec(); k.extend_from_slice(b"DB:"); k.extend_from_slice(&((1u64 ^ (1u64<<63)).to_be_bytes())); k };
    let schema = r#"{"id":1,"db_name":{"O":"test","L":"test"},"tables":[{"id":90,"name":{"O":"t1","L":"t1"},"pk_is_handle":true,"is_common_handle":false,"cols":[
        {"id":1,"name":{"O":"id","L":"id"},"type":{"Tp":3,"Flag":3,"Flen":11,"Decimal":0}},
        {"id":2,"name":{"O":"name","L":"name"},"type":{"Tp":253,"Flag":0,"Flen":64,"Decimal":0}},
        {"id":3,"name":{"O":"amount","L":"amount"},"type":{"Tp":5,"Flag":0,"Flen":22,"Decimal":0}},
        {"id":4,"name":{"O":"created","L":"created"},"type":{"Tp":12,"Flag":0,"Flen":19,"Decimal":0}},
        {"id":5,"name":{"O":"extra","L":"extra"},"type":{"Tp":252,"Flag":0,"Flen":0,"Decimal":0}}]}]}"#;
    db.put_cf(def, append_ts(&meta_key, t1), schema.as_bytes()).unwrap();
    db.put_cf(wr, append_ts(&meta_key, t1), &wr_value(1, t1)).unwrap();
    db.flush_cf(def).unwrap(); db.flush_cf(wr).unwrap();
    // update bob, delete carol
    let uk2 = record_key(90, 2);
    db.put_cf(def, append_ts(&uk2, t2), &encode_row_v2("bob-updated", 99.5, created, b"v2-h2")).unwrap();
    db.put_cf(wr, append_ts(&uk2, t2), &wr_value(1, t2)).unwrap();
    let uk3 = record_key(90, 3);
    db.put_cf(wr, append_ts(&uk3, t3), &wr_value(2, t3)).unwrap();
    db.flush_cf(def).unwrap(); db.flush_cf(wr).unwrap();
    // GC: delete carol v1 + alice/bob v1 payloads (tombstones in memtable -> flush)
    let uk1 = record_key(90, 1);
    db.delete_cf(def, append_ts(&uk3, t1)).unwrap();
    db.delete_cf(wr, append_ts(&uk3, t1)).unwrap();
    db.delete_cf(def, append_ts(&uk2, t1)).unwrap();
    db.delete_cf(def, append_ts(&uk1, t1)).unwrap();
    db.flush_cf(def).unwrap(); db.flush_cf(wr).unwrap();
    println!("{}", dir.display());
}
