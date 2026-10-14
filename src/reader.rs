//! Reading TiKV's RocksDB offline: whole-DB read-only mode and per-SST
//! forensic mode (bypasses RocksDB-level tombstone shadowing so data that GC
//! marked for deletion but compaction hasn't physically erased is still
//! visible).

use std::collections::BTreeMap;
use std::path::Path;

use rocksdb::{DB, Options, ReadOptions};

use crate::keycodec::*;
use crate::mvcc::*;

/// One recovered row version of a record key.
#[derive(Debug, Clone)]
pub struct Recovered {
    pub user_key: Vec<u8>,
    pub commit_ts: u64,
    pub start_ts: u64,
    pub payload: Vec<u8>,
    pub source: String,
}

#[derive(Default)]
pub struct Collector {
    /// (user_key, commit_ts) -> (start_ts, write_type, short_value)
    writes: BTreeMap<(Vec<u8>, u64), (u64, u8, Option<Vec<u8>>)>,
    /// (user_key, start_ts) -> row payload
    defaults: BTreeMap<(Vec<u8>, u64), Vec<u8>>,
    /// Fallback for write-value formats whose start_ts we cannot parse
    /// (TiKV v6.x char-type): user_key -> distinct short_values (all versions
    /// found). Order across versions is unknowable without ts.
    pub direct: BTreeMap<Vec<u8>, Vec<Vec<u8>>>,
    pub stat_write_tombstones: usize,
    pub stat_default_tombstones: usize,
    pub stat_write_entries: usize,
    pub stat_default_entries: usize,
}

impl Collector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a write-CF physical entry (key = user_key + commit_ts BE).
    pub fn add_write_physical(&mut self, physical_key: &[u8], value: Option<&[u8]>) {
        let (uk, commit_ts) = match split_mvcc_ts(physical_key) {
            Some(x) => x,
            None => return,
        };
        let uk = uk.to_vec();
        match value {
            None | Some([]) => {
                self.stat_write_tombstones += 1;
                // physically deleted (GC) — remember the hole so assemble can
                // stop history reconstruction at it
                self.writes.insert((uk, commit_ts), (0, WRITE_TYPE_NONE, None));
            }
            Some(v) => {
                match parse_write_value(v) {
                    Some(w) => {
                        self.stat_write_entries += 1;
                        self.writes
                            .insert((uk, commit_ts), (w.start_ts, w.write_type, w.short_value));
                    }
                    None => {
                        // unknown value format (TiKV v6.x char-type): salvage
                        // the whole value for direct recovery
                        if v.first() == Some(&0x50) && v.len() > 4 {
                            let entry = self.direct.entry(uk).or_default();
                            if !entry.contains(&v.to_vec()) {
                                entry.push(v.to_vec());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Add a default-CF physical entry (key = user_key + start_ts BE).
    pub fn add_default_physical(&mut self, physical_key: &[u8], value: Option<&[u8]>) {
        let (uk, start_ts) = match split_mvcc_ts(physical_key) {
            Some(x) => x,
            None => return,
        };
        match value {
            None | Some([]) => self.stat_default_tombstones += 1,
            Some(v) => {
                self.stat_default_entries += 1;
                self.defaults.insert((uk.to_vec(), start_ts), v.to_vec());
            }
        }
    }

    /// Same as add_write_physical but with uk/ts already split (SST mode).
    pub fn add_write_physical_key(&mut self, uk: &[u8], commit_ts: u64, value: &[u8]) {
        match parse_write_value(value) {
            Some(w) => {
                self.stat_write_entries += 1;
                self.writes
                    .insert((uk.to_vec(), commit_ts), (w.start_ts, w.write_type, w.short_value));
            }
            None => {
                // unknown value format: still salvage Put payloads when the
                // type char says Put and a short value seems present
                if value.first() == Some(&0x50) && value.len() > 4 {
                    let entry = self.direct.entry(uk.to_vec()).or_default();
                    if !entry.contains(&value.to_vec()) {
                        // store the WHOLE value; payload extraction happens at
                        // output time (format-specific tail = short_value)
                        entry.push(value.to_vec());
                    }
                }
            }
        }
    }

    /// Same as add_default_physical but with uk/ts already split (SST mode).
    pub fn add_default_physical_key(&mut self, uk: &[u8], start_ts: u64, value: &[u8]) {
        self.stat_default_entries += 1;
        self.defaults.insert((uk.to_vec(), start_ts), value.to_vec());
    }

    /// Assemble row versions for keys matching `prefix`.
    /// as_of: only versions committed at or before this TSO are visible.
    pub fn assemble(
        &self,
        prefix: Option<&[u8]>,
        as_of: Option<u64>,
        _include_deleted: bool,
    ) -> Vec<Recovered> {
        let limit_ts = as_of.unwrap_or(u64::MAX);
        let mut out = Vec::new();

        // distinct user keys, in order
        let mut user_keys: Vec<&Vec<u8>> = Vec::new();
        let mut last: Option<&Vec<u8>> = None;
        for ((uk, _), _) in &self.writes {
            if last != Some(uk) {
                user_keys.push(uk);
                last = Some(uk);
            }
        }

        for uk in user_keys {
            if let Some(p) = prefix {
                if !uk.starts_with(p) {
                    continue;
                }
            }
            // entries for this uk, newest commit_ts first
            let mut rev: Vec<(u64, &(u64, u8, Option<Vec<u8>>))> = self
                .writes
                .range((uk.clone(), 0)..(uk.clone(), u64::MAX))
                .map(|(k, v)| (k.1, v))
                .collect();
            rev.reverse();

            // pick the newest PUT visible at as_of, stopping at a newer
            // visible DELETE or a physical tombstone hole
            let mut picked: Option<(u64, &(u64, u8, Option<Vec<u8>>))> = None;
            for (cts, e) in &rev {
                let (_, wt, _) = **e;
                if *cts > limit_ts {
                    continue; // committed after as_of: not visible yet
                }
                match wt {
                    WRITE_TYPE_PUT => {
                        picked = Some((*cts, *e));
                        break;
                    }
                    WRITE_TYPE_DELETE | WRITE_TYPE_ROLLBACK | WRITE_TYPE_NONE => break,
                    _ => continue, // lock-like records: look further back
                }
            }
            let (commit_ts, (start_ts, wt, short_value)) = match picked {
                Some(x) => x,
                None => continue,
            };
            if *wt != WRITE_TYPE_PUT {
                continue;
            }
            let payload = match short_value {
                Some(sv) => sv.clone(),
                None => match self.defaults.get(&(uk.clone(), *start_ts)) {
                    Some(p) => p.clone(),
                    None => continue, // value physically gone — cannot decode
                },
            };
            out.push(Recovered {
                user_key: uk.clone(),
                commit_ts,
                start_ts: *start_ts,
                payload,
                source: String::new(),
            });
        }
        out
    }

    pub fn raw_versions(
        &self,
        prefix: Option<&[u8]>,
    ) -> Vec<(Vec<u8>, u64, u8, u64, Option<&Vec<u8>>)> {
        let mut out = Vec::new();
        for ((uk, commit_ts), (start_ts, wt, sv)) in &self.writes {
            if let Some(p) = prefix {
                if !uk.starts_with(p) {
                    continue;
                }
            }
            out.push((uk.clone(), *commit_ts, *wt, *start_ts, sv.as_ref()));
        }
        out
    }

    pub fn default_payload(&self, uk: &[u8], start_ts: u64) -> Option<&Vec<u8>> {
        self.defaults.get(&(uk.to_vec(), start_ts))
    }

    /// Fallback recovery from direct puts (unknown write-value format):
    /// returns one Recovered per distinct payload per key. The payload passed
    /// to the caller is the WHOLE write value; the caller extracts the row.
    pub fn assemble_direct(&self, prefix: Option<&[u8]>) -> Vec<Recovered> {
        let mut out = Vec::new();
        for (uk, payloads) in &self.direct {
            if let Some(p) = prefix {
                if !uk.starts_with(p) {
                    continue;
                }
            }
            for (i, pv) in payloads.iter().enumerate() {
                out.push(Recovered {
                    user_key: uk.clone(),
                    commit_ts: 0,
                    start_ts: 0,
                    payload: pv.clone(),
                    source: format!("direct#{i}"),
                });
            }
        }
        out
    }
}

/// Open a TiKV kvdb (data/db dir) fully read-only, tolerating a non-graceful
/// shutdown and stale WALs.
pub fn open_read_only(path: &Path) -> Result<DB, String> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_max_open_files(256);
    opts.set_skip_stats_update_on_db_open(true);
    let cf_names = DB::list_cf(&opts, path).map_err(|e| format!("list_cf: {e}"))?;
    let cfs: Vec<&str> = cf_names.iter().map(|s| s.as_str()).collect();
    DB::open_cf_for_read_only(&opts, path, cfs, false)
        .map_err(|e| format!("open read-only: {e} (is TiKV still running on this dir? copy the data dir instead)"))
}

/// Scan a CF for a key prefix, feeding entries (USER key + optional value;
/// None/empty value = RocksDB tombstone). Handles both raw-key stores and
/// TiKV's `z`+memcomparable wrapped stores.
pub fn scan_cf_prefix(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    prefix: &[u8],
    mut on_entry: impl FnMut(&[u8], Option<&[u8]>),
) -> Result<(), String> {
    use crate::keycodec::{decode_z_parts, encode_z_key};
    let mut it = db.raw_iterator_cf_opt(cf, ReadOptions::default());
    // 数据目录可能同时存在裸 key（'t'/'m' 开头）和 z 编码 key（0x7a 开头，
    // 排序在裸 key 之后）：从两者中较小的 seek 点开始，混合迭代。
    let raw_seek = prefix.to_vec();
    let z_seek = encode_z_key(prefix);
    let seek_from = if z_seek < raw_seek { z_seek } else { raw_seek };
    it.seek(&seek_from);
    let dbg = std::env::var("TODU_DEBUG").is_ok();
    let mut iter_n = 0usize;
    while it.valid() {
        let Some(k) = it.key() else { break };
        let key = k.to_vec();
        let val = it.value().map(|v| v.to_vec());
        let mut hit = false;
        let mut brk = false;
        if key.first() == Some(&b'z') {
            match decode_z_parts(&key) {
                Some((user_key, rest)) => {
                    // user key 不含 mvcc ts（ts 在编码块之后的 8 字节里），
                    // 拼回去保持下游 split_mvcc_ts 语义
                    let mut full = user_key;
                    full.extend_from_slice(&rest[..rest.len().min(8)]);
                    if full.starts_with(prefix) {
                        hit = true;
                        on_entry(&full, val.as_deref().filter(|v| !v.is_empty()));
                    } else if full.as_slice() > prefix {
                        // 已越过前缀范围（z 空间内解码序 = 存储序）
                        brk = true;
                    }
                }
                None => { /* 无法解码的内部 key，跳过 */ }
            }
        } else if key.starts_with(prefix) {
            hit = true;
            on_entry(&key, val.as_deref().filter(|v| !v.is_empty()));
        } else if key.as_slice() > prefix {
            brk = true;
        }
        if dbg {
            eprintln!("[scan] n={iter_n} key0={:02x} len={} uk_head={} hit={hit} brk={brk}",
                key[0], key.len(),
                if key.first() == Some(&b'z') {
                    decode_z_parts(&key).map(|(u, _)| hex::encode(&u[..u.len().min(8)])).unwrap_or_else(|| "BAD".into())
                } else {
                    hex::encode(&key[..key.len().min(8)])
                });
        }
        if brk { break; }
        iter_n += 1;
        it.next();
    }
    let _ = iter_n;
    let st = it.status();
    if let Err(e) = st {
        return Err(format!("iterator error: {e}"));
    }
    Ok(())
}

/// Forensic mode: read one SST file's raw entries with our pure-Rust SST
/// parser, bypassing tombstones that live in OTHER files. Entries shadowed by
/// newer tombstones elsewhere are still returned here — that is the point.
///
/// cf_hint: TiKV SSTs on disk belong to a CF, but a bare table file doesn't
/// say which. "auto" classifies by value shape; force with "write"/"default".
pub fn read_sst_file(
    path: &Path,
    cf_hint: Option<&str>,
    c: &mut Collector,
) -> Result<(usize, usize), String> {
    let reader = crate::sst::SstReader::open(path)?;
    let mut n_write = 0usize;
    let mut n_default = 0usize;
    let src = path.to_string_lossy().to_string();
    reader.iterate(|internal_key, value| {
        step_raw_entry(internal_key, Some(value), cf_hint, c, &src, &mut n_write, &mut n_default);
        true
    })?;
    Ok((n_write, n_default))
}

fn step_raw_entry(
    key: &[u8],
    value: Option<&[u8]>,
    cf_hint: Option<&str>,
    c: &mut Collector,
    src: &str,
    n_write: &mut usize,
    n_default: &mut usize,
) {
    use crate::keycodec::{decode_z_parts, parse_internal_suffix, KTYPE_DELETION, KTYPE_VALUE};
    let (internal_uk, _seq, vtype) = match parse_internal_suffix(key) {
        Some(x) => x,
        None => (key, 0, KTYPE_VALUE),
    };
    let value = match value { Some(v) => v, None => return };
    // stored = z + ENC(user_key) + ts(8B)：解出 user_key 与 mvcc ts
    let (uk, commit_or_start_ts) = if internal_uk.first() == Some(&b'z') {
        match decode_z_parts(internal_uk) {
            Some((d, rest)) if rest.len() >= 8 => (d, u64::from_be_bytes(rest[..8].try_into().unwrap())),
            Some((d, _)) => (d, 0),
            None => return,
        }
    } else {
        match split_mvcc_ts(internal_uk) {
            Some((d, ts)) => (d.to_vec(), ts),
            None => return,
        }
    };
    let _ = src;
    if value.is_empty() || vtype == KTYPE_DELETION {
        c.stat_write_tombstones += 1;
        return;
    }
    let force_write = cf_hint == Some("write");
    let force_default = cf_hint == Some("default");
    let is_write = force_write
        || (!force_default && {
            // row format v2 payloads start with 0x80, which write types never do
            if value[0] == 0x80 {
                false
            } else {
                looks_like_write_value(value)
            }
        });
    if is_write {
        let before = c.stat_write_entries;
        c.add_write_physical_key(&uk, commit_or_start_ts, value);
        if c.stat_write_entries > before {
            *n_write += 1;
        }
    } else {
        let before = c.stat_default_entries;
        c.add_default_physical_key(&uk, commit_or_start_ts, value);
        if c.stat_default_entries > before {
            *n_default += 1;
        }
    }
}
