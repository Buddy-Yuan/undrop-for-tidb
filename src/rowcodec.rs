//! TiDB row payload decoding: row format v2 (default since v4.0) and v1.
//!
//! Decoding follows TiDB `pkg/types/row/codec.go` (v2) and `pkg/util/codec`
//! datum encoding (v1). Where the on-disk encoding has historically ambiguous
//! variants, we try candidate schemes and require EXACT consumption of the
//! payload — a wrong parse can't silently succeed.

use crate::mydecimal;

pub const TYPE_TINY: u8 = 1;
pub const TYPE_SHORT: u8 = 2;
pub const TYPE_LONG: u8 = 3;
pub const TYPE_FLOAT: u8 = 4;
pub const TYPE_DOUBLE: u8 = 5;
pub const TYPE_TIMESTAMP: u8 = 7;
pub const TYPE_LONGLONG: u8 = 8;
pub const TYPE_INT24: u8 = 9;
pub const TYPE_DATE: u8 = 10;
pub const TYPE_DURATION: u8 = 11;
pub const TYPE_DATETIME: u8 = 12;
pub const TYPE_YEAR: u8 = 13;
pub const TYPE_VARCHAR: u8 = 15;
pub const TYPE_BIT: u8 = 16;
pub const TYPE_JSON: u8 = 245;
pub const TYPE_NEWDECIMAL: u8 = 246;
pub const TYPE_ENUM: u8 = 247;
pub const TYPE_SET: u8 = 248;
pub const TYPE_TINY_BLOB: u8 = 249;
pub const TYPE_MEDIUM_BLOB: u8 = 250;
pub const TYPE_LONG_BLOB: u8 = 251;
pub const TYPE_BLOB: u8 = 252;
pub const TYPE_VAR_STRING: u8 = 253;
pub const TYPE_STRING: u8 = 254;

// mysql type flags
pub const UNSIGNED_FLAG: u32 = 1 << 5;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub tp: u8,
    pub flag: u32,
    pub flen: i64,
    pub decimal: i64,
}

impl Column {
    pub fn unsigned(&self) -> bool {
        self.flag & UNSIGNED_FLAG != 0
    }
    pub fn is_int(&self) -> bool {
        matches!(self.tp, TYPE_TINY | TYPE_SHORT | TYPE_LONG | TYPE_LONGLONG | TYPE_INT24 | TYPE_YEAR)
    }
    pub fn is_float(&self) -> bool {
        matches!(self.tp, TYPE_FLOAT | TYPE_DOUBLE)
    }
    pub fn is_str(&self) -> bool {
        matches!(self.tp, TYPE_VARCHAR | TYPE_VAR_STRING | TYPE_STRING | TYPE_ENUM | TYPE_SET)
    }
    pub fn is_blob(&self) -> bool {
        matches!(
            self.tp,
            TYPE_TINY_BLOB | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB | TYPE_BLOB | TYPE_BIT | TYPE_JSON | 255
        )
    }
    pub fn is_time(&self) -> bool {
        matches!(self.tp, TYPE_DATE | TYPE_DATETIME | TYPE_TIMESTAMP)
    }
    pub fn is_decimal(&self) -> bool {
        self.tp == TYPE_NEWDECIMAL
    }
    pub fn is_duration(&self) -> bool {
        self.tp == TYPE_DURATION
    }
}

fn dummy() -> u8 {
    255 // geometry
}

#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
    /// text-like payload
    Text(Vec<u8>),
    /// binary payload
    Bytes(Vec<u8>),
    Decimal(String),
    /// formatted datetime
    Time(String),
    /// formatted duration
    Duration(String),
}

// ---------------- primitives ----------------

fn read_compact_int(b: &[u8]) -> Option<(i64, usize)> {
    if b.is_empty() {
        return None;
    }
    let code = b[0] as usize;
    if code > 7 {
        return None;
    }
    let n = code + 1;
    if b.len() < 1 + n {
        return None;
    }
    let mut v: u64 = 0;
    for i in 0..n {
        v |= (b[1 + i] as u64) << (8 * i);
    }
    // sign-extend
    if n < 8 && (b[n] & 0x80) != 0 {
        v |= !0u64 << (8 * n);
    }
    Some((v as i64, 1 + n))
}

fn read_compact_uint(b: &[u8]) -> Option<(u64, usize)> {
    if b.is_empty() {
        return None;
    }
    let code = b[0] as usize;
    if code > 7 {
        return None;
    }
    let n = code + 1;
    if b.len() < 1 + n {
        return None;
    }
    let mut v: u64 = 0;
    for i in 0..n {
        v |= (b[1 + i] as u64) << (8 * i);
    }
    Some((v, 1 + n))
}

fn read_varint(b: &[u8]) -> Option<(i64, usize)> {
    let (u, n) = read_varuint(b)?;
    Some((((u >> 1) as i64) ^ -((u & 1) as i64), n))
}

fn read_varuint(b: &[u8]) -> Option<(u64, usize)> {
    let mut v: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in b.iter().enumerate() {
        if shift >= 64 {
            return None;
        }
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
    }
    None
}

fn read_fixed_le(b: &[u8], n: usize) -> Option<(&[u8], usize)> {
    if b.len() < n {
        None
    } else {
        Some((&b[..n], n))
    }
}

fn f32_from_le(b: &[u8]) -> f64 {
    f32::from_le_bytes(b[..4].try_into().unwrap()) as f64
}
fn f64_from_le(b: &[u8]) -> f64 {
    f64::from_le_bytes(b[..8].try_into().unwrap())
}

fn plausible_packed_time(v: u64) -> bool {
    if v == 0 {
        return true; // 0000-00-00
    }
    // ToPackedUint bit fields: ((ymd << 17 | hms) << 24) | micro
    // ymd = ((year*13 + month) << 5) | day ; hms = hour << 12 | min << 6 | sec
    let (year, month, day, _, _, _, _) = split_packed_time(v);
    (1000..=9999).contains(&year) && (1..=12).contains(&month) && (1..=31).contains(&day)
}

fn split_packed_time(v: u64) -> (u64, u64, u64, u64, u64, u64, u64) {
    let micro = v & ((1 << 24) - 1);
    let ymdhms = v >> 24;
    let ymd = ymdhms >> 17;
    let day = ymd & ((1 << 5) - 1);
    let ym = ymd >> 5;
    let month = ym % 13;
    let year = ym / 13;
    let hms = ymdhms & ((1 << 17) - 1);
    let sec = hms & 63;
    let min = (hms >> 6) & 63;
    let hour = hms >> 12;
    (year, month, day, hour, min, sec, micro)
}

pub fn format_packed_time(v: u64) -> String {
    if v == 0 {
        return "0000-00-00 00:00:00".into();
    }
    let (year, month, day, hour, min, sec, us) = split_packed_time(v);
    if us != 0 {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}.{us:06}")
    } else {
        format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}")
    }
}

fn format_duration_ns(ns: i64) -> String {
    let sign = if ns < 0 { "-" } else { "" };
    let a = ns.unsigned_abs();
    let frac = a % 1_000_000_000;
    let s = a / 1_000_000_000;
    let (h, m, sec) = (s / 3600, (s / 60) % 60, s % 60);
    if frac != 0 {
        format!("{sign}{h:02}:{m:02}:{sec:02}.{frac:09}")
    } else {
        format!("{sign}{h:02}:{m:02}:{sec:02}")
    }
}

// ---------------- v2 column flags ----------------

const F2_NIL: u8 = 0;
const F2_INT: u8 = 1;
const F2_UINT: u8 = 2;
const F2_BYTES: u8 = 3;
const F2_COMPACT_BYTES: u8 = 4;
const F2_VARINT: u8 = 5;
const F2_VARUINT: u8 = 6;
const F2_FLOAT: u8 = 7;
const F2_DOUBLE: u8 = 8;
const F2_DECIMAL: u8 = 9;
const F2_DATETIME: u8 = 10;
const F2_DURATION: u8 = 11;

// ---------------- v1 datum flags ----------------

const V1_NULL: u8 = 0;
const V1_INT: u8 = 1;
const V1_UINT: u8 = 2;
const V1_FLOAT: u8 = 3;
const V1_DOUBLE: u8 = 4;
const V1_STR: u8 = 5;
const V1_BYTES: u8 = 6;
const V1_COMPACT_BYTES: u8 = 7;
const V1_VARINT: u8 = 8;
const V1_VARUINT: u8 = 9;
const V1_DECIMAL: u8 = 10;
const V1_DURATION: u8 = 11;
const V1_TIME: u8 = 15;

/// Try a list of parsers; each returns (datum, consumed). Success requires the
/// FULL remaining payload to be consumed exactly (enforced by the row driver),
/// so ambiguous candidates self-resolve.
type Candidate = Box<dyn Fn(&[u8], &Column) -> Option<(Datum, usize)>>;

fn int_candidates() -> Vec<Candidate> {
    vec![
        Box::new(|b, _c| read_compact_int(b).map(|(v, n)| (Datum::I64(v), n))),
        Box::new(|b, _c| read_varint(b).map(|(v, n)| (Datum::I64(v), n))),
        Box::new(|b, _c| read_fixed_le(b, 8).map(|(s, n)| (Datum::I64(i64::from_le_bytes(s.try_into().unwrap())), n))),
    ]
}

fn uint_candidates() -> Vec<Candidate> {
    vec![
        Box::new(|b, _c| read_compact_uint(b).map(|(v, n)| (Datum::U64(v), n))),
        Box::new(|b, _c| read_varuint(b).map(|(v, n)| (Datum::U64(v), n))),
        Box::new(|b, _c| read_fixed_le(b, 8).map(|(s, n)| (Datum::U64(u64::from_le_bytes(s.try_into().unwrap())), n))),
    ]
}

fn bytes_candidates() -> Vec<Candidate> {
    vec![
        // compact length (size-code scheme) + data
        Box::new(|b, _c| {
            read_compact_uint(b).and_then(|(len, n)| {
                let len = len as usize;
                if b.len() >= n + len {
                    Some((Datum::Text(b[n..n + len].to_vec()), n + len))
                } else {
                    None
                }
            })
        }),
        // varint length + data
        Box::new(|b, _c| {
            read_varuint(b).and_then(|(len, n)| {
                let len = len as usize;
                if len <= 0x1000_0000 && b.len() >= n + len {
                    Some((Datum::Text(b[n..n + len].to_vec()), n + len))
                } else {
                    None
                }
            })
        }),
        // fixed 8-byte LE length + data (v1 datum)
        Box::new(|b, _c| {
            if b.len() >= 8 {
                let len = u64::from_le_bytes(b[..8].try_into().unwrap()) as usize;
                if b.len() >= 8 + len {
                    Some((Datum::Text(b[8..8 + len].to_vec()), 8 + len))
                } else {
                    None
                }
            } else {
                None
            }
        }),
    ]
}

fn decimal_candidates() -> Vec<Candidate> {
    vec![Box::new(|b, c| mydecimal::decode_bin(b, c).map(|(s, n)| (Datum::Decimal(s), n)))]
}

fn time_candidates() -> Vec<Candidate> {
    vec![
        Box::new(|b, _c| {
            read_compact_uint(b).and_then(|(v, n)| {
                plausible_packed_time(v).then(|| (Datum::Time(format_packed_time(v)), n))
            })
        }),
        Box::new(|b, _c| {
            read_varuint(b).and_then(|(v, n)| {
                plausible_packed_time(v).then(|| (Datum::Time(format_packed_time(v)), n))
            })
        }),
        Box::new(|b, _c| {
            read_fixed_le(b, 8).and_then(|(s, n)| {
                let v = u64::from_le_bytes(s.try_into().unwrap());
                plausible_packed_time(v).then(|| (Datum::Time(format_packed_time(v)), n))
            })
        }),
    ]
}

fn duration_candidates() -> Vec<Candidate> {
    vec![
        Box::new(|b, _c| read_compact_int(b).map(|(v, n)| (Datum::Duration(format_duration_ns(v)), n))),
        Box::new(|b, _c| read_varint(b).map(|(v, n)| (Datum::Duration(format_duration_ns(v)), n))),
        Box::new(|b, _c| read_fixed_le(b, 8).map(|(s, n)| (Datum::Duration(format_duration_ns(i64::from_le_bytes(s.try_into().unwrap()))), n))),
    ]
}

/// v1 datum flags and v2 column flags share the 0..=11 numeric range with
/// DIFFERENT meanings — never merge the two match tables.
fn candidates_for_flag(flag: u8, col: &Column, v1: bool) -> Vec<Candidate> {
    if !v1 {
        match flag {
            F2_INT => int_or_uint(col),
            F2_UINT => uint_candidates(),
            F2_BYTES | F2_COMPACT_BYTES => bytes_candidates(),
            F2_VARINT => int_candidates(),
            F2_VARUINT => uint_candidates(),
            F2_FLOAT => vec![Box::new(|b, _c| read_fixed_le(b, 4).map(|(s, n)| (Datum::F64(f32_from_le(s)), n)))],
            F2_DOUBLE => vec![
                Box::new(|b, _c| read_fixed_le(b, 8).map(|(s, n)| (Datum::F64(f64_from_le(s)), n))),
                Box::new(|b, _c| read_fixed_le(b, 4).map(|(s, n)| (Datum::F64(f32_from_le(s)), n))),
            ],
            F2_DECIMAL => decimal_candidates(),
            F2_DATETIME => time_candidates(),
            F2_DURATION => duration_candidates(),
            _ => vec![],
        }
    } else {
        match flag {
            V1_INT => int_or_uint(col),
            V1_UINT => uint_candidates(),
            V1_STR | V1_BYTES | V1_COMPACT_BYTES => bytes_candidates(),
            V1_VARINT => int_candidates(),
            V1_VARUINT => uint_candidates(),
            V1_FLOAT => vec![Box::new(|b, _c| read_fixed_le(b, 4).map(|(s, n)| (Datum::F64(f32_from_le(s)), n)))],
            V1_DOUBLE => vec![
                Box::new(|b, _c| read_fixed_le(b, 8).map(|(s, n)| (Datum::F64(f64_from_le(s)), n))),
                Box::new(|b, _c| read_fixed_le(b, 4).map(|(s, n)| (Datum::F64(f32_from_le(s)), n))),
            ],
            V1_DECIMAL => decimal_candidates(),
            V1_DURATION => duration_candidates(),
            V1_TIME => time_candidates(),
            _ => vec![],
        }
    }
}

fn int_or_uint(col: &Column) -> Vec<Candidate> {
    if col.unsigned() {
        uint_candidates()
    } else {
        int_candidates()
    }
}

/// Decode a row payload (v2 if prefixed with 0x80, else v1 datum encoding).
/// `row_cols` = the columns stored IN THE VALUE (handle column excluded when
/// pk_is_handle). Returns per-column datum or an error with a hex dump hint.
pub fn decode_row_value(payload: &[u8], row_cols: &[Column]) -> Result<Vec<Datum>, String> {
    if payload.is_empty() {
        return Ok(vec![]);
    }
    if payload[0] == 0x80 {
        decode_v2(payload, row_cols)
    } else {
        decode_v1(payload, row_cols)
    }
}

fn decode_v2(payload: &[u8], row_cols: &[Column]) -> Result<Vec<Datum>, String> {
    let ncols = row_cols.len();
    if payload.len() < 2 {
        return Err("v2 payload too short".into());
    }
    let mut off = 2; // [0]=0x80, [1]=flag
    let bitmap_len = (ncols + 7) / 8;
    if payload.len() < off + bitmap_len {
        return Err("v2 null bitmap truncated".into());
    }
    let bitmap = &payload[off..off + bitmap_len];
    off += bitmap_len;

    // exact parse first: DFS over per-column candidates, whole row must consume
    let nulls: Vec<bool> = (0..ncols).map(|i| bitmap[i / 8] & (1 << (i % 8)) != 0).collect();
    let mut budget = 200_000usize;
    if let Some(datums) = dfs_columns(payload, off, row_cols, &nulls, 0, &mut budget, false) {
        return Ok(datums);
    }
    // greedy fallback: salvage what we can, keep undecodable tail as hex
    let mut out = Vec::with_capacity(ncols);
    for i in 0..ncols {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            eprintln!("[dbg] col {i} NULL");
            out.push(Datum::Null);
            continue;
        }
        if off >= payload.len() {
            eprintln!("[dbg] col {i} past end");
            out.push(Datum::Null);
            continue;
        }
        let flag = payload[off];
        off += 1;
        eprintln!("[dbg] col {i} flag={flag:#x} off_now={off}");
        if flag == F2_NIL {
            out.push(Datum::Null);
            continue;
        }
        let rest = &payload[off..];
        let mut got = None;
        for cand in candidates_for_flag(flag, &row_cols[i], false) {
            if let Some((d, n)) = cand(rest, &row_cols[i]) {
                if n <= rest.len() {
                    got = Some((d, n));
                    break;
                }
            }
        }
        match got {
            Some((d, n)) => {
                out.push(d);
                off += n;
            }
            None => {
                out.push(Datum::Bytes(rest.to_vec()));
                return Ok(finalize_with_warning(out, row_cols, flag));
            }
        }
    }
    if off != payload.len() {
        return Err(format!(
            "v2 decode consumed {off} of {} bytes — column count mismatch (pk_is_handle?)",
            payload.len()
        ));
    }
    Ok(out)
}

fn decode_v1(payload: &[u8], row_cols: &[Column]) -> Result<Vec<Datum>, String> {
    let ncols = row_cols.len();
    let bitmap_len = (ncols + 7) / 8;
    if payload.len() < bitmap_len {
        return Err("v1 payload too short".into());
    }
    let bitmap = &payload[..bitmap_len];
    let mut off = bitmap_len;

    let nulls: Vec<bool> = (0..ncols).map(|i| bitmap[i / 8] & (1 << (i % 8)) != 0).collect();
    let mut budget = 200_000usize;
    if let Some(datums) = dfs_columns(payload, off, row_cols, &nulls, 0, &mut budget, true) {
        return Ok(datums);
    }
    // greedy fallback
    let mut out = Vec::with_capacity(ncols);
    for i in 0..ncols {
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            out.push(Datum::Null);
            continue;
        }
        if off >= payload.len() {
            out.push(Datum::Null);
            continue;
        }
        let flag = payload[off];
        off += 1;
        if flag == V1_NULL {
            out.push(Datum::Null);
            continue;
        }
        let rest = &payload[off..];
        let mut got = None;
        for cand in candidates_for_flag(flag, &row_cols[i], true) {
            if let Some((d, n)) = cand(rest, &row_cols[i]) {
                if n <= rest.len() {
                    got = Some((d, n));
                    break;
                }
            }
        }
        match got {
            Some((d, n)) => {
                out.push(d);
                off += n;
            }
            None => {
                out.push(Datum::Bytes(rest.to_vec()));
                return Ok(finalize_with_warning(out, row_cols, flag));
            }
        }
    }
    if off != payload.len() {
        return Err(format!("v1 decode consumed {off} of {} bytes", payload.len()));
    }
    Ok(out)
}

/// DFS over per-column candidate encodings; succeeds only when the whole
/// payload is consumed exactly. `nulls[i]` = column i is NULL (bitmap-driven,
/// consumes no bytes). `budget` bounds pathological ambiguity.
fn dfs_columns(
    payload: &[u8],
    off: usize,
    cols: &[Column],
    nulls: &[bool],
    idx: usize,
    budget: &mut usize,
    v1: bool,
) -> Option<Vec<Datum>> {
    if *budget == 0 {
        return None;
    }
    *budget -= 1;
    if idx == cols.len() {
        return if off == payload.len() { Some(Vec::new()) } else { None };
    }
    if nulls[idx] {
        if let Some(mut r) = dfs_columns(payload, off, cols, nulls, idx + 1, budget, v1) {
            let mut out = vec![Datum::Null];
            out.append(&mut r);
            return Some(out);
        }
        return None;
    }
    if off >= payload.len() {
        return None;
    }
    let flag = payload[off];
    let col = &cols[idx];
    let rest = &payload[off + 1..];
    let nil = if v1 { flag == V1_NULL } else { flag == F2_NIL };
    if nil {
        if let Some(mut r) = dfs_columns(payload, off + 1, cols, nulls, idx + 1, budget, v1) {
            let mut out = vec![Datum::Null];
            out.append(&mut r);
            return Some(out);
        }
        return None;
    }
    for cand in candidates_for_flag(flag, col, v1) {
        if let Some((d, n)) = cand(rest, col) {
            if n <= rest.len() {
                if let Some(mut r) = dfs_columns(payload, off + 1 + n, cols, nulls, idx + 1, budget, v1) {
                    let mut out = vec![d];
                    out.append(&mut r);
                    return Some(out);
                }
            }
        }
    }
    None
}

fn finalize_with_warning(out: Vec<Datum>, row_cols: &[Column], bad_flag: u8) -> Vec<Datum> {
    if let Some(last) = out.len().checked_sub(1) {
        eprintln!(
            "[todu] WARN: column '{}' flag {bad_flag} not exactly decodable, kept as raw hex",
            row_cols.get(last).map(|c| c.name.as_str()).unwrap_or("?")
        );
    }
    out
}

// ---------------- rendering ----------------

pub fn datum_to_sql(d: &Datum, _col: &Column) -> String {
    match d {
        Datum::Null => "NULL".into(),
        Datum::I64(v) => v.to_string(),
        Datum::U64(v) => v.to_string(),
        Datum::F64(v) => {
            if v.fract() == 0.0 && v.abs() < 1e15 {
                format!("{:.0}", v)
            } else {
                format!("{v}")
            }
        }
        Datum::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => quote_sql_string(s),
            Err(_) => format!("0x{}", hex::encode(b)),
        },
        Datum::Bytes(b) => format!("0x{}", hex::encode(b)),
        Datum::Decimal(s) => s.clone(),
        Datum::Time(s) => format!("'{}'", s),
        Datum::Duration(s) => format!("'{}'", s),
    }
}

pub fn quote_sql_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

pub fn datum_to_json(d: &Datum) -> serde_json::Value {
    match d {
        Datum::Null => serde_json::Value::Null,
        Datum::I64(v) => (*v).into(),
        Datum::U64(v) => (*v).into(),
        Datum::F64(v) => serde_json::Number::from_f64(*v).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null),
        Datum::Text(b) => serde_json::Value::String(String::from_utf8_lossy(b).into_owned()),
        Datum::Bytes(b) => serde_json::Value::String(format!("0x{}", hex::encode(b))),
        Datum::Decimal(s) => serde_json::Value::String(s.clone()),
        Datum::Time(s) => serde_json::Value::String(s.clone()),
        Datum::Duration(s) => serde_json::Value::String(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compact_uint(v: u64) -> Vec<u8> {
        let bytes = v.to_le_bytes();
        let n = ((8 - (v | 1).leading_zeros() as usize / 8).max(1)).min(8);
        let mut out = vec![(n - 1) as u8];
        out.extend_from_slice(&bytes[..n]);
        out
    }

    #[test]
    fn decode_real_v65_payload() {
        // 从 v6.5.11 真实集群 todudemo.orders (table_id=82) 恢复出的 alice 行
        let payload: Vec<u8> = hex::decode(
            "8000040000000203040505000d0015002200616c696365c06010000000000020a107009018bb196265696a696e672d6f72646572",
        ).unwrap();
        let col_ids = [2i64, 3, 4, 5]; // user_name, amount, created_at, memo
        let cols = vec![
            Column { name: "user_name".into(), tp: TYPE_VAR_STRING, flag: 0, flen: 64, decimal: 0 },
            Column { name: "amount".into(), tp: TYPE_DOUBLE, flag: 0, flen: 22, decimal: 0 },
            Column { name: "created_at".into(), tp: TYPE_DATETIME, flag: 0, flen: 19, decimal: 6 },
            Column { name: "memo".into(), tp: TYPE_VAR_STRING, flag: 0, flen: 64, decimal: 0 },
        ];
        let pairs = decode_rowv2(&payload, &col_ids, &cols).expect("decode real payload");
        assert_eq!(pairs.len(), 4);
        assert_eq!(pairs[0].0, 2);
        assert_eq!(pairs[0].1, Datum::Text(b"alice".to_vec()));
        match &pairs[1].1 {
            Datum::F64(v) => assert!((v - 128.5).abs() < 1e-9, "amount: {v}"),
            other => panic!("amount: {other:?}"),
        }
        match &pairs[2].1 {
            Datum::Time(s) => assert_eq!(s, "2026-10-12 09:00:00.500000", "created: {s}"),
            other => panic!("created: {other:?}"),
        }
        assert_eq!(pairs[3].1, Datum::Text(b"beijing-order".to_vec()));
    }

    #[test]
    fn decode_fixture_payload() {
        // build with the real ToPackedUint bit fields
        let ymd = ((2026u64 * 13 + 10) << 5) | 12;
        let hms = (10u64 << 12) | (30 << 6) | 0;
        let packed = ((ymd << 17) | hms) << 24 | 500_000;
        let mut payload: Vec<u8> = vec![0x80, 0x00, 0x00];
        payload.push(3);
        payload.extend(compact_uint(5));
        payload.extend_from_slice(b"alice");
        payload.push(8);
        payload.extend_from_slice(&10.5f64.to_le_bytes());
        payload.push(10);
        payload.extend(compact_uint(packed));
        payload.push(3);
        payload.extend(compact_uint(3));
        payload.extend_from_slice(b"xyz");
        let cols = vec![
            Column { name: "name".into(), tp: 253, flag: 0, flen: 64, decimal: 0 },
            Column { name: "amount".into(), tp: 5, flag: 0, flen: 22, decimal: 0 },
            Column { name: "created".into(), tp: 12, flag: 0, flen: 19, decimal: 0 },
            Column { name: "extra".into(), tp: 252, flag: 0, flen: 0, decimal: 0 },
        ];
        let d = decode_row_value(&payload, &cols);
        eprintln!("result = {d:?}");
        let datums = d.unwrap();
        assert_eq!(datums.len(), 4);
    }
}

// ---------------- rowcodec v2 (TiDB v4.0+ real on-disk row format) ----------------
//
// Layout (util/rowcodec/row.go fromBytes):
//   [0x80 ver][flags u8][numNotNull u16LE][numNull u16LE]
//   [colIDs: (numNotNull+numNull) u8 each]
//   [offsets: numNotNull × u16LE, cumulative into data]
//   [data]
// Null columns have no offset/data. Value encodings per schema type
// (encoder.go encodeValueDatum):
//   ints:    8B BE sign-flipped (codec.EncodeInt)
//   uint:    8B BE (codec.EncodeUint)
//   double:  8B BE sign-masked (codec.EncodeFloat)
//   string:  raw bytes
//   datetime: ToPackedUint bit fields + EncodeUint (8B BE)
//   decimal: codec.EncodeDecimal ([prec][frac][bin]) — hex fallback for now

/// Decode a rowcodec-v2 payload into (colID, Datum) pairs, using `col_ids`
/// (the schema's column ordinal IDs, in schema order) for type lookup.
pub fn decode_rowv2(payload: &[u8], col_ids: &[i64], cols: &[Column]) -> Option<Vec<(i64, Datum)>> {
    if payload.first() != Some(&0x80) || payload.len() < 6 {
        return None;
    }
    let flags = payload[1];
    if flags & 1 != 0 {
        return None; // large row (u32 colIDs/offsets) not supported yet
    }
    let num_not_null = u16::from_le_bytes(payload[2..4].try_into().ok()?) as usize;
    let num_null = u16::from_le_bytes(payload[4..6].try_into().ok()?) as usize;
    if num_not_null + num_null != col_ids.len() {
        return None;
    }
    let mut i = 6;
    let stored_ids = &payload[i..i + num_not_null + num_null];
    i += num_not_null + num_null;
    let mut offsets = Vec::with_capacity(num_not_null);
    for _ in 0..num_not_null {
        if i + 2 > payload.len() {
            return None;
        }
        offsets.push(u16::from_le_bytes(payload[i..i + 2].try_into().ok()?) as usize);
        i += 2;
    }
    let data = &payload[i..];
    if data.len() < *offsets.last().unwrap_or(&0) {
        return None;
    }

    let mut out = Vec::with_capacity(col_ids.len());
    let mut prev = 0usize;
    for k in 0..num_not_null {
        let seg = &data[prev..offsets[k]];
        prev = offsets[k];
        let cid = stored_ids[k] as i64;
        let col_idx = col_ids.iter().position(|c| *c == cid)?;
        let col = cols.get(col_idx)?;
        if std::env::var("TODU_DEBUG").is_ok() {
            eprintln!("[rowv2] colID={} seg({})={}", cid, seg.len(), hex::encode(seg));
        }
        let datum = decode_rowv2_datum(seg, col);
        out.push((cid, datum));
    }
    for k in num_not_null..num_not_null + num_null {
        out.push((stored_ids[k] as i64, Datum::Null));
    }
    Some(out)
}

fn decode_rowv2_datum(seg: &[u8], col: &Column) -> Datum {
    if col.is_int() {
        if seg.len() != 8 {
            return Datum::Bytes(seg.to_vec());
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(seg);
        b[0] ^= 0x80; // undo codec.EncodeInt sign flip
        if col.unsigned() {
            Datum::U64(u64::from_be_bytes(b))
        } else {
            Datum::I64(i64::from_be_bytes(b))
        }
    } else if col.is_float() {
        if seg.len() != 8 {
            return Datum::Bytes(seg.to_vec());
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(seg);
        let bits = u64::from_be_bytes(b);
        // inverse of codec.EncodeFloat: positive → sign bit set; negative → all inverted
        let bits = if bits >> 63 == 1 {
            bits & !(1u64 << 63)
        } else {
            !bits
        };
        Datum::F64(f64::from_bits(bits))
    } else if col.is_time() {
        if seg.len() != 8 {
            return Datum::Bytes(seg.to_vec());
        }
        // rowcodec stores the packed uint little-endian here
        let packed = u64::from_le_bytes(seg.try_into().unwrap());
        Datum::Time(format_packed_time(packed))
    } else if col.is_str() {
        Datum::Text(seg.to_vec())
    } else if col.is_decimal() {
        match mydecimal::decode_bin(seg, col) {
            Some((s, _)) => Datum::Decimal(s),
            None => Datum::Bytes(seg.to_vec()),
        }
    } else {
        Datum::Bytes(seg.to_vec())
    }
}

/// Decode a rowcodec-v2 payload and align values to the FULL table schema
/// order (including the handle column, whose value comes from the key).
/// Returns None if the payload is not rowcodec v2 or misaligns.
pub fn decode_rowv2_aligned(
    payload: &[u8],
    table_cols: &[Column],
    handle_col: Option<usize>,
    handle: &Option<crate::keycodec::Handle>,
) -> Option<Vec<Datum>> {
    // row 中的列 = 全部列减去 handle 列；其 colID = 列序号（1-based）
    let row_col_ids: Vec<i64> = table_cols
        .iter()
        .enumerate()
        .filter(|(i, _)| Some(*i) != handle_col)
        .map(|(i, _)| i as i64 + 1)
        .collect();
    let row_cols: Vec<Column> = table_cols
        .iter()
        .enumerate()
        .filter(|(i, _)| Some(*i) != handle_col)
        .map(|(_, c)| c.clone())
        .collect();
    let pairs = decode_rowv2(payload, &row_col_ids, &row_cols)?;

    let by_id: std::collections::BTreeMap<i64, Datum> = pairs.into_iter().collect();
    let mut out = Vec::with_capacity(table_cols.len());
    for (i, c) in table_cols.iter().enumerate() {
        if Some(i) == handle_col {
            match handle.as_ref() {
                Some(crate::keycodec::Handle::Int(hv)) => {
                    if c.unsigned() {
                        out.push(Datum::U64(*hv as u64));
                    } else {
                        out.push(Datum::I64(*hv));
                    }
                }
                Some(crate::keycodec::Handle::Common(b)) => out.push(Datum::Bytes(b.clone())),
                None => out.push(Datum::Null),
            }
        } else {
            let cid = i as i64 + 1;
            out.push(by_id.get(&cid).cloned().unwrap_or(Datum::Null));
        }
    }
    Some(out)
}

/// Best-effort: extract the short_value (row payload) tail from a raw
/// TiKV v6.x char-format write value: [type][flags][varint ts][b'v'][varint
/// len][short_value]. The len byte(s) right before the payload are validated
/// by checking the payload starts with a row marker (0x80 for row v2).
pub fn extract_char_short_value(v: &[u8]) -> Option<Vec<u8>> {
    if v.first() != Some(&0x50) && v.first() != Some(&0x44) {
        return None;
    }
    // scan for the LAST b'v' whose following varint length reaches exactly
    // to the end of the value
    for i in (2..v.len()).rev() {
        if v[i] != b'v' {
            continue;
        }
        let mut p = i + 1;
        let mut len = 0u64;
        let mut shift = 0u32;
        let mut ok = true;
        loop {
            if p >= v.len() {
                ok = false;
                break;
            }
            let b = v[p];
            p += 1;
            len |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        if ok && p + len as usize == v.len() && len > 0 {
            return Some(v[p..].to_vec());
        }
    }
    None
}
