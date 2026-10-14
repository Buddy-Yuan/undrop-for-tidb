//! TiKV MVCC record parsing (write CF value format, version assembly).

pub const WRITE_TYPE_NONE: u8 = 0;
pub const WRITE_TYPE_PUT: u8 = 1;
pub const WRITE_TYPE_DELETE: u8 = 2;
pub const WRITE_TYPE_ROLLBACK: u8 = 3;
pub const WRITE_TYPE_LOCK: u8 = 4;

/// A parsed write-CF record.
#[derive(Debug, Clone)]
pub struct WriteRecord {
    pub write_type: u8,
    pub start_ts: u64,
    pub short_value: Option<Vec<u8>>,
}

fn read_leb128(v: &[u8], i: &mut usize) -> Option<u64> {
    let mut val: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *i >= v.len() || shift > 63 {
            return None;
        }
        let b = v[*i];
        *i += 1;
        val |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(val);
        }
        shift += 7;
    }
}

/// TiKV v6.x "char-type" write value: [b'P'|b'D'|b'R'][flags u8]
/// [varint start_ts][when Put: b'v' + varint len + short_value].
/// (Verified against live v6.5 clusters: type chars P/D/R, flags bit3 =
/// has_short_value, start_ts LEB128 with plausible TSO value.)
fn parse_write_value_char(v: &[u8]) -> Option<WriteRecord> {
    if v.len() < 4 {
        return None;
    }
    let _unused_marker = 0;
    let write_type = match v[0] {
        0x50 => WRITE_TYPE_PUT,
        0x44 => WRITE_TYPE_DELETE,
        0x52 => WRITE_TYPE_ROLLBACK,
        _ => return None,
    };
    let mut i = 2;
    let start_ts = read_leb128(v, &mut i)?;
    // start_ts sanity: TSO physical time in 2020..2100
    let phys_days = (start_ts >> 18) / 1000 / 86400;
    if !(18262..=47482).contains(&phys_days) {
        return None;
    }
    let mut short_value = None;
    if write_type == WRITE_TYPE_PUT && i < v.len() && v[i] == b'v' {
        i += 1;
        let len = read_leb128(v, &mut i)? as usize;
        if i + len > v.len() {
            return None;
        }
        short_value = Some(v[i..i + len].to_vec());
    }
    Some(WriteRecord { write_type, start_ts, short_value })
}

/// Parse the value of a write-CF entry.
/// Modern format: [type(1)][start_ts(8 BE)][flags(1) when present][short_value]
/// Old format (very early TiKV): [type(1)][start_ts(8)][short_value] with no flags byte.
/// If the value doesn't look like a write record at all, returns None.
pub fn parse_write_value(v: &[u8]) -> Option<WriteRecord> {
    // TiKV v6.x char-type format ('P'/'D'/'R' + varint ts) — try first
    if matches!(v.first(), Some(0x50) | Some(0x44) | Some(0x52)) {
        if let Some(w) = parse_write_value_char(v) {
            return Some(w);
        }
    }
    if v.len() < 9 {
        return None;
    }
    let write_type = v[0];
    if write_type > WRITE_TYPE_LOCK {
        return None;
    }
    let start_ts = u64::from_be_bytes(v[1..9].try_into().unwrap());
    let rest = &v[9..];
    let short_value = if rest.is_empty() {
        None
    } else if rest[0] == 0 || rest[0] == 1 {
        // modern format: first rest byte is flags, bit0 = has short value
        if rest[0] & 0x01 != 0 && rest.len() > 1 {
            Some(rest[1..].to_vec())
        } else {
            None
        }
    } else {
        // old format: short_value stored directly
        Some(rest.to_vec())
    };
    Some(WriteRecord { write_type, start_ts, short_value })
}

/// Does this (key-suffixed) entry look like a write-CF record?
pub fn looks_like_write_value(v: &[u8]) -> bool {
    // v6.x char-type format
    if matches!(v.first(), Some(0x50) | Some(0x44) | Some(0x52)) && parse_write_value_char(v).is_some() {
        return true;
    }
    if v.len() < 9 {
        return false;
    }
    if v[0] > WRITE_TYPE_LOCK {
        return false;
    }
    // start_ts sanity: nonzero and plausible (TSO physical ms since epoch <<18)
    let ts = u64::from_be_bytes(v[1..9].try_into().unwrap());
    if ts == 0 {
        return false;
    }
    let phys_ms = ts >> 18;
    (2020..=2100).contains(&(phys_ms / 1000 / 3600 / 24 / 365 + 1970))
}

pub fn write_type_name(t: u8) -> &'static str {
    match t {
        WRITE_TYPE_NONE => "None",
        WRITE_TYPE_PUT => "Put",
        WRITE_TYPE_DELETE => "Delete",
        WRITE_TYPE_ROLLBACK => "Rollback",
        WRITE_TYPE_LOCK => "Lock",
        _ => "?",
    }
}

/// Convert a wall-clock datetime string to a TiDB TSO (physical ms << 18).
pub fn datetime_to_tso(s: &str) -> Option<u64> {
    let s = s.trim();
    // accept "2026-10-12 10:00:00" or epoch-ms integer
    if let Ok(ms) = s.parse::<u64>() {
        return Some(ms << 18);
    }
    let fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d"];
    for f in fmts {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, f) {
            return Some((naive_to_millis(dt) as u64) << 18);
        }
        if let Ok(d) = chrono::NaiveDate::parse_from_str(s, f) {
            if let Some(dt) = d.and_hms_opt(0, 0, 0) {
                return Some((naive_to_millis(dt) as u64) << 18);
            }
        }
    }
    None
}

fn naive_to_millis(dt: chrono::NaiveDateTime) -> i64 {
    use chrono::TimeZone;
    #[allow(unused_imports)]
    // wall-clock input is in the machine's local timezone, matching how a
    // human would copy the time out of a TiDB client (session tz = SYSTEM)
    chrono::Local.from_local_datetime(&dt).single().map(|d| d.timestamp_millis()).unwrap_or_else(|| chrono::Utc.from_utc_datetime(&dt).timestamp_millis())
}

pub fn tso_to_datetime(tso: u64) -> String {
    let ms = (tso >> 18) as i64;
    use chrono::TimeZone;
    chrono::Local
        .timestamp_millis_opt(ms)
        .single()
        .map(|d| format!("{}", d.format("%Y-%m-%d %H:%M:%S%.3f")))
        .unwrap_or_else(|| format!("tso({tso})"))
}
