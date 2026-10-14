//! TiDB record key / meta key codec.
//!
//! Record key layout (TiKV txnkv, default/write CF before MVCC ts suffix):
//!   b't' + table_id(BE u64) + b'r' + handle
//!   int handle    : 8-byte BE i64
//!   common handle : memcomparable-encoded index value bytes (variable length)
//! Index key layout:
//!   b't' + table_id(BE u64) + b'i' + index_id(BE u64) + encoded value
//! Meta key layout:
//!   b'm' + b"DB:" + db_id (8-byte, order-preserving encoded)

pub const TABLE_PREFIX: u8 = b't';
pub const RECORD_PREFIX_SEP: u8 = b'r';
pub const INDEX_PREFIX_SEP: u8 = b'i';
pub const META_PREFIX: u8 = b'm';

#[derive(Debug, Clone, PartialEq)]
pub enum Handle {
    Int(i64),
    /// common handle raw bytes (memcomparable), kept for display
    Common(Vec<u8>),
}

/// Parse `t{EncodeInt(tableID)}_r{EncodeInt(handle)}` (TiDB tablecodec:
/// ints are big-endian with the FIRST byte sign-flipped; separator `_r`).
pub fn parse_record_key(key: &[u8]) -> Option<(i64, Handle)> {
    if key.len() < 1 + 8 + 2 + 8 || key[0] != TABLE_PREFIX || &key[9..11] != b"_r" {
        return None;
    }
    let table_id = i64::from_be_bytes(key[1..9].try_into().unwrap()) ^ (1i64 << 63);
    let rest = &key[11..];
    if rest.len() == 8 {
        let h = i64::from_be_bytes(rest.try_into().unwrap()) ^ (1i64 << 63);
        Some((table_id, Handle::Int(h)))
    } else {
        Some((table_id, Handle::Common(rest.to_vec())))
    }
}

/// EncodeInt: big-endian with first byte sign-flipped.
pub fn flip_int(v: i64) -> [u8; 8] {
    let mut b = v.to_be_bytes();
    b[0] ^= 0x80;
    b
}

pub fn table_record_prefix(table_id: i64) -> Vec<u8> {
    let mut k = Vec::with_capacity(11);
    k.push(TABLE_PREFIX);
    k.extend_from_slice(&flip_int(table_id));
    k.extend_from_slice(b"_r");
    k
}

/// Is this key a meta key `mDB:<dbID>`? (TiDB stores it as a decimal string)
pub fn is_meta_db_key(key: &[u8]) -> Option<i64> {
    if key.starts_with(b"mDB:") {
        let rest = &key[4..];
        if !rest.is_empty() && rest.iter().all(|b| b.is_ascii_digit()) {
            return std::str::from_utf8(rest).ok()?.parse::<i64>().ok();
        }
    }
    None
}

pub fn meta_db_key(db_id: i64) -> Vec<u8> {
    format!("mDB:{}", db_id).into_bytes()
}

/// Strip the 8-byte MVCC timestamp suffix from a TiKV physical key.
/// (key, ts) — ts is the suffix interpreted as BE u64 (commit_ts on write CF,
/// start_ts on default CF).
pub fn split_mvcc_ts(physical_key: &[u8]) -> Option<(&[u8], u64)> {
    if physical_key.len() < 8 {
        return None;
    }
    let (uk, ts) = physical_key.split_at(physical_key.len() - 8);
    Some((uk, u64::from_be_bytes(ts.try_into().unwrap())))
}

pub fn append_ts(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut k = user_key.to_vec();
    k.extend_from_slice(&ts.to_be_bytes());
    k
}

/// Internal-key suffix on per-SST raw iteration: 8 bytes, LITTLE-ENDIAN
/// (EncodeFixed64) = (seq << 8) | value_type.
pub const KTYPE_DELETION: u8 = 0x00;
pub const KTYPE_VALUE: u8 = 0x01;

pub fn parse_internal_suffix(key: &[u8]) -> Option<(&[u8], u64, u8)> {
    if key.len() < 8 {
        return None;
    }
    let (uk, sfx) = key.split_at(key.len() - 8);
    let n = u64::from_le_bytes(sfx.try_into().unwrap());
    Some((uk, n >> 8, (n & 0xff) as u8))
}

/// Decode a TiKV kvdb stored key: `z` + memcomparable-encoded user key.
/// TiKV wraps every txn key as `z` + EncodeBytes(user_key + mvcc_ts) before
/// writing to RocksDB (MyRocks memcomparable format: 8-byte groups of data
/// padded with 0x00, followed by marker byte `0xFF - pad_count`).
/// Decode a stored kvdb key: `z` + EncodeBytes(user_key) + [mvcc_ts(8B)].
/// Returns (user_key, leftover_after_encoding). The mvcc timestamp lives
/// OUTSIDE the encoded blob, immediately after it (before the RocksDB
/// internal-key suffix in SST files).
pub fn decode_z_parts(stored: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    if stored.first() != Some(&b'z') {
        return None;
    }
    let b = &stored[1..];
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0usize;
    while i < b.len() {
        if b.len() < i + 9 {
            return None;
        }
        let group = &b[i..i + 9];
        let pad = 0xFFu8.checked_sub(group[8])?;
        if pad > 8 {
            return None;
        }
        let real = 8 - pad as usize;
        out.extend_from_slice(&group[..real]);
        i += 9;
        if pad != 0 {
            break;
        }
    }
    Some((out, b[i..].to_vec()))
}

pub fn decode_z_key(stored: &[u8]) -> Option<Vec<u8>> {
    decode_z_parts(stored).map(|(uk, _)| uk)
}

/// Encode a key the same way TiKV does (for prefix seek positions).
pub fn encode_z_key(user_key: &[u8]) -> Vec<u8> {
    let mut out = vec![b'z'];
    let mut i = 0usize;
    while i <= user_key.len() {
        let remain = user_key.len() - i;
        if remain >= 8 {
            out.extend_from_slice(&user_key[i..i + 8]);
            out.push(0xFF);
        } else {
            out.extend_from_slice(&user_key[i..]);
            out.extend(std::iter::repeat(0u8).take(8 - remain));
            out.push(0xFF - (8 - remain) as u8);
            break;
        }
        i += 8;
    }
    out
}

pub fn format_handle(h: &Handle) -> String {
    match h {
        Handle::Int(v) => v.to_string(),
        Handle::Common(b) => format!("common:{}", hex::encode(b)),
    }
}
