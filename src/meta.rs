//! TiDB meta decoding: `mDB:` keys hold JSON-serialized DBInfo/TableInfo.
//! This gives us table name → table ID and column types without a live TiDB.

use crate::rowcodec::Column;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct TableDef {
    pub db: String,
    pub name: String,
    pub id: i64,
    pub pk_is_handle: bool,
    pub is_common_handle: bool,
    /// index into `cols` of the column whose value lives in the KEY (int
    /// handle), if any. Its value must be merged back into output.
    pub handle_col: Option<usize>,
    pub cols: Vec<Column>,
}

fn get<'a>(v: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|k| v.get(*k))
}

fn get_str(v: &Value, keys: &[&str]) -> Option<String> {
    get(v, keys).and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn get_i64(v: &Value, keys: &[&str]) -> Option<i64> {
    get(v, keys).and_then(|x| x.as_i64())
}

fn get_u64(v: &Value, keys: &[&str]) -> Option<u64> {
    get(v, keys).and_then(|x| x.as_u64())
}

fn parse_cistr(v: &Value) -> String {
    match v {
        Value::Object(_) => get_str(v, &["O", "o"]).unwrap_or_default(),
        Value::String(s) => s.clone(),
        _ => String::new(),
    }
}

fn parse_column(v: &Value) -> Option<Column> {
    let name = get(v, &["name"]).map(parse_cistr)?;
    let tp = get(v, &["type", "ftype"]).and_then(|t| {
        t.get("Tp").or_else(|| t.get("tp")).and_then(|x| x.as_u64())
    })? as u8;
    let flag = get(v, &["type", "ftype"]).and_then(|t| {
        t.get("Flag").or_else(|| t.get("flag")).and_then(|x| x.as_u64())
    })? as u32;
    let flen = get(v, &["type", "ftype"])
        .and_then(|t| t.get("Flen").or_else(|| t.get("flen")).and_then(|x| x.as_i64()))
        .unwrap_or(0);
    let decimal = get(v, &["type", "ftype"])
        .and_then(|t| t.get("Decimal").or_else(|| t.get("decimal")).and_then(|x| x.as_i64()))
        .unwrap_or(0);
    Some(Column { name, tp, flag, flen, decimal })
}

fn parse_table(db: &str, v: &Value) -> Option<TableDef> {
    let id = get_i64(v, &["id"])?;
    let name = get(v, &["name"]).map(parse_cistr)?;
    let cols = get(v, &["cols", "columns"])
        .and_then(|c| c.as_array())
        .map(|arr| arr.iter().filter_map(parse_column).collect::<Vec<_>>())
        .unwrap_or_default();
    if cols.is_empty() {
        return None;
    }
    // int handle column: single-column PK, NOT NULL, integer type
    // (PriKeyFlag = 1<<1 = 2, NotNullFlag = 1<<0 = 1)
    let handle_col = cols
        .iter()
        .position(|c| c.is_int() && (c.flag & 2) != 0 && (c.flag & 1) != 0);
    let pk_is_handle = get(v, &["pk_is_handle"]).and_then(|x| x.as_bool()).unwrap_or(false)
        || handle_col.is_some();
    Some(TableDef {
        db: db.to_string(),
        name,
        id,
        pk_is_handle,
        is_common_handle: get(v, &["is_common_handle"]).and_then(|x| x.as_bool()).unwrap_or(false),
        handle_col,
        cols,
    })
}

/// Parse one `mDB:` value (JSON DBInfo). Returns (db_name, tables).
pub fn parse_db_info(payload: &[u8]) -> Option<(String, Vec<TableDef>)> {
    let v: Value = serde_json::from_slice(payload).ok()?;
    let db_name = get(&v, &["db_name", "name"]).map(parse_cistr)?;
    let tables = get(&v, &["tables"])
        .and_then(|t| t.as_array())
        .map(|arr| arr.iter().filter_map(|tv| parse_table(&db_name, tv)).collect::<Vec<_>>())
        .unwrap_or_default();
    Some((db_name, tables))
}

/// Columns that physically live in the row VALUE: when pk_is_handle, the int
/// PK column is stored in the key, not the value.
pub fn row_columns(t: &TableDef) -> Vec<Column> {
    match (t.pk_is_handle, t.handle_col) {
        (true, Some(i)) => {
            let mut v = t.cols.clone();
            v.remove(i);
            v
        }
        _ => t.cols.clone(),
    }
}
