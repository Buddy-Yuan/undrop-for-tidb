//! todu — TiDB/TiKV Offline Data Unloader
//!
//! Reads TiKV's RocksDB files directly (offline) and recovers table data by
//! table name, the way Oracle ODU/BBED work on datafiles. Works after TiDB GC
//! has marked data for deletion as long as RocksDB compaction has not
//! physically erased it yet.

use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use todu::keycodec::{format_handle, parse_record_key, table_record_prefix, Handle, META_PREFIX};
use todu::meta;
use todu::meta::TableDef;
use todu::mvcc::{datetime_to_tso, tso_to_datetime, write_type_name};
use todu::reader::{open_read_only, read_sst_file, scan_cf_prefix, Collector};
use todu::rowcodec::{decode_row_value, datum_to_json, datum_to_sql, Column, Datum};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        usage();
        std::process::exit(2);
    }
    let code = match args[0].as_str() {
        "meta" => cmd_meta(&Cli::parse(&args[1..])),
        "fixture" => cmd_fixture(&Cli::parse(&args[1..])),
        "recover" => cmd_recover(&Cli::parse(&args[1..])),
        "browse" => cmd_browse(&Cli::parse(&args[1..])),
        "decode-row" => cmd_decode_row(&Cli::parse(&args[1..])),
        "help" | "--help" | "-h" => {
            usage();
            0
        }
        other => {
            eprintln!("todu: unknown command '{other}'\n");
            usage();
            2
        }
    };
    std::process::exit(code);
}

// ---------------------------------------------------------------- CLI plumbing

struct Cli {
    opts: HashMap<String, String>,
    flags: Vec<String>,
}

impl Cli {
    fn parse(args: &[String]) -> Cli {
        let mut opts = HashMap::new();
        let mut flags = Vec::new();
        let mut i = 0;
        while i < args.len() {
            let a = &args[i];
            if let Some(name) = a.strip_prefix("--") {
                if let Some((k, v)) = name.split_once('=') {
                    opts.insert(k.to_string(), v.to_string());
                } else if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                    opts.insert(name.to_string(), args[i + 1].clone());
                    i += 1;
                } else {
                    flags.push(name.to_string());
                }
            }
            i += 1;
        }
        Cli { opts, flags }
    }
    fn opt(&self, name: &str) -> Option<&str> {
        self.opts.get(name).map(|s| s.as_str())
    }
    fn req(&self, name: &str) -> Result<String, String> {
        self.opt(name)
            .map(|s| s.to_string())
            .ok_or_else(|| format!("missing required option --{name}"))
    }
    fn flag(&self, name: &str) -> bool {
        self.flags.iter().any(|f| f == name)
    }
}

fn usage() {
    println!(
        r#"todu — TiDB/TiKV Offline Data Unloader (recover table data from RocksDB files, like Oracle ODU/BBED)

WORKFLOW (data lost after DELETE/UPDATE/DROP, GC already ran):
  1. STOP TiKV on this store (or better: copy the whole data/db dir elsewhere).
  2. Freeze physical erasure: in TiKV config set rocksdb.defaultcf.disable-auto-compactions=true
     and restart — compaction is what really deletes GC-marked versions.
  3. Run todu below against a COPY of <tikv-data>/db.

SUBCOMMANDS:
  todu fixture [--out DIR]
      Generate a demo TiKV data dir simulating an accident (3 rows, one
      UPDATED, one DELETEd, GC already run) — for manual testing, no cluster
      needed. Default dir: /tmp/todu-fixture.

  todu meta --db DIR [--as-of TIME] [--schema-out FILE]
      List databases/tables found in TiDB meta (mDB: keys): table name -> table id + columns.

  todu recover (--db DIR | --sst FILE[,FILE...]) [--cf auto|write|default]
      (--table DB.TABLE | --table-id N) [--schema FILE]
      [--as-of "YYYY-MM-DD HH:MM:SS" | --as-of-tso TSO]
      [--format sql|csv|json|raw] [--out FILE] [--limit N]
      Recover a table's rows (latest visible version per row key).

  todu browse (--db DIR | --sst FILE[,FILE...]) --table-id N [--limit N] [--values]
      BBED-style: dump every MVCC version (commit_ts, type, start_ts, payload hex).

  todu decode-row --hex HEX --schema FILE
      Decode one row payload hex with a schema file (debugging).

NOTES:
  * --sst mode reads raw SST files directly, BYPASSING tombstones that live in
    other files — this is how you reach data GC marked but compaction has not
    erased. Run it on ALL *.sst files under data/db (they may belong to either
    the default or write CF; --cf auto classifies by value shape).
  * --as-of also works on meta/recover to time-travel before a mistake.
  * Row payload decoding follows TiDB row format v2 (and v1); undecodable
    columns are kept as hex so no data is silently dropped.
"#
    );
}

// ---------------------------------------------------------------- data source

/// Build a Collector from --db (prefix-scoped scan) or --sst (whole files).
fn build_collector(cli: &Cli, prefix: &[u8]) -> Result<Collector, String> {
    let mut c = Collector::new();
    if let Some(dbpath) = cli.opt("db") {
        let db = open_read_only(Path::new(dbpath))?;
        for cf_name in ["default", "write"] {
            let cf = match db.cf_handle(cf_name) {
                Some(h) => h,
                None => continue,
            };
            let mut err = None;
            scan_cf_prefix(&db, cf, prefix, |k, v| {
                if cf_name == "write" {
                    c.add_write_physical(k, v);
                } else {
                    c.add_default_physical(k, v);
                }
            })
            .unwrap_or_else(|e| err = Some(e));
            if let Some(e) = err {
                return Err(e);
            }
        }
    } else if let Some(ssts) = cli.opt("sst") {
        let hint = cli.opt("cf");
        let mut skipped = 0usize;
        for f in ssts.split(',') {
            let f = f.trim();
            if f.is_empty() {
                continue;
            }
            if let Err(e) = read_sst_file(Path::new(f), hint, &mut c) {
                eprintln!("[todu] WARN: skip {f}: {e}");
                skipped += 1;
            }
        }
        if skipped > 0 {
            eprintln!("[todu] WARN: {skipped} SST file(s) skipped");
        }
    } else {
        return Err("need --db DIR or --sst FILE[,FILE...]".into());
    }
    if cli.flag("verbose") {
        eprintln!(
            "[todu] stats: write_entries={} default_entries={} write_tombstones={} default_tombstones={}",
            c.stat_write_entries, c.stat_default_entries, c.stat_write_tombstones, c.stat_default_tombstones
        );
    }
    Ok(c)
}

fn as_of_tso(cli: &Cli) -> Result<Option<u64>, String> {
    if let Some(tso) = cli.opt("as-of-tso") {
        return Ok(Some(tso.parse::<u64>().map_err(|_| "bad --as-of-tso")?));
    }
    if let Some(s) = cli.opt("as-of") {
        return datetime_to_tso(s)
            .map(Some)
            .ok_or_else(|| format!("bad --as-of '{s}' (use \"YYYY-MM-DD HH:MM:SS\")"));
    }
    Ok(None)
}

// ---------------------------------------------------------------- schema I/O

fn collect_meta_tables(c: &Collector, as_of: Option<u64>) -> Vec<TableDef> {
    let versions = c.assemble(Some(&[META_PREFIX]), as_of, false);
    let mut tables = Vec::new();
    for v in versions {
        if let Some((_, mut ts)) = meta::parse_db_info(&v.payload) {
            tables.append(&mut ts);
        }
    }
    tables
}

fn find_table(tables: &[TableDef], dotted: &str) -> Result<TableDef, String> {
    let (db, name) = match dotted.split_once('.') {
        Some(x) => x,
        None => ("", dotted),
    };
    tables
        .iter()
        .find(|t| {
            t.name.eq_ignore_ascii_case(name)
                && (db.is_empty() || t.db.eq_ignore_ascii_case(db))
        })
        .cloned()
        .ok_or_else(|| {
            format!(
                "table '{dotted}' not found in meta; run `todu meta` to list, or pass --table-id + --schema"
            )
        })
}

fn load_schema_file(path: &str, table_id: Option<i64>) -> Result<TableDef, String> {
    let raw = fs::read(path).map_err(|e| format!("read {path}: {e}"))?;
    let v: serde_json::Value = serde_json::from_slice(&raw).map_err(|e| format!("parse {path}: {e}"))?;
    let candidates: Vec<TableDef> = match &v {
        serde_json::Value::Array(arr) => arr.iter().filter_map(tabledef_from_json).collect(),
        _ => tabledef_from_json(&v).into_iter().collect(),
    };
    match table_id {
        Some(tid) => candidates
            .into_iter()
            .find(|t| t.id == tid)
            .ok_or_else(|| format!("{path}: no table with id {tid}")),
        None if candidates.len() == 1 => Ok(candidates.into_iter().next().unwrap()),
        None => Err(format!(
            "{path}: contains {} tables, pass --table-id to pick one",
            candidates.len()
        )),
    }
}

fn tabledef_to_json(t: &TableDef) -> serde_json::Value {
    use serde_json::json;
    json!({
        "db": t.db,
        "name": t.name,
        "table_id": t.id,
        "pk_is_handle": t.pk_is_handle,
        "is_common_handle": t.is_common_handle,
        "handle_col": t.handle_col,
        "columns": t.cols.iter().map(|c| json!({
            "name": c.name, "tp": c.tp, "flag": c.flag, "flen": c.flen, "decimal": c.decimal,
        })).collect::<Vec<_>>(),
    })
}

fn tabledef_from_json(v: &serde_json::Value) -> Option<TableDef> {
    let obj = v.as_object()?;
    let arr = obj
        .get("columns")
        .or_else(|| v.as_array().map(|_| obj.get("columns")).flatten())
        .and_then(|c| c.as_array())?;
    let cols: Vec<Column> = arr
        .iter()
        .map(|c| Column {
            name: c.get("name").and_then(|x| x.as_str()).unwrap_or("?").to_string(),
            tp: c.get("tp").and_then(|x| x.as_u64()).unwrap_or(254) as u8,
            flag: c.get("flag").and_then(|x| x.as_u64()).unwrap_or(0) as u32,
            flen: c.get("flen").and_then(|x| x.as_i64()).unwrap_or(0),
            decimal: c.get("decimal").and_then(|x| x.as_i64()).unwrap_or(0),
        })
        .collect();
    if cols.is_empty() {
        return None;
    }
    Some(TableDef {
        db: obj.get("db").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        name: obj.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        id: obj.get("table_id").and_then(|x| x.as_i64()).unwrap_or(0),
        pk_is_handle: obj.get("pk_is_handle").and_then(|x| x.as_bool()).unwrap_or(false),
        is_common_handle: obj.get("is_common_handle").and_then(|x| x.as_bool()).unwrap_or(false),
        handle_col: obj.get("handle_col").and_then(|x| x.as_u64()).map(|x| x as usize),
        cols,
    })
}

// ---------------------------------------------------------------- meta cmd

fn cmd_meta(cli: &Cli) -> i32 {
    let as_of = match as_of_tso(cli) {
        Ok(a) => a,
        Err(e) => return fail(&e),
    };
    let c = match build_collector(cli, &[META_PREFIX]) {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };
    let tables = collect_meta_tables(&c, as_of);
    if tables.is_empty() {
        eprintln!("no TiDB meta found (mDB: keys). Was the cluster ever initialized, or is meta GC'd too?");
        return 1;
    }
    let mut cur_db = String::new();
    for t in &tables {
        if t.db != cur_db {
            cur_db = t.db.clone();
            println!("database `{}`", cur_db);
        }
        let cols: Vec<String> = t
            .cols
            .iter()
            .map(|c| format!("{}(tp={})", c.name, c.tp))
            .collect();
        println!(
            "  table `{}` id={} pk_is_handle={} common_handle={} handle_col={:?} cols=[{}]",
            t.name, t.id, t.pk_is_handle, t.is_common_handle, t.handle_col, cols.join(", ")
        );
    }
    if let Some(out) = cli.opt("schema-out") {
        let arr: Vec<serde_json::Value> = tables.iter().map(tabledef_to_json).collect();
        let json = serde_json::to_string_pretty(&serde_json::Value::Array(arr)).unwrap();
        fs::write(out, json).map(|_| eprintln!("[todu] schema written to {out}")).ok();
    }
    0
}

// ---------------------------------------------------------------- recover cmd

fn cmd_recover(cli: &Cli) -> i32 {
    let as_of = match as_of_tso(cli) {
        Ok(a) => a,
        Err(e) => return fail(&e),
    };

    // 1. resolve table definition
    let table: TableDef = if let Some(schema) = cli.opt("schema") {
        let tid = match cli.opt("table-id") {
            Some(s) => match s.parse::<i64>() {
                Ok(n) => Some(n),
                Err(_) => return fail("bad --table-id"),
            },
            None => None,
        };
        match load_schema_file(schema, tid) {
            Ok(t) => t,
            Err(e) => return fail(&e),
        }
    } else if let Some(tname) = cli.opt("table") {
        let mc = match build_collector(cli, &[META_PREFIX]) {
            Ok(c) => c,
            Err(e) => return fail(&e),
        };
        let tables = collect_meta_tables(&mc, as_of);
        match find_table(&tables, tname) {
            Ok(t) => t,
            Err(e) => return fail(&e),
        }
    } else {
        return fail("need --table DB.TABLE (or --table-id + --schema)");
    };
    eprintln!(
        "[todu] table `{}.{}` table_id={} pk_is_handle={} common_handle={}",
        table.db, table.name, table.id, table.pk_is_handle, table.is_common_handle
    );

    // 2. scan the table's record-key range
    let prefix = table_record_prefix(table.id);
    let c = match build_collector(cli, &prefix) {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };
    let mut versions = c.assemble(Some(&prefix), as_of, false);
    // Fallback: write values whose format we cannot fully parse (TiKV v6.x
    // char-type) still carry the row in their short_value tail — recover
    // every distinct payload for keys the version assembler missed.
    let have: std::collections::HashSet<Vec<u8>> = versions.iter().map(|v| v.user_key.clone()).collect();
    for v in c.assemble_direct(Some(&prefix)) {
        if !have.contains(&v.user_key) {
            versions.push(v);
        }
    }
    eprintln!("[todu] assembled {} row versions", versions.len());

    // 3. decode & emit
    let row_cols = meta::row_columns(&table);
    let limit = cli.opt("limit").and_then(|l| l.parse::<usize>().ok());
    let out_path = cli.opt("out").map(|p| std::path::PathBuf::from(p));
    let writer: Box<dyn Write> = match &out_path {
        Some(p) => match fs::File::create(p) {
            Ok(f) => Box::new(BufWriter::new(f)),
            Err(e) => return fail(&format!("create {}: {e}", p.display())),
        },
        None => Box::new(BufWriter::new(std::io::stdout().lock())),
    };
    let mut w = writer;
    let fmt = cli.opt("format").unwrap_or("sql");

    if fmt == "sql" {
        let qcols: Vec<String> = table.cols.iter().map(|c| format!("`{}`", c.name)).collect();
        writeln!(
            w,
            "-- todu recover: `{}.{}` (table_id={}){}",
            table.db,
            table.name,
            table.id,
            as_of.map(|t| format!(" as-of tso={} ({})", t, tso_to_datetime(t))).unwrap_or_default()
        )
        .ok();
        writeln!(w, "-- rows are the latest version visible per key; verify before applying.").ok();
        writeln!(w, "SET NAMES utf8mb4;").ok();
        writeln!(w, "INSERT INTO `{}.{}` ({}) VALUES", table.db, table.name, qcols.join(", ")).ok();
    } else if fmt == "csv" {
        let cols: Vec<String> = table.cols.iter().map(|c| c.name.clone()).collect();
        writeln!(w, "{}", cols.join(",")).ok();
    }

    let mut emitted = 0usize;
    let mut batch = 0usize;
    for v in &versions {
        if let Some(l) = limit {
            if emitted >= l {
                break;
            }
        }
        let handle = parse_record_key(&v.user_key).map(|(_, h)| h);
        // direct-fallback rows carry the whole write value; pull the row tail
        let payload = if v.source.starts_with("direct") {
            todu::rowcodec::extract_char_short_value(&v.payload).unwrap_or_else(|| v.payload.clone())
        } else {
            v.payload.clone()
        };
        // rowcodec v2 (TiDB v4.0+ 真实行格式): 按 colID 解码并按 schema 列序对齐
        let datums = if payload.first() == Some(&0x80) {
            match todu::rowcodec::decode_rowv2_aligned(
                &payload,
                &table.cols,
                table.handle_col,
                &handle,
            ) {
                Some(d) => d,
                None => match decode_row_value(&payload, &row_cols) {
                    Ok(mut d) => {
                        if let (Some(Handle::Int(hv)), Some(hidx)) = (&handle, table.handle_col) {
                            let col = &table.cols[hidx];
                            let dv = if col.unsigned() { Datum::U64(*hv as u64) } else { Datum::I64(*hv) };
                            let hidx = hidx.min(d.len());
                            d.insert(hidx, dv);
                        }
                        d
                    }
                    Err(e) => {
                        eprintln!("[todu] WARN row decode failed ({}), raw hex", e);
                        vec![Datum::Bytes(payload.clone())]
                    }
                },
            }
        } else {
            match decode_row_value(&payload, &row_cols) {
                Ok(mut d) => {
                    if let (Some(Handle::Int(hv)), Some(hidx)) = (&handle, table.handle_col) {
                        let col = &table.cols[hidx];
                        let dv = if col.unsigned() { Datum::U64(*hv as u64) } else { Datum::I64(*hv) };
                        let hidx = hidx.min(d.len());
                        d.insert(hidx, dv);
                    }
                    d
                }
                Err(e) => {
                    eprintln!("[todu] WARN row decode failed ({}), raw hex", e);
                    vec![Datum::Bytes(payload.clone())]
                }
            }
        };

        if datums.len() != table.cols.len() && fmt != "raw" {
            eprintln!(
                "[todu] WARN handle={:?}: decoded {} fields, schema has {} — row kept but check alignment",
                handle.as_ref().map(format_handle).unwrap_or_default(),
                datums.len(),
                table.cols.len()
            );
        }
        match fmt {
            "sql" => {
                let mut vals = Vec::with_capacity(table.cols.len());
                for (i, c) in table.cols.iter().enumerate() {
                    vals.push(datums.get(i).map(|d| datum_to_sql(d, c)).unwrap_or_else(|| "NULL".into()));
                }
                if batch > 0 {
                    write!(w, ",").ok();
                }
                writeln!(w, "({})", vals.join(", ")).ok();
                batch += 1;
                if batch >= 200 {
                    batch = 0;
                    writeln!(w, ";").ok();
                    writeln!(w, "INSERT INTO `{}.{}` ({}) VALUES",
                        table.db, table.name, qcols_join(&table)).ok();
                }
            }
            "csv" => {
                let row: Vec<String> = (0..table.cols.len())
                    .map(|i| match datums.get(i) {
                        Some(Datum::Null) | None => "".to_string(),
                        Some(Datum::Text(b)) => String::from_utf8_lossy(b).replace('"', "\"\""),
                        Some(Datum::Bytes(b)) => {
                            format!("0x{}", hex::encode(b))
                        }
                        Some(d) => datum_to_sql(d, &table.cols[i]),
                    })
                    .collect();
                writeln!(w, "{}", row.join(",")).ok();
            }
            "json" => {
                use serde_json::json;
                let mut obj = serde_json::Map::new();
                obj.insert("_commit_ts".into(), json!(v.commit_ts));
                obj.insert("_start_ts".into(), json!(v.start_ts));
                if let Some(h) = &handle {
                    obj.insert("_handle".into(), json!(format_handle(h)));
                }
                for (i, c) in table.cols.iter().enumerate() {
                    let dv = datums.get(i).map(|d| datum_to_json(d)).unwrap_or(serde_json::Value::Null);
                    obj.insert(c.name.clone(), dv);
                }
                writeln!(w, "{}", serde_json::Value::Object(obj)).ok();
            }
            _ => {
                writeln!(
                    w,
                    "{} {:016x} {:016x} {}",
                    hex::encode(&v.user_key),
                    v.commit_ts,
                    v.start_ts,
                    hex::encode(&v.payload)
                )
                .ok();
            }
        }
        emitted += 1;
    }
    if fmt == "sql" {
        writeln!(w, ";").ok();
    }
    w.flush().ok();
    eprintln!("[todu] done: {emitted} rows written ({fmt})");
    if out_path.is_some() {
        eprintln!("[todu] output: {}", out_path.unwrap().display());
    }
    0
}

fn qcols_join(t: &TableDef) -> String {
    t.cols.iter().map(|c| format!("`{}`", c.name)).collect::<Vec<_>>().join(", ")
}

// ---------------------------------------------------------------- browse cmd

fn cmd_browse(cli: &Cli) -> i32 {
    let tid: i64 = match cli.req("table-id").ok().and_then(|s| s.parse().ok()) {
        Some(t) => t,
        None => return fail("need --table-id"),
    };
    let prefix = table_record_prefix(tid);
    let c = match build_collector(cli, &prefix) {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };
    let limit = cli.opt("limit").and_then(|l| l.parse::<usize>().ok()).unwrap_or(usize::MAX);
    let versions = c.raw_versions(Some(&prefix));
    eprintln!("[todu] {} write versions under prefix {}", versions.len(), hex::encode(&prefix));
    let mut last_key = Vec::new();
    let mut n = 0usize;
    for (uk, commit_ts, wt, start_ts, sv) in versions {
        if uk != last_key {
            last_key = uk.clone();
            let h = parse_record_key(&uk).map(|(_, h)| format_handle(&h)).unwrap_or_default();
            println!("=== key={} handle={}", hex::encode(&uk), h);
        }
        let payload_desc = match sv {
            Some(sv) => format!("short_value({}B)={}", sv.len(), hex::encode(&sv)),
            None => match c.default_payload(&uk, start_ts) {
                Some(p) if cli.flag("values") => format!("default({}B)={}", p.len(), hex::encode(p)),
                Some(p) => format!("default({}B)", p.len()),
                None => "NO PAYLOAD (erased or not yet found)".to_string(),
            },
        };
        println!(
            "  commit_ts={} ({}) type={} start_ts={} {}",
            commit_ts,
            tso_to_datetime(commit_ts),
            write_type_name(wt),
            start_ts,
            payload_desc
        );
        n += 1;
        if n >= limit {
            break;
        }
    }
    0
}

// ---------------------------------------------------------------- decode-row cmd

fn cmd_decode_row(cli: &Cli) -> i32 {
    let hexs = match cli.req("hex") {
        Ok(h) => h,
        Err(e) => return fail(&e),
    };
    let payload = match hex::decode(hexs.trim()) {
        Ok(p) => p,
        Err(e) => return fail(&format!("bad hex: {e}")),
    };
    let schema = match cli.req("schema") {
        Ok(s) => s,
        Err(e) => return fail(&e),
    };
    let t = match load_schema_file(&schema, None) {
        Ok(t) => t,
        Err(e) => return fail(&e),
    };
    let row_cols = meta::row_columns(&t);
    match decode_row_value(&payload, &row_cols) {
        Ok(datums) => {
            for (i, d) in datums.iter().enumerate() {
                let name = row_cols.get(i).map(|c| c.name.as_str()).unwrap_or("?");
                println!("{name} = {}", datum_to_sql(d, row_cols.get(i).unwrap_or(&Column {
                    name: "?".into(), tp: 254, flag: 0, flen: 0, decimal: 0,
                })));
            }
            0
        }
        Err(e) => fail(&format!("decode: {e}")),
    }
}

fn cmd_fixture(cli: &Cli) -> i32 {
    match todu::fixture::run(cli.opt("out")) {
        Ok(dir) => {
            println!("[todu] fixture written to {dir}");
            println!("[todu] simulated scene: table test.t1 (id=90), rows alice/bob/carol");
            println!("[todu]   bob   -> UPDATEd at +1s  (old version GC-shadowed)");
            println!("[todu]   carol -> DELETEd at +2s  (v1 payload GC'd + DELETE marker)");
            println!("[todu] try:");
            println!(r#"  todu meta --db {dir}"#);
            println!(r#"  todu recover --db {dir} --table test.t1 --format sql"#);
            println!(r#"  todu recover --db {dir} --table test.t1 --as-of "2026-01-01 00:00:00" --format csv"#);
            println!(r#"  SST=$(ls {dir}/*.sst | paste -sd, -)"#);
            println!(r#"  todu recover --sst "$SST" --table test.t1 --as-of "2026-01-01 00:00:00" --format csv"#);
            println!(r#"  todu browse --sst "$SST" --table-id 90 --values"#);
            0
        }
        Err(e) => fail(&e),
    }
}

fn fail(msg: &str) -> i32 {
    eprintln!("todu: error: {msg}");
    1
}
