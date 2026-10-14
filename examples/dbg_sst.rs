// Debug helper: iterate an SST via todu's parser, or z-decode a raw db dir.
use rocksdb::{DB, Options, ReadOptions};
use todu::keycodec::decode_z_parts;

fn main() {
    let path = std::env::args().nth(1).unwrap();
    if path.ends_with(".sst") {
        match todu::sst::SstReader::open(std::path::Path::new(&path)) {
            Ok(r) => {
                let mut cnt = 0;
                let _ = r.iterate(|k, v| {
                    cnt += 1;
                    println!("entry {cnt}: key={} vlen={}", hex::encode(&k[..k.len().min(24)]), v.len());
                    cnt < 5
                });
                println!("total>={cnt}");
            }
            Err(e) => println!("ERR: {e}"),
        }
    } else {
        // treat as rocksdb dir: raw scan of write CF with z decode
        let mut opts = Options::default();
        opts.create_if_missing(false);
        let cfs = DB::list_cf(&opts, &path).unwrap();
        let db = DB::open_cf_for_read_only(&opts, &path, cfs.iter().map(|s| s.as_str()).collect::<Vec<_>>(), false).unwrap();
        for cfname in ["write", "default"] {
            let Some(cf) = db.cf_handle(cfname) else { continue };
            let mut it = db.raw_iterator_cf_opt(cf, ReadOptions::default());
            it.seek_to_first();
            let mut n = 0u64;
            while it.valid() {
                let k = it.key().unwrap().to_vec();
                if let Some((uk, _rest)) = decode_z_parts(&k) {
                    n += 1;
                    if n <= 5 {
                        println!("[{cfname}] uk={}", String::from_utf8_lossy(&uk[..uk.len().min(24)]));
                    }
                }
                it.next();
            }
            println!("[{cfname}] z-decoded: {n}");
        }
    }
}
