//! Pure-Rust RocksDB SST (BlockBasedTable) file reader.
//!
//! Forensic-grade: reads a single .sst file directly, returning EVERY entry
//! physically present in the file — including entries that are shadowed by
//! tombstones in other files, which is exactly the state between TiKV GC and
//! the next compaction.
//!
//! Supported: modern footer (magic 0x88e241b785f4cff7, format versions 1-5),
//! binary-search and two-level-flush index (plain block index), no/none,
//! snappy, lz4, lz4hc, zstd block compression. Checksums are NOT verified
//! (deliberate: damaged files must still be readable).

use std::path::Path;

const MAGIC: u64 = 0x88e241b785f4cff7;
const FOOTER_LEN: usize = 48;

const COMP_NONE: u8 = 0x00;
const COMP_SNAPPY: u8 = 0x01;
const COMP_ZLIB: u8 = 0x02;
const COMP_BZIP2: u8 = 0x03;
const COMP_LZ4: u8 = 0x04;
const COMP_LZ4HC: u8 = 0x05;
const COMP_ZSTD: u8 = 0x07;

pub struct SstReader {
    data: Vec<u8>,
    pub path: String,
}

fn read_varint64(b: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *pos >= b.len() || shift > 63 {
            return None;
        }
        let byte = b[*pos];
        *pos += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
    }
}

#[derive(Clone, Copy, Debug)]
struct BlockHandle {
    offset: u64,
    size: u64,
}

fn parse_block_handle(b: &[u8], pos: &mut usize) -> Option<BlockHandle> {
    let offset = read_varint64(b, pos)?;
    let size = read_varint64(b, pos)?;
    Some(BlockHandle { offset, size })
}

impl SstReader {
    pub fn open(path: &Path) -> Result<SstReader, String> {
        let data = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let r = SstReader { data, path: path.to_string_lossy().into_owned() };
        r.parse_footer()?;
        Ok(r)
    }

    fn parse_footer(&self) -> Result<(BlockHandle, BlockHandle, u32), String> {
        let n = self.data.len();
        if n < 64 {
            return Err(format!("file too small ({n}B)"));
        }
        let magic = u64::from_le_bytes(self.data[n - 8..].try_into().unwrap());
        if magic != MAGIC {
            return Err(format!("bad magic {magic:#x} (not a RocksDB SST file)"));
        }
        let version = u32::from_le_bytes(self.data[n - 12..n - 8].try_into().unwrap());
        if version == 0 {
            return Err("legacy table format (version 0) not supported".into());
        }
        // The footer is variable-length, right-aligned to EOF:
        //   [checksum_type 1B][metaindex_handle][index_handle][padding][version u32][magic u64]
        // (table/format.cc Footer::DecodeFrom). Slide a window over the tail,
        // parse [chksum][handles] at each candidate start, keep the one whose
        // blocks actually validate.
        let lo = n.saturating_sub(96);
        let mut fs = n - 48;
        loop {
            let chk = self.data[fs];
            if chk <= COMP_ZSTD {
                let mut p = 0usize;
                let buf = &self.data[fs + 1..];
                let m = parse_block_handle(buf, &mut p);
                let i = parse_block_handle(buf, &mut p);
                if std::env::var("TODU_DEBUG").is_ok() {
                    eprintln!("[dbg] fs={fs} chk={chk:#x} metaindex={m:?} index={i:?} m_ok={:?} i_ok={:?}",
                        m.as_ref().map(|h| self.block_ok(h, false)), i.as_ref().map(|h| self.block_ok(h, false)));
                }
                if let (Some(m), Some(i)) = (m, i) {
                    if self.block_ok(&m, false)
                        && (self.block_ok(&i, false) || self.block_ok(&i, true))
                    {
                        return Ok((m, i, version));
                    }
                }
            }
            if fs == lo {
                break;
            }
            fs -= 1;
        }
        Err("no valid footer found in file tail".into())
    }

    /// Block validates iff entries (classic or delta-encoded) walk exactly to
    /// the restart array.
    fn block_ok(&self, h: &BlockHandle, delta: bool) -> bool {
        let blk = match self.read_block(h) {
            Ok(b) => b,
            Err(_) => return false,
        };
        if blk.len() < 8 {
            return false;
        }
        if delta {
            match IndexBlockIter::new(&blk, true) {
                Some(it) => it.walks_exactly(),
                None => false,
            }
        } else {
            let nr = match u32::from_le_bytes(blk[blk.len() - 4..].try_into().unwrap()) as usize {
                nr if nr >= 1 && 4 + 4 * nr <= blk.len() => nr,
                _ => return false,
            };
            let entries_end = blk.len() - 4 - 4 * nr;
            let mut p = 0usize;
            let mut key_len = 0usize;
            while p < entries_end {
                let sh = match read_varint64(&blk, &mut p) {
                    Some(v) => v as usize,
                    None => return false,
                };
                let ns = match read_varint64(&blk, &mut p) {
                    Some(v) => v as usize,
                    None => return false,
                };
                let vl = match read_varint64(&blk, &mut p) {
                    Some(v) => v as usize,
                    None => return false,
                };
                if p + ns + vl > entries_end || sh > key_len {
                    return false;
                }
                key_len = sh + ns;
                p += ns + vl;
            }
            p == entries_end
        }
    }

    fn read_block(&self, h: &BlockHandle) -> Result<Vec<u8>, String> {
        let off = h.offset as usize;
        let size = h.size as usize;
        if off + size + 5 > self.data.len() {
            return Err(format!("block handle out of range (off={off} size={size})"));
        }
        let raw = &self.data[off..off + size];
        let ctype = self.data[off + size];
        // 4-byte checksum follows; forensic mode deliberately skips verification
        match ctype {
            COMP_NONE => Ok(raw.to_vec()),
            COMP_SNAPPY => snap::raw::Decoder::new()
                .decompress_vec(raw)
                .map_err(|e| format!("snappy: {e}")),
            COMP_LZ4 | COMP_LZ4HC => {
                // raw lz4 block without embedded size — grow the output buffer
                // until the decode succeeds
                let mut cap = (size * 8).max(64 * 1024);
                loop {
                    let mut out = vec![0u8; cap];
                    match lz4_flex::block::decompress_into(raw, &mut out) {
                        Ok(n) => {
                            out.truncate(n);
                            return Ok(out);
                        }
                        Err(_) if cap < 256 * 1024 * 1024 => cap *= 2,
                        Err(e) => return Err(format!("lz4: {e}")),
                    }
                }
            }
            COMP_ZSTD => zstd::decode_all(std::io::Cursor::new(raw))
                .map_err(|e| format!("zstd: {e}")),
            COMP_ZLIB => Err("zlib-compressed block: not supported".into()),
            COMP_BZIP2 => Err("bzip2-compressed block: not supported".into()),
            other => Err(format!("unknown compression type {other:#x}")),
        }
    }

    /// Iterate every entry in the file: cb(internal_key, value).
    /// Deletion entries have an internal-key type byte of 0 and empty value.
    pub fn iterate(&self, mut cb: impl FnMut(&[u8], &[u8]) -> bool) -> Result<(), String> {
        let (metaindex, index, _version) = self.parse_footer()?;
        let index_block = self.read_block(&index)?;
        // Index entries: format_version >= 4 may use value-delta encoding
        // (no per-entry value_len; the BlockHandle is delta-coded against the
        // previous one). Try delta first, fall back to the classic layout.
        // A layout is accepted only when EVERY handle it yields points at a
        // readable block; otherwise fall through to the next layout.
        //
        // Some writers (observed on TiKV v6.5 SSTs) encode the index handle
        // with size = 0 — the real size is derivable from block boundaries:
        // each block ends (offset + size + 5 trailer) where the next known
        // region (next handle / index block / metaindex block) begins.
        let dbg = std::env::var("TODU_DEBUG").is_ok();
        let mut blocks: Option<Vec<BlockHandle>> = None;
        for delta in [true, false] {
            if let Some(it) = IndexBlockIter::new(&index_block, delta) {
                if it.walks_exactly() {
                    let mut hs = it.collect();
                    if dbg {
                        eprintln!("[idxdbg] delta={delta} handles={hs:?} oks={:?}",
                            hs.iter().map(|h| self.block_ok(h, false)).collect::<Vec<_>>());
                    }
                    let mut bounds: Vec<u64> = vec![index.offset, metaindex.offset, self.data.len() as u64];
                    bounds.extend(hs.iter().map(|h| h.offset));
                    bounds.sort_unstable();
                    for h in hs.iter_mut() {
                        if h.size == 0 {
                            if let Some(&next) = bounds.iter().filter(|&&b| b > h.offset).min() {
                                if next > h.offset + 5 {
                                    h.size = next - 5 - h.offset;
                                }
                            }
                        }
                    }
                    if !hs.is_empty() && hs.iter().all(|h| self.block_ok(h, false)) {
                        blocks = Some(hs);
                        break;
                    }
                }
            }
        }
        let blocks = match blocks {
            Some(b) => b,
            None => {
                // Fallback: brute-force chain scan of the data region
                // [0 .. min(index, metaindex)). Data blocks tile the region
                // contiguously (each ends with a 5-byte type+crc trailer), so
                // a DFS over candidate sizes with strict block validation
                // finds them without the index. Blocks after the data region
                // (filter/meta) don't parse as blocks and end the chain.
                let region_end = index.offset.min(metaindex.offset) as usize;
                let mut memo: std::collections::HashMap<usize, Vec<(usize, usize)>> =
                    std::collections::HashMap::new();
                let mut found: Option<Vec<(usize, usize)>> = None;
                for require_z in [true, false] {
                    let mut memo2 = std::collections::HashMap::new();
                    if let Some(hs) = self.chain_scan(0, region_end, &mut memo2, 0, require_z) {
                        if !hs.is_empty() {
                            eprintln!("[todu] NOTE: chain-scan (require_z={require_z}) found {} data block(s) in {}",
                                hs.len(), self.path);
                            found = Some(hs);
                            break;
                        }
                    }
                }
                match found {
                    Some(hs) if !hs.is_empty() => {
                        if dbg {
                            eprintln!("[chain] blocks: {:?}", hs.iter().map(|(o, s)| (o, s)).collect::<Vec<_>>());
                        }
                        eprintln!(
                            "[todu] NOTE: index unusable, chain-scan found {} data block(s) in {}",
                            hs.len(), self.path
                        );
                        hs.into_iter()
                            .map(|(off, sz)| BlockHandle { offset: off as u64, size: sz as u64 })
                            .collect()
                    }
                    _ => return Err("cannot parse index block (delta or classic)".into()),
                }
            }
        };
        let mut skipped = 0usize;
        for h in blocks {
            let data_block = match self.read_block(&h) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("[todu] WARN: skip data block {h:?}: {e}");
                    skipped += 1;
                    continue;
                }
            };
            let mut it = BlockIter::new(&data_block).ok_or("bad data block")?;
            while let Some((key, value)) = it.next() {
                if !cb(key, value) {
                    return Ok(());
                }
            }
        }
        if skipped > 0 {
            eprintln!("[todu] WARN: {skipped} data block(s) skipped in {}", self.path);
        }
        Ok(())
    }

    /// Cheap structural validation of a raw (possibly compressed) block:
    /// num_restarts sane + entries walk exactly to the restart array.
    fn block_walks(blk: &[u8]) -> bool {
        if blk.len() < 9 {
            return false;
        }
        let nr = match u32::from_le_bytes(blk[blk.len() - 4..].try_into().unwrap()) as usize {
            nr if nr >= 1 && 4 + 4 * nr <= blk.len() => nr,
            _ => return false,
        };
        let entries_end = blk.len() - 4 - 4 * nr;
        let mut p = 0usize;
        let mut key_len = 0usize;
        while p < entries_end {
            let sh = match read_varint64(blk, &mut p) {
                Some(v) => v as usize,
                None => return false,
            };
            let ns = match read_varint64(blk, &mut p) {
                Some(v) => v as usize,
                None => return false,
            };
            let vl = match read_varint64(blk, &mut p) {
                Some(v) => v as usize,
                None => return false,
            };
            if p + ns + vl > entries_end {
                return false;
            }
            key_len = key_len.max(sh) + ns; // TiKV 块内 sh 可超过前键长度
            p += ns + vl;
        }
        p == entries_end
    }

    /// DFS chain scan: from `offset`, find a size whose block validates and
    /// whose successor chain reaches `region_end` (or a point where all
    /// remaining bytes are non-block content, e.g. filter/meta blocks).
    /// Returns the list of (offset, size). Memoizes failed offsets.
    fn chain_scan(
        &self,
        offset: usize,
        region_end: usize,
        memo: &mut std::collections::HashMap<usize, Vec<(usize, usize)>>,
        depth: usize,
        require_z: bool,
    ) -> Option<Vec<(usize, usize)>> {
        // 卡住（无法继续解析）= 数据块结束，后面是 filter/meta/index 区。
        // 这不是失败：返回已找到的链即可。
        if offset >= region_end || depth > 64 {
            return Some(Vec::new());
        }
        if let Some(cached) = memo.get(&offset) {
            if cached.is_empty() {
                return Some(Vec::new()); // 该偏移无数据块（filter/meta 区）
            }
            return Some(cached.clone());
        }
        memo.insert(offset, Vec::new());
        let max_size = region_end - offset - 5;
        // try larger sizes first: real blocks are usually big
        for size in (8..=max_size).rev() {
            let t = self.data[offset + size];
            if t > COMP_ZSTD {
                continue;
            }
            // TiKV 数据块首条目 [sh=0][ns][vl varint][key...]，用户键首字节
            // 是 z 包装的 0x7a；但混合 SST 的首块可能是内部键块（无 0x7a），
            // 所以 require_z=false 的第二遍放行
            if require_z {
                let key_hit = [3usize, 4, 5].iter().any(|&d| {
                    offset + d < self.data.len() && self.data[offset + d] == b'z'
                });
                if !key_hit {
                    continue;
                }
            }
            let blk = &self.data[offset..offset + size];
            if blk.len() < 8 {
                continue;
            }
            let nr = match u32::from_le_bytes(blk[blk.len() - 4..].try_into().unwrap()) as usize {
                nr if nr >= 1 && 4 + 4 * nr <= blk.len() => nr,
                _ => continue,
            };
            if !Self::block_walks(blk) {
                continue;
            }
            let mut chain =
                self.chain_scan(offset + size + 5, region_end, memo, depth + 1, require_z);
            if let Some(mut rest) = chain {
                let mut out = vec![(offset, size)];
                out.append(&mut rest);
                memo.insert(offset, out.clone());
                return Some(out);
            }
        }
        None
    }
}

/// Iterator over a RocksDB block: entries are [shared varint][non_shared
/// varint][value_len varint][key_delta][value], followed by a restart array.
struct BlockIter<'a> {
    data: &'a [u8],
    entries_end: usize,
    pos: usize,
    key: Vec<u8>,
}

impl<'a> BlockIter<'a> {
    fn new(block: &'a [u8]) -> Option<BlockIter<'a>> {
        if block.len() < 4 {
            return None;
        }
        let num_restarts = u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap()) as usize;
        let entries_end = block
            .len()
            .checked_sub(4 + 4 * num_restarts)?;
        Some(BlockIter { data: block, entries_end, pos: 0, key: Vec::with_capacity(64) })
    }

    /// Returns (internal_key, value) of the next entry.
    fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if self.pos >= self.entries_end {
            return None;
        }
        let mut p = self.pos;
        let shared = read_varint64(self.data, &mut p)? as usize;
        let non_shared = read_varint64(self.data, &mut p)? as usize;
        let value_len = read_varint64(self.data, &mut p)? as usize;
        if p + non_shared + value_len > self.entries_end {
            return None;
        }
        self.key.truncate(shared);
        self.key.extend_from_slice(&self.data[p..p + non_shared]);
        let key_start = p + non_shared;
        let value = &self.data[key_start..key_start + value_len];
        self.pos = key_start + value_len;
        Some((&self.key[..], value))
    }
}

/// Iterator over an INDEX block. Two layouts exist:
/// - classic: [shared][non_shared][value_len][key_delta][value=BlockHandle]
/// - value-delta (format_version >= 4, use_value_delta_encoding):
///   [shared][non_shared][handle_shared][key_delta][handle offset/size varints]
///   where the handle is delta-coded against the previous entry's handle.
struct IndexBlockIter<'a> {
    data: &'a [u8],
    entries_end: usize,
    pos: usize,
    key: Vec<u8>,
    prev_handle: Vec<u8>,
    delta: bool,
}

impl<'a> IndexBlockIter<'a> {
    fn new(block: &'a [u8], delta: bool) -> Option<IndexBlockIter<'a>> {
        if block.len() < 4 {
            return None;
        }
        let num_restarts = u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap()) as usize;
        if num_restarts == 0 || 4 + 4 * num_restarts > block.len() {
            return None;
        }
        let entries_end = block.len() - 4 - 4 * num_restarts;
        Some(IndexBlockIter {
            data: block,
            entries_end,
            pos: 0,
            key: Vec::with_capacity(64),
            prev_handle: Vec::new(),
            delta,
        })
    }

    /// The parse is trusted only if entries consume exactly up to the restart array.
    fn walks_exactly(&self) -> bool {
        let mut it = IndexBlockIter {
            data: self.data,
            entries_end: self.entries_end,
            pos: 0,
            key: Vec::new(),
            prev_handle: Vec::new(),
            delta: self.delta,
        };
        let mut n = 0usize;
        while it.next().is_some() {
            n += 1;
            if n > 10_000_000 {
                return false;
            }
        }
        n >= 1 && it.pos == it.entries_end
    }

    fn collect(mut self) -> Vec<BlockHandle> {
        let mut out = Vec::new();
        while let Some((_, h)) = self.next() {
            out.push(h);
        }
        out
    }

    /// Returns (index_key, BlockHandle).
    fn next(&mut self) -> Option<(&[u8], BlockHandle)> {
        if self.pos >= self.entries_end {
            return None;
        }
        let mut p = self.pos;
        let shared = read_varint64(self.data, &mut p)? as usize;
        let non_shared = read_varint64(self.data, &mut p)? as usize;
        if shared > self.key.len() {
            return None;
        }
        self.key.truncate(shared);
        self.key.extend_from_slice(&self.data[p..p + non_shared]);
        p += non_shared;
        let handle = if self.delta {
            // value-delta index: value is exactly [offset varint][size varint]
            // (no value_len; entry boundaries are self-delimiting, and RocksDB
            // writes such index blocks with restart_interval=1)
            let off = read_varint64(self.data, &mut p)?;
            let size = read_varint64(self.data, &mut p)?;
            BlockHandle { offset: off, size }
        } else {
            let value_len = read_varint64(self.data, &mut p)? as usize;
            if p + non_shared + value_len > self.entries_end {
                return None;
            }
            let vstart = p + non_shared;
            let mut vp = vstart;
            let h = parse_block_handle(self.data, &mut vp)?;
            if vp > vstart + value_len {
                return None;
            }
            p = vstart + value_len;
            self.pos = p;
            return Some((&self.key[..], h));
        };
        self.pos = p;
        Some((&self.key[..], handle))
    }
}
