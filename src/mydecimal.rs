//! MySQL/TiDB NEWDECIMAL binary ("bin") format decoder, best effort.
//!
//! Layout: [precision(1)][frac(1)][negative flag(1)][digit words BE]
//! Digit words: 9 decimal digits per 4-byte word, the leading/trailing partial
//! word carries (digits % 9) digits sized per DIGIT2BYTES. We validate by
//! exact consumption; on nonsense output the caller keeps a hex fallback.

const DIGIT2BYTES: [usize; 9] = [0, 1, 1, 2, 2, 3, 3, 4, 4];

pub fn decode_bin(b: &[u8], col: &crate::rowcodec::Column) -> Option<(String, usize)> {
    if b.len() < 3 {
        return None;
    }
    // Prefer schema-derived precision/frac; the on-disk header is authoritative
    // when it looks sane.
    let (prec, frac) = {
        let hp = b[0] as i64;
        let hf = b[1] as i64;
        if hp >= hf && hf >= 0 && hp <= 65 {
            (hp, hf)
        } else {
            (col.flen.max(0) as i64, col.decimal.max(0) as i64)
        }
    };
    if prec == 0 || prec > 65 || frac > 30 || frac > prec {
        return None;
    }
    let intg = prec - frac;
    let intg_words = (intg / 9) as usize;
    let leading_digits = (intg % 9) as usize;
    let frac_words = (frac / 9) as usize;
    let trailing_digits = (frac % 9) as usize;

    let data_len = DIGIT2BYTES[leading_digits]
        + 4 * intg_words
        + 4 * frac_words
        + DIGIT2BYTES[trailing_digits];
    let total = 3 + data_len;
    if b.len() < total {
        return None;
    }
    let data = &b[3..total];

    // negative flag: byte2, or MySQL-style 0x80 mask XORed into the first data
    // byte by older encodings — accept either.
    let (neg, data): (bool, Vec<u8>) = if b[2] == 0 {
        (false, data.to_vec())
    } else if b[2] == 1 {
        (true, data.to_vec())
    } else {
        // masked encoding: sign stored in high bit of first byte (set = positive)
        let sign_mask = data[0] & 0x80 != 0;
        let d0 = data[0] & 0x7f;
        let mut d = vec![d0];
        d.extend_from_slice(&data[1..]);
        (!sign_mask, d)
    };
    let data: &[u8] = &data;

    let mut pos = 0usize;
    let mut intg_digits: Vec<u32> = Vec::with_capacity(intg as usize);
    if leading_digits > 0 {
        let n = DIGIT2BYTES[leading_digits];
        let mut word: u32 = 0;
        for i in 0..n {
            word = (word << 8) | data[pos + i] as u32;
        }
        pos += n;
        let mut s = word.to_string();
        if s.len() > leading_digits {
            s = s[s.len() - leading_digits..].to_string();
        }
        for c in s.bytes() {
            intg_digits.push((c - b'0') as u32);
        }
    }
    for _ in 0..intg_words {
        let word = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let s = format!("{word:09}");
        for c in s.bytes() {
            intg_digits.push((c - b'0') as u32);
        }
    }

    let mut frac_digits: Vec<u32> = Vec::with_capacity(frac as usize);
    for _ in 0..frac_words {
        let word = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let s = format!("{word:09}");
        for c in s.bytes() {
            frac_digits.push((c - b'0') as u32);
        }
    }
    if trailing_digits > 0 {
        let n = DIGIT2BYTES[trailing_digits];
        let mut word: u32 = 0;
        for i in 0..n {
            word = (word << 8) | data[pos + i] as u32;
        }
        pos += n;
        let s = word.to_string();
        let s = if s.len() > trailing_digits {
            s[s.len() - trailing_digits..].to_string()
        } else {
            s
        };
        for c in s.bytes() {
            frac_digits.push((c - b'0') as u32);
        }
    }

    // sanity: all digits must be 0-9
    if intg_digits.iter().any(|&d| d > 9) || frac_digits.iter().any(|&d| d > 9) {
        return None;
    }

    let mut out = String::new();
    if neg {
        out.push('-');
    }
    // strip leading zeros but keep one
    let mut it = intg_digits.iter().skip_while(|&&d| d == 0);
    let mut any = false;
    for &d in it.by_ref() {
        out.push((b'0' + d as u8) as char);
        any = true;
    }
    if !any {
        out.push('0');
    }
    if frac > 0 {
        out.push('.');
        for &d in &frac_digits {
            out.push((b'0' + d as u8) as char);
        }
    }
    Some((out, total))
}
