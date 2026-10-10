//! Small helpers: SQLite result codes, big-endian integers, varints, a hasher for page numbers.

use std::hash::{BuildHasherDefault, Hasher};

pub const RC_OK: i32 = 0;
pub const RC_ERROR: i32 = 1;
pub const RC_BUSY: i32 = 5;
pub const RC_IOERR: i32 = 10;
pub const RC_CORRUPT: i32 = 11;
pub const RC_NOTFOUND: i32 = 12;
pub const RC_CANTOPEN: i32 = 14;
pub const RC_MISUSE: i32 = 21;
pub const RC_NOTADB: i32 = 26;
pub const RC_ROW: i32 = 100;
pub const RC_DONE: i32 = 101;
pub const RC_BUSY_SNAPSHOT: i32 = RC_BUSY | (2 << 8);
pub const RC_IOERR_SHORT_READ: i32 = RC_IOERR | (2 << 8);
pub const RC_IOERR_WRITE: i32 = RC_IOERR | (3 << 8);
pub const RC_IOERR_FSYNC: i32 = RC_IOERR | (4 << 8);
pub const RC_IOERR_SHMSIZE: i32 = RC_IOERR | (19 << 8);

/// Internal results of a publication (never seen by SQLite).
pub const ERR_CONFLICT: i32 = 0x4d57;
pub const ERR_CONFLICT_SCHEMA: i32 = 0x4d58;
pub const ERR_CONFLICT_READ: i32 = 0x4d59;
pub const ERR_RELOC_NA: i32 = 0x4d5a;

#[inline]
pub fn be32(p: &[u8]) -> u32 {
    u32::from_be_bytes([p[0], p[1], p[2], p[3]])
}
#[inline]
pub fn be16(p: &[u8]) -> usize {
    ((p[0] as usize) << 8) | p[1] as usize
}
#[inline]
pub fn put32(p: &mut [u8], v: u32) {
    p[..4].copy_from_slice(&v.to_be_bytes());
}

/// Decodes a SQLite varint; the length is 0 if it runs past the end of p.
pub fn varint(p: &[u8]) -> (u64, usize) {
    let mut v: u64 = 0;
    for i in 0..8 {
        if i >= p.len() {
            return (0, 0);
        }
        v = (v << 7) | (p[i] & 0x7f) as u64;
        if p[i] & 0x80 == 0 {
            return (v, i + 1);
        }
    }
    if p.len() < 9 {
        return (0, 0);
    }
    ((v << 8) | p[8] as u64, 9)
}

pub fn put_varint(mut v: u64) -> Vec<u8> {
    let mut o = [0u8; 9];
    if v >> 56 != 0 {
        o[8] = v as u8;
        v >>= 8;
        for i in (0..8).rev() {
            o[i] = (v & 0x7f) as u8 | 0x80;
            v >>= 7;
        }
        return o.to_vec();
    }
    let mut n = 1;
    let mut t = v >> 7;
    while t != 0 {
        n += 1;
        t >>= 7;
    }
    for i in (0..n).rev() {
        o[i] = (v & 0x7f) as u8 | if i == n - 1 { 0 } else { 0x80 };
        v >>= 7;
    }
    o[..n].to_vec()
}

/// A hasher for page numbers (a multiplicative hash: no need for SipHash here).
#[derive(Default)]
pub struct PageHasher(u64);
impl Hasher for PageHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x9E3779B97F4A7C15);
        }
    }
    fn write_u32(&mut self, i: u32) {
        self.0 = (i as u64).wrapping_mul(0x9E3779B97F4A7C15);
    }
    fn write_usize(&mut self, i: usize) {
        self.0 = (i as u64).wrapping_mul(0x9E3779B97F4A7C15);
    }
}
pub type PageMap<V> = std::collections::HashMap<u32, V, BuildHasherDefault<PageHasher>>;

pub fn debug_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("MW_DEBUG").is_some())
}

macro_rules! debugf {
    ($($arg:tt)*) => {
        if $crate::util::debug_on() { eprintln!("multiwriter: {}", format!($($arg)*)); }
    };
}
pub(crate) use debugf;
