//! The rebase (placeholder until the port lands).
use crate::lane::Lane;
use crate::util::*;
use libsqlite3_sys as ffi;

#[derive(Default)]
pub struct Queue {}

pub struct LaneRebase {
    pub on: bool,
    pub norebase: bool,
    pub no_backoff: bool,
    pub streak: i32,
    pub skip: i32,
    pub cdb: *mut ffi::sqlite3,
}
impl Default for LaneRebase {
    fn default() -> Self {
        LaneRebase { on: false, norebase: false, no_backoff: false, streak: 0, skip: 0, cdb: std::ptr::null_mut() }
    }
}

pub fn sql_skip(mut s: &[u8]) -> &[u8] {
    loop {
        while let Some((&c, rest)) = s.split_first() {
            if matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c) { s = rest } else { break }
        }
        if s.starts_with(b"--") {
            match s.iter().position(|&c| c == b'\n') { Some(i) => s = &s[i..], None => return &[] }
        } else if s.starts_with(b"/*") {
            match s[2..].windows(2).position(|w| w == b"*/") { Some(i) => s = &s[i + 4..], None => return &[] }
        } else {
            return s;
        }
    }
}

impl Lane {
    pub fn stmt_note(&mut self, _ending: bool, _st: *mut ffi::sqlite3_stmt) {}
    pub fn can_rebase(&mut self, _pg1: Option<&[u8]>, _cookie: u32) -> bool { false }
    pub fn rebase(&mut self, _imgs: &[&[u8]], _cookie: u32) -> Result<u64, i32> { Err(ERR_CONFLICT) }
    pub fn rebase_refused(&mut self) {}
}
