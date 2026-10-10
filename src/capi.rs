//! A thin layer over the C API for the connections the engine opens itself (the scratch database of the catalog, the
//! helper of the rebase) and for looking at the statements of an application's connection.

use crate::util::*;
use libsqlite3_sys as ffi;
use std::ffi::{c_int, CStr, CString};
use std::ptr;

pub struct CConn {
    pub db: *mut ffi::sqlite3,
}

impl CConn {
    pub fn open(name: &str, flags: c_int, vfs: Option<&str>) -> Result<CConn, i32> {
        let zn = CString::new(name).map_err(|_| RC_CANTOPEN)?;
        let zv = vfs.map(|v| CString::new(v).unwrap());
        let mut db = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3_open_v2(zn.as_ptr(), &mut db, flags, zv.as_ref().map_or(ptr::null(), |v| v.as_ptr())) };
        let c = CConn { db };
        if rc != RC_OK {
            return Err(rc);
        }
        Ok(c)
    }
    pub fn exec(&self, sql: &str) -> i32 {
        crate::vfs::exec(self.db, sql)
    }
    pub fn prepare(&self, sql: &str) -> Result<CStmt, i32> {
        prepare_on(self.db, sql)
    }
    pub fn db_config(&self, op: c_int, v: c_int) {
        unsafe { ffi::sqlite3_db_config(self.db, op, v, ptr::null_mut::<c_int>()) };
    }
    /// Runs sql and calls f for every row.
    pub fn query(&self, sql: &str, mut f: impl FnMut(&CStmt)) -> i32 {
        let Ok(s) = self.prepare(sql) else { return RC_ERROR };
        loop {
            match s.step() {
                RC_ROW => f(&s),
                RC_DONE => return RC_OK,
                rc => return rc,
            }
        }
    }
}

impl Drop for CConn {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe { ffi::sqlite3_close(self.db) };
        }
    }
}

pub struct CStmt {
    pub p: *mut ffi::sqlite3_stmt,
}

pub fn prepare_on(db: *mut ffi::sqlite3, sql: &str) -> Result<CStmt, i32> {
    let mut p = ptr::null_mut();
    let rc = unsafe { ffi::sqlite3_prepare_v2(db, sql.as_ptr() as *const _, sql.len() as c_int, &mut p, ptr::null_mut()) };
    if rc != RC_OK || p.is_null() {
        if !p.is_null() {
            unsafe { ffi::sqlite3_finalize(p) };
        }
        return Err(if rc == RC_OK { RC_ERROR } else { rc });
    }
    Ok(CStmt { p })
}

impl Drop for CStmt {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_finalize(self.p) };
    }
}

impl CStmt {
    pub fn step(&self) -> i32 {
        unsafe { ffi::sqlite3_step(self.p) }
    }
    pub fn reset(&self) {
        unsafe { ffi::sqlite3_reset(self.p) };
    }
    pub fn clear(&self) {
        unsafe { ffi::sqlite3_clear_bindings(self.p) };
    }
    pub fn bind_i64(&self, i: usize, v: i64) -> i32 {
        unsafe { ffi::sqlite3_bind_int64(self.p, i as c_int, v) }
    }
    pub fn bind_f64(&self, i: usize, v: f64) -> i32 {
        unsafe { ffi::sqlite3_bind_double(self.p, i as c_int, v) }
    }
    pub fn bind_null(&self, i: usize) -> i32 {
        unsafe { ffi::sqlite3_bind_null(self.p, i as c_int) }
    }
    /// Binds text or a blob (copied: SQLITE_TRANSIENT).
    pub fn bind_bytes(&self, i: usize, b: &[u8], text: bool) -> i32 {
        static EMPTY: [u8; 1] = [0];
        let ptr = if b.is_empty() { EMPTY.as_ptr() } else { b.as_ptr() };
        unsafe {
            if text {
                ffi::sqlite3_bind_text(self.p, i as c_int, ptr as *const _, b.len() as c_int, ffi::SQLITE_TRANSIENT())
            } else {
                ffi::sqlite3_bind_blob(self.p, i as c_int, ptr as *const _, b.len() as c_int, ffi::SQLITE_TRANSIENT())
            }
        }
    }
    pub fn col_count(&self) -> usize {
        unsafe { ffi::sqlite3_column_count(self.p) as usize }
    }
    pub fn col_type(&self, i: usize) -> c_int {
        unsafe { ffi::sqlite3_column_type(self.p, i as c_int) }
    }
    pub fn col_i64(&self, i: usize) -> i64 {
        unsafe { ffi::sqlite3_column_int64(self.p, i as c_int) }
    }
    pub fn col_f64(&self, i: usize) -> f64 {
        unsafe { ffi::sqlite3_column_double(self.p, i as c_int) }
    }
    pub fn col_name(&self, i: usize) -> String {
        unsafe { CStr::from_ptr(ffi::sqlite3_column_name(self.p, i as c_int)) }.to_string_lossy().into_owned()
    }
    /// The text of a column (None for NULL).
    pub fn col_text(&self, i: usize) -> Option<String> {
        unsafe {
            let z = ffi::sqlite3_column_text(self.p, i as c_int);
            if z.is_null() {
                return None;
            }
            let n = ffi::sqlite3_column_bytes(self.p, i as c_int) as usize;
            Some(String::from_utf8_lossy(std::slice::from_raw_parts(z, n)).into_owned())
        }
    }
    pub fn col_blob(&self, i: usize) -> Vec<u8> {
        unsafe {
            let z = ffi::sqlite3_column_blob(self.p, i as c_int) as *const u8;
            let n = ffi::sqlite3_column_bytes(self.p, i as c_int) as usize;
            if z.is_null() || n == 0 { Vec::new() } else { std::slice::from_raw_parts(z, n).to_vec() }
        }
    }
}

/// Quotes an SQL identifier.
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
