//! The statement hook: `sqlite3_trace_v2` on every connection opened after the first engine open (installed by an
//! auto-extension). It raises the schema barrier before a DDL statement runs, and on a mw_rebase=1 connection it notes
//! what the statements of a transaction read (rebase.rs).

use crate::lane::Lane;
use crate::store::lock;
use libsqlite3_sys as ffi;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_uint, c_void};
use std::sync::{Mutex, Once, OnceLock};

const TRACE_STMT: c_uint = 0x01;
const TRACE_PROFILE: c_uint = 0x02;

static HOOK: Once = Once::new();

/// sqlite3* -> lane of its main database (a cache of the file control that asks the VFS).
fn lanes() -> &'static Mutex<HashMap<usize, usize>> {
    static L: OnceLock<Mutex<HashMap<usize, usize>>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register_hook() {
    HOOK.call_once(|| unsafe {
        ffi::sqlite3_auto_extension(Some(connection_init));
    });
}

pub fn forget_lane(l: *mut Lane) {
    lock(lanes()).retain(|_, v| *v != l as usize);
}

/// The lane of the main database of a connection (null: not opened with the engine).
pub fn lane_of_db(db: *mut ffi::sqlite3) -> *mut Lane {
    if let Some(&l) = lock(lanes()).get(&(db as usize)) {
        return l as *mut Lane;
    }
    let mut l: *mut Lane = std::ptr::null_mut();
    let rc = unsafe { ffi::sqlite3_file_control(db, c"main".as_ptr(), crate::vfs::FCNTL_LANE, &mut l as *mut *mut Lane as *mut c_void) };
    if rc != 0 || l.is_null() {
        return std::ptr::null_mut();
    }
    lock(lanes()).insert(db as usize, l as usize);
    unsafe { (*l).rb.cdb = db };
    l
}

unsafe extern "C" fn connection_init(db: *mut ffi::sqlite3, _err: *mut *mut c_char, _api: *const ffi::sqlite3_api_routines) -> c_int {
    unsafe {
        let mut ctx = std::ptr::null_mut();
        let fname = ffi::sqlite3_db_filename(db, c"main".as_ptr());
        if !fname.is_null() && crate::vfs::uri_bool(fname, "mw_rebase", false) && !crate::vfs::uri_bool(fname, "mw_norebase", false) {
            ctx = 1 as *mut c_void;
        }
        ffi::sqlite3_trace_v2(db, TRACE_STMT | TRACE_PROFILE, Some(trace_cb), ctx);
    }
    0
}

/// The first keyword of an SQL text, after white space and comments.
pub fn first_keyword(sql: &[u8]) -> &[u8] {
    let s = crate::rebase::sql_skip(sql);
    let n = s.iter().take_while(|c| c.is_ascii_alphabetic()).count();
    &s[..n]
}

fn is_ddl(sql: &[u8]) -> bool {
    let k = first_keyword(sql);
    ["CREATE", "DROP", "ALTER", "REINDEX", "VACUUM"].iter().any(|w| k.eq_ignore_ascii_case(w.as_bytes()))
}

unsafe extern "C" fn trace_cb(typ: c_uint, ctx: *mut c_void, p: *mut c_void, _x: *mut c_void) -> c_int {
    if typ != TRACE_STMT && typ != TRACE_PROFILE {
        return 0;
    }
    unsafe {
        let st = p as *mut ffi::sqlite3_stmt;
        let db = ffi::sqlite3_db_handle(st);
        if !ctx.is_null() {
            let l = lane_of_db(db);
            if !l.is_null() {
                (*l).stmt_note(typ == TRACE_PROFILE, st);
            }
        }
        let z = ffi::sqlite3_sql(st);
        if z.is_null() || !is_ddl(std::ffi::CStr::from_ptr(z).to_bytes()) {
            return 0;
        }
        let l = lane_of_db(db);
        if l.is_null() {
            return 0;
        }
        if typ == TRACE_STMT {
            (*l).ddl_begin();
        } else if (*l).ddl_active && !(*l).snap_held {
            (*l).ddl_end();
        }
    }
    0
}
