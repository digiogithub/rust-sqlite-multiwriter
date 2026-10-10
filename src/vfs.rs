//! The VFS. It wraps the platform VFS of SQLite: the main file of a database opened with the engine (URI
//! "vfs=multiwriter" or "mw=1") becomes a lane, and its "-wal" lives in memory; every other file (temporary files, a
//! database opened without the engine, ATTACH, VACUUM INTO) is opened by the platform VFS as it is.

use crate::db;
use crate::lane::{Lane, SHM_MAX_REGIONS, SHM_REGION};
use crate::util::*;
use libsqlite3_sys as ffi;
use std::cell::Cell;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering::*};
use std::sync::Once;

pub const VFS_NAME: &str = "multiwriter";

#[repr(C)]
struct MwFile {
    base: ffi::sqlite3_file,
    lane: *mut Lane,
    is_wal: bool,
}

/// The platform file of a wrapped main file lives right after the header.
const HDR: usize = (std::mem::size_of::<MwFile>() + 15) & !15;

static ROOT: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(ptr::null_mut());
static REGISTER: Once = Once::new();
static mut REG_RC: c_int = 0;

thread_local! {
    /// The lane whose main file this thread touched last (pairs a new WAL with its lane).
    static TLS_LANE: Cell<*mut Lane> = const { Cell::new(ptr::null_mut()) };
}

fn root() -> &'static ffi::sqlite3_vfs {
    unsafe { &*ROOT.load(Acquire) }
}

unsafe fn real(p: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { (p as *mut u8).add(HDR) as *mut ffi::sqlite3_file }
}

unsafe fn lane_of<'a>(p: *mut ffi::sqlite3_file) -> &'a mut Lane {
    unsafe { &mut *(*(p as *mut MwFile)).lane }
}

fn note(l: &mut Lane) {
    if !l.wal_bound {
        TLS_LANE.with(|c| c.set(l as *mut Lane));
    }
}

unsafe fn rmethods(f: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    unsafe { &*(*f).pMethods }
}

/// Registers the VFS (idempotent). Called by `register()`.
pub fn register() -> c_int {
    REGISTER.call_once(|| unsafe {
        let rc = ffi::sqlite3_initialize();
        if rc != 0 {
            REG_RC = rc;
            return;
        }
        let r = ffi::sqlite3_vfs_find(ptr::null());
        if r.is_null() {
            REG_RC = RC_ERROR;
            return;
        }
        ROOT.store(r, Release);
        let mut v: ffi::sqlite3_vfs = ptr::read(r);
        v.zName = CString::new(VFS_NAME).unwrap().into_raw();
        v.pNext = ptr::null_mut();
        v.pAppData = ptr::null_mut();
        v.szOsFile = (*r).szOsFile + HDR as c_int;
        v.xOpen = Some(vfs_open);
        v.xDelete = Some(vfs_delete);
        let p = Box::into_raw(Box::new(v));
        REG_RC = ffi::sqlite3_vfs_register(p, 0);
    });
    unsafe { REG_RC }
}

unsafe fn uri_param(z: *const c_char, name: &str) -> Option<String> {
    let n = CString::new(name).unwrap();
    let v = unsafe { ffi::sqlite3_uri_parameter(z, n.as_ptr()) };
    if v.is_null() { None } else { Some(unsafe { CStr::from_ptr(v) }.to_string_lossy().into_owned()) }
}

pub(crate) unsafe fn uri_bool(z: *const c_char, name: &str, dflt: bool) -> bool {
    match unsafe { uri_param(z, name) } {
        None => dflt,
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "yes" => true,
            "0" | "false" | "off" | "no" => false,
            s => s.parse::<i64>().map_or(dflt, |n| n != 0),
        },
    }
}

unsafe fn uri_int(z: *const c_char, name: &str, dflt: i64) -> i64 {
    unsafe { uri_param(z, name) }.and_then(|v| v.parse().ok()).unwrap_or(dflt)
}

/// 0 = the platform VFS as it is, 1 = the engine, -1 = a value of mw= that is not 0 or 1 (the open fails).
unsafe fn mode_for(z: *const c_char, flags: c_int) -> i32 {
    if z.is_null() || flags & ffi::SQLITE_OPEN_MAIN_DB == 0 {
        return 0;
    }
    match unsafe { uri_param(z, "mw") } {
        None => {
            // vfs=multiwriter turns the engine on; the databases that VACUUM INTO or ATTACH open have plain names
            if unsafe { uri_param(z, "vfs") }.as_deref() == Some(VFS_NAME) { 1 } else { 0 }
        }
        Some(v) => match v.to_ascii_lowercase().as_str() {
            "0" | "false" | "off" | "no" => 0,
            "1" | "true" | "on" | "yes" => 1,
            _ => -1,
        },
    }
}

unsafe fn root_open(z: *const c_char, f: *mut ffi::sqlite3_file, flags: c_int, out: *mut c_int) -> c_int {
    let r = ROOT.load(Acquire);
    unsafe { ((*r).xOpen.unwrap())(r, z, f, flags, out) }
}

unsafe extern "C" fn vfs_open(_v: *mut ffi::sqlite3_vfs, z: *const c_char, p: *mut ffi::sqlite3_file, flags: c_int, out: *mut c_int) -> c_int {
    unsafe {
        // the WAL of a database opened with the engine lives in memory only
        if flags & ffi::SQLITE_OPEN_WAL != 0 && !z.is_null() {
            let name = CStr::from_ptr(z).to_string_lossy();
            if let Some(base) = name.strip_suffix("-wal") {
                if db::lookup(base).is_some() {
                    let l = TLS_LANE.with(|c| c.get());
                    if l.is_null() {
                        return RC_CANTOPEN;
                    }
                    let lr = &mut *l;
                    if lr.wal_bound || lr.db.path != base {
                        return RC_CANTOPEN;
                    }
                    lr.wal_bound = true;
                    lr.wal.size = 0;
                    let f = p as *mut MwFile;
                    (*f).lane = l;
                    (*f).is_wal = true;
                    (*f).base.pMethods = &WAL_IO;
                    if !out.is_null() {
                        *out = flags;
                    }
                    return RC_OK;
                }
            }
        }
        let mode = mode_for(z, flags);
        if mode < 0 {
            return RC_CANTOPEN;
        }
        if mode == 0 {
            return root_open(z, p, flags, out);
        }
        let path = CStr::from_ptr(z).to_string_lossy().into_owned();
        if uri_param(z, "cache").as_deref() == Some("shared") {
            return RC_CANTOPEN; // a shared cache shares one pager between connections: no private lanes
        }
        if flags & ffi::SQLITE_OPEN_READONLY != 0 && !is_clean_wal_db(&path) {
            return root_open(z, p, flags, out);
        }
        if flags & ffi::SQLITE_OPEN_CREATE == 0 && std::fs::metadata(&path).is_err() {
            return RC_CANTOPEN;
        }
        let rc = ensure_wal_db(&path);
        if rc != RC_OK {
            return rc;
        }
        let rf = real(p);
        let rc = root_open(z, rf, flags, out);
        if rc != RC_OK {
            return rc;
        }
        let d = db::acquire(&path);
        let mut l = Box::new(Lane::new(d.clone()));
        l.readcheck = uri_bool(z, "mw_readcheck", true);
        l.noreloc = uri_bool(z, "mw_noreloc", false);
        l.nomerge = uri_bool(z, "mw_nomerge", false);
        l.core.noroute = uri_bool(z, "mw_noroute", false);
        l.hot_credit = uri_int(z, "mw_hot_credit", crate::lane::DEFAULT_HOT_CREDIT as i64) as i32;
        l.rb.norebase = uri_bool(z, "mw_norebase", false);
        l.rb.on = uri_bool(z, "mw_rebase", false) && !l.rb.norebase;
        l.rb.no_backoff = !uri_bool(z, "mw_rebase_backoff", true);
        let rc = open_main(rf, &d, &path);
        if rc != RC_OK {
            drop(l);
            if let Some(m) = (*rf).pMethods.as_ref() {
                (m.xClose.unwrap())(rf);
            }
            db::release(&d);
            return rc;
        }
        let e = d.engine();
        if uri_param(z, "mw_log_max_mb").is_some() {
            d.log_max_bytes.store(uri_int(z, "mw_log_max_mb", 32) << 20, Relaxed);
        }
        if uri_param(z, "mw_gc").is_some() {
            d.gc_interval.store(uri_int(z, "mw_gc", 64), Relaxed);
        }
        if uri_param(z, "mw_base_cache_mb").is_some() {
            e.store.base_limit.store(uri_int(z, "mw_base_cache_mb", 64) << 20, Relaxed);
        }
        let lp = Box::into_raw(l);
        let f = p as *mut MwFile;
        (*f).lane = lp;
        (*f).is_wal = false;
        (*f).base.pMethods = &LANE_IO;
        note(&mut *lp);
        crate::hook::register_hook();
        RC_OK
    }
}

/// The first lane of a database creates the page store from the real file's header and opens (or recovers) the log.
unsafe fn open_main(rf: *mut ffi::sqlite3_file, d: &std::sync::Arc<db::MwDb>, path: &str) -> c_int {
    let mut h = [0u8; 100];
    let rc = unsafe { (rmethods(rf).xRead.unwrap())(rf, h.as_mut_ptr() as *mut c_void, 100, 0) };
    if rc != RC_OK {
        return rc;
    }
    if h[52..56] != [0, 0, 0, 0] {
        return RC_CANTOPEN; // auto_vacuum: page relocation is not supported
    }
    let mut pgsz = ((h[16] as usize) << 8) | h[17] as usize;
    if pgsz == 1 {
        pgsz = 65536;
    }
    if pgsz < 512 || pgsz & (pgsz - 1) != 0 {
        return RC_NOTADB;
    }
    let _g = crate::store::lock(&d.open_mu);
    if let Some(e) = d.engine_opt() {
        return if e.store.pgsz == pgsz { RC_OK } else { RC_MISUSE };
    }
    let Ok(real) = std::fs::OpenOptions::new().read(true).write(true).open(path) else { return RC_CANTOPEN };
    let size = real.metadata().map_or(0, |m| m.len());
    let st = crate::store::Store::new(pgsz, (size / pgsz as u64) as u32, h[20] as usize);
    match crate::log::Log::open(path, pgsz, &st) {
        Err(rc) => rc,
        Ok((lg, last)) => {
            crate::store::lock(&st.seq).compacted_epoch = lg.base_epoch();
            d.set_engine(db::Engine { store: st, log: lg, real }, last);
            d.start_compactor();
            RC_OK
        }
    }
}

unsafe extern "C" fn vfs_delete(_v: *mut ffi::sqlite3_vfs, z: *const c_char, sync_dir: c_int) -> c_int {
    unsafe {
        if !z.is_null() {
            let name = CStr::from_ptr(z).to_string_lossy();
            if let Some(base) = name.strip_suffix("-wal") {
                if db::lookup(base).is_some() {
                    return RC_OK;
                }
            }
        }
        let r = ROOT.load(Acquire);
        ((*r).xDelete.unwrap())(r, z, sync_dir)
    }
}

// MARK: - WAL-mode bootstrap

/// The first 100 bytes of the file, through the platform VFS (closing a descriptor of the file would drop every POSIX
/// lock the process holds on it; the platform VFS keeps it open while others hold locks).
fn read_header(path: &str) -> Option<[u8; 100]> {
    let r = root();
    let mut buf = vec![0u64; (r.szOsFile as usize).div_ceil(8)];
    let f = buf.as_mut_ptr() as *mut ffi::sqlite3_file;
    let zp = CString::new(path).ok()?;
    let mut out = 0;
    unsafe {
        if root_open(zp.as_ptr(), f, ffi::SQLITE_OPEN_READONLY | ffi::SQLITE_OPEN_MAIN_DB, &mut out) != RC_OK {
            return None;
        }
        let mut h = [0u8; 100];
        let rc = match (*f).pMethods.as_ref() {
            Some(m) => (m.xRead.unwrap())(f, h.as_mut_ptr() as *mut c_void, 100, 0),
            None => RC_IOERR,
        };
        if let Some(m) = (*f).pMethods.as_ref() {
            (m.xClose.unwrap())(f);
        }
        if rc == RC_OK { Some(h) } else { None }
    }
}

fn stale_wal(path: &str) -> bool {
    std::fs::metadata(format!("{path}-wal")).is_ok_and(|m| m.len() > 0)
}

fn is_clean_wal_db(path: &str) -> bool {
    read_header(path).is_some_and(|h| h[18] == 2 && h[19] == 2) && !stale_wal(path)
}

/// Converts a new or rollback-journal database to WAL mode with a private stock connection, and checkpoints a real
/// -wal left over by stock use.
fn ensure_wal_db(path: &str) -> c_int {
    let stale = stale_wal(path);
    if read_header(path).is_some_and(|h| h[18] == 2 && h[19] == 2) && !stale {
        return RC_OK;
    }
    let Ok(zp) = CString::new(path) else { return RC_CANTOPEN };
    let mut c: *mut ffi::sqlite3 = ptr::null_mut();
    let mut rc = unsafe { ffi::sqlite3_open_v2(zp.as_ptr(), &mut c, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_PRIVATECACHE, root().zName) };
    if rc == RC_OK {
        unsafe { ffi::sqlite3_busy_timeout(c, 5000) };
        rc = exec(c, "PRAGMA journal_mode=WAL");
        if rc == RC_OK && stale {
            rc = exec(c, "PRAGMA wal_checkpoint(TRUNCATE)");
        }
    }
    unsafe { ffi::sqlite3_close(c) };
    if rc != RC_OK {
        return rc;
    }
    // a file that is not in WAL mode would have its writes dropped by the lane: refuse to open rather than lose commits
    if !read_header(path).is_some_and(|h| h[18] == 2 && h[19] == 2) || stale_wal(path) {
        return RC_BUSY;
    }
    RC_OK
}

pub(crate) fn exec(db: *mut ffi::sqlite3, sql: &str) -> c_int {
    let s = CString::new(sql).unwrap();
    unsafe { ffi::sqlite3_exec(db, s.as_ptr(), None, ptr::null_mut(), ptr::null_mut()) }
}

// MARK: - the main database file of a lane

static LANE_IO: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(lm_close),
    xRead: Some(lm_read),
    xWrite: Some(lm_write),
    xTruncate: Some(lm_truncate),
    xSync: Some(lm_sync),
    xFileSize: Some(lm_file_size),
    xLock: Some(lm_lock),
    xUnlock: Some(lm_unlock),
    xCheckReservedLock: Some(lm_check_reserved),
    xFileControl: Some(lm_file_control),
    xSectorSize: Some(lm_sector_size),
    xDeviceCharacteristics: Some(lm_device_char),
    xShmMap: Some(lm_shm_map),
    xShmLock: Some(lm_shm_lock),
    xShmBarrier: Some(lm_shm_barrier),
    xShmUnmap: Some(lm_shm_unmap),
    xFetch: Some(lm_fetch),
    xUnfetch: Some(lm_unfetch),
};

unsafe extern "C" fn lm_close(p: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let f = p as *mut MwFile;
        let lp = (*f).lane;
        TLS_LANE.with(|c| {
            if c.get() == lp {
                c.set(ptr::null_mut())
            }
        });
        let l = Box::from_raw(lp);
        let d = l.db.clone();
        crate::hook::forget_lane(lp);
        drop(l); // (the snapshot, the schema barrier and the helper of the rebase go with it)
        let rf = real(p);
        let rc = match (*rf).pMethods.as_ref() {
            Some(m) => (m.xClose.unwrap())(rf),
            None => RC_OK,
        };
        (*f).base.pMethods = ptr::null();
        db::release(&d);
        rc
    }
}

unsafe extern "C" fn lm_read(p: *mut ffi::sqlite3_file, buf: *mut c_void, n: c_int, off: i64) -> c_int {
    unsafe {
        let l = lane_of(p);
        note(l);
        l.main_read(std::slice::from_raw_parts_mut(buf as *mut u8, n as usize), off as u64)
    }
}

unsafe extern "C" fn lm_write(_p: *mut ffi::sqlite3_file, _b: *const c_void, _n: c_int, _off: i64) -> c_int {
    RC_OK // swallowed: only the compactor writes the real file
}
unsafe extern "C" fn lm_truncate(_p: *mut ffi::sqlite3_file, _s: i64) -> c_int {
    RC_OK
}
unsafe extern "C" fn lm_sync(_p: *mut ffi::sqlite3_file, _f: c_int) -> c_int {
    RC_OK
}

unsafe extern "C" fn lm_file_size(p: *mut ffi::sqlite3_file, out: *mut i64) -> c_int {
    unsafe {
        let l = lane_of(p);
        note(l);
        *out = l.main_file_size();
    }
    RC_OK
}

// File locks are virtualised above SHARED: a lane never writes the real file. The real SHARED lock is kept.
unsafe extern "C" fn lm_lock(p: *mut ffi::sqlite3_file, lvl: c_int) -> c_int {
    unsafe {
        note(lane_of(p));
        if lvl <= ffi::SQLITE_LOCK_SHARED {
            let rf = real(p);
            return (rmethods(rf).xLock.unwrap())(rf, lvl);
        }
    }
    RC_OK
}

unsafe extern "C" fn lm_unlock(p: *mut ffi::sqlite3_file, lvl: c_int) -> c_int {
    if lvl >= ffi::SQLITE_LOCK_SHARED {
        return RC_OK;
    }
    unsafe {
        let rf = real(p);
        (rmethods(rf).xUnlock.unwrap())(rf, lvl)
    }
}

unsafe extern "C" fn lm_check_reserved(_p: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    unsafe { *out = 0 };
    RC_OK
}

fn sqlite_string(s: &str) -> *mut c_char {
    unsafe {
        let p = ffi::sqlite3_malloc(s.len() as c_int + 1) as *mut u8;
        if !p.is_null() {
            ptr::copy_nonoverlapping(s.as_ptr(), p, s.len());
            *p.add(s.len()) = 0;
        }
        p as *mut c_char
    }
}

fn auto_vacuum_is_none(z: &str) -> bool {
    match z.to_ascii_lowercase().as_str() {
        "none" => true,
        "full" | "incremental" => false,
        s => s.parse::<i64>().map_or(true, |v| v != 1 && v != 2),
    }
}

pub const FCNTL_LANE: c_int = 0x4d570003;

unsafe extern "C" fn lm_file_control(p: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    unsafe {
        let l = lane_of(p);
        note(l);
        match op {
            FCNTL_LANE => {
                *(arg as *mut *mut Lane) = l as *mut Lane;
                return RC_OK;
            }
            ffi::SQLITE_FCNTL_MMAP_SIZE => {
                *(arg as *mut i64) = 0; // reads must go through xRead
                return RC_OK;
            }
            ffi::SQLITE_FCNTL_SIZE_HINT => return RC_OK,
            ffi::SQLITE_FCNTL_PRAGMA => {
                let az = arg as *mut *mut c_char;
                if (*az.add(1)).is_null() {
                    return RC_NOTFOUND;
                }
                let name = CStr::from_ptr(*az.add(1)).to_string_lossy().to_ascii_lowercase();
                let val = if (*az.add(2)).is_null() { String::new() } else { CStr::from_ptr(*az.add(2)).to_string_lossy().into_owned() };
                let fail = |msg: String| {
                    *az = sqlite_string(&msg);
                    RC_ERROR
                };
                if name == "journal_mode" && !val.is_empty() && !val.eq_ignore_ascii_case("wal") {
                    return fail(format!("multiwriter: journal_mode={val} is not supported (WAL only)"));
                }
                if name == "locking_mode" && val.eq_ignore_ascii_case("exclusive") {
                    return fail("multiwriter: locking_mode=EXCLUSIVE is not supported".into());
                }
                if name == "auto_vacuum" && !val.is_empty() && !auto_vacuum_is_none(&val) {
                    return fail(format!("multiwriter: auto_vacuum={val} is not supported (NONE only)"));
                }
                if name == "synchronous" && !val.is_empty() {
                    l.sync_level = match val.to_ascii_lowercase().as_str() {
                        "off" => 0,
                        "normal" => 1,
                        "full" => 2,
                        "extra" => 3,
                        s => s.parse().unwrap_or(l.sync_level),
                    };
                    return RC_NOTFOUND; // observed, not handled: SQLite still executes it
                }
                if name == "mmap_size" && !val.is_empty() {
                    *az = sqlite_string("0");
                    return RC_OK;
                }
                return RC_NOTFOUND;
            }
            _ => {}
        }
        let rf = real(p);
        (rmethods(rf).xFileControl.unwrap())(rf, op, arg)
    }
}

unsafe extern "C" fn lm_sector_size(p: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let rf = real(p);
        (rmethods(rf).xSectorSize.unwrap())(rf)
    }
}

unsafe extern "C" fn lm_device_char(p: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let rf = real(p);
        (rmethods(rf).xDeviceCharacteristics.unwrap())(rf)
    }
}

// The wal-index of a lane is private memory: no other connection can observe it.
unsafe extern "C" fn lm_shm_map(p: *mut ffi::sqlite3_file, page: c_int, size: c_int, extend: c_int, pp: *mut *mut c_void) -> c_int {
    unsafe {
        let l = lane_of(p);
        note(l);
        if size as usize != SHM_REGION || page as usize >= SHM_MAX_REGIONS {
            return RC_IOERR_SHMSIZE;
        }
        while page as usize >= l.shm.len() {
            if extend == 0 {
                *pp = ptr::null_mut();
                return RC_OK;
            }
            l.shm.push(vec![0u8; SHM_REGION].into_boxed_slice());
        }
        *pp = l.shm[page as usize].as_mut_ptr() as *mut c_void;
    }
    RC_OK
}

unsafe extern "C" fn lm_shm_lock(p: *mut ffi::sqlite3_file, ofst: c_int, n: c_int, flags: c_int) -> c_int {
    unsafe { lane_of(p).on_shm_lock(ofst, n, flags) }
}

unsafe extern "C" fn lm_shm_barrier(_p: *mut ffi::sqlite3_file) {
    std::sync::atomic::compiler_fence(SeqCst);
}

unsafe extern "C" fn lm_shm_unmap(_p: *mut ffi::sqlite3_file, _d: c_int) -> c_int {
    RC_OK // the regions are freed with the lane
}

unsafe extern "C" fn lm_fetch(_p: *mut ffi::sqlite3_file, _o: i64, _n: c_int, pp: *mut *mut c_void) -> c_int {
    unsafe { *pp = ptr::null_mut() }; // no mmap
    RC_OK
}

unsafe extern "C" fn lm_unfetch(_p: *mut ffi::sqlite3_file, _o: i64, _q: *mut c_void) -> c_int {
    RC_OK
}

// MARK: - the private WAL (memory only)

static WAL_IO: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(wal_close),
    xRead: Some(wal_read),
    xWrite: Some(wal_write),
    xTruncate: Some(wal_truncate),
    xSync: Some(lm_sync),
    xFileSize: Some(wal_file_size),
    xLock: Some(wal_nolock),
    xUnlock: Some(wal_nolock),
    xCheckReservedLock: Some(lm_check_reserved),
    xFileControl: Some(wal_file_control),
    xSectorSize: Some(wal_sector_size),
    xDeviceCharacteristics: Some(wal_device_char),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn wal_close(p: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let l = lane_of(p);
        l.wal_bound = false;
        l.wal.size = 0;
        (*p).pMethods = ptr::null();
    }
    RC_OK
}

unsafe extern "C" fn wal_read(p: *mut ffi::sqlite3_file, buf: *mut c_void, n: c_int, off: i64) -> c_int {
    unsafe { lane_of(p).wal_read(std::slice::from_raw_parts_mut(buf as *mut u8, n as usize), off as u64) }
}

unsafe extern "C" fn wal_write(p: *mut ffi::sqlite3_file, buf: *const c_void, n: c_int, off: i64) -> c_int {
    unsafe { lane_of(p).wal_write(std::slice::from_raw_parts(buf as *const u8, n as usize), off as u64) }
}

unsafe extern "C" fn wal_truncate(p: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe { lane_of(p).wal_truncate(size as u64) };
    RC_OK
}

unsafe extern "C" fn wal_file_size(p: *mut ffi::sqlite3_file, out: *mut i64) -> c_int {
    unsafe { *out = lane_of(p).wal.size as i64 };
    RC_OK
}

unsafe extern "C" fn wal_nolock(_p: *mut ffi::sqlite3_file, _l: c_int) -> c_int {
    RC_OK
}
unsafe extern "C" fn wal_file_control(_p: *mut ffi::sqlite3_file, _op: c_int, _a: *mut c_void) -> c_int {
    RC_NOTFOUND
}
unsafe extern "C" fn wal_sector_size(_p: *mut ffi::sqlite3_file) -> c_int {
    512
}
unsafe extern "C" fn wal_device_char(_p: *mut ffi::sqlite3_file) -> c_int {
    0
}
