//! The rebase (URI mw_rebase=1). A commit whose pages conflict with newer commits only on pages that it wrote is not
//! necessarily a logical conflict: two writers may change different rows of one page. Its row changes (rowdiff.rs) are
//! replayed with ordinary SQL on a helper connection at the latest snapshot instead of refusing it.
//!
//! What is checked: the schema is the one the transaction ran with; every row it changed or deleted is, at the latest
//! snapshot, exactly what it was at its snapshot; a row it inserted is not there yet, and the constraints hold. The rows
//! it read and did not change are checked by its statements: a transaction that ran a SELECT, or a statement that is not
//! a point statement (by its bytecode), is not rebased. The replays are batched: the first connection that queues
//! becomes the leader and replays everything queued in one transaction of its helper, a savepoint for each request.

use crate::capi::*;
use crate::catalog::{type_len, Catalog, Tab, COL_BLOB, COL_FLOAT, COL_INTEGER, COL_TEXT};
use crate::lane::Lane;
use crate::rowdiff::{ChgKind, RowChange};
use crate::store::lock;
use crate::util::*;
use libsqlite3_sys as ffi;
use std::collections::{HashMap, VecDeque};
use std::ffi::{c_int, CStr};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering::*};
use std::sync::Arc;

const MAX_ATTEMPTS: usize = 200;
const GATE_AFTER: usize = 1; // lost attempts before the publication gate is closed for the next one
const BATCH: usize = 64;
const RD_OTHER: i32 = 0;
const RD_DML: i32 = 1;

pub struct RbReq {
    sync_level: i32,
    chg: Vec<RowChange>,
    cookie: u32,
    fk: bool,
    state: AtomicI32, // 0 waiting, 1 committed, 2 refused (provisional until done)
    done: AtomicBool,
    epoch: AtomicU64,
}

#[derive(Default)]
pub struct Queue {
    reqs: VecDeque<Arc<RbReq>>,
    leader: bool,
}

#[derive(Default)]
struct TStmt {
    sel: Option<CStmt>,
    ins: Option<CStmt>,
    del: Option<CStmt>,
    upd: Vec<(u64, CStmt)>,
}

pub struct LaneRebase {
    pub on: bool,
    pub norebase: bool,
    pub no_backoff: bool,
    pub streak: i32,
    pub skip: i32,
    pub cdb: *mut ffi::sqlite3,
    cat: Option<Catalog>,
    ts: Vec<TStmt>,
    sync: i32,
    helper: Option<CConn>,
    hl: *mut Lane,
    rd_cur: *mut ffi::sqlite3_stmt,
    rd_cur_kind: i32,
    rd_dep: bool,
    rd_db: *mut ffi::sqlite3,
    rd_chg_base: i32,
    rd_cache: HashMap<String, (bool, u64)>,
}

impl Default for LaneRebase {
    fn default() -> Self {
        LaneRebase {
            on: false,
            norebase: false,
            no_backoff: false,
            streak: 0,
            skip: 0,
            cdb: ptr::null_mut(),
            cat: None,
            ts: Vec::new(),
            sync: -1,
            helper: None,
            hl: ptr::null_mut(),
            rd_cur: ptr::null_mut(),
            rd_cur_kind: RD_OTHER,
            rd_dep: false,
            rd_db: ptr::null_mut(),
            rd_chg_base: 0,
            rd_cache: HashMap::new(),
        }
    }
}

impl Drop for LaneRebase {
    fn drop(&mut self) {
        self.ts.clear(); // (the statements before their connection)
        self.helper = None;
    }
}

// MARK: - what the statements of a transaction read

pub fn sql_skip(mut s: &[u8]) -> &[u8] {
    loop {
        while let Some((&c, rest)) = s.split_first() {
            if matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c) {
                s = rest
            } else {
                break;
            }
        }
        if s.starts_with(b"--") {
            match s.iter().position(|&c| c == b'\n') {
                Some(i) => s = &s[i..],
                None => return &[],
            }
        } else if s.starts_with(b"/*") {
            match s[2..].windows(2).position(|w| w == b"*/") {
                Some(i) => s = &s[i + 4..],
                None => return &[],
            }
        } else {
            return s;
        }
    }
}

fn prefix_ci(s: &[u8], p: &str) -> bool {
    s.len() >= p.len() && s[..p.len()].eq_ignore_ascii_case(p.as_bytes())
}

fn stmt_kind(sql: &[u8]) -> i32 {
    let s = sql_skip(sql);
    if ["INSERT", "REPLACE", "UPDATE", "DELETE"].iter().any(|k| prefix_ci(s, k)) { RD_DML } else { RD_OTHER }
}

fn has_word(sql: &[u8], w: &str) -> bool {
    let wb = |c: u8| c == b'_' || c.is_ascii_alphanumeric();
    let n = w.len();
    (0..sql.len().saturating_sub(n - 1)).any(|i| sql[i..i + n].eq_ignore_ascii_case(w.as_bytes()) && (i == 0 || !wb(sql[i - 1])) && (i + n == sql.len() || !wb(sql[i + n])))
}

fn stmt_reads(sql: &[u8]) -> bool {
    let mut s = sql_skip(sql);
    while s.first() == Some(&b'(') {
        s = sql_skip(&s[1..]);
    }
    prefix_ci(s, "SELECT") || prefix_ci(s, "WITH") || prefix_ci(s, "VALUES")
}

/// An INSERT with several rows in VALUES is compiled with a coroutine, which the bytecode check refuses.
fn multirow_insert(sql: &[u8]) -> bool {
    let s = sql_skip(sql);
    if !prefix_ci(s, "INSERT") && !prefix_ci(s, "REPLACE") {
        return false;
    }
    for (i, &c) in s.iter().enumerate() {
        if c == b')' {
            let q = sql_skip(&s[i + 1..]);
            if q.first() == Some(&b',') && sql_skip(&q[1..]).first() == Some(&b'(') {
                return true;
            }
        }
    }
    false
}

const DENY: &[&str] = &["SorterSort", "SorterNext", "SeekScan", "OpenEphemeral", "OpenAutoindex", "OpenPseudo", "VOpen", "VFilter", "VNext", "VUpdate", "VColumn", "Gosub", "BeginSubrtn", "InitCoroutine", "Yield", "Sort"];
const CONST_OPS: &[&str] = &["Integer", "Int64", "Real", "String8", "String", "Blob", "Null", "Function", "PureFunc", "Copy", "SCopy", "IntCopy", "Variable", "Add", "Subtract", "Multiply", "Divide", "Remainder", "Concat", "Cast", "Affinity", "Not", "BitAnd", "BitOr", "ShiftLeft", "ShiftRight", "Negative", "AddImm", "Noop", "MustBeInt", "RealAffinity"];
const FK_BODY: &[&str] = &["Column", "Ne", "Eq", "Lt", "Le", "Gt", "Ge", "FkCounter", "Integer", "Copy", "SCopy", "Affinity", "IsNull", "NotNull", "Rowid", "IdxGT", "IdxGE", "IdxLT", "IdxLE", "DeferredSeek", "IdxRowid", "Goto", "MustBeInt", "Null", "String8", "Int64", "Real", "Variable"];

/// A point statement: an INSERT ... VALUES, or an UPDATE/DELETE of one row by its rowid or a unique index, that reads
/// nothing else (by its bytecode: no loop, no cursor opened for reading, no virtual table; what a foreign key adds is
/// allowed).
fn bytecode_is_point(db: *mut ffi::sqlite3, sql: &str) -> bool {
    let Ok(e) = prepare_on(db, &format!("EXPLAIN {sql}")) else { return false };
    let mut ops: Vec<(String, i64, i64, i64)> = Vec::new();
    while e.step() == RC_ROW {
        let op = e.col_text(1).unwrap_or_default();
        if ops.len() >= 400 {
            return false;
        }
        if !ops.is_empty() && op == "Init" {
            break; // the sub-programs of foreign key actions follow
        }
        ops.push((op, e.col_i64(0), e.col_i64(2), e.col_i64(3)));
    }
    drop(e);
    let rdcur: Vec<i64> = ops.iter().filter(|o| o.0 == "OpenRead").map(|o| o.2).collect();
    let mut verified_to: i64 = -1;
    let mut i = 0;
    while i < ops.len() {
        let op = ops[i].0.as_str();
        if DENY.contains(&op) {
            return false;
        }
        if op == "Once" {
            let mut k = i + 1;
            while k < ops.len() && ops[k].1 < ops[i].3 {
                if !CONST_OPS.contains(&ops[k].0.as_str()) {
                    return false;
                }
                k += 1;
            }
            i += 1;
            continue;
        }
        if op == "Program" {
            i += 1;
            continue;
        }
        let is_read = rdcur.contains(&ops[i].2);
        if op == "Rewind" || op == "Last" || (is_read && op.starts_with("Seek")) {
            let j = (i + 1..ops.len()).find(|&k| (ops[k].0 == "Next" || ops[k].0 == "Prev") && ops[k].2 == ops[i].2);
            let Some(j) = j else {
                if op.starts_with("Seek") {
                    i += 1;
                    continue; // a lookup of one row
                }
                return false;
            };
            if !is_read {
                return false; // a loop over the table that is written: a scan
            }
            let body = &ops[i + 1..j];
            if body.iter().any(|o| !FK_BODY.contains(&o.0.as_str())) || !body.iter().any(|o| o.0 == "FkCounter") {
                return false;
            }
            verified_to = j as i64;
            i = j + 1;
            continue;
        }
        if (op == "Next" || op == "Prev") && i as i64 > verified_to {
            return false;
        }
        i += 1;
    }
    true
}

impl Lane {
    fn stmt_is_point(&mut self, db: *mut ffi::sqlite3, sql: &str) -> bool {
        let generation = self.db.schema_gen.load(Acquire);
        if let Some(&(p, g)) = self.rb.rd_cache.get(sql)
            && g == generation {
                return p;
            }
        if multirow_insert(sql.as_bytes()) {
            return false;
        }
        let p = bytecode_is_point(db, sql);
        if self.rb.rd_cache.len() > 64 {
            self.rb.rd_cache.clear();
        }
        self.rb.rd_cache.insert(sql.to_string(), (p, generation));
        p
    }

    /// Called by the statement hook of a mw_rebase=1 connection when a statement starts and ends.
    pub fn stmt_note(&mut self, ending: bool, st: *mut ffi::sqlite3_stmt) {
        unsafe {
            let db = ffi::sqlite3_db_handle(st);
            if !ending {
                if ffi::sqlite3_get_autocommit(db) != 0 {
                    self.rb.rd_dep = false; // (this statement starts a transaction)
                    self.rb.rd_chg_base = ffi::sqlite3_total_changes(db);
                }
                let z = ffi::sqlite3_sql(st);
                let sql = if z.is_null() { String::new() } else { CStr::from_ptr(z).to_string_lossy().into_owned() };
                if prefix_ci(sql_skip(sql.as_bytes()), "EXPLAIN") {
                    return; // (our own)
                }
                self.rb.rd_db = db;
                if stmt_reads(sql.as_bytes()) || has_word(sql.as_bytes(), "REPLACE") {
                    self.rb.rd_dep = true; // (REPLACE deletes rows that no change counter counts)
                }
                self.rb.rd_cur = st;
                self.rb.rd_cur_kind = stmt_kind(sql.as_bytes());
                if self.rb.rd_cur_kind == RD_DML && !self.rb.rd_dep && !self.stmt_is_point(db, &sql) {
                    self.rb.rd_dep = true;
                }
                return;
            }
            if self.rb.rd_cur != st {
                return;
            }
            self.rb.rd_cur = ptr::null_mut();
            if self.rb.rd_cur_kind == RD_DML && ffi::sqlite3_stmt_readonly(st) == 0 && ffi::sqlite3_changes(db) == 0 {
                self.rb.rd_dep = true; // an UPDATE or DELETE that found nothing has read that the row is absent
            }
        }
    }

    /// Did the transaction read rows that it may not have changed? (Then it is not rebased.)
    fn reads_unchanged(&self, nnet: usize) -> bool {
        let r = &self.rb;
        if r.rd_dep || r.rd_cur.is_null() || r.rd_db.is_null() {
            return true;
        }
        unsafe {
            let mut alive = false;
            let mut st = ffi::sqlite3_next_stmt(r.rd_db, ptr::null_mut());
            while !st.is_null() {
                if st == r.rd_cur && ffi::sqlite3_stmt_busy(st) != 0 {
                    alive = true;
                }
                st = ffi::sqlite3_next_stmt(r.rd_db, st);
            }
            if !alive {
                return true; // the hook was replaced by the application's own trace callback
            }
            // the changes SQLite counted must be exactly the net row changes found in the pages (a no-op UPDATE is counted
            // and writes no page)
            let mut counted = (ffi::sqlite3_total_changes(r.rd_db) as u32).wrapping_sub(r.rd_chg_base as u32) as usize;
            if r.rd_cur_kind == RD_DML {
                counted += 1;
            }
            counted != nnet
        }
    }

    /// The connection asked for it, it is not a helper, it is the first commit of its snapshot, and it changed no schema
    /// and no field of the header the application can set.
    pub fn can_rebase(&mut self, pg1: Option<&[u8]>, cookie: u32) -> bool {
        if self.rb.norebase || !self.rb.on || self.commit_base > 0 {
            return false;
        }
        if self.rb.skip > 0 {
            self.rb.skip -= 1;
            return false;
        }
        if let Some(pg1) = pg1 {
            if be32(&pg1[40..]) != cookie {
                return false;
            }
            let mut h = [0u8; 28];
            let e = self.db.engine();
            if !e.store.read(1, self.tx.snapshot_epoch, 44, &mut h) && crate::fsio::pread(&e.real, &mut h, 44).map_or(true, |n| n < 28) {
                return false;
            }
            if h[..] != pg1[44..72] {
                return false;
            }
        }
        true
    }

    /// Refused: the next conflicts of this connection are probably true ones too: they are not replayed, 1, 3, 7...
    pub fn rebase_refused(&mut self) {
        if !self.rb.no_backoff {
            self.rb.streak = (self.rb.streak + 1).min(5);
            self.rb.skip = (1 << self.rb.streak) - 1;
        }
    }
}

// MARK: - records

struct RecCols<'a> {
    ty: Vec<u64>,
    off: Vec<usize>,
    ln: Vec<usize>,
    rec: &'a [u8],
}

fn parse_rec(rec: &[u8]) -> Option<RecCols<'_>> {
    let (hs, h) = varint(rec);
    if h == 0 || hs as usize > rec.len() {
        return None;
    }
    let mut r = RecCols { ty: Vec::new(), off: Vec::new(), ln: Vec::new(), rec };
    let (mut pos, mut hp) = (hs as usize, h);
    while hp < hs as usize && r.ty.len() < 2048 {
        let (t, k) = varint(&rec[hp..hs as usize]);
        if k == 0 {
            return None;
        }
        hp += k;
        let l = type_len(t);
        r.ty.push(t);
        r.off.push(pos);
        r.ln.push(l);
        pos += l;
    }
    if pos <= rec.len() { Some(r) } else { None }
}

fn bind_col(s: &CStmt, idx: usize, r: &RecCols, col: usize) -> i32 {
    let (t, p, l) = (r.ty[col], &r.rec[r.off[col]..], r.ln[col]);
    match t {
        0 => s.bind_null(idx),
        8 | 9 => s.bind_i64(idx, (t - 8) as i64),
        1..=6 => {
            let mut x = p[0] as i8 as i64;
            for &b in &p[1..l] {
                x = (x << 8) | b as i64;
            }
            s.bind_i64(idx, x)
        }
        7 => s.bind_f64(idx, f64::from_bits(u64::from_be_bytes(p[..8].try_into().unwrap()))),
        t if t >= 12 => s.bind_bytes(idx, &p[..l], t & 1 == 1),
        _ => RC_ERROR,
    }
}

fn val_serial(v: &crate::catalog::SqlVal) -> (u64, Vec<u8>) {
    match v.typ {
        COL_INTEGER => {
            let x = v.i;
            if x == 0 {
                return (8, vec![]);
            }
            if x == 1 {
                return (9, vec![]);
            }
            let nb: u64 = match x {
                -128..=127 => 1,
                -32768..=32767 => 2,
                -8388608..=8388607 => 3,
                -2147483648..=2147483647 => 4,
                _ if (-(1i64 << 47)..(1i64 << 47)).contains(&x) => 5,
                _ => 6,
            };
            let l = type_len(nb);
            (nb, x.to_be_bytes()[8 - l..].to_vec())
        }
        COL_FLOAT => (7, v.d.to_bits().to_be_bytes().to_vec()),
        COL_TEXT => (13 + 2 * v.b.len() as u64, v.b.clone()),
        COL_BLOB => (12 + 2 * v.b.len() as u64, v.b.clone()),
        _ => (0, vec![]),
    }
}

/// Completes a row written before an ALTER TABLE ADD COLUMN with the defaults of the columns it lacks.
fn rec_pad(t: &Tab, r: &RecCols) -> Vec<u8> {
    let (mut hdr, mut data) = (Vec::new(), Vec::new());
    for i in 0..t.nrec {
        if i < r.ty.len() {
            hdr.extend(put_varint(r.ty[i]));
            data.extend_from_slice(&r.rec[r.off[i]..r.off[i] + r.ln[i]]);
        } else {
            let (ty, d) = val_serial(&t.dflt[i]);
            hdr.extend(put_varint(ty));
            data.extend(d);
        }
    }
    let mut hl = 1;
    while put_varint((hdr.len() + hl) as u64).len() != hl {
        hl = put_varint((hdr.len() + hl) as u64).len();
    }
    let mut out = put_varint((hdr.len() + hl) as u64);
    out.extend(hdr);
    out.extend(data);
    out
}

// MARK: - the statements of a table on the helper

fn wr_pk_conds(t: &Tab) -> String {
    let mut c = Vec::new();
    let mut k = 1;
    for i in 0..t.nrec {
        if !t.writable(i) {
            continue;
        }
        k += 1;
        if i < t.npk_rec {
            c.push(format!("{}=?{k}", quote_ident(&t.rec_name[i])));
        }
    }
    c.join(" AND ")
}

impl TStmt {
    fn prepare(&mut self, h: &CConn, t: &Tab) -> bool {
        let rid = if t.without_rowid { "" } else {
            match t.rowid_name() {
                Some(r) => r,
                None => return false,
            }
        };
        let (mut cols, mut marks, mut conds) = (Vec::new(), Vec::new(), String::new());
        let mut k = 1;
        for i in 0..t.nrec {
            if !t.writable(i) {
                continue;
            }
            k += 1;
            let (q, pk) = (quote_ident(&t.rec_name[i]), format!("?{k}"));
            if t.without_rowid && i < t.npk_rec {
                conds += &format!(" AND {q}={pk}");
            } else if t.noaff[i] {
                conds += &format!(" AND {q} COLLATE BINARY IS {pk} AND typeof({q}) = typeof({pk})");
            } else {
                conds += &format!(" AND {q} COLLATE BINARY IS {pk}");
            }
            cols.push(q);
            marks.push(pk);
        }
        let tn = quote_ident(&t.name);
        let qs = if t.without_rowid {
            [format!("SELECT 1 FROM {tn} WHERE 1{conds}"), format!("INSERT INTO {tn}({}) VALUES({})", cols.join(","), marks.join(",")), format!("DELETE FROM {tn} WHERE {}", wr_pk_conds(t))]
        } else {
            let (c, m) = (cols.iter().map(|c| format!(",{c}")).collect::<String>(), marks.iter().map(|m| format!(",{m}")).collect::<String>());
            [format!("SELECT 1 FROM {tn} WHERE {rid}=?1{conds}"), format!("INSERT INTO {tn}({rid}{c}) VALUES(?1{m})"), format!("DELETE FROM {tn} WHERE {rid}=?1")]
        };
        match (h.prepare(&qs[0]), h.prepare(&qs[1]), h.prepare(&qs[2])) {
            (Ok(a), Ok(b), Ok(c)) => {
                (self.sel, self.ins, self.del) = (Some(a), Some(b), Some(c));
                true
            }
            _ => false,
        }
    }

    fn update(&mut self, h: &CConn, t: &Tab, mask: u64) -> Option<&CStmt> {
        if let Some(i) = self.upd.iter().position(|u| u.0 == mask) {
            return Some(&self.upd[i].1);
        }
        let rid = t.rowid_name().unwrap_or("rowid");
        let mut sets = Vec::new();
        let (mut bit, mut k) = (0, 1 + if t.without_rowid { t.npk_rec } else { 0 });
        for i in 0..t.nrec {
            if !t.writable(i) {
                continue;
            }
            if mask >> bit & 1 == 1 {
                k += 1;
                sets.push(format!("{}=?{k}", quote_ident(&t.rec_name[i])));
            }
            bit += 1;
        }
        let w = if t.without_rowid { wr_pk_conds(t) } else { format!("{rid}=?1") };
        let st = h.prepare(&format!("UPDATE {} SET {} WHERE {w}", quote_ident(&t.name), sets.join(","))).ok()?;
        if self.upd.len() >= 8 {
            self.upd.remove(0);
        }
        self.upd.push((mask, st));
        self.upd.last().map(|u| &u.1)
    }
}

const BIND_ALL: i32 = 0;
const BIND_MASK: i32 = 1;
const BIND_KEY: i32 = 2;

fn bind_row(s: &CStmt, t: &Tab, rowid: i64, r: Option<&RecCols>, mode: i32, only: u64) -> bool {
    s.reset();
    s.clear();
    s.bind_i64(1, rowid);
    let (mut k, mut bit) = (1, 0);
    for i in 0..t.nrec {
        if !t.writable(i) {
            continue;
        }
        let key = t.without_rowid && i < t.npk_rec;
        let take = mode == BIND_ALL || (mode == BIND_MASK && ((bit < 64 && only >> bit & 1 == 1) || key)) || (mode == BIND_KEY && key);
        bit += 1;
        if !take {
            continue;
        }
        k += 1;
        if let Some(r) = r
            && bind_col(s, k, r, i) != RC_OK {
                return false;
            }
    }
    true
}

/// The writable columns an update changes (naming an unchanged indexed column would rewrite its index).
fn changed_mask(t: &Tab, o: &RecCols, n: &RecCols) -> Option<u64> {
    let (mut mask, mut bit) = (0u64, 0);
    for i in 0..t.nrec {
        if !t.writable(i) {
            continue;
        }
        if bit >= 64 {
            return None;
        }
        if !(t.without_rowid && i < t.npk_rec) && (o.ty[i] != n.ty[i] || o.rec[o.off[i]..o.off[i] + o.ln[i]] != n.rec[n.off[i]..n.off[i] + n.ln[i]]) {
            mask |= 1 << bit;
        }
        bit += 1;
    }
    Some(mask)
}

fn step_once(s: &CStmt) -> i32 {
    let rc = s.step();
    s.reset();
    rc
}

/// One row change in the open transaction of the helper: OK, ERR_CONFLICT (a true conflict) or RC_BUSY (lost a race).
fn apply_chg(h: &CConn, lt: &Tab, ts: &mut TStmt, c: &RowChange) -> i32 {
    if ts.sel.is_none() && !ts.prepare(h, lt) {
        return ERR_CONFLICT;
    }
    let padded_old;
    let padded_new;
    let mut oldr = None;
    let mut newr = None;
    if let Some(o) = &c.old_rec {
        let Some(r) = parse_rec(o).filter(|r| r.ty.len() <= lt.nrec) else { return ERR_CONFLICT };
        if r.ty.len() < lt.nrec {
            padded_old = rec_pad(lt, &r);
            oldr = parse_rec(&padded_old);
            if oldr.is_none() {
                return ERR_CONFLICT;
            }
        } else {
            oldr = Some(r);
        }
    }
    if let Some(n) = &c.new_rec {
        let Some(r) = parse_rec(n).filter(|r| r.ty.len() <= lt.nrec) else { return ERR_CONFLICT };
        if r.ty.len() < lt.nrec {
            padded_new = rec_pad(lt, &r);
            newr = parse_rec(&padded_new);
            if newr.is_none() {
                return ERR_CONFLICT;
            }
        } else {
            newr = Some(r);
        }
    }
    if let Some(o) = &oldr {
        // it is the row that the transaction saw
        let sel = ts.sel.as_ref().unwrap();
        if !bind_row(sel, lt, c.rowid, Some(o), BIND_ALL, 0) {
            return ERR_CONFLICT;
        }
        let src = step_once(sel);
        if src & 0xff == RC_BUSY {
            return RC_BUSY;
        }
        if src != RC_ROW {
            return ERR_CONFLICT;
        }
    }
    let src = match c.kind {
        ChgKind::Delete => {
            let del = ts.del.as_ref().unwrap();
            if !bind_row(del, lt, c.rowid, if lt.without_rowid { oldr.as_ref() } else { None }, BIND_KEY, 0) {
                return ERR_CONFLICT;
            }
            step_once(del)
        }
        ChgKind::Update => {
            let (Some(o), Some(n)) = (&oldr, &newr) else { return ERR_CONFLICT };
            let Some(mask) = changed_mask(lt, o, n) else { return ERR_CONFLICT };
            if mask == 0 {
                return RC_OK; // only a derived column changed
            }
            if lt.is_parent && mask & lt.refmask != 0 {
                return ERR_CONFLICT; // a key a foreign key refers to changes: the ON UPDATE actions are not replayed
            }
            let Some(us) = ts.update(h, lt, mask) else { return ERR_CONFLICT };
            if !bind_row(us, lt, c.rowid, Some(n), BIND_MASK, mask) {
                return ERR_CONFLICT;
            }
            step_once(us)
        }
        ChgKind::Insert => {
            let ins = ts.ins.as_ref().unwrap();
            if !bind_row(ins, lt, c.rowid, newr.as_ref(), BIND_ALL, 0) {
                return ERR_CONFLICT;
            }
            step_once(ins)
        }
    };
    if src == RC_DONE {
        RC_OK
    } else if src & 0xff == RC_BUSY {
        RC_BUSY
    } else {
        ERR_CONFLICT
    }
}

/// One request: deletes, then updates, then inserts. With foreign keys enforced: updates, deletes from the children to
/// the parents, inserts from the parents to the children.
fn replay_req(h: &CConn, cat: &Catalog, ts: &mut [TStmt], rq: &RbReq) -> i32 {
    if rq.fk {
        let mut fl = vec![0u8; cat.tabs.len()];
        for c in &rq.chg {
            let Some(x) = cat.by_name(&c.tab) else { return ERR_CONFLICT };
            if cat.tabs[x].is_parent {
                fl[x] |= match c.kind {
                    ChgKind::Delete => 1,
                    ChgKind::Insert => 2,
                    _ => 0,
                };
                if fl[x] == 3 {
                    return ERR_CONFLICT;
                }
            }
        }
    }
    let phases = if rq.fk { [ChgKind::Update, ChgKind::Delete, ChgKind::Insert] } else { [ChgKind::Delete, ChgKind::Update, ChgKind::Insert] };
    for phase in phases {
        let steps = if rq.fk && phase != ChgKind::Update { cat.maxrank } else { 0 };
        for step in 0..=steps {
            let rank = if phase == ChgKind::Delete { cat.maxrank - step } else { step };
            for c in rq.chg.iter().filter(|c| c.kind == phase) {
                let Some(x) = cat.by_name(&c.tab) else { return ERR_CONFLICT };
                let lt = &cat.tabs[x];
                if !lt.ok {
                    return ERR_CONFLICT;
                }
                if steps > 0 && lt.rank != rank {
                    continue;
                }
                let r = apply_chg(h, lt, &mut ts[x], c);
                if r != RC_OK {
                    return r;
                }
            }
        }
    }
    RC_OK
}

impl Lane {
    fn helper_open(&mut self) -> bool {
        if self.rb.helper.is_some() {
            return true;
        }
        let mut esc = String::new();
        for ch in self.db.path.chars() {
            match ch {
                '%' => esc.push_str("%25"),
                '?' => esc.push_str("%3F"),
                '#' => esc.push_str("%23"),
                c => esc.push(c),
            }
        }
        let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_URI | ffi::SQLITE_OPEN_NOMUTEX | ffi::SQLITE_OPEN_PRIVATECACHE;
        let Ok(h) = CConn::open(&format!("file:{esc}?mw=1&mw_norebase=1"), flags, Some(crate::VFS_NAME)) else { return false };
        unsafe { ffi::sqlite3_busy_timeout(h.db, 0) };
        h.db_config(ffi::SQLITE_DBCONFIG_ENABLE_TRIGGER, 0); // (the effects of a trigger are rows of the write set)
        h.db_config(ffi::SQLITE_DBCONFIG_ENABLE_FKEY, 0);
        self.rb.hl = crate::hook::lane_of_db(h.db);
        self.rb.helper = Some(h);
        true
    }

    /// All the requests of a batch in one transaction at the snapshot the helper has now. OK: committed (or nothing
    /// could be replayed: every state is final); RC_BUSY: lost the race, again.
    fn group_once(&mut self, b: &[Arc<RbReq>]) -> i32 {
        let rb = &mut self.rb;
        let h = rb.helper.as_ref().unwrap();
        let cat = rb.cat.as_ref().unwrap();
        for rq in b {
            rq.state.store(0, Relaxed);
        }
        h.db_config(ffi::SQLITE_DBCONFIG_ENABLE_FKEY, b[0].fk as c_int);
        if h.exec("BEGIN") != RC_OK {
            return RC_BUSY;
        }
        let mut cookie = None;
        h.query("PRAGMA schema_version", |s| cookie = Some(s.col_i64(0) as u32));
        let Some(cookie) = cookie else {
            h.exec("ROLLBACK");
            for rq in b {
                rq.state.store(2, Relaxed);
            }
            return RC_OK;
        };
        let (mut ok, mut rc) = (0, RC_OK);
        for rq in b {
            if rq.cookie != cookie || rq.cookie != cat.cookie || rq.fk != b[0].fk {
                rq.state.store(2, Relaxed);
                continue;
            }
            h.exec("SAVEPOINT g");
            match replay_req(h, cat, &mut rb.ts, rq) {
                RC_OK => {
                    h.exec("RELEASE g");
                    rq.state.store(1, Relaxed);
                    ok += 1;
                }
                RC_BUSY => {
                    rc = RC_BUSY;
                    break;
                }
                _ => {
                    // a true conflict: this request alone is taken back
                    h.exec("ROLLBACK TO g");
                    h.exec("RELEASE g");
                    rq.state.store(2, Relaxed);
                }
            }
        }
        if rc == RC_BUSY || ok == 0 {
            h.exec("ROLLBACK");
            return rc;
        }
        let crc = h.exec("COMMIT");
        if crc != RC_OK {
            h.exec("ROLLBACK");
            if crc & 0xff == RC_BUSY {
                return RC_BUSY;
            }
            for rq in b {
                let _ = rq.state.compare_exchange(1, 2, Relaxed, Relaxed);
            }
            return RC_OK;
        }
        let epoch = if rb.hl.is_null() { 0 } else { unsafe { (*rb.hl).tx.commit_epoch } };
        for rq in b {
            if rq.state.load(Relaxed) == 1 {
                if epoch != 0 {
                    rq.epoch.store(epoch, Relaxed);
                } else {
                    rq.state.store(2, Relaxed);
                }
            }
        }
        RC_OK
    }

    fn run_batch(&mut self, b: &[Arc<RbReq>]) {
        let db = self.db.clone();
        if !self.helper_open() {
            for rq in b {
                rq.state.store(2, Relaxed);
            }
            return;
        }
        // the helper commits the batch at the strongest level any of its connections asked for
        let lvl = b.iter().map(|r| r.sync_level).max().unwrap_or(2).max(self.sync_level);
        if self.rb.sync != lvl {
            self.rb.helper.as_ref().unwrap().exec(&format!("PRAGMA synchronous={lvl}"));
            self.rb.sync = lvl;
        }
        let hl_id = if self.rb.hl.is_null() { 0 } else { unsafe { (*self.rb.hl).id() } };
        let (mut attempt, mut closed) = (0, false);
        while attempt < MAX_ATTEMPTS {
            if attempt == GATE_AFTER && hl_id != 0 {
                db.gate_close(hl_id); // the next attempt runs against a frozen state
                closed = true;
            }
            if self.group_once(b) != RC_BUSY {
                break;
            }
            db.n.rebase_retries.fetch_add(1, Relaxed);
            if !closed {
                std::thread::yield_now();
            }
            attempt += 1;
        }
        if attempt == MAX_ATTEMPTS {
            for rq in b {
                rq.state.store(2, Relaxed);
            }
        }
        if closed {
            db.gate_open();
        }
        let done = b.iter().filter(|r| r.state.load(Relaxed) == 1).count() as i64;
        db.n.rebases.fetch_add(done, Relaxed);
        if done > 1 {
            db.n.rebases_grouped.fetch_add(done, Relaxed);
        }
    }

    /// The commit conflicted on pages that it wrote: replay its row changes. Ok(epoch): committed (the lane's own pages
    /// are not published); Err(ERR_CONFLICT): refused.
    pub fn rebase(&mut self, imgs: &[&[u8]], cookie: u32) -> Result<u64, i32> {
        let db = self.db.clone();
        if self.rb.cat.as_ref().is_none_or(|c| c.cookie != cookie) {
            self.rb.ts.clear();
            let cat = self.build_catalog();
            self.rb.ts = (0..cat.tabs.len()).map(|_| TStmt::default()).collect();
            self.rb.cat = Some(cat);
        }
        let cat = self.rb.cat.take().unwrap();
        let ok = cat.rebasable && cat.cookie == cookie;
        let why = cat.why.clone();
        let has_fk = cat.has_fk;
        let chg = if ok {
            let pgnos = self.ws_pgnos.clone();
            let ws = self.ws_index.clone();
            self.rowdiff(&pgnos, imgs, &ws, &cat)
        } else {
            Err("")
        };
        self.rb.cat = Some(cat);
        if !ok {
            debugf!("rebase: catalog: {why}");
            db.n.unrebasable.fetch_add(1, Relaxed);
            return Err(ERR_CONFLICT);
        }
        let chg = match chg {
            Ok(c) if !c.is_empty() => c,
            Ok(_) | Err(_) => {
                debugf!("rebase: rowdiff: {:?}", chg.err());
                db.n.unrebasable.fetch_add(1, Relaxed);
                return Err(ERR_CONFLICT);
            }
        };
        if self.reads_unchanged(chg.len()) {
            debugf!("rebase: the transaction read rows it did not change");
            db.n.unrebasable.fetch_add(1, Relaxed);
            return Err(ERR_CONFLICT);
        }
        let mut fk = false;
        if has_fk && !self.rb.rd_db.is_null() {
            let mut on: c_int = 0;
            unsafe { ffi::sqlite3_db_config(self.rb.rd_db, ffi::SQLITE_DBCONFIG_ENABLE_FKEY, -1 as c_int, &mut on as *mut c_int) };
            fk = on != 0;
        }
        let rq = Arc::new(RbReq { sync_level: self.sync_level, chg, cookie, fk, state: AtomicI32::new(0), done: AtomicBool::new(false), epoch: AtomicU64::new(0) });
        let mut q = lock(&db.rb_queue);
        q.reqs.push_back(rq.clone());
        while !rq.done.load(Acquire) {
            if !q.leader {
                q.leader = true;
                let n = q.reqs.len().min(BATCH);
                let batch: Vec<Arc<RbReq>> = q.reqs.drain(..n).collect();
                drop(q);
                self.run_batch(&batch);
                q = lock(&db.rb_queue);
                for x in &batch {
                    x.done.store(true, Release);
                }
                q.leader = false;
                db.rb_cv.notify_all();
            } else {
                q = db.rb_cv.wait(q).unwrap_or_else(|e| e.into_inner());
            }
        }
        drop(q);
        if rq.state.load(Acquire) == 1 {
            return Ok(rq.epoch.load(Acquire));
        }
        debugf!("rebase: the replay was refused (a true conflict)");
        Err(ERR_CONFLICT)
    }
}
