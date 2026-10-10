//! The schema as the rebase needs it: for every table b-tree, how its records are laid out and whether the database
//! can be replayed at all. Built from sqlite_schema as the transaction's snapshot has it (the rows are read from the
//! pages) and parsed by SQLite itself in a scratch in-memory database.

use crate::capi::*;
use crate::lane::Lane;
use crate::util::*;
use libsqlite3_sys as ffi;

pub const COL_INTEGER: i32 = 1;
pub const COL_FLOAT: i32 = 2;
pub const COL_TEXT: i32 = 3;
pub const COL_BLOB: i32 = 4;
pub const COL_NULL: i32 = 5;

#[derive(Clone, Debug)]
pub struct SqlVal {
    pub typ: i32,
    pub i: i64,
    pub d: f64,
    pub b: Vec<u8>,
}

#[derive(Default, Debug)]
pub struct Tab {
    pub root: u32,
    pub name: String,
    pub ok: bool,
    pub without_rowid: bool,
    pub npk_rec: usize,
    pub alias_pk: bool,
    pub nrec: usize,
    pub alias_rec: Option<usize>,
    pub rec_name: Vec<String>,
    pub rec_gen: Vec<bool>,
    pub noaff: Vec<bool>,
    pub dflt: Vec<SqlVal>,
    pub fk_parent: Vec<String>,
    pub fk_to: Vec<Option<String>>,
    pub is_parent: bool,
    pub refmask: u64,
    pub rank: i32,
    pub pk_name: Vec<String>,
}

impl Tab {
    /// The writable columns: neither generated nor the rowid alias.
    pub fn writable(&self, i: usize) -> bool {
        !self.rec_gen[i] && Some(i) != self.alias_rec
    }
    pub fn writable_bit(&self, name: &str) -> Option<usize> {
        (0..self.nrec).filter(|&i| self.writable(i)).position(|i| self.rec_name[i].eq_ignore_ascii_case(name))
    }
    pub fn rowid_name(&self) -> Option<&'static str> {
        ["rowid", "_rowid_", "oid"].into_iter().find(|c| !(0..self.nrec).any(|i| !self.rec_gen[i] && self.rec_name[i].eq_ignore_ascii_case(c)))
    }
}

#[derive(Default, Debug)]
pub struct Catalog {
    pub tabs: Vec<Tab>,
    pub cookie: u32,
    pub rebasable: bool,
    pub has_wr: bool,
    pub has_fk: bool,
    pub maxrank: i32,
    pub why: String,
}

impl Catalog {
    pub fn by_root(&self, root: u32) -> Option<usize> {
        self.tabs.iter().position(|t| t.root == root)
    }
    pub fn by_name(&self, name: &str) -> Option<usize> {
        self.tabs.iter().position(|t| t.name.eq_ignore_ascii_case(name))
    }
}

pub fn type_len(t: u64) -> usize {
    const L: [usize; 12] = [0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0];
    if t < 12 { L[t as usize] } else { ((t - 12) / 2) as usize }
}

/// A text or integer column of a record, as a string.
fn rec_col_text(rec: &[u8], col: usize) -> Option<String> {
    let (hs, hl) = varint(rec);
    if hl == 0 || hs as usize > rec.len() {
        return None;
    }
    let (mut pos, mut hp) = (hs as usize, hl);
    let mut idx = 0;
    while hp < hs as usize {
        let (t, k) = varint(&rec[hp..hs as usize]);
        if k == 0 {
            return None;
        }
        hp += k;
        let l = type_len(t);
        if idx == col {
            if pos + l > rec.len() {
                return None;
            }
            return match t {
                t if t >= 13 && t & 1 == 1 => Some(String::from_utf8_lossy(&rec[pos..pos + l]).into_owned()),
                1..=6 => {
                    let mut v = rec[pos] as i8 as i64;
                    for b in 1..l {
                        v = (v << 8) | rec[pos + b] as i64;
                    }
                    Some(v.to_string())
                }
                8 => Some("0".into()),
                9 => Some("1".into()),
                _ => None,
            };
        }
        pos += l;
        idx += 1;
    }
    None
}

struct SchemaRow {
    typ: String,
    name: String,
    sql: String,
    root: u32,
}

fn starts_ci(s: &str, p: &str) -> bool {
    let s = s.trim_start_matches([' ', '\n', '\t', '\r']);
    s.len() >= p.len() && s[..p.len()].eq_ignore_ascii_case(p)
}

fn type_has_no_affinity(t: &str) -> bool {
    if t.is_empty() {
        return true;
    }
    let u = t.to_ascii_uppercase();
    if u.contains("INT") || u.contains("CHAR") || u.contains("CLOB") || u.contains("TEXT") {
        return false;
    }
    u.contains("BLOB")
}

fn eval_default(scratch: &CConn, expr: Option<&str>) -> SqlVal {
    let mut v = SqlVal { typ: COL_NULL, i: 0, d: 0.0, b: Vec::new() };
    let Some(expr) = expr else { return v };
    scratch.query(&format!("SELECT {expr}"), |s| {
        v.typ = s.col_type(0);
        match v.typ {
            COL_INTEGER => v.i = s.col_i64(0),
            COL_FLOAT => v.d = s.col_f64(0),
            COL_TEXT => v.b = s.col_text(0).unwrap_or_default().into_bytes(),
            COL_BLOB => v.b = s.col_blob(0),
            _ => {}
        }
    });
    v
}

fn tab_parse(scratch: &CConn, r: &SchemaRow) -> Tab {
    let mut t = Tab { root: r.root, name: r.name.clone(), ..Default::default() };
    if r.sql.is_empty() || !starts_ci(&r.sql, "CREATE TABLE") || starts_ci(&r.sql, "CREATE VIRTUAL") {
        return t;
    }
    if r.name.len() >= 7 && r.name[..7].eq_ignore_ascii_case("sqlite_") {
        return t;
    }
    t.without_rowid = r.sql.to_ascii_uppercase().contains("WITHOUT ROWID");
    if scratch.exec(&r.sql) != RC_OK {
        return t;
    }
    let qn = quote_ident(&r.name);
    scratch.query(&format!("PRAGMA table_list({qn})"), |s| {
        let wr = (0..s.col_count()).find(|&i| s.col_name(i) == "wr");
        if let (Some(wr), Some(nm)) = (wr, s.col_text(1))
            && nm.eq_ignore_ascii_case(&r.name) {
                t.without_rowid = s.col_i64(wr) != 0;
            }
    });
    struct Col {
        name: String,
        typ: String,
        dflt: Option<String>,
        pk: i64,
        hidden: i64,
    }
    let mut cols = Vec::new();
    scratch.query(&format!("PRAGMA table_xinfo({qn})"), |s| {
        cols.push(Col { name: s.col_text(1).unwrap_or_default(), typ: s.col_text(2).unwrap_or_default(), dflt: s.col_text(4), pk: s.col_i64(5), hidden: s.col_i64(6) });
    });
    let npk = cols.iter().filter(|c| c.pk > 0).count();
    if !t.without_rowid && npk == 1 && cols.iter().any(|c| c.pk == 1 && c.typ.eq_ignore_ascii_case("INTEGER")) {
        t.alias_pk = true;
        // `x INTEGER PRIMARY KEY DESC` is not an alias of the rowid: SQLite makes an index for the key (origin "pk")
        scratch.query(&format!("PRAGMA index_list({qn})"), |s| {
            if s.col_text(3).as_deref() == Some("pk") {
                t.alias_pk = false;
            }
        });
    }
    let mut order = Vec::new();
    if t.without_rowid {
        for k in 1..=npk as i64 {
            order.extend(cols.iter().enumerate().filter(|(_, c)| c.pk == k).map(|(i, _)| i));
        }
        t.npk_rec = order.len();
        order.extend(cols.iter().enumerate().filter(|(_, c)| c.pk <= 0).map(|(i, _)| i));
        // the key must compare as bytes would: BINARY and ascending
        let mut plain = npk > 0;
        let mut pk_index = None;
        scratch.query(&format!("PRAGMA index_list({qn})"), |s| {
            if s.col_text(3).as_deref() == Some("pk") {
                pk_index = s.col_text(1);
            }
        });
        match pk_index {
            None => plain = false,
            Some(ix) => {
                scratch.query(&format!("PRAGMA index_xinfo({})", quote_ident(&ix)), |s| {
                    if s.col_i64(5) != 0 && (s.col_i64(3) != 0 || !s.col_text(4).is_some_and(|c| c.eq_ignore_ascii_case("BINARY"))) {
                        plain = false;
                    }
                });
            }
        }
        if !plain {
            return t;
        }
    } else {
        order.extend(0..cols.len());
    }
    for &i in &order {
        let c = &cols[i];
        if c.hidden == 2 {
            continue; // generated, virtual: not stored
        }
        if t.alias_pk && c.pk == 1 {
            t.alias_rec = Some(t.rec_name.len());
        }
        t.noaff.push(type_has_no_affinity(&c.typ));
        t.rec_name.push(c.name.clone());
        t.rec_gen.push(c.hidden == 3);
        t.dflt.push(eval_default(scratch, if c.hidden == 3 { None } else { c.dflt.as_deref() }));
    }
    t.nrec = t.rec_name.len();
    t.pk_name = cols.iter().filter(|c| c.pk > 0).map(|c| c.name.clone()).collect();
    scratch.query(&format!("PRAGMA foreign_key_list({qn})"), |s| {
        t.fk_parent.push(s.col_text(2).unwrap_or_default());
        t.fk_to.push(s.col_text(4));
    });
    t.ok = true;
    t
}

impl Catalog {
    /// Which tables are parents, which of their columns are referred to, and a rank (parents before children).
    fn fk_graph(&mut self) {
        for i in 0..self.tabs.len() {
            for k in 0..self.tabs[i].fk_parent.len() {
                self.has_fk = true;
                let Some(p) = self.by_name(&self.tabs[i].fk_parent[k].clone()) else { continue };
                if p == i {
                    self.rebasable = false;
                    self.why = "the database has a table with a foreign key to itself".into();
                    return;
                }
                self.tabs[p].is_parent = true;
                let names: Vec<String> = match &self.tabs[i].fk_to[k] {
                    Some(to) => vec![to.clone()],
                    None => self.tabs[p].pk_name.clone(),
                };
                for n in names {
                    if let Some(b) = self.tabs[p].writable_bit(&n).filter(|&b| b < 64) {
                        self.tabs[p].refmask |= 1 << b;
                    }
                }
            }
        }
        for round in 0..=self.tabs.len() {
            let mut changed = false;
            for i in 0..self.tabs.len() {
                let mut r = 0;
                for pn in &self.tabs[i].fk_parent {
                    if let Some(p) = self.by_name(pn) {
                        r = r.max(self.tabs[p].rank + 1);
                    }
                }
                if r != self.tabs[i].rank {
                    self.tabs[i].rank = r;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
            if round == self.tabs.len() {
                self.rebasable = false;
                self.why = "the foreign keys of the database are circular".into();
                return;
            }
        }
        self.maxrank = self.tabs.iter().map(|t| t.rank).max().unwrap_or(0);
    }
}

impl Lane {
    /// The full payload of a table leaf cell at the snapshot: its local part, then the overflow chain.
    fn cell_payload(&mut self, pg: &[u8], off: usize, pgsz: usize) -> Option<Vec<u8>> {
        let (p, a) = varint(&pg[off..]);
        if a == 0 {
            return None;
        }
        let (_, b) = varint(&pg[off + a..]);
        if b == 0 {
            return None;
        }
        let u = pgsz as u64;
        let (x, m) = (u - 35, (u - 12) * 32 / 255 - 23);
        let start = off + a + b;
        let (mut local, mut ovfl) = (p, 0u32);
        if p > x {
            let k = m + (p - m) % (u - 4);
            local = if k <= x { k } else { m };
            if start + local as usize + 4 > pgsz {
                return None;
            }
            ovfl = be32(&pg[start + local as usize..]);
        } else if start + local as usize > pgsz {
            return None;
        }
        let mut buf = pg[start..start + local as usize].to_vec();
        let mut tmp = vec![0u8; pgsz];
        while ovfl != 0 && (buf.len() as u64) < p {
            if !self.snap_page(ovfl, &mut tmp) {
                return None;
            }
            let take = (p - buf.len() as u64).min(u - 4) as usize;
            buf.extend_from_slice(&tmp[4..4 + take]);
            ovfl = be32(&tmp);
        }
        if buf.len() as u64 == p { Some(buf) } else { None }
    }

    fn schema_collect(&mut self, pgno: u32, out: &mut Vec<SchemaRow>, depth: usize) {
        let pgsz = self.db.engine().store.pgsz;
        let mut pg = vec![0u8; pgsz];
        if depth > 8 || !self.snap_page(pgno, &mut pg) {
            return;
        }
        let base = if pgno == 1 { 100 } else { 0 };
        match pg[base] {
            0x0d => {
                for i in 0..be16(&pg[base + 3..]) {
                    let off = be16(&pg[base + 8 + 2 * i..]);
                    if off >= pgsz {
                        continue;
                    }
                    let Some(rec) = self.cell_payload(&pg, off, pgsz) else { continue };
                    let (Some(typ), Some(name)) = (rec_col_text(&rec, 0), rec_col_text(&rec, 1)) else { continue };
                    if typ != "table" && typ != "trigger" {
                        continue;
                    }
                    let root = rec_col_text(&rec, 3).and_then(|r| r.parse().ok()).unwrap_or(0);
                    out.push(SchemaRow { typ, name, sql: rec_col_text(&rec, 4).unwrap_or_default(), root });
                }
            }
            0x05 => {
                for i in 0..be16(&pg[base + 3..]) {
                    let off = be16(&pg[base + 12 + 2 * i..]);
                    if off + 4 <= pgsz {
                        self.schema_collect(be32(&pg[off..]), out, depth + 1);
                    }
                }
                self.schema_collect(be32(&pg[base + 8..]), out, depth + 1);
            }
            _ => {}
        }
    }

    /// The schema at the lane's snapshot.
    pub fn build_catalog(&mut self) -> Catalog {
        let mut c = Catalog { rebasable: true, ..Default::default() };
        let pgsz = self.db.engine().store.pgsz;
        let mut p1 = vec![0u8; pgsz];
        if self.snap_page(1, &mut p1) {
            c.cookie = be32(&p1[40..]);
            if be32(&p1[56..]) != 1 {
                c.rebasable = false;
                c.why = "the text encoding is not UTF-8".into();
            } else if p1[20] != 0 {
                c.rebasable = false;
                c.why = "the pages have reserved bytes".into();
            }
        } else {
            c.rebasable = false;
            c.why = "no page 1".into();
        }
        let mut rows = Vec::new();
        self.schema_collect(1, &mut rows, 0);
        let Ok(scratch) = CConn::open(":memory:", ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE, None) else {
            c.rebasable = false;
            c.why = "no scratch database".into();
            return c;
        };
        for r in &rows {
            if r.typ == "trigger" {
                c.rebasable = false;
                c.why = "the database has a trigger".into();
                continue;
            }
            if starts_ci(&r.sql, "CREATE VIRTUAL") {
                c.rebasable = false;
                c.why = "the database has a virtual table".into();
            }
            let t = tab_parse(&scratch, r);
            c.has_wr |= t.without_rowid;
            c.tabs.push(t);
        }
        c.fk_graph();
        c
    }
}
