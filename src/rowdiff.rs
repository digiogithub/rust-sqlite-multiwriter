//! The net row changes of a write set, read from the page images: from the pages a transaction wrote and the pages the
//! same numbers had at its snapshot, the rows of every table b-tree that were inserted, changed or deleted. It decodes
//! SQLite's file format directly; the net change over the whole write set makes splits and merges disappear, and the
//! freelist is walked because a page that the transaction frees is not rewritten. Which table a page belongs to is
//! found when it is needed. Whatever cannot be decoded exactly makes the result "unsupported".

use crate::catalog::{type_len, Catalog};
use crate::lane::Lane;
use crate::util::*;
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChgKind {
    Insert = 1,
    Update = 2,
    Delete = 3,
}

#[derive(Clone, Debug)]
pub struct RowChange {
    pub kind: ChgKind,
    pub tab: String,
    pub rowid: i64,
    pub old_rec: Option<Vec<u8>>,
    pub new_rec: Option<Vec<u8>>,
}

struct RdRow {
    root: u32,
    wr: bool,
    rowid: i64,
    rec: Vec<u8>,
    key: Vec<u8>,
    bad: bool,
    chain: Vec<u32>,
}

fn page_kind(pg: &[u8], pgno: u32) -> u8 {
    if pgno == 1 {
        return b'S';
    }
    match pg[0] {
        0x0d => b'L',
        0x05 => b'T',
        0x0a | 0x02 => b'I',
        _ => b'O',
    }
}

fn row_cmp(p: &RdRow, q: &RdRow) -> Ordering {
    p.root.cmp(&q.root).then_with(|| if p.wr { p.key.len().cmp(&q.key.len()).then_with(|| p.key.cmp(&q.key)) } else { p.rowid.cmp(&q.rowid) })
}

struct Ctx<'a> {
    imgs: &'a [&'a [u8]],
    ws: &'a PageMap<usize>,
    cat: &'a Catalog,
    pgsz: usize,
    owner: HashMap<u32, u32>,
}

impl Lane {
    /// Page pgno at the transaction's snapshot; false if it does not exist there.
    pub fn snap_page(&mut self, pgno: u32, dst: &mut [u8]) -> bool {
        let snap = self.tx.snapshot_epoch;
        if pgno == 0 || pgno > self.dbsize_at(snap) {
            return false;
        }
        if self.db.engine().store.read(pgno, snap, 0, dst) {
            return true;
        }
        self.db.read_real(pgno, dst)
    }
}

impl Ctx<'_> {
    fn ws_image(&self, pgno: u32) -> Option<&[u8]> {
        self.ws.get(&pgno).map(|&i| self.imgs[i])
    }

    fn freelist_of(&self, l: &mut Lane, new_state: bool, page1: &[u8]) -> Option<Vec<u32>> {
        let mut out = Vec::new();
        let mut tmp = vec![0u8; self.pgsz];
        let mut trunk = be32(&page1[32..]);
        let mut guard = 0;
        while trunk != 0 && guard < 1_000_000 {
            guard += 1;
            let tp: Vec<u8> = match if new_state { self.ws_image(trunk) } else { None } {
                Some(x) => x.to_vec(),
                None => {
                    if !l.snap_page(trunk, &mut tmp) {
                        return None;
                    }
                    tmp.clone()
                }
            };
            let (next, cnt) = (be32(&tp), be32(&tp[4..]) as usize);
            if cnt > (self.pgsz - 8) / 4 {
                return None;
            }
            out.push(trunk);
            for k in 0..cnt {
                out.push(be32(&tp[8 + 4 * k..]));
            }
            trunk = next;
        }
        out.sort_unstable();
        Some(out)
    }

    /// The table that owns a page that existed at the snapshot: walk down from the root of every table.
    fn old_owner(&mut self, l: &mut Lane, pgno: u32) -> u32 {
        if let Some(&o) = self.owner.get(&pgno) {
            return o;
        }
        let o = self.old_owner_walk(l, pgno);
        if o != 0 {
            self.owner.insert(pgno, o);
        }
        o
    }

    fn old_owner_walk(&self, l: &mut Lane, pgno: u32) -> u32 {
        let pgsz = self.pgsz;
        let mut tmp = vec![0u8; pgsz];
        let mut tmp2 = vec![0u8; pgsz];
        if !l.snap_page(pgno, &mut tmp) || be16(&tmp[3..]) == 0 {
            return 0;
        }
        let off = match tmp[0] {
            0x05 => be16(&tmp[12..]),
            0x0d => be16(&tmp[8..]),
            _ => return 0,
        };
        if off + 4 >= pgsz {
            return 0;
        }
        let key = if tmp[0] == 0x0d {
            let (_, a) = varint(&tmp[off..pgsz]);
            if a == 0 {
                return 0;
            }
            let (k, b) = varint(&tmp[off + a..pgsz]);
            if b == 0 {
                return 0;
            }
            k as i64
        } else {
            let (k, b) = varint(&tmp[off + 4..pgsz]);
            if b == 0 {
                return 0;
            }
            k as i64
        };
        for t in &self.cat.tabs {
            if !t.ok || t.without_rowid || t.root <= 1 {
                continue;
            }
            let mut page = t.root;
            for _ in 0..24 {
                if page == 0 {
                    break;
                }
                if page == pgno {
                    return t.root;
                }
                if !l.snap_page(page, &mut tmp2) || tmp2[0] != 0x05 {
                    break;
                }
                let mut next = be32(&tmp2[8..]);
                for c in 0..be16(&tmp2[3..]) {
                    let co = be16(&tmp2[12 + 2 * c..]);
                    if co + 5 > pgsz {
                        next = 0;
                        break;
                    }
                    let (k, n) = varint(&tmp2[co + 4..pgsz]);
                    if n == 0 {
                        next = 0;
                        break;
                    }
                    if key <= k as i64 {
                        next = be32(&tmp2[co..]);
                        break;
                    }
                }
                page = next;
            }
        }
        0
    }

    fn full_payload(&self, l: &mut Lane, new_state: bool, local: &[u8], total: u64, mut ovfl: u32, chain: &mut Vec<u32>) -> Option<Vec<u8>> {
        let mut buf = local.to_vec();
        let mut tmp = vec![0u8; self.pgsz];
        while ovfl != 0 && (buf.len() as u64) < total {
            let pg: &[u8] = match if new_state { self.ws_image(ovfl) } else { None } {
                Some(x) => x,
                None => {
                    if !l.snap_page(ovfl, &mut tmp) {
                        return None;
                    }
                    &tmp
                }
            };
            let take = (total - buf.len() as u64).min(self.pgsz as u64 - 4) as usize;
            if new_state {
                chain.push(ovfl);
            }
            buf.extend_from_slice(&pg[4..4 + take]);
            ovfl = be32(pg);
        }
        if buf.len() as u64 == total { Some(buf) } else { None }
    }

    /// Adds the rows of one table leaf page (0x0d).
    fn rows_of(&self, l: &mut Lane, new_state: bool, pg: &[u8], rs: &mut Vec<RdRow>, root: u32) -> bool {
        let u = self.pgsz as u64;
        let (m, x) = ((u - 12) * 32 / 255 - 23, u - 35);
        for i in 0..be16(&pg[3..]) {
            let off = be16(&pg[8 + 2 * i..]);
            if off < 8 || off >= self.pgsz {
                return false;
            }
            let c = &pg[off..self.pgsz];
            let (p, a) = varint(c);
            if a == 0 {
                return false;
            }
            let (rid, b) = varint(&c[a..]);
            if b == 0 {
                return false;
            }
            let (mut local, mut ovfl) = (p, 0u32);
            if p > x {
                let k = m + (p - m) % (u - 4);
                local = if k <= x { k } else { m };
                if a + b + local as usize + 4 > c.len() {
                    return false;
                }
                ovfl = be32(&c[a + b + local as usize..]);
            } else if a + b + local as usize > c.len() {
                return false;
            }
            let mut row = RdRow { root, wr: false, rowid: rid as i64, rec: c[a + b..a + b + local as usize].to_vec(), key: Vec::new(), bad: false, chain: Vec::new() };
            if ovfl != 0 {
                let mut ch = Vec::new();
                match self.full_payload(l, new_state, &row.rec.clone(), p, ovfl, &mut ch) {
                    Some(full) => row.rec = full,
                    None => row.bad = true,
                }
                row.chain = ch;
            }
            rs.push(row);
        }
        true
    }

    /// Adds the rows of one page of a WITHOUT ROWID table (leaf 0x0a or interior 0x02).
    fn rows_of_index(&self, l: &mut Lane, new_state: bool, pg: &[u8], rs: &mut Vec<RdRow>, root: u32) -> bool {
        let npk = self.cat.by_root(root).map_or(0, |t| self.cat.tabs[t].npk_rec);
        let interior = pg[0] == 0x02;
        let hdr = if interior { 12 } else { 8 };
        let u = self.pgsz as u64;
        let (m, x) = ((u - 12) * 32 / 255 - 23, (u - 12) * 64 / 255 - 23);
        for i in 0..be16(&pg[3..]) {
            let off = be16(&pg[hdr + 2 * i..]);
            if off < hdr || off >= self.pgsz {
                return false;
            }
            let mut c = &pg[off..self.pgsz];
            if interior {
                c = &c[4..];
            }
            let (p, a) = varint(c);
            if a == 0 {
                return false;
            }
            let (mut local, mut ovfl) = (p, 0u32);
            if p > x {
                let k = m + (p - m) % (u - 4);
                local = if k <= x { k } else { m };
                if a + local as usize + 4 > c.len() {
                    return false;
                }
                ovfl = be32(&c[a + local as usize..]);
            } else if a + local as usize > c.len() {
                return false;
            }
            let mut row = RdRow { root, wr: true, rowid: 0, rec: c[a..a + local as usize].to_vec(), key: Vec::new(), bad: false, chain: Vec::new() };
            if ovfl != 0 {
                let mut ch = Vec::new();
                match self.full_payload(l, new_state, &row.rec.clone(), p, ovfl, &mut ch) {
                    Some(full) => row.rec = full,
                    None => row.bad = true,
                }
                row.chain = ch;
            }
            if !row.bad {
                let (hs, h) = varint(&row.rec);
                if h == 0 || hs as usize > row.rec.len() {
                    row.bad = true;
                } else {
                    let (mut pos, mut hp, mut col) = (hs as usize, h, 0);
                    while col < npk && hp < hs as usize {
                        let (t, k) = varint(&row.rec[hp..hs as usize]);
                        let ln = type_len(t);
                        if k == 0 || pos + ln > row.rec.len() {
                            row.bad = true;
                            break;
                        }
                        let (a, b) = (row.rec[hp..hp + k].to_vec(), row.rec[pos..pos + ln].to_vec());
                        row.key.extend(a);
                        row.key.extend(b);
                        hp += k;
                        pos += ln;
                        col += 1;
                    }
                    if col < npk {
                        row.bad = true;
                    }
                }
            }
            rs.push(row);
        }
        true
    }

    /// Lists, at the snapshot, every page of the trees of the WITHOUT ROWID tables.
    fn s_build(&self, l: &mut Lane, s: &mut HashMap<u32, u32>) -> bool {
        let mut tmp = vec![0u8; self.pgsz];
        let mut reads = 0;
        for t in &self.cat.tabs {
            if !t.without_rowid || t.root <= 1 {
                continue;
            }
            s.insert(t.root, t.root);
            let mut depth = 0;
            let mut pg = t.root;
            while depth < 40 {
                reads += 1;
                if reads > 200_000 || !l.snap_page(pg, &mut tmp) {
                    return false;
                }
                if tmp[0] == 0x0a {
                    break;
                }
                if tmp[0] != 0x02 {
                    return false;
                }
                pg = be32(&tmp[8..]);
                depth += 1;
            }
            let mut lvl = vec![t.root];
            for d in 0..depth {
                let mut nxt = Vec::new();
                for &p in &lvl {
                    reads += 1;
                    if reads > 200_000 || !l.snap_page(p, &mut tmp) || tmp[0] != 0x02 {
                        return false;
                    }
                    let nc = be16(&tmp[3..]);
                    for c in 0..=nc {
                        let ch = if c == 0 {
                            be32(&tmp[8..])
                        } else {
                            let off = be16(&tmp[12 + 2 * (c - 1)..]);
                            if off + 4 > self.pgsz {
                                return false;
                            }
                            be32(&tmp[off..])
                        };
                        s.insert(ch, t.root);
                        if d + 1 < depth {
                            nxt.push(ch);
                        }
                    }
                }
                lvl = nxt;
            }
        }
        true
    }
}

fn children(pg: &[u8], pgsz: usize) -> Vec<u32> {
    let mut out = vec![be32(&pg[8..])];
    for c in 0..be16(&pg[3..]) {
        let off = be16(&pg[12 + 2 * c..]);
        if off + 4 <= pgsz {
            out.push(be32(&pg[off..]));
        }
    }
    out
}

impl Lane {
    /// The row changes of the write set, or why they cannot be decoded exactly.
    pub fn rowdiff(&mut self, pgnos: &[u32], imgs: &[&[u8]], ws: &PageMap<usize>, cat: &Catalog) -> Result<Vec<RowChange>, &'static str> {
        let pgsz = self.db.engine().store.pgsz;
        let mut r = Ctx { imgs, ws, cat, pgsz, owner: HashMap::new() };
        let (mut oldr, mut newr) = (Vec::new(), Vec::new());
        let mut oldimg = vec![0u8; pgsz];
        let mut p1old = vec![0u8; pgsz];
        if !self.snap_page(1, &mut p1old) {
            return Err("no page 1");
        }
        let fo = if be32(&p1old[36..]) > 0 { r.freelist_of(self, false, &p1old).ok_or("the freelist cannot be read")? } else { Vec::new() };
        let fnw = {
            let p1new = r.ws_image(1);
            let touched = p1new.is_some() || pgnos.iter().any(|p| fo.binary_search(p).is_ok());
            if !touched {
                fo.clone()
            } else {
                let src = p1new.map(|x| x.to_vec()).unwrap_or_else(|| p1old.clone());
                r.freelist_of(self, true, &src).ok_or("the freelist cannot be read")?
            }
        };
        let mut ov: HashMap<u32, u32> = HashMap::new();
        let mut s_map: HashMap<u32, u32> = HashMap::new();
        let mut cm: HashMap<u32, u32> = HashMap::new();
        let mut s_ready = false;
        if cat.has_wr {
            for (i, &pgno) in pgnos.iter().enumerate() {
                let pg = imgs[i];
                if pgno != 1 && (pg[0] == 0x05 || pg[0] == 0x02) {
                    for ch in children(pg, pgsz) {
                        cm.insert(ch, pgno);
                    }
                }
            }
        }
        macro_rules! ensure_s {
            () => {
                if cat.has_wr && !s_ready {
                    s_ready = true;
                    if !r.s_build(self, &mut s_map) {
                        return Err("the pages of the WITHOUT ROWID tables cannot be listed");
                    }
                }
            };
        }
        let new_owner = |r: &mut Ctx, l: &mut Lane, ov: &HashMap<u32, u32>, pgno: u32| -> u32 {
            if cat.by_root(pgno).is_some() {
                return pgno;
            }
            if let Some(&o) = ov.get(&pgno) {
                return o;
            }
            r.old_owner(l, pgno)
        };
        for _pass in 0..3 {
            let (mut unresolved, before) = (0, ov.len());
            for (i, &pgno) in pgnos.iter().enumerate() {
                let pg = imgs[i];
                if pgno == 1 || pg[0] != 0x05 {
                    continue;
                }
                let root = new_owner(&mut r, self, &ov, pgno);
                if root == 0 {
                    unresolved += 1;
                    continue;
                }
                for ch in children(pg, pgsz) {
                    ov.insert(ch, root);
                }
            }
            if unresolved == 0 || ov.len() == before {
                break;
            }
        }
        let snap = self.tx.snapshot_epoch;
        let snap_size = self.dbsize_at(snap);
        let tab_ok = |root: u32| cat.by_root(root).is_some_and(|t| cat.tabs[t].ok);
        for (i, &pgno) in pgnos.iter().enumerate() {
            let kn = page_kind(imgs[i], pgno);
            if kn == b'S' || kn == b'T' {
                continue;
            }
            let (in_fn, in_fo) = (fnw.binary_search(&pgno).is_ok(), fo.binary_search(&pgno).is_ok());
            if kn == b'L' && !in_fn {
                let root = new_owner(&mut r, self, &ov, pgno);
                if root == 0 {
                    return Err("a page of a table that the transaction wrote has no known owner");
                }
                if !tab_ok(root) {
                    return Err("a table that cannot be replayed was written");
                }
                if !r.rows_of(self, true, imgs[i], &mut newr, root) {
                    return Err("a leaf page does not decode");
                }
            }
            if kn == b'I' && cat.has_wr && !in_fn {
                ensure_s!();
                let (mut root, mut cur) = (0, pgno);
                for _ in 0..64 {
                    if cat.by_root(cur).is_some_and(|t| cat.tabs[t].without_rowid) {
                        root = cur;
                        break;
                    }
                    match cm.get(&cur) {
                        None => {
                            root = s_map.get(&cur).copied().unwrap_or(0);
                            break;
                        }
                        Some(&par) => {
                            if !r.ws_image(par).is_some_and(|p| p[0] == 0x02) {
                                break;
                            }
                            cur = par;
                        }
                    }
                }
                if root != 0 {
                    if !tab_ok(root) {
                        return Err("a table that cannot be replayed was written");
                    }
                    if !r.rows_of_index(self, true, imgs[i], &mut newr, root) {
                        return Err("a page of a WITHOUT ROWID table does not decode");
                    }
                }
            }
            let have_old = pgno <= snap_size && self.snap_page(pgno, &mut oldimg);
            if have_old && cat.has_wr && page_kind(&oldimg, pgno) == b'I' && !in_fo {
                ensure_s!();
                if let Some(&root) = s_map.get(&pgno) {
                    if !tab_ok(root) {
                        return Err("a table that cannot be replayed was written");
                    }
                    let cp = oldimg.clone();
                    if !r.rows_of_index(self, false, &cp, &mut oldr, root) {
                        return Err("a page of a WITHOUT ROWID table does not decode");
                    }
                }
            }
            if have_old && page_kind(&oldimg, pgno) == b'L' && !in_fo {
                let root = r.old_owner(self, pgno);
                if root == 0 {
                    return Err("a page of a table that the transaction wrote has no known owner");
                }
                if !tab_ok(root) {
                    return Err("a table that cannot be replayed was written");
                }
                let cp = oldimg.clone();
                if !r.rows_of(self, false, &cp, &mut oldr, root) {
                    return Err("a leaf page does not decode");
                }
            }
        }
        // pages the transaction freed and did not rewrite: their old rows are gone
        for &pg in &fnw {
            if fo.binary_search(&pg).is_ok() || r.ws_image(pg).is_some() || !self.snap_page(pg, &mut oldimg) {
                continue;
            }
            if page_kind(&oldimg, pg) == b'I' && cat.has_wr {
                ensure_s!();
                if let Some(&wroot) = s_map.get(&pg) {
                    if !tab_ok(wroot) {
                        return Err("a table that cannot be replayed was written");
                    }
                    let cp = oldimg.clone();
                    if !r.rows_of_index(self, false, &cp, &mut oldr, wroot) {
                        return Err("a page of a WITHOUT ROWID table does not decode");
                    }
                }
                continue;
            }
            if page_kind(&oldimg, pg) != b'L' {
                continue;
            }
            let root = r.old_owner(self, pg);
            if root == 0 {
                return Err("a page that the transaction freed has no known owner");
            }
            if !tab_ok(root) {
                return Err("a table that cannot be replayed was written");
            }
            let cp = oldimg.clone();
            if !r.rows_of(self, false, &cp, &mut oldr, root) {
                return Err("a leaf page does not decode");
            }
        }
        // overflow pages written that no cell of the write set owns
        let mut nch: Vec<u32> = newr.iter().flat_map(|x: &RdRow| x.chain.iter().copied()).collect();
        nch.sort_unstable();
        for (i, &pg) in pgnos.iter().enumerate() {
            if pg == 1 || page_kind(imgs[i], pg) != b'O' {
                continue;
            }
            if fnw.binary_search(&pg).is_ok() || fo.binary_search(&pg).is_ok() || nch.binary_search(&pg).is_ok() {
                continue;
            }
            return Err("an overflow page was written without its cell");
        }
        if oldr.iter().chain(newr.iter()).any(|x| x.bad) {
            return Err("a record does not decode");
        }
        oldr.sort_by(row_cmp);
        newr.sort_by(row_cmp);
        let name = |root: u32| cat.by_root(root).map(|t| cat.tabs[t].name.clone()).unwrap_or_default();
        let mut out = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < oldr.len() || j < newr.len() {
            let c = if i >= oldr.len() {
                Ordering::Greater
            } else if j >= newr.len() {
                Ordering::Less
            } else {
                row_cmp(&oldr[i], &newr[j])
            };
            match c {
                Ordering::Less => {
                    let o = &oldr[i];
                    out.push(RowChange { kind: ChgKind::Delete, tab: name(o.root), rowid: o.rowid, old_rec: Some(o.rec.clone()), new_rec: None });
                    i += 1;
                }
                Ordering::Greater => {
                    let w = &newr[j];
                    out.push(RowChange { kind: ChgKind::Insert, tab: name(w.root), rowid: w.rowid, old_rec: None, new_rec: Some(w.rec.clone()) });
                    j += 1;
                }
                Ordering::Equal => {
                    let (o, w) = (&oldr[i], &newr[j]);
                    if o.rec != w.rec {
                        out.push(RowChange { kind: ChgKind::Update, tab: name(w.root), rowid: w.rowid, old_rec: Some(o.rec.clone()), new_rec: Some(w.rec.clone()) });
                    }
                    i += 1;
                    j += 1;
                }
            }
        }
        Ok(out)
    }
}
