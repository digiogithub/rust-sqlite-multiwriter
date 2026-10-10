//! Page relocation: a commit that conflicts only because other commits extended the file.
//!
//! Two transactions that start from the same snapshot and both append to the database allocate the same new page
//! numbers and both rewrite page 1 (the in-header size). Everything that refers to a page this transaction created
//! lives in pages this transaction wrote, so the new pages can be renumbered above the current end of the file if the
//! references are rewritten (child pointers of interior pages, overflow pointers of cells and of overflow pages); page
//! 1 is merged. It refuses unless every check holds (see the Go port and the original for the list), and the result
//! is published through the normal validation.

use crate::btree::interior_merge;
use crate::db::Validation;
use crate::lane::Lane;
use crate::util::*;
use std::sync::atomic::Ordering::*;

const K_NONE: u8 = 0;
const K_BTREE: u8 = 1;
const K_OVERFLOW: u8 = 2;

struct Rctx {
    lo: u32,
    hi: u32,
    delta: u32,
    pgsz: usize,
    usable: usize,
    kind: Vec<u8>,
    refs: Vec<u32>,
    queue: Vec<u32>,
    ok: bool,
}

impl Rctx {
    fn enqueue(&mut self, p: u32, kind: u8) {
        let i = (p - self.lo - 1) as usize;
        self.refs[i] += 1;
        if self.refs[i] > 1 || (self.kind[i] != K_NONE && self.kind[i] != kind) {
            self.ok = false;
            return;
        }
        self.kind[i] = kind;
        self.queue.push(p);
    }

    fn patch(&mut self, pg: &mut [u8], off: usize, kind: u8) {
        let p = be32(&pg[off..]);
        if p > self.lo && p <= self.hi {
            put32(&mut pg[off..], p + self.delta);
            self.enqueue(p, kind);
        }
    }

    fn patch_btree_page(&mut self, pg: &mut [u8]) {
        let typ = pg[0];
        if !matches!(typ, 0x02 | 0x05 | 0x0a | 0x0d) {
            self.ok = false;
            return;
        }
        let interior = typ == 0x02 || typ == 0x05;
        let hdr = if interior { 12 } else { 8 };
        let ncell = be16(&pg[3..]);
        if hdr + 2 * ncell > self.pgsz {
            self.ok = false;
            return;
        }
        if interior {
            self.patch(pg, 8, K_BTREE);
        }
        let u = self.usable;
        let min_local = (u - 12) * 32 / 255 - 23;
        let max_local = if typ == 0x0d { u - 35 } else { (u - 12) * 64 / 255 - 23 };
        for i in 0..ncell {
            if !self.ok {
                return;
            }
            let off = be16(&pg[hdr + 2 * i..]);
            if off < hdr + 2 * ncell || off + 4 > self.pgsz {
                self.ok = false;
                return;
            }
            if typ == 0x05 {
                self.patch(pg, off, K_BTREE);
                continue;
            }
            let mut q = off;
            if typ == 0x02 {
                self.patch(pg, q, K_BTREE);
                q += 4;
            }
            let (payload, n) = varint(&pg[q..self.pgsz]);
            if n == 0 {
                self.ok = false;
                return;
            }
            q += n;
            if typ == 0x0d {
                let (_, n) = varint(&pg[q..self.pgsz]);
                if n == 0 {
                    self.ok = false;
                    return;
                }
                q += n;
            }
            if payload > max_local as u64 {
                let mut local = min_local + ((payload - min_local as u64) % (u as u64 - 4)) as usize;
                if local > max_local {
                    local = min_local;
                }
                if q + local + 4 > self.pgsz {
                    self.ok = false;
                    return;
                }
                self.patch(pg, q + local, K_OVERFLOW);
            }
        }
    }
}

impl Lane {
    /// Publishes the transaction again with its new pages moved above the current end of the file: the epoch, or
    /// ERR_RELOC_NA (not a growth-only conflict), ERR_CONFLICT (beaten by other extensions) or another error.
    pub fn relocate(&mut self, v0: &Validation, pgnos: &[u32], imgs: &[&[u8]], ws_dbsize: u32, snap_dbsize: u32, sync: bool) -> Result<u64, i32> {
        let db = self.db.clone();
        let e = db.engine();
        let st = &e.store;
        let pgsz = st.pgsz;
        if !self.own_pg.is_empty() || ws_dbsize <= snap_dbsize {
            return Err(ERR_RELOC_NA);
        }
        let growth = ws_dbsize - snap_dbsize;
        let i1 = pgnos.iter().position(|&p| p == 1).ok_or(ERR_RELOC_NA)?;
        if pgnos.iter().filter(|&&p| p > snap_dbsize).count() as u32 != growth || pgnos.iter().any(|&p| p > ws_dbsize) {
            return Err(ERR_RELOC_NA); // the new pages must be the whole range
        }
        // page 1 may differ from the snapshot's only in the change counter, the size and version-valid-for
        let mut snap1 = [0u8; 100];
        if !st.read(1, self.tx.snapshot_epoch, 0, &mut snap1) && crate::fsio::pread(&e.real, &mut snap1, 0).map_or(true, |n| n < 100) {
            return Err(ERR_RELOC_NA);
        }
        let our1 = imgs[i1];
        for b in 0..100 {
            if (24..32).contains(&b) || (92..96).contains(&b) {
                continue;
            }
            if snap1[b] != our1[b] {
                return Err(ERR_RELOC_NA);
            }
        }
        let reserved = our1[20] as usize;
        let pending = (1073741824 / pgsz) as u32 + 1; // the lock-byte page
        let mut latest = vec![0u8; pgsz];
        for _attempt in 0..4 {
            // pages that existed in the snapshot and have a newer version: an interior table page is merged, anything else
            // is a real conflict
            let mut cf = Vec::new();
            for (i, &p) in pgnos.iter().enumerate() {
                if p != 1 && p <= snap_dbsize && st.head_epoch(p) > self.tx.snapshot_epoch {
                    if imgs[i][0] == 0x05 && !self.nomerge {
                        cf.push(i);
                    } else {
                        return Err(ERR_RELOC_NA);
                    }
                }
            }
            db.reloc_lock();
            self.core.holds_reloc = true;
            let unlock = |l: &mut Lane| {
                if std::mem::take(&mut l.core.holds_reloc) {
                    l.db.reloc_unlock();
                }
            };
            let e_merge = {
                let pc = crate::store::lock(&db.p1_cache);
                if pc.1 != 0 && pc.1 == st.p1_head.load(Acquire) {
                    latest.copy_from_slice(&pc.0);
                    Some(pc.1)
                } else {
                    None
                }
            };
            let Some(e_merge) = e_merge.or_else(|| st.head_image(1, &mut latest)) else {
                unlock(self);
                return Err(ERR_RELOC_NA);
            };
            let cur = be32(&latest[28..]);
            let ctr = be32(&latest[24..]);
            let new_size = cur + growth;
            if be32(&latest[92..]) != ctr || cur < snap_dbsize || latest[40..44] != snap1[40..44] || latest[16..22] != snap1[16..22] || cur == snap_dbsize
                || (snap_dbsize < pending && ws_dbsize >= pending) || (cur < pending && new_size >= pending)
            {
                unlock(self);
                return Err(ERR_RELOC_NA);
            }
            let delta = cur - snap_dbsize;
            let mut c = Rctx { lo: snap_dbsize, hi: ws_dbsize, delta, pgsz, usable: pgsz - reserved, kind: vec![0; growth as usize], refs: vec![0; growth as usize], queue: Vec::new(), ok: true };
            let mut nim: Vec<Vec<u8>> = imgs.iter().map(|x| x.to_vec()).collect();
            let mut slot_of_new = vec![0usize; growth as usize];
            for (i, &p) in pgnos.iter().enumerate() {
                if p > snap_dbsize {
                    slot_of_new[(p - snap_dbsize - 1) as usize] = i;
                }
            }
            for (i, &p) in pgnos.iter().enumerate() {
                if p != 1 && p <= snap_dbsize && c.ok {
                    c.patch_btree_page(&mut nim[i]);
                }
            }
            let mut qi = 0;
            while qi < c.queue.len() && c.ok {
                let pq = c.queue[qi];
                let slot = slot_of_new[(pq - snap_dbsize - 1) as usize];
                if c.kind[(pq - snap_dbsize - 1) as usize] == K_BTREE {
                    c.patch_btree_page(&mut nim[slot]);
                } else {
                    c.patch(&mut nim[slot], 0, K_OVERFLOW); // an overflow page: its first 4 bytes point to the next one
                }
                qi += 1;
            }
            if !c.ok || c.refs.iter().any(|&r| r != 1) {
                unlock(self);
                return Err(ERR_RELOC_NA); // each new page referenced exactly once
            }
            let npg: Vec<u32> = pgnos.iter().map(|&p| if p > snap_dbsize { p + delta } else { p }).collect();
            nim[i1].copy_from_slice(&latest);
            put32(&mut nim[i1][24..], ctr + 1);
            put32(&mut nim[i1][92..], ctr + 1);
            put32(&mut nim[i1][28..], new_size);
            let mut own: Vec<(u32, u64)> = vec![(1, e_merge)];
            let mut nmerged = 0;
            let mut theirs = vec![0u8; pgsz];
            let mut basei = vec![0u8; pgsz];
            let mut merged = vec![0u8; pgsz];
            for &slot in &cf {
                let Some(eh) = st.head_image(pgnos[slot], &mut theirs) else {
                    unlock(self);
                    return Err(ERR_RELOC_NA);
                };
                if eh <= self.tx.snapshot_epoch {
                    continue;
                }
                if !st.read(pgnos[slot], self.tx.snapshot_epoch, 0, &mut basei) && !db.read_real(pgnos[slot], &mut basei) {
                    unlock(self);
                    return Err(ERR_RELOC_NA);
                }
                if !interior_merge(&basei, &nim[slot], &theirs, pgsz, reserved, &mut merged) {
                    unlock(self);
                    return Err(ERR_RELOC_NA);
                }
                nim[slot].copy_from_slice(&merged);
                own.push((pgnos[slot], eh));
                nmerged += 1;
            }
            own.sort_by_key(|x| x.0);
            let v = Validation {
                snapshot_epoch: v0.snapshot_epoch,
                check_cookie: v0.check_cookie,
                cookie: v0.cookie,
                read_pgnos: v0.read_pgnos.clone(),
                own_pgnos: own.iter().map(|x| x.0).collect(),
                own_epochs: own.iter().map(|x| x.1).collect(),
            };
            let refs: Vec<&[u8]> = nim.iter().map(|x| x.as_slice()).collect();
            let r = db.publish(Some(&mut self.core), Some(&v), &npg, &refs, new_size, cur, sync); // (releases the relocation lock)
            unlock(self);
            match r {
                Ok(epoch) => {
                    db.n.relocations.fetch_add(1, Relaxed);
                    db.n.merges.fetch_add(nmerged, Relaxed);
                    return Ok(epoch);
                }
                Err(ERR_CONFLICT) => continue, // another commit extended the file meanwhile: read the latest again
                Err(rc) => return Err(rc),
            }
        }
        Err(ERR_CONFLICT)
    }
}
