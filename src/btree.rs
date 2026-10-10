//! Two refinements of page-level validation for interior b-tree pages.
//!
//! Routes: a transaction that read an interior page whose newer version still routes every child the transaction
//! went through (same divider keys on both sides of the child) took the same path, so the read is still valid. Only
//! for table b-trees: the cells of an index interior page are entries.
//!
//! Merge: two transactions that split different children of the same parent both rewrite the parent; the parent is
//! a sorted list of (child, upper bound) pairs, so the rewrites are merged three-way, refusing any child changed by
//! both sides.

use crate::util::*;
use std::collections::HashMap;

struct ICell<'a> {
    child: u32,
    key: &'a [u8],
}

struct INode<'a> {
    typ: u8,
    right: u32,
    cells: Vec<ICell<'a>>,
}

fn parse_interior(pg: &[u8], pgsz: usize, reserved: usize) -> Option<INode<'_>> {
    let typ = pg[0];
    if typ != 0x02 && typ != 0x05 {
        return None;
    }
    let ncell = be16(&pg[3..]);
    if 12 + 2 * ncell > pgsz {
        return None;
    }
    let u = pgsz - reserved;
    let (min_local, max_local) = ((u - 12) * 32 / 255 - 23, (u - 12) * 64 / 255 - 23);
    let mut cells = Vec::with_capacity(ncell);
    for i in 0..ncell {
        let off = be16(&pg[12 + 2 * i..]);
        if off < 12 + 2 * ncell || off + 5 > pgsz {
            return None;
        }
        let (v, n) = varint(&pg[off + 4..pgsz]);
        if n == 0 {
            return None;
        }
        let mut ln = n;
        if typ == 0x02 {
            let mut local = v as usize;
            if v > max_local as u64 {
                local = min_local + ((v - min_local as u64) % (u as u64 - 4)) as usize;
                if local > max_local {
                    local = min_local;
                }
                local += 4;
            }
            ln = n + local;
        }
        if off + 4 + ln > pgsz {
            return None;
        }
        cells.push(ICell { child: be32(&pg[off..]), key: &pg[off + 4..off + 4 + ln] });
    }
    Some(INode { typ, right: be32(&pg[8..]), cells })
}

impl INode<'_> {
    fn find_child(&self, c: u32) -> Option<usize> {
        self.cells.iter().position(|x| x.child == c).or(if self.right == c { Some(self.cells.len()) } else { None })
    }
}

/// The newer version `nw` of an interior table page routes every child of `used` (sorted) that `old` lists exactly as
/// `old` did.
pub fn interior_routes_same(old: &[u8], nw: &[u8], pgsz: usize, reserved: usize, used: &[u32]) -> bool {
    let (Some(a), Some(b)) = (parse_interior(old, pgsz, reserved), parse_interior(nw, pgsz, reserved)) else { return false };
    if a.typ != 0x05 || b.typ != 0x05 {
        return false;
    }
    let mut any = false;
    for i in 0..=a.cells.len() {
        let c = if i < a.cells.len() { a.cells[i].child } else { a.right };
        if used.binary_search(&c).is_err() {
            continue;
        }
        any = true;
        let Some(j) = b.find_child(c) else { return false };
        let (a_lo, b_lo, a_hi, b_hi) = (i > 0, j > 0, i < a.cells.len(), j < b.cells.len());
        if a_lo != b_lo || a_hi != b_hi {
            return false;
        }
        if a_lo && a.cells[i - 1].key != b.cells[j - 1].key {
            return false;
        }
        if a_hi && a.cells[i].key != b.cells[j].key {
            return false;
        }
    }
    any
}

#[derive(Clone)]
struct MCell<'a> {
    child: u32,
    key: i64,
    inf: bool,
    kb: &'a [u8],
}

fn parse_table_interior(pg: &[u8], pgsz: usize) -> Option<Vec<MCell<'_>>> {
    if pg[0] != 0x05 {
        return None;
    }
    let ncell = be16(&pg[3..]);
    if 12 + 2 * ncell > pgsz {
        return None;
    }
    let mut out = Vec::with_capacity(ncell + 1);
    for i in 0..ncell {
        let off = be16(&pg[12 + 2 * i..]);
        if off < 12 + 2 * ncell || off + 5 > pgsz {
            return None;
        }
        let (v, n) = varint(&pg[off + 4..pgsz]);
        if n == 0 {
            return None;
        }
        out.push(MCell { child: be32(&pg[off..]), key: v as i64, inf: false, kb: &pg[off + 4..off + 4 + n] });
    }
    out.push(MCell { child: be32(&pg[8..]), key: 0, inf: true, kb: &[] });
    Some(out)
}

fn same_hi(a: &MCell, b: &MCell) -> bool {
    a.inf == b.inf && (a.inf || a.kb == b.kb)
}

fn cmp_cell(a: &MCell, b: &MCell) -> std::cmp::Ordering {
    if a.inf != b.inf {
        return if a.inf { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
    }
    a.key.cmp(&b.key)
}

fn child_map(c: &[MCell]) -> Option<HashMap<u32, usize>> {
    let mut m = HashMap::with_capacity(c.len());
    for (i, x) in c.iter().enumerate() {
        if m.insert(x.child, i).is_some() {
            return None;
        }
    }
    Some(m)
}

/// Merges three-way an interior table page two transactions rewrote from the same base. `ours` must already use final
/// page numbers for its new children.
pub fn interior_merge(base: &[u8], ours: &[u8], theirs: &[u8], pgsz: usize, reserved: usize, out: &mut [u8]) -> bool {
    let (Some(b), Some(o), Some(h)) = (parse_table_interior(base, pgsz), parse_table_interior(ours, pgsz), parse_table_interior(theirs, pgsz)) else {
        return false;
    };
    let (Some(mb), Some(mo), Some(mh)) = (child_map(&b), child_map(&o), child_map(&h)) else { return false };
    for x in &b {
        if !mo.contains_key(&x.child) || !mh.contains_key(&x.child) {
            return false; // every child of the base must still be listed on both sides
        }
    }
    let mut m: Vec<MCell> = Vec::with_capacity(o.len() + h.len());
    for x in &o {
        let mut c = x.clone();
        match (mb.get(&x.child), mh.get(&x.child)) {
            (Some(&ib), ih) => {
                let changed_o = !same_hi(x, &b[ib]);
                let changed_h = ih.is_some_and(|&ih| !same_hi(&h[ih], &b[ib]));
                if changed_o && changed_h {
                    return false; // both moved the same child's upper bound
                }
                if changed_h {
                    c = h[*ih.unwrap()].clone();
                }
            }
            (None, Some(_)) => return false, // a child new in ours cannot exist in theirs
            (None, None) => {}
        }
        m.push(c);
    }
    for x in &h {
        if !mb.contains_key(&x.child) {
            if mo.contains_key(&x.child) {
                return false;
            }
            m.push(x.clone());
        }
    }
    m.sort_by(cmp_cell);
    let infs = m.iter().filter(|x| x.inf).count();
    for i in 1..m.len() {
        if !m[i].inf && cmp_cell(&m[i - 1], &m[i]) != std::cmp::Ordering::Less {
            return false; // keys strictly increasing
        }
    }
    if infs != 1 || !m.last().unwrap().inf {
        return false;
    }
    let usable = pgsz - reserved;
    let ncell = m.len() - 1;
    let total: usize = m[..ncell].iter().map(|x| 4 + x.kb.len()).sum();
    if 12 + 2 * ncell + total > usable {
        return false; // does not fit: the parent would have to split
    }
    out[..pgsz].fill(0);
    out[0] = 0x05;
    out[3] = (ncell >> 8) as u8;
    out[4] = ncell as u8;
    let content = usable - total;
    out[5] = ((content & 0xffff) >> 8) as u8;
    out[6] = content as u8;
    put32(&mut out[8..], m[ncell].child);
    let mut pos = usable;
    for (i, x) in m[..ncell].iter().enumerate() {
        pos -= 4 + x.kb.len();
        put32(&mut out[pos..], x.child);
        out[pos + 4..pos + 4 + x.kb.len()].copy_from_slice(x.kb);
        out[12 + 2 * i] = (pos >> 8) as u8;
        out[13 + 2 * i] = pos as u8;
    }
    true
}
