//! The versioned committed page store.
//!
//! Every committed page image is kept as a version (page number, commit epoch) until the compactor has written it
//! into the database file and no snapshot can still see an older one. A reader at snapshot S reads the newest version
//! with an epoch <= S, else the page of the real file.
//!
//! Locking is striped: the chains of the pages with the same pgno % STRIPES share one mutex. Readers lock one stripe,
//! copy the bytes out and unlock; a publisher locks the stripes of every page it wrote or only read (ascending),
//! validates, installs the new versions invisibly and unlocks. Epochs and log places are assigned under `seq`.

use crate::util::*;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering::*};
use std::sync::{Mutex, MutexGuard};

pub const STRIPES: usize = 256;
pub const DEFAULT_BASE_LIMIT: i64 = 64 << 20;

pub struct PageVersion {
    pub epoch: u64,
    pub data: Box<[u8]>,
}

#[derive(Default)]
pub struct Chain {
    pub v: Vec<PageVersion>,
    pub dirty: bool,
    pub queued: bool,
}

impl Chain {
    pub fn head(&self) -> u64 {
        self.v.last().map_or(0, |x| x.epoch)
    }
    /// The index of the newest version with epoch <= snap.
    pub fn find(&self, snap: u64) -> Option<usize> {
        let i = self.v.partition_point(|x| x.epoch <= snap);
        if i == 0 { None } else { Some(i - 1) }
    }
}

pub type Stripe = PageMap<Chain>;

#[derive(Clone, Copy)]
pub struct SizeRec {
    pub epoch: u64,
    pub dbsize: u32,
}

pub struct Seq {
    pub sizes: Vec<SizeRec>,
    pub base_dbsize: u32,
    pub compacted_epoch: u64,
}

impl Seq {
    pub fn dbsize(&self, snap: u64) -> u32 {
        let i = self.sizes.partition_point(|s| s.epoch <= snap);
        if i == 0 { self.base_dbsize } else { self.sizes[i - 1].dbsize }
    }
}

#[derive(Default)]
pub struct Lists {
    pub dirty: Vec<u32>,
    pub cand: Vec<u32>,
}

pub struct Store {
    pub pgsz: usize,
    pub reserved: usize,
    pub stripes: Vec<Mutex<Stripe>>,
    pub seq: Mutex<Seq>,
    pub lists: Mutex<Lists>,
    pub p1_cookie: AtomicU32,
    pub p1_head: AtomicU64,
    pub base_bytes: AtomicI64,
    pub base_limit: AtomicI64,
    pub versions: AtomicI64,
    pub bytes: AtomicI64,
    pub reclaimed: AtomicI64,
    pub gc_runs: AtomicI64,
}

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Store {
    pub fn new(pgsz: usize, base_dbsize: u32, reserved: usize) -> Store {
        Store {
            pgsz,
            reserved,
            stripes: (0..STRIPES).map(|_| Mutex::new(Stripe::default())).collect(),
            seq: Mutex::new(Seq { sizes: Vec::new(), base_dbsize, compacted_epoch: 0 }),
            lists: Mutex::new(Lists::default()),
            p1_cookie: AtomicU32::new(0),
            p1_head: AtomicU64::new(0),
            base_bytes: AtomicI64::new(0),
            base_limit: AtomicI64::new(DEFAULT_BASE_LIMIT),
            versions: AtomicI64::new(0),
            bytes: AtomicI64::new(0),
            reclaimed: AtomicI64::new(0),
            gc_runs: AtomicI64::new(0),
        }
    }

    #[inline]
    pub fn stripe_of(pgno: u32) -> usize {
        pgno as usize % STRIPES
    }

    /// Copies bytes [poff, poff+dst.len()) of the newest version of pgno with epoch <= snap. False if none.
    pub fn read(&self, pgno: u32, snap: u64, poff: usize, dst: &mut [u8]) -> bool {
        let s = lock(&self.stripes[Self::stripe_of(pgno)]);
        if let Some(c) = s.get(&pgno)
            && let Some(i) = c.find(snap) {
                dst.copy_from_slice(&c.v[i].data[poff..poff + dst.len()]);
                return true;
            }
        false
    }

    /// A page read from the real file is remembered as an epoch-0 version (bounded by base_limit), so the next
    /// transaction (whose page cache is cold by design) copies it from memory. Only when the chain is empty.
    pub fn cache_base(&self, pgno: u32, image: &[u8]) {
        if self.base_bytes.load(Relaxed) + self.pgsz as i64 > self.base_limit.load(Relaxed) {
            return;
        }
        let mut s = lock(&self.stripes[Self::stripe_of(pgno)]);
        let c = s.entry(pgno).or_default();
        if c.v.is_empty() {
            c.v.push(PageVersion { epoch: 0, data: image.into() });
            self.base_bytes.fetch_add(self.pgsz as i64, Relaxed);
            self.versions.fetch_add(1, Relaxed);
            self.bytes.fetch_add(self.pgsz as i64, Relaxed);
        }
    }

    pub fn dbsize(&self, snap: u64) -> u32 {
        lock(&self.seq).dbsize(snap)
    }

    pub fn head_epoch(&self, pgno: u32) -> u64 {
        lock(&self.stripes[Self::stripe_of(pgno)]).get(&pgno).map_or(0, |c| c.head())
    }

    /// The newest committed image of pgno and its epoch, atomically.
    pub fn head_image(&self, pgno: u32, dst: &mut [u8]) -> Option<u64> {
        let s = lock(&self.stripes[Self::stripe_of(pgno)]);
        let c = s.get(&pgno)?;
        let v = c.v.last()?;
        dst.copy_from_slice(&v.data);
        Some(v.epoch)
    }

    /// Appends a version (the caller holds the page's stripe). The pages that must join the lists are collected.
    pub fn install(&self, pgno: u32, c: &mut Chain, epoch: u64, data: Box<[u8]>, lp: &mut Lists) {
        if !c.v.is_empty() && !c.queued {
            c.queued = true;
            lp.cand.push(pgno);
        }
        if !c.dirty {
            c.dirty = true;
            lp.dirty.push(pgno);
        }
        if pgno == 1 {
            self.p1_cookie.store(be32(&data[40..]), Relaxed);
            self.p1_head.store(epoch, Release);
        }
        c.v.push(PageVersion { epoch, data });
        self.versions.fetch_add(1, Relaxed);
        self.bytes.fetch_add(self.pgsz as i64, Relaxed);
    }

    pub fn splice(&self, lp: Lists) {
        if lp.cand.is_empty() && lp.dirty.is_empty() {
            return;
        }
        let mut l = lock(&self.lists);
        l.cand.extend(lp.cand);
        l.dirty.extend(lp.dirty);
    }

    /// Installs a commit read back from the log (before the database is used).
    pub fn install_recovered(&self, epoch: u64, dbsize: u32, pgnos: &[u32], images: &[&[u8]]) {
        let mut lp = Lists::default();
        for (i, &pg) in pgnos.iter().enumerate() {
            let mut s = lock(&self.stripes[Self::stripe_of(pg)]);
            let c = s.entry(pg).or_default();
            self.install(pg, c, epoch, images[i].into(), &mut lp);
        }
        self.splice(lp);
        lock(&self.seq).sizes.push(SizeRec { epoch, dbsize });
    }

    /// Locks a set of stripes in ascending order.
    pub fn lock_stripes(&self, pages: &[&[u32]]) -> StripeGuards<'_> {
        let mut set = [false; STRIPES];
        for ps in pages {
            for &p in *ps {
                set[Self::stripe_of(p)] = true;
            }
        }
        let mut idx = [u16::MAX; STRIPES];
        let mut guards = Vec::new();
        for s in 0..STRIPES {
            if set[s] {
                idx[s] = guards.len() as u16;
                guards.push(lock(&self.stripes[s]));
            }
        }
        StripeGuards { idx, guards }
    }
}

pub struct StripeGuards<'a> {
    idx: [u16; STRIPES],
    guards: Vec<MutexGuard<'a, Stripe>>,
}

impl<'a> StripeGuards<'a> {
    pub fn stripe(&mut self, pgno: u32) -> &mut Stripe {
        let i = self.idx[Store::stripe_of(pgno)];
        &mut self.guards[i as usize]
    }
    pub fn get(&mut self, pgno: u32) -> Option<&Chain> {
        self.stripe(pgno).get(&pgno)
    }
    pub fn chain(&mut self, pgno: u32) -> &mut Chain {
        self.stripe(pgno).entry(pgno).or_default()
    }
}
