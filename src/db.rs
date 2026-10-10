//! Per-database state shared by every connection (lane) of this process that opened the same file, the commit
//! protocol, garbage collection and compaction.

use crate::btree::interior_routes_same;
use crate::fsio;
use crate::log::{Log, Place};
use crate::store::{lock, Lists, SizeRec, Store};
use crate::util::*;
use std::collections::HashMap;
use std::fs::File;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

/// What the first lane of a database opens: the page store, the log and the database file.
pub struct Engine {
    pub store: Store,
    pub log: Log,
    pub real: File,
}

pub struct ActiveSnap {
    pub epoch: u64,
    pub write_locked: Arc<AtomicBool>,
}

#[derive(Default)]
pub struct Counters {
    pub commits: AtomicI64,
    pub aborts: AtomicI64,
    pub snapshots: AtomicI64,
    pub page_conflicts: AtomicI64,
    pub read_conflicts: AtomicI64,
    pub schema_conflicts: AtomicI64,
    pub relocations: AtomicI64,
    pub merges: AtomicI64,
    pub reads_saved: AtomicI64,
    pub compactions: AtomicI64,
    pub compacted_pages: AtomicI64,
    pub hot_serialised: AtomicI64,
    pub backpressure: AtomicI64,
    pub pages_published: AtomicI64,
    pub rebases: AtomicI64,
    pub rebases_grouped: AtomicI64,
    pub rebase_retries: AtomicI64,
    pub unrebasable: AtomicI64,
}

pub struct MwDb {
    pub path: String,
    refs: Mutex<usize>,
    pub open_mu: Mutex<()>,
    engine: OnceLock<Engine>,

    pub active: Mutex<HashMap<usize, ActiveSnap>>,
    pub epoch: AtomicU64,
    pub next_epoch: AtomicU64,
    vis: Mutex<()>,
    vis_cv: Condvar,
    vis_parked: AtomicI32,

    pub schema_gen: AtomicU64,
    pub last_schema_epoch: AtomicU64,

    reloc_mu: Mutex<bool>,
    reloc_cv: Condvar,
    pub p1_cache: Mutex<(Vec<u8>, u64)>,

    pub hot_held: AtomicI32,
    pub ddl_owner: AtomicUsize,
    pub gate_closing: AtomicI32,
    pub gate_active: AtomicI32,
    pub gate_owner: AtomicUsize,
    pub rb_queue: Mutex<crate::rebase::Queue>,
    pub rb_cv: Condvar,

    pub failed: AtomicBool,
    compact_mu: Mutex<()>,
    compactor: Mutex<Option<(Arc<(Mutex<bool>, Condvar)>, JoinHandle<()>)>>,
    pub log_max_bytes: AtomicI64,
    pub gc_interval: AtomicI64,
    pub_since_gc: AtomicI64,
    pub n: Counters,
}

static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<MwDb>>>> = OnceLock::new();
fn registry() -> &'static Mutex<HashMap<String, Arc<MwDb>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn acquire(path: &str) -> Arc<MwDb> {
    let mut r = lock(registry());
    if let Some(db) = r.get(path) {
        *lock(&db.refs) += 1;
        return db.clone();
    }
    let db = Arc::new(MwDb {
        path: path.to_string(),
        refs: Mutex::new(1),
        open_mu: Mutex::new(()),
        engine: OnceLock::new(),
        active: Mutex::new(HashMap::new()),
        epoch: AtomicU64::new(1),
        next_epoch: AtomicU64::new(1),
        vis: Mutex::new(()),
        vis_cv: Condvar::new(),
        vis_parked: AtomicI32::new(0),
        schema_gen: AtomicU64::new(1),
        last_schema_epoch: AtomicU64::new(0),
        reloc_mu: Mutex::new(false),
        reloc_cv: Condvar::new(),
        p1_cache: Mutex::new((Vec::new(), 0)),
        hot_held: AtomicI32::new(0),
        ddl_owner: AtomicUsize::new(0),
        gate_closing: AtomicI32::new(0),
        gate_active: AtomicI32::new(0),
        gate_owner: AtomicUsize::new(0),
        rb_queue: Mutex::new(crate::rebase::Queue::default()),
        rb_cv: Condvar::new(),
        failed: AtomicBool::new(false),
        compact_mu: Mutex::new(()),
        compactor: Mutex::new(None),
        log_max_bytes: AtomicI64::new(32 << 20),
        gc_interval: AtomicI64::new(64),
        pub_since_gc: AtomicI64::new(0),
        n: Counters::default(),
    });
    r.insert(path.to_string(), db.clone());
    db
}

pub fn lookup(path: &str) -> Option<Arc<MwDb>> {
    lock(registry()).get(path).cloned()
}

/// Drops a reference. The last one stops the compactor, materialises everything into the database file (it is then
/// an ordinary SQLite database again) and removes the log. A failed database keeps its log for recovery.
pub fn release(db: &Arc<MwDb>) {
    let mut r = lock(registry());
    {
        let mut refs = lock(&db.refs);
        *refs -= 1;
        if *refs > 0 {
            return;
        }
    }
    r.remove(&db.path);
    drop(r);
    db.stop_compactor();
    if let Some(e) = db.engine.get() {
        let mut clean = false;
        if !db.failed.load(Acquire) && db.compact().is_ok() {
            clean = e.log.base_epoch() == db.epoch.load(Acquire) && db.next_epoch.load(Acquire) == db.epoch.load(Acquire);
        }
        e.log.close(clean);
    }
}

impl MwDb {
    pub fn engine(&self) -> &Engine {
        self.engine.get().expect("the database is not open")
    }
    pub fn engine_opt(&self) -> Option<&Engine> {
        self.engine.get()
    }
    pub fn set_engine(&self, e: Engine, last: u64) {
        self.epoch.store(last, Release);
        self.next_epoch.store(last, Release);
        let _ = self.engine.set(e);
    }

    pub fn visible_epoch(&self) -> u64 {
        self.epoch.load(Acquire)
    }

    /// The oldest snapshot in use (the visible epoch when none is).
    pub fn oldest_active(&self) -> u64 {
        let a = lock(&self.active);
        let mut oldest = self.visible_epoch();
        for s in a.values() {
            oldest = oldest.min(s.epoch);
        }
        oldest
    }

    /// Epoch E becomes visible only after E-1 did, so readers never observe a hole.
    pub fn make_visible(&self, e: u64) -> Result<(), i32> {
        if self.epoch.load(Acquire) >= e {
            return Ok(());
        }
        for _ in 0..200 {
            if self.epoch.load(Acquire) >= e - 1 {
                break;
            }
            if self.failed.load(Acquire) {
                return Err(RC_IOERR);
            }
            std::thread::yield_now();
        }
        if self.epoch.load(Acquire) < e - 1 {
            let mut g = lock(&self.vis);
            self.vis_parked.fetch_add(1, SeqCst);
            while self.epoch.load(Acquire) < e - 1 {
                if self.failed.load(Acquire) {
                    self.vis_parked.fetch_sub(1, SeqCst);
                    return Err(RC_IOERR);
                }
                g = self.vis_cv.wait(g).unwrap_or_else(|x| x.into_inner());
            }
            self.vis_parked.fetch_sub(1, SeqCst);
        }
        let _ = self.epoch.compare_exchange(e - 1, e, AcqRel, Acquire);
        if self.vis_parked.load(SeqCst) > 0 {
            let _g = lock(&self.vis);
            self.vis_cv.notify_all();
        }
        Ok(())
    }

    pub fn wake_all(&self) {
        let _g = lock(&self.vis);
        self.vis_cv.notify_all();
    }

    pub fn read_real(&self, pgno: u32, dst: &mut [u8]) -> bool {
        let e = self.engine();
        match fsio::pread(&e.real, dst, (pgno as u64 - 1) * dst.len() as u64) {
            Ok(n) => {
                dst[n..].fill(0);
                n > 0
            }
            Err(_) => false,
        }
    }

    // MARK: - the relocation lock (one relocation at a time: they all chase the same end of the file)

    pub fn reloc_lock(&self) {
        let mut g = lock(&self.reloc_mu);
        while *g {
            g = self.reloc_cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        *g = true;
    }
    pub fn reloc_unlock(&self) {
        *lock(&self.reloc_mu) = false;
        self.reloc_cv.notify_one();
    }

    // MARK: - the publication gate (see rebase.rs)

    pub fn gate_enter(&self, lane_id: usize) {
        loop {
            if self.gate_closing.load(SeqCst) == 0 || (lane_id != 0 && self.gate_owner.load(SeqCst) == lane_id) {
                self.gate_active.fetch_add(1, SeqCst);
                if self.gate_closing.load(SeqCst) == 0 || (lane_id != 0 && self.gate_owner.load(SeqCst) == lane_id) {
                    return;
                }
                self.gate_active.fetch_sub(1, SeqCst);
            }
            std::thread::yield_now();
        }
    }
    pub fn gate_exit(&self) {
        self.gate_active.fetch_sub(1, SeqCst);
    }
    pub fn gate_close(&self, owner: usize) {
        self.gate_owner.store(owner, SeqCst);
        self.gate_closing.store(1, SeqCst);
        while self.gate_active.load(SeqCst) > 0 {
            std::thread::yield_now();
        }
        while self.epoch.load(Acquire) != self.next_epoch.load(Acquire) && !self.failed.load(Acquire) {
            std::thread::yield_now();
        }
    }
    pub fn gate_open(&self) {
        self.gate_closing.store(0, SeqCst);
        self.gate_owner.store(0, SeqCst);
    }

    // MARK: - publication

    /// Validates a transaction against v and installs it atomically at a new epoch, writes its log record and makes
    /// it visible. Returns the epoch, or an internal conflict code or an SQLite error code.
    pub fn publish(&self, lane: Option<&mut crate::lane::LaneCore>, v: Option<&Validation>, pgnos: &[u32], imgs: &[&[u8]], ws_dbsize: u32, snap_dbsize: u32, sync: bool) -> Result<u64, i32> {
        let e = self.engine();
        let st = &e.store;
        if self.failed.load(Acquire) {
            return Err(RC_IOERR);
        }
        let (lane_id, noroute) = lane.as_ref().map_or((0, false), |l| (l.id, l.noroute));
        // a relocation holds the relocation lock: it is released as soon as its pages are installed
        let mut holds_reloc = lane.is_some_and(|l| std::mem::take(&mut l.holds_reloc));
        let reloc_release = |h: &mut bool| {
            if std::mem::take(h) {
                self.reloc_unlock();
            }
        };
        let copies: Vec<Box<[u8]>> = imgs.iter().map(|x| Box::<[u8]>::from(*x)).collect();
        let empty: Vec<u32> = Vec::new();
        let reads = v.map_or(&empty, |v| &v.read_pgnos);
        self.gate_enter(lane_id);
        let mut g = st.lock_stripes(&[pgnos, reads]);

        // ---- validate
        if let Some(v) = v {
            let mut rc = RC_OK;
            let mut used: Option<Vec<u32>> = None;
            for &rp in reads {
                let snap = v.snap_for(rp);
                if g.get(rp).map_or(0, |c| c.head()) <= snap {
                    continue;
                }
                let (old, newest) = match g.get(rp) {
                    None => (None, None),
                    Some(c) => (c.find(snap).map(|i| c.v[i].data.to_vec()), c.v.last().map(|x| x.data.to_vec())),
                };
                if !noroute && lane_id != 0
                    && let Some(newest) = newest {
                        // an interior page changed: is the route this transaction took through it unchanged?
                        let used = used.get_or_insert_with(|| {
                            let mut u: Vec<u32> = reads.iter().chain(pgnos.iter()).copied().collect();
                            u.sort_unstable();
                            u
                        });
                        let old = old.or_else(|| {
                            let mut b = vec![0u8; st.pgsz];
                            if self.read_real(rp, &mut b) { Some(b) } else { None }
                        });
                        if let Some(old) = old
                            && interior_routes_same(&old, &newest, st.pgsz, st.reserved, used) {
                                self.n.reads_saved.fetch_add(1, Relaxed);
                                continue;
                            }
                    }
                self.n.read_conflicts.fetch_add(1, Relaxed);
                rc = ERR_CONFLICT_READ;
                break;
            }
            if rc == RC_OK {
                for &pg in pgnos {
                    if g.get(pg).map_or(0, |c| c.head()) > v.snap_for(pg) {
                        self.n.page_conflicts.fetch_add(1, Relaxed);
                        rc = ERR_CONFLICT;
                        break;
                    }
                }
            }
            // a schema change invalidates everything the transaction did, even on other pages
            if rc == RC_OK && v.check_cookie && st.p1_head.load(Acquire) > v.snap_for(1) && st.p1_cookie.load(Relaxed) != v.cookie {
                self.n.schema_conflicts.fetch_add(1, Relaxed);
                rc = ERR_CONFLICT_SCHEMA;
            }
            if rc != RC_OK {
                drop(g);
                self.gate_exit();
                reloc_release(&mut holds_reloc);
                return Err(rc);
            }
        }

        // ---- assign the epoch, the size record and the place in the log: the serialisation point
        let mut seq = lock(&st.seq);
        let epoch = self.next_epoch.load(Acquire) + 1;
        let last_size = seq.sizes.last().map_or(seq.base_dbsize, |s| s.dbsize);
        // a transaction that did not change the size must not roll the recorded size back to its stale view
        let new_size = if ws_dbsize != snap_dbsize { ws_dbsize } else { last_size };
        let place = match e.log.assign(epoch, pgnos.len()) {
            Ok(p) => p,
            Err(rc) => {
                drop(seq);
                drop(g);
                self.gate_exit();
                reloc_release(&mut holds_reloc);
                return Err(rc);
            }
        };
        seq.sizes.push(SizeRec { epoch, dbsize: new_size });
        self.next_epoch.store(epoch, Release);
        drop(seq);

        let prev_p1 = (st.p1_head.load(Acquire), st.p1_cookie.load(Relaxed));
        let mut wrote_p1 = false;
        let mut lp = Lists::default();
        for (i, data) in copies.into_iter().enumerate() {
            let pg = pgnos[i];
            if pg == 1 {
                wrote_p1 = true;
                if holds_reloc {
                    // hand the new page 1 to the next relocation (it is the newest version)
                    *lock(&self.p1_cache) = (data.to_vec(), epoch);
                }
            }
            let c = g.chain(pg);
            st.install(pg, c, epoch, data, &mut lp);
        }
        st.splice(lp);
        drop(g);
        self.gate_exit();
        reloc_release(&mut holds_reloc); // (a relocation: the next one may go, our pages are installed)

        // ---- outside the locks: persist, then become visible (in epoch order)
        if let Err(rc) = e.log.write(&place, epoch, new_size, pgnos, imgs) {
            self.take_back(epoch, &place, pgnos, wrote_p1, prev_p1);
            return Err(rc);
        }
        if sync
            && let Err(rc) = e.log.sync() {
                self.failed.store(true, Release); // outcome uncertain: stop accepting commits
                self.wake_all();
                return Err(rc);
            }
        self.make_visible(epoch)?;

        // the log only shrinks through compaction: ask the compactor, and throttle if it cannot keep up
        let lim = self.log_max_bytes.load(Relaxed);
        if lim > 0 {
            let b = e.log.bytes();
            if b > lim {
                self.kick_compactor();
                if b > lim * 16 {
                    self.n.backpressure.fetch_add(1, Relaxed);
                    let r = b as f64 / (lim * 16) as f64;
                    let us = (500.0 * r * r * r).min(50000.0);
                    std::thread::sleep(Duration::from_micros(us as u64));
                }
            }
        }
        self.n.commits.fetch_add(1, Relaxed);
        self.n.pages_published.fetch_add(pgnos.len() as i64, Relaxed);
        let gi = self.gc_interval.load(Relaxed);
        if gi > 0 && self.pub_since_gc.fetch_add(1, Relaxed) + 1 >= gi {
            self.gc();
        }
        Ok(epoch)
    }

    /// The record never made it. If nothing was assigned after it the commit is taken back cleanly; otherwise a
    /// successor may already sit behind the hole: the database is failed until it is reopened.
    fn take_back(&self, epoch: u64, place: &Place, pgnos: &[u32], wrote_p1: bool, prev_p1: (u64, u32)) {
        let e = self.engine();
        let st = &e.store;
        let mut seq = lock(&st.seq);
        let latest = self.next_epoch.load(Acquire) == epoch && e.log.unassign(place, pgnos.len());
        if latest {
            seq.sizes.pop();
            self.next_epoch.store(epoch - 1, Release);
        } else {
            self.failed.store(true, Release);
        }
        drop(seq);
        if latest {
            let mut g = st.lock_stripes(&[pgnos]);
            for &pg in pgnos {
                let c = g.chain(pg);
                if c.v.last().is_some_and(|x| x.epoch == epoch) {
                    c.v.pop();
                    st.versions.fetch_sub(1, Relaxed);
                    st.bytes.fetch_sub(st.pgsz as i64, Relaxed);
                }
            }
            if wrote_p1 && st.p1_head.load(Acquire) == epoch {
                st.p1_cookie.store(prev_p1.1, Relaxed);
                st.p1_head.store(prev_p1.0, Release);
            }
            let mut pc = lock(&self.p1_cache);
            if pc.1 == epoch {
                pc.1 = 0;
            }
        }
        self.wake_all();
    }

    // MARK: - garbage collection

    /// Reclaims versions that no active or future snapshot can observe.
    pub fn gc(&self) -> i64 {
        let Some(e) = self.engine_opt() else { return 0 };
        let st = &e.store;
        self.pub_since_gc.store(0, Relaxed);
        let oldest = self.oldest_active();
        let compacted = lock(&st.seq).compacted_epoch;
        let cand = std::mem::take(&mut lock(&st.lists).cand);
        let mut keep = Vec::new();
        let mut reclaimed = 0i64;
        for pgno in cand {
            let Ok(mut s) = st.stripes[Store::stripe_of(pgno)].try_lock() else {
                keep.push(pgno); // a committer holds it: the next collection looks at it
                continue;
            };
            let Some(c) = s.get_mut(&pgno) else { continue };
            let mut drop_n = match c.find(oldest) {
                Some(i) => i,
                None => 0,
            };
            if c.v.last().is_some_and(|l| l.epoch <= oldest && l.epoch <= compacted) {
                drop_n = c.v.len();
            }
            for x in &c.v[..drop_n] {
                if x.epoch == 0 {
                    st.base_bytes.fetch_sub(st.pgsz as i64, Relaxed);
                }
            }
            if drop_n > 0 {
                c.v.drain(..drop_n);
                st.bytes.fetch_sub(st.pgsz as i64 * drop_n as i64, Relaxed);
                st.versions.fetch_sub(drop_n as i64, Relaxed);
                reclaimed += drop_n as i64;
            }
            if c.v.is_empty() {
                s.remove(&pgno);
            } else {
                keep.push(pgno);
            }
        }
        if !keep.is_empty() {
            lock(&st.lists).cand.extend(keep);
        }
        let mut seq = lock(&st.seq);
        let si = seq.sizes.partition_point(|s| s.epoch <= oldest);
        if si > 1 {
            seq.sizes.drain(..si - 1);
        }
        drop(seq);
        st.reclaimed.fetch_add(reclaimed, Relaxed);
        st.gc_runs.fetch_add(1, Relaxed);
        reclaimed
    }

    // MARK: - compaction

    /// Writes the committed pages that no snapshot holds back into the database file (see the module doc of lib.rs).
    pub fn compact(&self) -> Result<crate::CompactResult, i32> {
        let mut out = crate::CompactResult::default();
        let Some(e) = self.engine_opt() else { return Ok(out) };
        if self.failed.load(Acquire) {
            return Err(RC_IOERR);
        }
        let st = &e.store;
        let _cg = lock(&self.compact_mu);
        let t = self.oldest_active();
        let base = e.log.base_epoch();
        if t <= base {
            return Ok(out);
        }
        // commits with synchronous < FULL are visible before they are on the disk: the log must have them first
        e.log.sync()?;
        // 1. write the newest version <= T of every page changed since the last base
        let dirty = std::mem::take(&mut lock(&st.lists).dirty);
        let mut werr = false;
        for &pgno in &dirty {
            let page = {
                let s = lock(&st.stripes[Store::stripe_of(pgno)]);
                s.get(&pgno).and_then(|c| c.find(t).filter(|&k| c.v[k].epoch > base).map(|k| c.v[k].data.to_vec()))
            };
            if let Some(page) = page {
                if fsio::pwrite(&e.real, &page, (pgno as u64 - 1) * st.pgsz as u64).is_err() {
                    werr = true;
                    break;
                }
                out.pages_written += 1;
            }
        }
        // the file size at T, from the in-header size of page 1 as of T
        let size_pages = {
            let s = lock(&st.stripes[Store::stripe_of(1)]);
            s.get(&1).and_then(|c| c.find(t).map(|k| be32(&c.v[k].data[28..]))).unwrap_or(0)
        };
        if !werr && size_pages > 0 && e.real.set_len(size_pages as u64 * st.pgsz as u64).is_err() {
            werr = true;
        }
        if !werr && fsio::fsync(&e.real).is_err() {
            werr = true;
        }
        // 2. the new base, durably
        if !werr && e.log.set_base(t).is_err() {
            werr = true;
        }
        // 3. settle the dirty list
        let mut again = Vec::new();
        let mut cand = Vec::new();
        for &pgno in &dirty {
            let mut s = lock(&st.stripes[Store::stripe_of(pgno)]);
            let Some(c) = s.get_mut(&pgno) else { continue };
            if werr || c.head() > t {
                again.push(pgno);
            } else {
                c.dirty = false;
                if !c.v.is_empty() && !c.queued {
                    c.queued = true;
                    cand.push(pgno);
                }
            }
        }
        {
            let mut l = lock(&st.lists);
            l.dirty.extend(again);
            l.cand.extend(cand);
        }
        if werr {
            return Err(RC_IOERR);
        }
        {
            let mut seq = lock(&st.seq);
            seq.compacted_epoch = t;
            if size_pages > 0 {
                seq.base_dbsize = size_pages;
            }
            let d = seq.sizes.partition_point(|s| s.epoch <= t);
            seq.sizes.drain(..d);
            e.log.drop_up_to(t, self.next_epoch.load(Acquire) == t);
        }
        out.target_epoch = t;
        out.versions_freed = self.gc();
        self.n.compactions.fetch_add(1, Relaxed);
        self.n.compacted_pages.fetch_add(out.pages_written as i64, Relaxed);
        Ok(out)
    }

    // MARK: - the background compactor

    pub fn start_compactor(self: &Arc<Self>) {
        let mut c = lock(&self.compactor);
        if c.is_some() {
            return;
        }
        let sig = Arc::new((Mutex::new(false), Condvar::new()));
        let weak = Arc::downgrade(self);
        let s2 = sig.clone();
        let h = std::thread::Builder::new()
            .name("mw-compactor".into())
            .spawn(move || loop {
                {
                    let g = lock(&s2.0);
                    let (g, _) = s2.1.wait_timeout(g, Duration::from_secs(1)).unwrap_or_else(|e| e.into_inner());
                    if *g {
                        return;
                    }
                }
                let Some(db) = weak.upgrade() else { return };
                if let Some(e) = db.engine_opt()
                    && e.log.bytes() > db.log_max_bytes.load(Relaxed) {
                        let _ = db.compact();
                    }
            })
            .expect("compactor thread");
        *c = Some((sig, h));
    }

    pub fn kick_compactor(&self) {
        if let Some((sig, _)) = lock(&self.compactor).as_ref() {
            sig.1.notify_one();
        }
    }

    fn stop_compactor(&self) {
        let c = lock(&self.compactor).take();
        if let Some((sig, h)) = c {
            *lock(&sig.0) = true;
            sig.1.notify_all();
            let _ = h.join();
        }
    }
}

pub struct Validation {
    pub snapshot_epoch: u64,
    pub check_cookie: bool,
    pub cookie: u32,
    pub read_pgnos: Vec<u32>,
    /// pages this same snapshot already committed itself (sorted), and the epoch each is validated against
    pub own_pgnos: Vec<u32>,
    pub own_epochs: Vec<u64>,
}

impl Validation {
    pub fn snap_for(&self, pgno: u32) -> u64 {
        match self.own_pgnos.binary_search(&pgno) {
            Ok(i) => self.own_epochs[i],
            Err(_) => self.snapshot_epoch,
        }
    }
}
