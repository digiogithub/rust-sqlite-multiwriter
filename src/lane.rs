//! Private write lanes. Each connection gets its own in-memory "-wal" (SQLite believes it is the only writer), its
//! own private wal-index with locks that are always granted, and a main-database file whose writes are swallowed (the
//! real file is only ever written by the compactor).
//!
//! The write set of a transaction is the set of pages of the frames SQLite appended to the private WAL up to the
//! commit frame. When the snapshot ends the private WAL is emptied and the wal-index header invalidated: SQLite's own
//! recovery then rebuilds an empty WAL and resets its page cache. Every transaction therefore starts with a cold
//! cache, which is also what makes the set of pages read through xRead a complete read set.
//!
//! A transaction is the interval during which the connection holds a WAL read mark: the first read lock begins the
//! snapshot, its release ends it.

use crate::db::{ActiveSnap, MwDb, Validation};
use crate::store::lock;
use crate::util::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::*};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const SHM_REGION: usize = 32768;
pub const SHM_MAX_REGIONS: usize = 1024;
const WARM_AFTER: u32 = 64;
pub const DEFAULT_HOT_CREDIT: i32 = 16;

static NEXT_LANE: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TxState {
    #[default]
    Idle,
    Active,
    Prepared,
    Committed,
    Aborted,
}

#[derive(Default, Clone, Copy)]
pub struct TxInfo {
    pub tx_id: u64,
    pub snapshot_epoch: u64,
    pub commit_epoch: u64,
    pub state: TxState,
    pub is_writer: bool,
}

#[derive(Default)]
pub struct MemWal {
    pub buf: Vec<u8>,
    pub size: usize,
    pub pgsz: usize,
    pub commit_seen: bool,
}

/// What the publication needs to know about the lane that publishes.
pub struct LaneCore {
    pub id: usize,
    pub noroute: bool,
    pub holds_reloc: bool,
    /// Holds the hot-spot turn (released by the publication as soon as the commit is visible, or at the end).
    pub holds_hot: bool,
}

pub struct Lane {
    pub core: LaneCore,
    pub db: Arc<MwDb>,
    pub readcheck: bool,
    pub noreloc: bool,
    pub nomerge: bool,
    pub hot_credit: i32,
    pub sync_level: i32,

    pub snap_held: bool,
    pub tx: TxInfo,
    pub write_locked: Arc<AtomicBool>,
    tried_write: bool,

    rs_bits: Vec<u8>,
    rs_list: Vec<u32>,
    reads_since: u32,
    warm: bool,
    warm_epoch: u64,

    pub wal: MemWal,
    pub wal_bound: bool,
    pub shm: Vec<Box<[u8]>>,

    pub ws_pgnos: Vec<u32>,
    ws_frame: Vec<usize>,
    pub ws_index: PageMap<usize>,
    ws_dbsize: u32,
    pub commit_base: usize,

    pub own_pg: Vec<u32>,
    own_ep: Vec<u64>,
    poisoned: bool,
    dsz: Option<(u64, u32)>,

    consec_aborts: i32,
    retry_credit: i32,
    pub ddl_active: bool,

    pub rb: crate::rebase::LaneRebase,
}

impl Lane {
    pub fn new(db: Arc<MwDb>) -> Lane {
        Lane {
            core: LaneCore { id: NEXT_LANE.fetch_add(1, Relaxed), noroute: false, holds_reloc: false, holds_hot: false },
            db,
            readcheck: true,
            noreloc: false,
            nomerge: false,
            hot_credit: DEFAULT_HOT_CREDIT,
            sync_level: 2,
            snap_held: false,
            tx: TxInfo::default(),
            write_locked: Arc::new(AtomicBool::new(false)),
            tried_write: false,
            rs_bits: Vec::new(),
            rs_list: Vec::new(),
            reads_since: 0,
            warm: false,
            warm_epoch: 0,
            wal: MemWal::default(),
            wal_bound: false,
            shm: Vec::new(),
            ws_pgnos: Vec::new(),
            ws_frame: Vec::new(),
            ws_index: PageMap::default(),
            ws_dbsize: 0,
            commit_base: 0,
            own_pg: Vec::new(),
            own_ep: Vec::new(),
            poisoned: false,
            dsz: None,
            consec_aborts: 0,
            retry_credit: 0,
            ddl_active: false,
            rb: Default::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.core.id
    }

    fn shm0_clear(&mut self) {
        if let Some(r) = self.shm.first_mut() {
            r[..96].fill(0);
        }
    }

    // MARK: - snapshots

    pub fn snapshot(&self) -> u64 {
        if self.snap_held { self.tx.snapshot_epoch } else { self.db.visible_epoch() }
    }

    pub fn snapshot_begin(&mut self) {
        if self.snap_held {
            return;
        }
        let db = self.db.clone();
        // hot spots: a connection that keeps losing takes a per-database turn before its snapshot (20 ms at most)
        if self.retry_credit > 0 && !self.core.holds_hot {
            for _ in 0..400 {
                if db.hot_held.compare_exchange(0, 1, AcqRel, Acquire).is_ok() {
                    self.core.holds_hot = true;
                    db.n.hot_serialised.fetch_add(1, Relaxed);
                    break;
                }
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        // the epoch is read under the registry lock and the lane registered before it is released: the GC relies on it
        {
            let mut a = lock(&db.active);
            self.tx.snapshot_epoch = db.visible_epoch();
            a.insert(self.id(), ActiveSnap { epoch: self.tx.snapshot_epoch, write_locked: self.write_locked.clone() });
            self.snap_held = true;
        }
        self.tx.tx_id += 1;
        self.tx.commit_epoch = 0;
        self.tx.state = TxState::Active;
        self.tx.is_writer = false;
        self.tried_write = false;
        self.reads_since = self.reads_since.saturating_add(1);
        if self.warm && self.warm_epoch == self.tx.snapshot_epoch {
            self.warm = false; // the cached pages are right and the read set stays
        } else {
            if self.warm {
                self.shm0_clear(); // another epoch: SQLite resets its cache when it looks at the header again
            }
            self.warm = false;
            for &p in &self.rs_list {
                self.rs_bits[p as usize] = 0;
            }
            self.rs_list.clear();
        }
        self.ws_pgnos.clear();
        self.dsz = None;
        db.n.snapshots.fetch_add(1, Relaxed);
    }

    pub fn snapshot_end(&mut self) {
        if !self.snap_held {
            return;
        }
        {
            let mut a = lock(&self.db.active);
            a.remove(&self.id());
            self.snap_held = false;
            if self.core.holds_hot {
                self.core.holds_hot = false;
                self.db.hot_held.store(0, Release);
            }
            if !self.tx.is_writer && self.retry_credit > 0 {
                self.retry_credit -= 1;
            }
        }
        if self.ddl_active {
            self.ddl_end();
        }
        if self.tx.state == TxState::Active {
            self.tx.state = if self.tx.is_writer { TxState::Aborted } else { TxState::Committed };
        }
    }

    /// Drops the private WAL and invalidates the wal-index header (all zeros: SQLite recovers the empty WAL and resets
    /// its page cache).
    pub fn reset(&mut self, keep_header: bool) {
        self.commit_base = 0;
        self.own_pg.clear();
        self.own_ep.clear();
        self.poisoned = false;
        self.wal.size = 0;
        self.wal.commit_seen = false;
        self.wal.pgsz = 0;
        if !keep_header {
            self.shm0_clear();
        }
    }

    /// Drives the lane from the locks SQLite takes in its (private) wal-index: slot 0 = the write lock, 3.. = read
    /// marks; a shared read mark is the snapshot.
    pub fn on_shm_lock(&mut self, ofst: i32, n: i32, flags: i32) -> i32 {
        const UNLOCK: i32 = 1;
        const SHARED: i32 = 4;
        const EXCLUSIVE: i32 = 8;
        if n != 1 {
            return RC_OK;
        }
        let lock_it = flags & UNLOCK == 0;
        if ofst >= 3 && flags & SHARED != 0 {
            if lock_it {
                self.snapshot_begin();
            } else if !self.write_locked.load(Acquire) {
                let warm = !self.tx.is_writer && self.reads_since >= WARM_AFTER && !self.tried_write && self.ws_pgnos.is_empty() && self.tx.state == TxState::Active;
                let ep = self.tx.snapshot_epoch;
                self.snapshot_end();
                self.reset(warm);
                self.warm = warm;
                self.warm_epoch = ep;
            }
        } else if ofst == 0 && flags & EXCLUSIVE != 0 && self.snap_held {
            if lock_it {
                self.tried_write = true;
                self.reads_since = 0;
                // a snapshot older than the newest schema change can never publish: fail fast (retryable)
                if self.tx.snapshot_epoch < self.db.last_schema_epoch.load(Acquire) && !self.ddl_active {
                    return RC_BUSY_SNAPSHOT;
                }
                // the schema barrier: no new write transaction while a schema change runs
                let o = self.db.ddl_owner.load(Acquire);
                if o != 0 && o != self.id() {
                    return RC_BUSY;
                }
                self.write_locked.store(true, Release);
                self.tx.is_writer = true;
            } else if self.write_locked.load(Acquire) {
                self.write_locked.store(false, Release);
            }
        }
        RC_OK
    }

    // MARK: - the schema barrier

    pub fn ddl_begin(&mut self) {
        if self.ddl_active {
            return;
        }
        let db = self.db.clone();
        let deadline = Instant::now() + Duration::from_secs(2);
        while db.ddl_owner.compare_exchange(0, self.id(), AcqRel, Acquire).is_err() {
            if Instant::now() > deadline {
                return; // give up waiting: the validation still protects us
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        self.ddl_active = true;
        while Instant::now() < deadline {
            let writers = lock(&db.active).iter().filter(|(id, s)| **id != self.id() && s.write_locked.load(Acquire)).count();
            if writers == 0 && db.next_epoch.load(Acquire) == db.epoch.load(Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
    }

    pub fn ddl_end(&mut self) {
        self.ddl_active = false;
        let _ = self.db.ddl_owner.compare_exchange(self.id(), 0, AcqRel, Acquire);
    }

    // MARK: - the main database file

    fn rs_mark(&mut self, pgno: u32) {
        let p = pgno as usize;
        if p >= self.rs_bits.len() {
            let mut n = self.rs_bits.len().max(4096);
            while n <= p {
                n *= 2;
            }
            self.rs_bits.resize(n, 0);
        }
        if self.rs_bits[p] == 0 {
            self.rs_bits[p] = 1;
            self.rs_list.push(pgno);
        }
    }

    /// Reads are served page by page: the newest committed version <= the snapshot, else the real file.
    pub fn main_read(&mut self, mut dst: &mut [u8], mut off: u64) -> i32 {
        let db = self.db.clone();
        let e = db.engine();
        let st = &e.store;
        let snap = self.snapshot();
        let pgsz = st.pgsz as u64;
        while !dst.is_empty() {
            let pgno = (off / pgsz) as u32 + 1;
            let poff = (off % pgsz) as usize;
            let take = (pgsz as usize - poff).min(dst.len());
            if self.readcheck && self.snap_held && pgno != 1 {
                self.rs_mark(pgno);
            }
            let (head, rest) = std::mem::take(&mut dst).split_at_mut(take);
            if !st.read(pgno, snap, poff, head) {
                match crate::fsio::pread(&e.real, head, off) {
                    Ok(n) if n == take => {
                        if poff == 0 && take == pgsz as usize {
                            st.cache_base(pgno, head);
                        }
                    }
                    Ok(n) => {
                        head[n..].fill(0);
                        rest.fill(0);
                        return RC_IOERR_SHORT_READ;
                    }
                    Err(_) => return RC_IOERR | (1 << 8),
                }
            }
            dst = rest;
            off += take as u64;
        }
        RC_OK
    }

    pub fn dbsize_at(&mut self, snap: u64) -> u32 {
        if let Some((e, v)) = self.dsz
            && e == snap {
                return v;
            }
        let v = self.db.engine().store.dbsize(snap);
        self.dsz = Some((snap, v));
        v
    }

    pub fn main_file_size(&mut self) -> i64 {
        let s = self.snapshot();
        self.dbsize_at(s) as i64 * self.db.engine().store.pgsz as i64
    }

    // MARK: - the private WAL

    pub fn wal_read(&self, dst: &mut [u8], off: u64) -> i32 {
        let w = &self.wal;
        let off = off as usize;
        if off >= w.size {
            dst.fill(0);
            return RC_IOERR_SHORT_READ;
        }
        let n = dst.len().min(w.size - off);
        dst[..n].copy_from_slice(&w.buf[off..off + n]);
        if n < dst.len() {
            dst[n..].fill(0);
            return RC_IOERR_SHORT_READ;
        }
        RC_OK
    }

    pub fn wal_write(&mut self, src: &[u8], off: u64) -> i32 {
        let off = off as usize;
        let end = off + src.len();
        let w = &mut self.wal;
        if end > w.buf.len() {
            let mut c = w.buf.len().max(65536);
            while c < end {
                c *= 2;
            }
            w.buf.resize(c, 0);
        }
        w.buf[off..end].copy_from_slice(src);
        w.size = w.size.max(end);
        let n = src.len();
        // a 32-byte header, then frames of (24-byte header + page); the page write ends a frame
        if off == 0 && n == 32 {
            w.pgsz = be32(&w.buf[8..]) as usize;
            return RC_OK;
        }
        if n == 24 {
            return RC_OK;
        }
        if w.commit_seen && w.pgsz != 0 && n < w.pgsz {
            return RC_OK; // padding after the commit frame
        }
        if w.pgsz == 0 || n != w.pgsz || off < 32 || (off - 32) % (w.pgsz + 24) != 24 {
            return RC_IOERR_WRITE; // unexpected layout: fail loudly
        }
        let k = (off - 32) / (w.pgsz + 24);
        let ntrunc = be32(&w.buf[32 + k * (w.pgsz + 24) + 4..]);
        if ntrunc != 0 {
            return self.commit_frame(k, ntrunc);
        }
        RC_OK
    }

    pub fn wal_truncate(&mut self, size: u64) {
        self.wal.size = self.wal.size.min(size as usize);
    }

    fn commit_frame(&mut self, k: usize, dbsize: u32) -> i32 {
        let fs = self.wal.pgsz + 24;
        self.ws_index.clear();
        self.ws_pgnos.clear();
        self.ws_frame.clear();
        self.ws_dbsize = dbsize;
        if k < self.commit_base {
            self.commit_base = 0;
        }
        for i in self.commit_base..=k {
            let o = 32 + i * fs;
            if o + fs > self.wal.size {
                return RC_IOERR;
            }
            let pg = be32(&self.wal.buf[o..]);
            if let Some(&j) = self.ws_index.get(&pg) {
                self.ws_frame[j] = i;
                continue;
            }
            self.ws_index.insert(pg, self.ws_pgnos.len());
            self.ws_pgnos.push(pg);
            self.ws_frame.push(i);
        }
        self.wal.commit_seen = true;
        self.tx.state = TxState::Prepared;
        let rc = self.publish();
        if rc == RC_OK {
            // the snapshot may outlive this commit: the next commit starts after these frames
            self.commit_base = k + 1;
            self.dsz = Some((self.tx.snapshot_epoch, dbsize));
            let ce = self.tx.commit_epoch;
            let mut t: Vec<(u32, u64)> = self.own_pg.iter().copied().zip(self.own_ep.iter().copied()).collect();
            t.extend(self.ws_pgnos.iter().map(|&p| (p, ce)));
            t.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            t.dedup_by_key(|x| x.0);
            self.own_pg = t.iter().map(|x| x.0).collect();
            self.own_ep = t.iter().map(|x| x.1).collect();
        }
        rc
    }

    pub fn schema_cookie(&self) -> u32 {
        let mut c = [0u8; 4];
        let e = self.db.engine();
        if !e.store.read(1, self.tx.snapshot_epoch, 40, &mut c) {
            let _ = crate::fsio::pread(&e.real, &mut c, 40);
        }
        be32(&c)
    }

    /// The image of a page of the write set (in the private WAL).
    pub fn ws_image(&self, i: usize) -> &[u8] {
        let fs = self.wal.pgsz + 24;
        let o = 32 + self.ws_frame[i] * fs + 24;
        &self.wal.buf[o..o + self.wal.pgsz]
    }

    /// Publishes the captured write set as one atomic commit at a new epoch.
    fn publish(&mut self) -> i32 {
        let db = self.db.clone();
        if !self.snap_held {
            return RC_IOERR; // a commit without a snapshot: a VFS stacked above does not forward the wal-index locks
        }
        if self.poisoned && !self.own_pg.is_empty() {
            self.tx.state = TxState::Aborted;
            db.n.aborts.fetch_add(1, Relaxed);
            return RC_BUSY_SNAPSHOT;
        }
        let snap_size = self.dbsize_at(self.tx.snapshot_epoch);
        let cookie = self.schema_cookie();
        let ro: Vec<u32> = if self.readcheck {
            self.rs_list.iter().copied().filter(|p| !self.ws_index.contains_key(p)).collect()
        } else {
            Vec::new()
        };
        let v = Validation {
            snapshot_epoch: self.tx.snapshot_epoch,
            check_cookie: true,
            cookie,
            read_pgnos: ro,
            own_pgnos: self.own_pg.clone(),
            own_epochs: self.own_ep.clone(),
        };
        let sync = self.sync_level >= 2;
        let pgnos = self.ws_pgnos.clone();
        let imgs: Vec<Vec<u8>> = (0..pgnos.len()).map(|i| self.ws_image(i).to_vec()).collect();
        let img_refs: Vec<&[u8]> = imgs.iter().map(|x| x.as_slice()).collect();
        let pg1 = self.ws_index.get(&1).map(|&i| imgs[i].clone());
        let mut res = db.publish(Some(&mut self.core), Some(&v), &pgnos, &img_refs, self.ws_dbsize, snap_size, sync);
        if res == Err(ERR_CONFLICT) && !self.noreloc {
            // was the conflict only that other commits extended the file? then the new pages are renumbered
            match self.relocate(&v, &pgnos, &img_refs, self.ws_dbsize, snap_size, sync) {
                Ok(e) => {
                    res = Ok(e);
                    self.poisoned = true;
                }
                Err(ERR_RELOC_NA) => {
                    if self.can_rebase(pg1.as_deref(), cookie) {
                        // a physical conflict only on pages that it wrote: replay its row changes
                        match self.rebase(&img_refs, cookie) {
                            Ok(e) => {
                                res = Ok(e);
                                self.rb.streak = 0;
                                self.poisoned = true;
                            }
                            Err(rc) => {
                                self.rebase_refused();
                                if rc != ERR_CONFLICT {
                                    res = Err(rc);
                                }
                            }
                        }
                    }
                }
                Err(rc) => res = Err(rc),
            }
        }
        match res {
            Ok(epoch) => {
                self.tx.commit_epoch = epoch;
                self.tx.state = TxState::Committed;
                self.consec_aborts = 0;
                if self.retry_credit > 0 {
                    self.retry_credit -= 1;
                }
                if pg1.is_some_and(|p| be32(&p[40..]) != cookie) {
                    db.last_schema_epoch.store(epoch, Release);
                    db.schema_gen.fetch_add(1, AcqRel);
                }
                RC_OK
            }
            Err(rc) if rc == ERR_CONFLICT || rc == ERR_CONFLICT_SCHEMA || rc == ERR_CONFLICT_READ => {
                self.tx.state = TxState::Aborted;
                db.n.aborts.fetch_add(1, Relaxed);
                self.consec_aborts += 1;
                if rc == ERR_CONFLICT && self.consec_aborts >= 16 && !self.rb.norebase {
                    self.retry_credit = self.hot_credit;
                }
                RC_BUSY_SNAPSHOT
            }
            Err(rc) => {
                self.tx.state = TxState::Aborted;
                db.n.aborts.fetch_add(1, Relaxed);
                rc
            }
        }
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        self.snapshot_end();
        if self.ddl_active {
            self.ddl_end();
        }
    }
}
