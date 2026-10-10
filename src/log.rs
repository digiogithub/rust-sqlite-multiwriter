//! The durable commit log. Committed state = the database file + the committed page versions; the versions live in
//! memory, so every commit is first appended here as one checksummed record.
//!
//! Files: "<db>-mw" (the meta header: format, page size, salt, compacted base epoch; it is also the lock that makes one
//! process the owner of the database) and the segments "<db>-mw.000001", ... A segment is written up to SEG_BYTES and
//! then the log rolls to the next one; compaction deletes the segments whose records are all in the database file.
//!
//! ```text
//! meta     (64 B): magic "MWRSLOG1", version, page size, base epoch, salt, checksum
//! segment  (32 B header): magic "MWRSSEG1", id, page size, salt, checksum
//! record   (40 B header + npages * (4 + pgsz)): magic, npages, epoch, dbsize, pgsz, 0, 0, checksum
//! ```
//!
//! Durability: the epoch and the place of a record are assigned together (epoch order == log order); the record is
//! written outside any lock; with synchronous=FULL the committer waits for an fsync that began after its write (group
//! commit); only then does the epoch become visible, after the previous one. Recovery replays the longest valid,
//! epoch-contiguous prefix of the records newer than the base and cuts the rest.

use crate::fsio;
use crate::store::{lock, Store};
use crate::util::*;
use std::fs::{File, OpenOptions};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::*};
use std::sync::{Arc, Condvar, Mutex};

const META_MAGIC: &[u8; 8] = b"MWRSLOG1";
const SEG_MAGIC: &[u8; 8] = b"MWRSSEG1";
const FORMAT: u32 = 1;
const META_SIZE: usize = 64;
pub const SEG_HDR: u64 = 32;
const REC_HDR: usize = 40;
const REC_MAGIC: u32 = 0x5231574d; // "MW1R"
const SEG_BYTES: u64 = 64 << 20;
const MIN_ROLL: u64 = 1 << 20;
const PREFILL_CHUNK: u64 = 2 << 20;
const SEED: u64 = 1469598103934665603;

/// A 64-bit word-at-a-time multiply/xor mixer: only has to detect torn or stale records.
pub fn mix64(mut h: u64, p: &[u8]) -> u64 {
    let mut c = p.chunks_exact(8);
    for w in &mut c {
        h ^= u64::from_le_bytes(w.try_into().unwrap());
        h = h.wrapping_mul(0x9E3779B97F4A7C15);
        h ^= h >> 32;
    }
    for &b in c.remainder() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

fn rec_checksum(salt: u64, hdr: &[u8], body: &[u8]) -> u64 {
    let mut h = [0u8; REC_HDR];
    h.copy_from_slice(&hdr[..REC_HDR]);
    h[32..40].fill(0);
    mix64(mix64(salt ^ SEED, &h), body)
}

pub struct Segment {
    pub id: u32,
    pub f: File,
    dirty: AtomicBool,
    /// The file is written ahead of its end with zeros: a record then overwrites blocks that exist, and its fsync does
    /// not have to commit a new size of the file. Records are only placed below `ready`; the prefiller writes above it.
    ready: AtomicU64,
    fill: Mutex<()>,
    filling: AtomicBool,
}

impl Segment {
    fn prefill(&self, n: u64) -> std::io::Result<()> {
        static ZERO: [u8; 256 << 10] = [0; 256 << 10];
        let from = self.ready.load(Acquire);
        let mut off = 0;
        while off < n {
            let k = (n - off).min(ZERO.len() as u64) as usize;
            fsio::pwrite(&self.f, &ZERO[..k], from + off)?;
            off += k as u64;
        }
        fsio::fsync(&self.f)?;
        self.ready.store(from + n, Release);
        Ok(())
    }
}

/// Where the next record goes (held under the store's seq lock and this mutex).
struct Cursor {
    segs: Vec<Arc<Segment>>,
    max_epoch: Vec<u64>,
    end: Vec<u64>,
    roll: bool,
}

pub struct Log {
    path: String,
    pgsz: usize,
    salt: u64,
    meta: File,
    base: AtomicU64,
    cur: Mutex<Cursor>,
    total: AtomicI64,
    sync: Mutex<SyncState>,
    sync_cv: Condvar,
    pub n_syncs: AtomicI64,
}

#[derive(Default)]
struct SyncState {
    started: u64,
    done: u64,
    running: bool,
}

pub struct Place {
    seg: Arc<Segment>,
    off: u64,
}

fn seg_path(base: &str, id: u32) -> String {
    format!("{base}.{id:06}")
}

fn list_segments(base: &str) -> Vec<u32> {
    let p = std::path::Path::new(base);
    let dir = p.parent().filter(|d| !d.as_os_str().is_empty()).map(|d| d.to_path_buf()).unwrap_or_else(|| ".".into());
    let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let mut ids = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if let Some(suf) = n.strip_prefix(&name).and_then(|s| s.strip_prefix('.'))
                && suf.len() == 6 && suf.bytes().all(|b| b.is_ascii_digit())
                    && let Ok(v) = suf.parse::<u32>()
                        && v > 0 {
                            ids.push(v);
                        }
        }
    }
    ids.sort_unstable();
    ids
}

fn encode_meta(pgsz: usize, base: u64, salt: u64) -> [u8; META_SIZE] {
    let mut b = [0u8; META_SIZE];
    b[..8].copy_from_slice(META_MAGIC);
    b[8..12].copy_from_slice(&FORMAT.to_le_bytes());
    b[12..16].copy_from_slice(&(pgsz as u32).to_le_bytes());
    b[16..24].copy_from_slice(&base.to_le_bytes());
    b[24..32].copy_from_slice(&salt.to_le_bytes());
    let c = mix64(SEED, &b[..56]);
    b[56..64].copy_from_slice(&c.to_le_bytes());
    b
}

fn le32(b: &[u8]) -> u32 {
    u32::from_le_bytes(b[..4].try_into().unwrap())
}
fn le64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

fn random_salt() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos()));
    h.write_u32(std::process::id());
    h.finish()
}

impl Log {
    /// Opens (or creates) the log of the database at db_path and installs the recovered commits in st. Returns the
    /// log and the epoch of the last recovered commit (the base when there is none).
    pub fn open(db_path: &str, pgsz: usize, st: &Store) -> Result<(Log, u64), i32> {
        let path = format!("{db_path}-mw");
        let meta = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path).map_err(|_| RC_CANTOPEN)?;
        if meta.try_lock().is_err() {
            return Err(RC_BUSY); // another process owns this database
        }
        let size = meta.metadata().map_err(|_| RC_IOERR)?.len();
        let ids = list_segments(&path);
        let mut hdr = [0u8; META_SIZE];
        let mut valid = false;
        if size >= META_SIZE as u64 && fsio::pread(&meta, &mut hdr, 0).map_err(|_| RC_IOERR)? == META_SIZE {
            if hdr[..5] == META_MAGIC[..5] && &hdr[..8] != META_MAGIC {
                return Err(RC_CANTOPEN); // another format: never read, never dropped
            }
            if &hdr[..8] == META_MAGIC && le32(&hdr[8..]) != FORMAT {
                return Err(RC_CANTOPEN);
            }
            valid = &hdr[..8] == META_MAGIC && le64(&hdr[56..]) == mix64(SEED, &hdr[..56]) && le32(&hdr[12..]) as usize == pgsz;
        }
        let mut lg = Log {
            path: path.clone(),
            pgsz,
            salt: 0,
            meta,
            base: AtomicU64::new(1),
            cur: Mutex::new(Cursor { segs: Vec::new(), max_epoch: Vec::new(), end: Vec::new(), roll: false }),
            total: AtomicI64::new(0),
            sync: Mutex::new(SyncState::default()),
            sync_cv: Condvar::new(),
            n_syncs: AtomicI64::new(0),
        };
        if !valid {
            // a log whose header does not check out but with commits behind it is not a new log
            for &id in &ids {
                if std::fs::metadata(seg_path(&path, id)).map_or(0, |m| m.len()) > SEG_HDR {
                    return Err(RC_CORRUPT);
                }
            }
            for &id in &ids {
                let _ = std::fs::remove_file(seg_path(&path, id));
            }
            lg.salt = random_salt();
            lg.meta.set_len(0).map_err(|_| RC_IOERR)?;
            lg.write_meta(1)?;
            fsio::sync_dir(&path);
            lg.new_segment(&mut lock(&lg.cur), 1)?;
            return Ok((lg, 1));
        }
        let base = le64(&hdr[16..]);
        lg.salt = le64(&hdr[24..]);
        lg.base.store(base, Relaxed);
        let last = lg.replay(&ids, base, st)?;
        let mut cur = lock(&lg.cur);
        if cur.segs.is_empty() {
            let next = ids.last().map_or(1, |x| x + 1);
            lg.new_segment(&mut cur, next)?;
        }
        drop(cur);
        Ok((lg, last))
    }

    fn replay(&self, ids: &[u32], base: u64, st: &Store) -> Result<u64, i32> {
        let mut last = base;
        let mut prev = 0u64;
        let mut stopped = false;
        let mut cur = lock(&self.cur);
        for &id in ids {
            let p = seg_path(&self.path, id);
            if stopped {
                let _ = std::fs::remove_file(&p); // beyond a hole: never acknowledged
                continue;
            }
            let f = OpenOptions::new().read(true).write(true).open(&p).map_err(|_| RC_IOERR)?;
            let size = f.metadata().map_err(|_| RC_IOERR)?.len();
            if size < SEG_HDR {
                drop(f);
                let _ = std::fs::remove_file(&p);
                stopped = true;
                continue;
            }
            let mut h = [0u8; SEG_HDR as usize];
            fsio::pread(&f, &mut h, 0).map_err(|_| RC_IOERR)?;
            if &h[..8] != SEG_MAGIC || le32(&h[8..]) != id || le32(&h[12..]) as usize != self.pgsz || le64(&h[16..]) != self.salt
                || le64(&h[24..]) != mix64(SEED, &h[..24])
            {
                if size > SEG_HDR {
                    return Err(RC_CORRUPT);
                }
                drop(f);
                let _ = std::fs::remove_file(&p);
                stopped = true;
                continue;
            }
            let mut off = SEG_HDR;
            let mut max_epoch = 0;
            let mut rh = [0u8; REC_HDR];
            let mut body = Vec::new();
            loop {
                if off + REC_HDR as u64 > size {
                    break;
                }
                fsio::pread(&f, &mut rh, off).map_err(|_| RC_IOERR)?;
                let npages = le32(&rh[4..]);
                let epoch = le64(&rh[8..]);
                let dbsize = le32(&rh[16..]);
                if le32(&rh) != REC_MAGIC || le32(&rh[20..]) as usize != self.pgsz || npages == 0 || npages > 1 << 24 {
                    break;
                }
                let blen = npages as u64 * (4 + self.pgsz as u64);
                if off + REC_HDR as u64 + blen > size {
                    break;
                }
                body.resize(blen as usize, 0);
                fsio::pread(&f, &mut body, off + REC_HDR as u64).map_err(|_| RC_IOERR)?;
                if le64(&rh[32..]) != rec_checksum(self.salt, &rh, &body) {
                    break;
                }
                if prev != 0 && epoch != prev + 1 {
                    break;
                }
                if epoch > base {
                    if epoch != last + 1 {
                        break;
                    }
                    let mut pgnos = Vec::with_capacity(npages as usize);
                    let mut imgs = Vec::with_capacity(npages as usize);
                    for e in body.chunks_exact(4 + self.pgsz) {
                        pgnos.push(le32(e));
                        imgs.push(&e[4..]);
                    }
                    st.install_recovered(epoch, dbsize, &pgnos, &imgs);
                    last = epoch;
                }
                prev = epoch;
                max_epoch = max_epoch.max(epoch);
                off += REC_HDR as u64 + blen;
            }
            if off < size {
                // the torn tail is cut durably: a record of it that survived a power failure could otherwise rejoin
                // the log behind new commits that reuse its epochs
                f.set_len(off).map_err(|_| RC_IOERR)?;
                fsio::fsync(&f).map_err(|_| RC_IOERR_FSYNC)?;
                stopped = true;
            }
            let seg = Arc::new(Segment { id, f, dirty: AtomicBool::new(false), ready: AtomicU64::new(off), fill: Mutex::new(()), filling: AtomicBool::new(false) });
            cur.segs.push(seg);
            cur.max_epoch.push(max_epoch);
            cur.end.push(off);
            self.total.fetch_add(off as i64, Relaxed);
        }
        Ok(last)
    }

    fn write_meta(&self, base: u64) -> Result<(), i32> {
        fsio::pwrite(&self.meta, &encode_meta(self.pgsz, base, self.salt), 0).map_err(|_| RC_IOERR_WRITE)?;
        fsio::fsync(&self.meta).map_err(|_| RC_IOERR_FSYNC)
    }

    fn new_segment(&self, cur: &mut Cursor, id: u32) -> Result<(), i32> {
        let p = seg_path(&self.path, id);
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&p).map_err(|_| RC_CANTOPEN)?;
        let mut h = [0u8; SEG_HDR as usize];
        h[..8].copy_from_slice(SEG_MAGIC);
        h[8..12].copy_from_slice(&id.to_le_bytes());
        h[12..16].copy_from_slice(&(self.pgsz as u32).to_le_bytes());
        h[16..24].copy_from_slice(&self.salt.to_le_bytes());
        let c = mix64(SEED, &h[..24]);
        h[24..32].copy_from_slice(&c.to_le_bytes());
        if fsio::pwrite(&f, &h, 0).is_err() || fsio::fsync(&f).is_err() {
            let _ = std::fs::remove_file(&p);
            return Err(RC_IOERR_WRITE);
        }
        fsio::sync_dir(&p);
        cur.segs.push(Arc::new(Segment { id, f, dirty: AtomicBool::new(false), ready: AtomicU64::new(SEG_HDR), fill: Mutex::new(()), filling: AtomicBool::new(false) }));
        cur.max_epoch.push(0);
        cur.end.push(SEG_HDR);
        self.total.fetch_add(SEG_HDR as i64, Relaxed);
        Ok(())
    }

    pub fn record_size(&self, n: usize) -> u64 {
        REC_HDR as u64 + n as u64 * (4 + self.pgsz as u64)
    }

    /// Reserves the place of the record of epoch (the store's seq lock held).
    pub fn assign(&self, epoch: u64, n: usize) -> Result<Place, i32> {
        let size = self.record_size(n);
        let mut cur = lock(&self.cur);
        let k = cur.segs.len() - 1;
        if cur.roll || (cur.end[k] + size > SEG_BYTES && cur.end[k] > SEG_HDR) {
            let id = cur.segs[k].id + 1;
            self.new_segment(&mut cur, id)?;
            cur.roll = false;
        }
        let k = cur.segs.len() - 1;
        let seg = cur.segs[k].clone();
        let off = cur.end[k];
        if off + size > seg.ready.load(Acquire) {
            // the prefiller is behind: wait for it, then fill what is still missing here
            let _g = lock(&seg.fill);
            let ready = seg.ready.load(Acquire);
            if off + size > ready {
                seg.prefill((off + size - ready).max(PREFILL_CHUNK)).map_err(|_| RC_IOERR_WRITE)?;
            }
        }
        cur.end[k] += size;
        if seg.ready.load(Acquire).saturating_sub(cur.end[k]) < PREFILL_CHUNK / 2 && !seg.filling.swap(true, AcqRel) {
            let s = seg.clone();
            std::thread::spawn(move || {
                {
                    let _g = lock(&s.fill);
                    let _ = s.prefill(PREFILL_CHUNK);
                }
                s.filling.store(false, Release);
            });
        }
        cur.max_epoch[k] = cur.max_epoch[k].max(epoch);
        self.total.fetch_add(size as i64, Relaxed);
        Ok(Place { seg, off })
    }

    /// Gives the place back if nothing was assigned after it (the store's seq lock held).
    pub fn unassign(&self, p: &Place, n: usize) -> bool {
        let size = self.record_size(n);
        let mut cur = lock(&self.cur);
        let k = cur.segs.len() - 1;
        if !Arc::ptr_eq(&cur.segs[k], &p.seg) || cur.end[k] != p.off + size {
            return false;
        }
        cur.end[k] = p.off;
        self.total.fetch_sub(size as i64, Relaxed);
        true
    }

    /// Writes the record of a commit at its place.
    pub fn write(&self, p: &Place, epoch: u64, dbsize: u32, pgnos: &[u32], imgs: &[&[u8]]) -> Result<(), i32> {
        let n = pgnos.len();
        let mut buf = vec![0u8; self.record_size(n) as usize];
        buf[0..4].copy_from_slice(&REC_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&(n as u32).to_le_bytes());
        buf[8..16].copy_from_slice(&epoch.to_le_bytes());
        buf[16..20].copy_from_slice(&dbsize.to_le_bytes());
        buf[20..24].copy_from_slice(&(self.pgsz as u32).to_le_bytes());
        {
            let body = &mut buf[REC_HDR..];
            for (i, e) in body.chunks_exact_mut(4 + self.pgsz).enumerate() {
                e[..4].copy_from_slice(&pgnos[i].to_le_bytes());
                e[4..].copy_from_slice(imgs[i]);
            }
        }
        let c = rec_checksum(self.salt, &buf[..REC_HDR], &buf[REC_HDR..]);
        buf[32..40].copy_from_slice(&c.to_le_bytes());
        fsio::pwrite(&p.seg.f, &buf, p.off).map_err(|_| RC_IOERR_WRITE)?;
        p.seg.dirty.store(true, Release);
        Ok(())
    }

    /// Returns when an fsync that began after the caller's write has completed (group commit).
    pub fn sync(&self) -> Result<(), i32> {
        let mut s = lock(&self.sync);
        let need = s.started + 1;
        while s.done < need {
            if s.running {
                s = self.sync_cv.wait(s).unwrap_or_else(|e| e.into_inner());
                continue;
            }
            s.running = true;
            s.started += 1;
            let mine = s.started;
            let segs: Vec<Arc<Segment>> = lock(&self.cur).segs.iter().filter(|x| x.dirty.swap(false, AcqRel)).cloned().collect();
            drop(s);
            let mut err = false;
            for x in &segs {
                if fsio::datasync(&x.f).is_err() {
                    err = true;
                }
            }
            s = lock(&self.sync);
            s.running = false;
            self.sync_cv.notify_all();
            if err {
                return Err(RC_IOERR_FSYNC);
            }
            s.done = mine;
            self.n_syncs.fetch_add(1, Relaxed);
        }
        Ok(())
    }

    pub fn base_epoch(&self) -> u64 {
        self.base.load(Acquire)
    }
    pub fn bytes(&self) -> i64 {
        self.total.load(Relaxed)
    }

    /// Durably records that the state up to base is in the database file.
    pub fn set_base(&self, base: u64) -> Result<(), i32> {
        self.write_meta(base)?;
        self.base.store(base, Release);
        Ok(())
    }

    /// Removes the segments whose records are all <= base (the store's seq lock held). The current one starts over
    /// when nothing was assigned after base, or is rolled over.
    pub fn drop_up_to(&self, base: u64, quiescent: bool) {
        let mut cur = lock(&self.cur);
        let n = cur.segs.len();
        let mut i = 0;
        let mut k = 0;
        while k + 1 < n {
            if cur.max_epoch[i] <= base {
                let s = cur.segs.remove(i);
                cur.max_epoch.remove(i);
                let end = cur.end.remove(i);
                let _ = std::fs::remove_file(seg_path(&self.path, s.id));
                self.total.fetch_sub(end as i64, Relaxed);
            } else {
                i += 1;
            }
            k += 1;
        }
        let k = cur.segs.len() - 1;
        if cur.max_epoch[k] <= base && cur.end[k] > SEG_HDR {
            if quiescent {
                // the records stay in the file as garbage below `ready`: their epochs are <= base, and the recovery stops
                // at the first record that does not continue the log
                self.total.fetch_sub((cur.end[k] - SEG_HDR) as i64, Relaxed);
                cur.end[k] = SEG_HDR;
                cur.max_epoch[k] = 0;
            } else if cur.end[k] > MIN_ROLL {
                cur.roll = true;
            }
        }
    }

    /// Closes the log; remove deletes its files (everything is in the database file).
    pub fn close(&self, remove: bool) {
        let mut cur = lock(&self.cur);
        if remove {
            for s in &cur.segs {
                let _ = std::fs::remove_file(seg_path(&self.path, s.id));
            }
            let _ = std::fs::remove_file(&self.path);
        }
        cur.segs.clear();
        let _ = self.meta.unlock();
    }
}
