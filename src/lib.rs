//! # sqlite-multiwriter
//!
//! Many concurrent writers on **one** SQLite database. A Rust port of the engine of
//! [sqliteai/sqlite-multiwriter](https://github.com/sqliteai/sqlite-multiwriter): a VFS, so the SQL, the schema and the
//! database file stay the same, and the file is an ordinary SQLite database once the last connection closes.
//!
//! Each connection writes into a private in-memory WAL on a snapshot; at commit its pages are validated against the
//! commits published meanwhile (first committer wins per page, plus validation of the pages it read), published at a
//! new epoch, made durable in a log with group commit, and compacted into the database file in the background.
//!
//! ```no_run
//! use rusqlite::{Connection, OpenFlags};
//! sqlite_multiwriter::register().unwrap();
//! let uri = sqlite_multiwriter::uri("app.db", &sqlite_multiwriter::Options::default());
//! let conn = Connection::open_with_flags(&uri, OpenFlags::default() | OpenFlags::SQLITE_OPEN_URI).unwrap();
//! ```
//!
//! A commit that loses against a concurrent one fails with `SQLITE_BUSY_SNAPSHOT`: run the whole transaction again
//! (`run_tx` does it).

mod btree;
mod capi;
mod catalog;
mod db;
mod fsio;
mod hook;
mod lane;
mod log;
mod rebase;
mod reloc;
mod rowdiff;
mod store;
mod util;
mod vfs;

use std::sync::atomic::Ordering::Relaxed;

pub use vfs::VFS_NAME;

/// SQLITE_BUSY_SNAPSHOT: the commit lost against a concurrent one; run the whole transaction again.
pub const BUSY_SNAPSHOT: i32 = util::RC_BUSY_SNAPSHOT;

/// Registers the VFS "multiwriter" in the SQLite of `libsqlite3-sys` (idempotent).
pub fn register() -> Result<(), i32> {
    match vfs::register() {
        0 => Ok(()),
        rc => Err(rc),
    }
}

/// Options of a database opened with the engine.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// Replay a commit that lost only on shared pages at the latest state instead of refusing it (mw_rebase=1).
    pub rebase: bool,
    /// Do not validate the pages a transaction read: fewer retries, but write skew becomes possible.
    pub disable_read_check: bool,
    /// Make a commit visible to other transactions only once it is on the disk, like stock SQLite
    /// (mw_durable_visible=1). By default a commit is visible as soon as it is published and its own COMMIT returns
    /// once it is durable: concurrent writers start from fresher snapshots and lose far fewer commits, but after a
    /// power failure a read-only transaction may have seen a commit whose COMMIT never returned.
    pub durable_visibility: bool,
    /// The size of the log before it is compacted into the database file (32 MB by default).
    pub log_max_mb: Option<u32>,
    /// Extra URI parameters.
    pub params: Vec<(String, String)>,
}

/// The URI that opens `path` with the engine (open it with SQLITE_OPEN_URI).
pub fn uri(path: &str, o: &Options) -> String {
    let abs = std::path::absolute(path).map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| path.to_string());
    let mut esc = String::with_capacity(abs.len());
    for c in abs.chars() {
        match c {
            '%' => esc.push_str("%25"),
            '?' => esc.push_str("%3F"),
            '#' => esc.push_str("%23"),
            '\\' => esc.push('/'),
            c => esc.push(c),
        }
    }
    let mut u = format!("file:{esc}?vfs={VFS_NAME}");
    if o.rebase {
        u.push_str("&mw_rebase=1");
    }
    if o.disable_read_check {
        u.push_str("&mw_readcheck=0");
    }
    if o.durable_visibility {
        u.push_str("&mw_durable_visible=1");
    }
    if let Some(mb) = o.log_max_mb {
        u.push_str(&format!("&mw_log_max_mb={mb}"));
    }
    for (k, v) in &o.params {
        u.push_str(&format!("&{k}={v}"));
    }
    u
}

/// The result of a compaction.
#[derive(Clone, Copy, Debug, Default)]
pub struct CompactResult {
    pub target_epoch: u64,
    pub pages_written: usize,
    pub versions_freed: i64,
}

/// Statistics of a database open with the engine in this process.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub epoch: u64,
    pub base_epoch: u64,
    pub active_snapshots: usize,
    pub commits: i64,
    pub aborts: i64,
    pub page_conflicts: i64,
    pub read_conflicts: i64,
    pub schema_conflicts: i64,
    pub relocations: i64,
    pub merges: i64,
    pub reads_saved: i64,
    pub hot_serialised: i64,
    pub compactions: i64,
    pub log_bytes: i64,
    pub log_syncs: i64,
    pub page_versions: i64,
    pub rebases: i64,
    pub rebases_grouped: i64,
    pub rebase_retries: i64,
    pub unrebasable: i64,
}

fn db_for(path: &str) -> Option<std::sync::Arc<db::MwDb>> {
    let abs = std::path::absolute(path).map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| path.to_string());
    // SQLite names the database by its full path with the symbolic links resolved
    db::lookup(&abs).or_else(|| std::fs::canonicalize(&abs).ok().and_then(|p| db::lookup(&p.to_string_lossy())))
}

/// Statistics of a database open with the engine in this process.
pub fn stats(path: &str) -> Option<Stats> {
    let d = db_for(path)?;
    let e = d.engine_opt()?;
    let n = &d.n;
    Some(Stats {
        epoch: d.visible_epoch(),
        base_epoch: e.log.base_epoch(),
        active_snapshots: store::lock(&d.active).len(),
        commits: n.commits.load(Relaxed),
        aborts: n.aborts.load(Relaxed),
        page_conflicts: n.page_conflicts.load(Relaxed),
        read_conflicts: n.read_conflicts.load(Relaxed),
        schema_conflicts: n.schema_conflicts.load(Relaxed),
        relocations: n.relocations.load(Relaxed),
        merges: n.merges.load(Relaxed),
        reads_saved: n.reads_saved.load(Relaxed),
        hot_serialised: n.hot_serialised.load(Relaxed),
        compactions: n.compactions.load(Relaxed),
        log_bytes: e.log.bytes(),
        log_syncs: e.log.n_syncs.load(Relaxed),
        page_versions: e.store.versions.load(Relaxed),
        rebases: n.rebases.load(Relaxed),
        rebases_grouped: n.rebases_grouped.load(Relaxed),
        rebase_retries: n.rebase_retries.load(Relaxed),
        unrebasable: n.unrebasable.load(Relaxed),
    })
}

/// Writes every committed page that no open snapshot holds back into the database file.
pub fn compact(path: &str) -> Result<CompactResult, i32> {
    db_for(path).ok_or(util::RC_MISUSE)?.compact()
}

#[cfg(feature = "rusqlite")]
mod rusqlite_support;
#[cfg(feature = "rusqlite")]
pub use rusqlite_support::*;
