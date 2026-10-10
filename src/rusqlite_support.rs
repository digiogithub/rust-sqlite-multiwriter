//! Helpers for rusqlite (feature "rusqlite", on by default).

use crate::{register, uri, Options, BUSY_SNAPSHOT};
use rusqlite::{Connection, OpenFlags};

/// Opens `path` with the engine. Open one connection per thread: every connection is a writer of its own.
pub fn open(path: &str, o: &Options) -> rusqlite::Result<Connection> {
    register().map_err(|rc| rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(rc), Some("registering the VFS".into())))?;
    let c = Connection::open_with_flags(uri(path, o), OpenFlags::default() | OpenFlags::SQLITE_OPEN_URI)?;
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    c.pragma_update(None, "journal_mode", "WAL")?;
    Ok(c)
}

/// Whether the error means: run the whole transaction again (SQLITE_BUSY_SNAPSHOT or SQLITE_BUSY).
pub fn is_busy_snapshot(e: &rusqlite::Error) -> bool {
    match e {
        rusqlite::Error::SqliteFailure(f, _) => f.extended_code == BUSY_SNAPSHOT || f.code == rusqlite::ErrorCode::DatabaseBusy,
        _ => false,
    }
}

/// Runs `f` in a transaction, and runs it again, whole, while the commit loses against a concurrent one. `f` may run
/// several times: it must not have effects outside the transaction.
pub fn run_tx<T>(c: &mut Connection, mut f: impl FnMut(&rusqlite::Transaction) -> rusqlite::Result<T>) -> rusqlite::Result<T> {
    let mut backoff = std::time::Duration::from_micros(50);
    for attempt in 0.. {
        let tx = c.transaction()?;
        let r = f(&tx).and_then(|v| tx.commit().map(|_| v));
        match r {
            Ok(v) => return Ok(v),
            Err(e) if is_busy_snapshot(&e) && attempt < 1000 => {
                if attempt > 2 {
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(std::time::Duration::from_millis(5));
                }
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
