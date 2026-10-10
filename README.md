# rust-sqlite-multiwriter

Many concurrent writers on **one** SQLite database, in Rust.

A Rust port of the engine of [sqliteai/sqlite-multiwriter](https://github.com/sqliteai/sqlite-multiwriter)
(article: [We solved SQLite's single-writer limitation](https://marcobambini.substack.com/p/we-solved-sqlites-single-writer-limitation)),
sister of [go-sqlite-multiwriter](https://github.com/digiogithub/go-sqlite-multiwriter). It is a **VFS**
registered in the SQLite of `libsqlite3-sys` (the one `rusqlite` uses with `bundled`): your SQL, your schema and
your `.db` file do not change, and the file is a plain SQLite database again when the last connection closes.

```rust
use sqlite_multiwriter::{open, run_tx, Options};

// one connection per thread: each one is an independent writer
let mut conn = open("app.db", &Options { rebase: true, ..Default::default() })?;
run_tx(&mut conn, |tx| {
    tx.execute("UPDATE account SET balance = balance + 10 WHERE id = ?1", [7])
})?; // retries the whole transaction when it loses against another commit (SQLITE_BUSY_SNAPSHOT)
```

Without the `rusqlite` feature (on by default), call `sqlite_multiwriter::register()` and open the URI
`file:app.db?vfs=multiwriter` with `SQLITE_OPEN_URI`.

## How it works

```
writer 1 --> private WAL --+
writer 2 --> private WAL --+--> validation and publication --> commit log --> .db file
writer 3 --> private WAL --+    (first committer wins)                       (compaction)
```

* **Private lanes**: every connection has its own in-memory WAL and wal-index; nobody waits for the write lock.
  Writes to the `.db` are discarded: only the compactor writes it.
* **Snapshots and page versions**: a `(page, epoch)` store with striped locks; every transaction reads its
  snapshot.
* **Commit**: the schema cookie is validated, as are the pages **read** (prevents write skew; an interior table
  page whose routing did not change is forgiven) and the pages **written** (first committer wins). On failure:
  `SQLITE_BUSY_SNAPSHOT`.
* **Relocation** of new pages when several commits grow the file at once, and three-way **merge** of interior
  pages.
* **Rebase** (`Options::rebase`): a commit that lost only because it shared a page with other rows is replayed
  row by row (decoded from the pages) at the latest state, in batches, on a helper connection.
* **Durability**: segmented `<db>-mw*` log with group commit (`synchronous=FULL`), crash recovery and background
  compaction.
* **Visible, then durable**: a commit is visible to new snapshots as soon as its record is written, while its
  fsync runs; its own `COMMIT` returns once it is on the disk. A snapshot taken during that fsync would otherwise be
  stale and bound to lose on any shared page (with 16 writers on the same pages this alone is 2–4× the
  throughput). An acknowledged commit never depends on one that could be lost (a commit that read another one is
  acknowledged only after an fsync round that started after that one was written, and a failed fsync fails every
  later commit). The one difference with stock SQLite: after a power failure, a *read-only* transaction may have
  seen a commit whose `COMMIT` never returned. `Options::durable_visibility` (`mw_durable_visible=1`) restores the
  strict order.
* **Hot-spot turn**: a connection that keeps losing takes a per-database turn before its next snapshot, handed on
  as soon as its commit is visible.
* **DDL barrier** via `sqlite3_trace_v2` (installed with `sqlite3_auto_extension`).

## Status

| | |
|---|---|
| Private lanes, snapshots, validation, relocation, merge, log, recovery, compaction, GC, DDL barrier | ✅ |
| Rebase (`mw_rebase=1`) | ✅ |
| Multi-process (`mw_mp=1`) | ⏳ (available in the Go version; here, one process per database for now: a second process gets `SQLITE_BUSY`) |

## Performance

`cargo run --release --example mwbench -- bulk|groups|hot 1,4,16 4` — 4 cores, local disk,
`synchronous=FULL`. `sqlite` = stock SQLite in WAL mode with `BEGIN IMMEDIATE`; `mw` = the engine; `mwr` = the
engine with rebase. `MW_DURABLE_VISIBLE=1` and `MW_PARAMS="k=v&..."` pass options to the engine.

| workload | writers | SQLite tx/s | mw tx/s | mwr tx/s | SQLite p99.9 | mw p99.9 | mwr p99.9 |
|---|---:|---:|---:|---:|---:|---:|---:|
| bulk (100 inserts/tx, own table) | 1 | **2,748** | 2,643 | 2,348 | 7 ms | 6 ms | 9 ms |
| | 4 | 3,218 | 3,545 | **3,625** | 22 ms | 21 ms | 18 ms |
| | 16 | 2,748 | **4,452** | 4,059 | 1,786 ms | 50 ms | 44 ms |
| groups (each its own row, same page) | 1 | **5,680** | 5,232 | 5,589 | 1.4 ms | 3.1 ms | 2.8 ms |
| | 4 | 4,247 | **7,799** (90 retries/100) | 6,553 (0 retries) | 2.8 ms | 5.4 ms | 5.9 ms |
| | 16 | 5,172 | **14,030** (177 retries/100) | 12,889 (0 retries) | 104 ms | 31 ms | 8.5 ms |
| hot (4 shared rows: real conflict) | 1 | **5,892** | 4,525 | 5,075 | 1.5 ms | 3.7 ms | 3.1 ms |
| | 4 | 5,459 | **8,402** | 8,002 | 2.3 ms | 5.6 ms | 5.1 ms |
| | 16 | 5,429 | **15,125** | 12,266 | 631 ms | 7.4 ms | 12 ms |

With `durable_visibility` (strict order) the contended workloads stay at about 3,000–3,700 tx/s with 16 writers
(`groups`/`hot`, 210 retries per 100 transactions). A single writer runs 4–20 % below stock SQLite (the engine's
own record and fsync per commit).

## Options (URI)

`vfs=multiwriter` (or `mw=1`) enables the engine; `mw_rebase=1`, `mw_rebase_backoff=0`, `mw_durable_visible=1`, `mw_readcheck=0`,
`mw_log_max_mb`, `mw_base_cache_mb`, `mw_gc`, `mw_noreloc`, `mw_nomerge`, `mw_noroute`. `MW_DEBUG=1` explains
every refusal of the rebase.

## Limits

WAL only (no `locking_mode=EXCLUSIVE` nor `auto_vacuum`), local file system, the database is only used through
the engine while it is open, and a refused transaction is retried whole. The rebase only replays "blind"
transactions (`INSERT ... VALUES`, `UPDATE`/`DELETE` by key, with no prior `SELECT`) and does not apply with
triggers, virtual tables or `AUTOINCREMENT`.

## Tests

```
cargo test --release      # concurrency, invariants, write skew, DDL, crash/recovery, rebase
```

Apache 2.0 license (see `LICENSE` and `NOTICE`).
