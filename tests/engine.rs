mod common;
use common::*;
use rand::{Rng, SeedableRng};
use sqlite_multiwriter::{is_busy_snapshot, run_tx, stats, Options};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::*};
use std::sync::Arc;

#[test]
fn smoke_concurrent_inserts() {
    let d = tmpdir("smoke");
    let path = path_in(&d, "s.db");
    let c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, w INT, v TEXT)").unwrap();
    let hs: Vec<_> = (0..8)
        .map(|w| {
            let path = path.clone();
            std::thread::spawn(move || {
                let mut c = conn(&path, &Options::default());
                for i in 0..200 {
                    run_tx(&mut c, |tx| tx.execute("INSERT INTO t(w, v) VALUES(?1, ?2)", (w, format!("row {w} {i}")))).unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 1600);
    integrity(&c);
    eprintln!("{:?}", stats(&path).unwrap());
}

/// Writers that each append to their own table: physical conflicts on page 1 and on the new page numbers, resolved
/// by the relocation.
#[test]
fn bulk_own_tables_relocate() {
    let d = tmpdir("bulk");
    let path = path_in(&d, "bulk.db");
    let c = conn(&path, &Options::default());
    for w in 0..8 {
        c.execute_batch(&format!("CREATE TABLE t{w}(id INTEGER PRIMARY KEY, v TEXT)")).unwrap();
    }
    let hs: Vec<_> = (0..8)
        .map(|w| {
            let path = path.clone();
            std::thread::spawn(move || {
                let mut c = conn(&path, &Options::default());
                for _ in 0..40 {
                    run_tx(&mut c, |tx| {
                        for r in 0..100 {
                            tx.execute(&format!("INSERT INTO t{w}(v) VALUES(?1)"), ["x".repeat(50 + r)])?;
                        }
                        Ok(())
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    for w in 0..8 {
        let n: i64 = c.query_row(&format!("SELECT count(*) FROM t{w}"), [], |r| r.get(0)).unwrap();
        assert_eq!(n, 4000);
    }
    integrity(&c);
    let s = stats(&path).unwrap();
    eprintln!("commits={} aborts={} relocations={} merges={}", s.commits, s.aborts, s.relocations, s.merges);
    assert!(s.relocations > 0, "expected relocations");
    drop(c);
    assert!(sidecars(&path).is_empty(), "sidecar files left: {:?}", sidecars(&path));
    let st = stock(&path);
    integrity(&st);
    let n: i64 = st.query_row("SELECT count(*) FROM t3", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 4000);
}

/// Lost updates: every committed increment is in the final value.
#[test]
fn hot_counters() {
    let d = tmpdir("hot");
    let path = path_in(&d, "hot.db");
    let c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE c(id INTEGER PRIMARY KEY, n INT); INSERT INTO c VALUES(0,0),(1,0),(2,0),(3,0);").unwrap();
    let done = Arc::new(AtomicI64::new(0));
    let hs: Vec<_> = (0..8)
        .map(|w| {
            let (path, done) = (path.clone(), done.clone());
            std::thread::spawn(move || {
                let mut c = conn(&path, &Options::default());
                let mut r = rand::rngs::StdRng::seed_from_u64(w);
                for _ in 0..150 {
                    let id: i64 = r.gen_range(0..4);
                    run_tx(&mut c, |tx| tx.execute("UPDATE c SET n = n + 1 WHERE id = ?1", [id])).unwrap();
                    done.fetch_add(1, Relaxed);
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let s: i64 = c.query_row("SELECT sum(n) FROM c", [], |r| r.get(0)).unwrap();
    assert_eq!(s, done.load(Relaxed), "lost updates");
    integrity(&c);
}

/// Transfers read both rows and write both: the total never changes, and readers always see a consistent total.
#[test]
fn transfers_invariant() {
    let d = tmpdir("bank");
    let path = path_in(&d, "bank.db");
    let c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE acct(id INTEGER PRIMARY KEY, bal INT, pad TEXT)").unwrap();
    for i in 0..200 {
        c.execute("INSERT INTO acct VALUES(?1, 1000, ?2)", (i, "p".repeat(200))).unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(AtomicI64::new(0));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (path, stop, bad) = (path.clone(), stop.clone(), bad.clone());
            std::thread::spawn(move || {
                let c = conn(&path, &Options::default());
                while !stop.load(Relaxed) {
                    let s: i64 = c.query_row("SELECT sum(bal) FROM acct", [], |r| r.get(0)).unwrap();
                    if s != 200_000 {
                        bad.fetch_add(1, Relaxed);
                    }
                }
            })
        })
        .collect();
    let writers: Vec<_> = (0..8)
        .map(|w| {
            let path = path.clone();
            std::thread::spawn(move || {
                let mut c = conn(&path, &Options::default());
                let mut r = rand::rngs::StdRng::seed_from_u64(w);
                for _ in 0..150 {
                    let (a, b): (i64, i64) = (r.gen_range(0..200), r.gen_range(0..200));
                    if a == b {
                        continue;
                    }
                    let amt: i64 = r.gen_range(0..50);
                    run_tx(&mut c, |tx| {
                        let ba: i64 = tx.query_row("SELECT bal FROM acct WHERE id = ?1", [a], |r| r.get(0))?;
                        tx.execute("UPDATE acct SET bal = ?1 WHERE id = ?2", (ba - amt, a))?;
                        tx.execute("UPDATE acct SET bal = bal + ?1 WHERE id = ?2", (amt, b))
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for h in writers {
        h.join().unwrap();
    }
    stop.store(true, Relaxed);
    for h in readers {
        h.join().unwrap();
    }
    let s: i64 = c.query_row("SELECT sum(bal) FROM acct", [], |r| r.get(0)).unwrap();
    assert_eq!(s, 200_000);
    assert_eq!(bad.load(Relaxed), 0, "inconsistent reads");
    integrity(&c);
}

/// Write skew: T1 reads A and writes B, T2 reads B and writes A. One must fail.
#[test]
fn write_skew_refused() {
    for rebase in [false, true] {
        let d = tmpdir("skew");
        let path = path_in(&d, "skew.db");
        let o = Options { rebase, ..Default::default() };
        let c = conn(&path, &o);
        c.execute_batch("CREATE TABLE oncall(id INTEGER PRIMARY KEY, on_call INT, pad TEXT)").unwrap();
        let pad = if rebase { String::new() } else { "a".repeat(3000) }; // (with the rebase: one page)
        c.execute("INSERT INTO oncall VALUES(1, 1, ?1), (2, 1, ?1)", [&pad]).unwrap();
        let mut c1 = conn(&path, &o);
        let mut c2 = conn(&path, &o);
        let t1 = c1.transaction().unwrap();
        let t2 = c2.transaction().unwrap();
        let n1: i64 = t1.query_row("SELECT sum(on_call) FROM oncall", [], |r| r.get(0)).unwrap();
        let n2: i64 = t2.query_row("SELECT sum(on_call) FROM oncall", [], |r| r.get(0)).unwrap();
        assert_eq!((n1, n2), (2, 2));
        t1.execute("UPDATE oncall SET on_call = 0 WHERE id = 1", []).unwrap();
        t2.execute("UPDATE oncall SET on_call = 0 WHERE id = 2", []).unwrap();
        t1.commit().unwrap();
        let e = t2.commit().unwrap_err();
        assert!(is_busy_snapshot(&e), "{e}");
        let n: i64 = c.query_row("SELECT sum(on_call) FROM oncall", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1);
    }
}

/// A long reader keeps its snapshot while others commit and the log is compacted.
#[test]
fn long_reader_and_compaction() {
    let d = tmpdir("long");
    let path = path_in(&d, "long.db");
    let o = Options { log_max_mb: Some(1), ..Default::default() };
    let c = conn(&path, &o);
    c.execute_batch("CREATE TABLE k(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for _ in 0..100 {
        c.execute("INSERT INTO k(v) VALUES(?1)", ["v".repeat(500)]).unwrap();
    }
    let mut r = conn(&path, &o);
    let rt = r.transaction().unwrap();
    let before: i64 = rt.query_row("SELECT count(*) FROM k", [], |r| r.get(0)).unwrap();
    for _ in 0..2000 {
        c.execute("INSERT INTO k(v) VALUES(?1)", ["w".repeat(500)]).unwrap();
    }
    sqlite_multiwriter::compact(&path).unwrap();
    let again: i64 = rt.query_row("SELECT count(*) FROM k", [], |r| r.get(0)).unwrap();
    assert_eq!(before, again, "the snapshot moved");
    drop(rt);
    drop(r);
    sqlite_multiwriter::compact(&path).unwrap();
    let s = stats(&path).unwrap();
    assert_eq!(s.base_epoch, s.epoch);
    let n: i64 = c.query_row("SELECT count(*) FROM k", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 2100);
    integrity(&c);
}

/// Schema changes while others write.
#[test]
fn ddl_while_writing() {
    let d = tmpdir("ddl");
    let path = path_in(&d, "ddl.db");
    let mut c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE base(id INTEGER PRIMARY KEY, v INT)").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let ins = Arc::new(AtomicI64::new(0));
    let hs: Vec<_> = (0..4)
        .map(|_| {
            let (path, stop, ins) = (path.clone(), stop.clone(), ins.clone());
            std::thread::spawn(move || {
                let mut c = conn(&path, &Options::default());
                while !stop.load(Relaxed) {
                    run_tx(&mut c, |tx| tx.execute("INSERT INTO base(v) VALUES(1)", [])).unwrap();
                    ins.fetch_add(1, Relaxed);
                }
            })
        })
        .collect();
    for i in 0..20 {
        run_tx(&mut c, |tx| tx.execute_batch(&format!("CREATE TABLE extra{i}(a, b)"))).unwrap();
        run_tx(&mut c, |tx| tx.execute_batch(&format!("CREATE INDEX ix{i} ON base(v)"))).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    stop.store(true, Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    let n: i64 = c.query_row("SELECT count(*) FROM base", [], |r| r.get(0)).unwrap();
    assert_eq!(n, ins.load(Relaxed));
    let t: i64 = c.query_row("SELECT count(*) FROM sqlite_schema WHERE name LIKE 'extra%' OR name LIKE 'ix%'", [], |r| r.get(0)).unwrap();
    assert_eq!(t, 40);
    integrity(&c);
}

/// Crash: a child process commits and dies without closing. Every acknowledged commit is there after recovery.
#[test]
fn crash_recovery() {
    if let Ok(path) = std::env::var("MW_CRASH_DB") {
        let ack = std::env::var("MW_CRASH_ACK").unwrap();
        let c = conn(&path, &Options::default());
        c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        let f = Arc::new(std::sync::Mutex::new(std::fs::File::create(ack).unwrap()));
        let hs: Vec<_> = (0..4)
            .map(|_| {
                let (path, f) = (path.clone(), f.clone());
                std::thread::spawn(move || {
                    let mut c = conn(&path, &Options::default());
                    for _ in 0..150 {
                        run_tx(&mut c, |tx| tx.execute("INSERT INTO t(v) VALUES(?1)", ["c".repeat(300)])).unwrap();
                        use std::io::Write;
                        writeln!(f.lock().unwrap(), "ok").unwrap();
                    }
                    std::mem::forget(c);
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        f.lock().unwrap().sync_all().unwrap();
        std::process::exit(7); // no close: the log holds the commits
    }
    let d = tmpdir("crash");
    let path = path_in(&d, "crash.db");
    let ack = path_in(&d, "ack");
    let st = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["crash_recovery", "--exact", "--nocapture", "--test-threads=1"])
        .env("MW_CRASH_DB", &path)
        .env("MW_CRASH_ACK", &ack)
        .status()
        .unwrap();
    assert_eq!(st.code(), Some(7));
    assert!(!sidecars(&path).is_empty(), "the child left no log");
    let acked = std::fs::read_to_string(&ack).unwrap().lines().count() as i64;
    let c = conn(&path, &Options::default());
    let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert!(n >= acked, "recovered {n}, acknowledged {acked}");
    integrity(&c);
    c.execute("INSERT INTO t(v) VALUES('after')", []).unwrap();
    drop(c);
    let st = stock(&path);
    let m: i64 = st.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(m, n + 1);
    integrity(&st);
}

/// A second process cannot open a database another process has open with the engine.
#[test]
fn second_process_refused() {
    if let Ok(path) = std::env::var("MW_SECOND_DB") {
        let r = sqlite_multiwriter::open(&path, &Options::default()).and_then(|c| c.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get::<_, i64>(0)));
        std::process::exit(if r.is_err() { 5 } else { 0 });
    }
    let d = tmpdir("two");
    let path = path_in(&d, "two.db");
    let c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE x(a)").unwrap();
    let st = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["second_process_refused", "--exact", "--test-threads=1"])
        .env("MW_SECOND_DB", &path)
        .status()
        .unwrap();
    assert_eq!(st.code(), Some(5));
}

#[test]
fn unsupported_pragmas() {
    let d = tmpdir("pragma");
    let path = path_in(&d, "p.db");
    let c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE x(a)").unwrap();
    assert!(c.execute_batch("PRAGMA journal_mode=DELETE").is_err());
    assert!(c.execute_batch("PRAGMA locking_mode=EXCLUSIVE").is_err());
    let m: String = c.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
    assert_eq!(m, "wal");
}

/// Overflow pages, indexes, deletes and savepoints under concurrency, against a model.
#[test]
fn mixed_workload_model() {
    let d = tmpdir("mix");
    let path = path_in(&d, "mix.db");
    let c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE m(id INTEGER PRIMARY KEY, owner INT, payload BLOB, tag TEXT UNIQUE); CREATE INDEX m_owner ON m(owner);").unwrap();
    let hs: Vec<_> = (0..6)
        .map(|w| {
            let path = path.clone();
            std::thread::spawn(move || {
                let mut c = conn(&path, &Options::default());
                let mut r = rand::rngs::StdRng::seed_from_u64(w + 42);
                let mut model: HashMap<i64, i64> = HashMap::new();
                let mut seq = 0;
                for _ in 0..120 {
                    let op = r.gen_range(0..10);
                    let size: i64 = 10 + r.gen_range(0..6000);
                    let ids: Vec<i64> = model.keys().copied().collect();
                    if op < 6 || ids.is_empty() {
                        seq += 1;
                        let tag = format!("w{w}-{seq}");
                        let id = run_tx(&mut c, |tx| {
                            tx.execute("INSERT INTO m(owner, payload, tag) VALUES(?1, randomblob(?2), ?3)", (w as i64, size, &tag))?;
                            Ok(tx.last_insert_rowid())
                        })
                        .unwrap();
                        model.insert(id, size);
                    } else if op < 8 {
                        let id = ids[r.gen_range(0..ids.len())];
                        run_tx(&mut c, |tx| tx.execute("DELETE FROM m WHERE id = ?1", [id])).unwrap();
                        model.remove(&id);
                    } else {
                        let id = ids[r.gen_range(0..ids.len())];
                        run_tx(&mut c, |tx| {
                            tx.execute_batch("SAVEPOINT s")?;
                            tx.execute("UPDATE m SET payload = randomblob(?1) WHERE id = ?2", (size, id))?;
                            tx.execute_batch("ROLLBACK TO s; RELEASE s")
                        })
                        .unwrap();
                    }
                }
                let mut st = c.prepare("SELECT id, length(payload) FROM m WHERE owner = ?1").unwrap();
                let got: HashMap<i64, i64> = st.query_map([w as i64], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|x| x.unwrap()).collect();
                assert_eq!(got, model, "owner {w}");
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    integrity(&c);
}

/// Writers in their own key ranges of one table: disjoint leaves, shared interior pages (merged) and growth (relocated).
#[test]
fn shared_table_key_ranges() {
    let d = tmpdir("ranges");
    let path = path_in(&d, "ranges.db");
    let c = conn(&path, &Options::default());
    c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    for w in 0..6i64 {
        for i in 0..20 {
            c.execute("INSERT INTO t VALUES(?1, ?2)", (w * 1_000_000 + i, "s".repeat(400))).unwrap();
        }
    }
    let hs: Vec<_> = (0..6i64)
        .map(|w| {
            let path = path.clone();
            std::thread::spawn(move || {
                let mut c = conn(&path, &Options::default());
                let mut next = w * 1_000_000 + 1000;
                for _ in 0..30 {
                    run_tx(&mut c, |tx| {
                        for r in 0..20 {
                            tx.execute("INSERT INTO t VALUES(?1, ?2)", (next + r, "v".repeat(400)))?;
                        }
                        Ok(())
                    })
                    .unwrap();
                    next += 20;
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let n: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 6 * 20 + 6 * 30 * 20);
    integrity(&c);
    let s = stats(&path).unwrap();
    eprintln!("commits={} aborts={} relocations={} merges={}", s.commits, s.aborts, s.relocations, s.merges);
    drop(c);
    integrity(&stock(&path));
}
