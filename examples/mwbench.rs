//! Compares the engine with stock SQLite (WAL, synchronous=FULL, BEGIN IMMEDIATE and a busy timeout).
//!
//!     cargo run --release --example mwbench -- bulk 1,4,16 4
//!
//! Workloads: bulk (100 inserts per transaction into the writer's own table), groups (each writer updates its own
//! row; the rows share a page), hot (every writer increments one of 4 rows: true conflicts).
//! Variants: sqlite, mw (the engine), mwr (the engine with the rebase). MW_DURABLE_VISIBLE=1 makes the engine's commits
//! visible only once durable.

use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use sqlite_multiwriter::{is_busy_snapshot, open, Options};
use std::sync::atomic::{AtomicI64, Ordering::*};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn conn(variant: &str, path: &str) -> Connection {
    match variant {
        "sqlite" => {
            let c = Connection::open_with_flags(path, OpenFlags::default()).unwrap();
            c.busy_timeout(Duration::from_secs(10)).unwrap();
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;").unwrap();
            c
        }
        v => {
            let durable = std::env::var("MW_DURABLE_VISIBLE").is_ok_and(|v| v == "1");
            // MW_PARAMS="k=v&k2=v2": extra URI parameters of the engine
            let params = std::env::var("MW_PARAMS")
                .unwrap_or_default()
                .split('&')
                .filter_map(|kv| kv.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
                .collect();
            let o = Options { rebase: v == "mwr", durable_visibility: durable, params, ..Default::default() };
            let c = open(path, &o).unwrap();
            c.execute_batch("PRAGMA synchronous=FULL;").unwrap();
            c
        }
    }
}

fn run(variant: &str, workload: &str, writers: usize, secs: f64, dir: &str) {
    let path = format!("{dir}/{variant}-{workload}-{writers}.db");
    for s in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{s}"));
    }
    let c = conn(variant, &path);
    match workload {
        "bulk" => {
            for w in 0..writers {
                c.execute_batch(&format!("CREATE TABLE t{w}(id INTEGER PRIMARY KEY, a INT, b TEXT)")).unwrap();
            }
        }
        _ => {
            c.execute_batch("CREATE TABLE r(id INTEGER PRIMARY KEY, n INT, pad TEXT)").unwrap();
            for i in 0..writers.max(4) {
                c.execute("INSERT INTO r VALUES(?1, 0, ?2)", (i as i64, "p".repeat(100))).unwrap();
            }
        }
    }
    let commits = Arc::new(AtomicI64::new(0));
    let retries = Arc::new(AtomicI64::new(0));
    let deadline = Instant::now() + Duration::from_secs_f64(secs);
    let hs: Vec<_> = (0..writers)
        .map(|w| {
            let (path, variant, workload, commits, retries) = (path.clone(), variant.to_string(), workload.to_string(), commits.clone(), retries.clone());
            std::thread::spawn(move || {
                let mut c = conn(&variant, &path);
                let behavior = if variant == "sqlite" { TransactionBehavior::Immediate } else { TransactionBehavior::Deferred };
                let mut lat = Vec::new();
                let mut i = 0i64;
                while Instant::now() < deadline {
                    let t0 = Instant::now();
                    loop {
                        let r = (|| -> rusqlite::Result<()> {
                            let tx = c.transaction_with_behavior(behavior)?;
                            match workload.as_str() {
                                "bulk" => {
                                    let mut st = tx.prepare_cached(&format!("INSERT INTO t{w}(a, b) VALUES(?1, ?2)"))?;
                                    for r in 0..100 {
                                        st.execute((i * 100 + r, "payload-payload-payload-payload"))?;
                                    }
                                }
                                "groups" => {
                                    tx.execute("UPDATE r SET n = n + 1 WHERE id = ?1", [w as i64])?;
                                }
                                _ => {
                                    tx.execute("UPDATE r SET n = n + 1 WHERE id = ?1", [(w as i64 + i) % 4])?;
                                }
                            }
                            tx.commit()
                        })();
                        match r {
                            Ok(()) => break,
                            Err(e) if is_busy_snapshot(&e) => {
                                retries.fetch_add(1, Relaxed);
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                    lat.push(t0.elapsed().as_secs_f64() * 1000.0);
                    commits.fetch_add(1, Relaxed);
                    i += 1;
                }
                lat
            })
        })
        .collect();
    let mut all: Vec<f64> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |p: f64| if all.is_empty() { 0.0 } else { all[(p * (all.len() - 1) as f64) as usize] };
    let n = commits.load(Relaxed);
    println!("{variant:<8} {workload:<7} {writers:>7} {:>10.0} {:>12.2} {:>9.2} {:>9.2} {:>9.2}", n as f64 / secs, 100.0 * retries.load(Relaxed) as f64 / n.max(1) as f64, q(0.5), q(0.99), q(0.999));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let workload = args.get(1).map_or("bulk", |s| s.as_str()).to_string();
    let writers: Vec<usize> = args.get(2).map_or("1,4,16", |s| s.as_str()).split(',').map(|x| x.parse().unwrap()).collect();
    let secs: f64 = args.get(3).map_or(4.0, |s| s.parse().unwrap());
    let dir = format!("mwbench-{}", std::process::id());
    std::fs::create_dir_all(&dir).unwrap();
    println!("{:<8} {:<7} {:>7} {:>10} {:>12} {:>9} {:>9} {:>9}", "variant", "workload", "writers", "tx/s", "retries/100", "p50 ms", "p99 ms", "p99.9 ms");
    for &w in &writers {
        for v in ["sqlite", "mw", "mwr"] {
            run(v, &workload, w, secs, &dir);
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
