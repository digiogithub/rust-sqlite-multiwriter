mod common;
use common::*;
use sqlite_multiwriter::{is_busy_snapshot, run_tx, stats, Options};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering::*};
use std::sync::Arc;

fn rebase_opts() -> Options {
    Options { rebase: true, ..Default::default() }
}

/// Writers that each update their own row, all rows on one page: a page conflict that is not a row conflict.
#[test]
fn rebase_shared_page() {
    let d = tmpdir("rb");
    let path = path_in(&d, "rb.db");
    let c = conn(&path, &rebase_opts());
    c.execute_batch("CREATE TABLE r(id INTEGER PRIMARY KEY, n INT, note TEXT)").unwrap();
    for i in 0..8 {
        c.execute("INSERT INTO r VALUES(?1, 0, 'x')", [i]).unwrap();
    }
    let retries = Arc::new(AtomicI64::new(0));
    let hs: Vec<_> = (0..8i64)
        .map(|w| {
            let (path, retries) = (path.clone(), retries.clone());
            std::thread::spawn(move || {
                let c = conn(&path, &rebase_opts());
                for _ in 0..150 {
                    loop {
                        match c.execute("UPDATE r SET n = n + 1 WHERE id = ?1", [w]) {
                            Ok(_) => break,
                            Err(e) if is_busy_snapshot(&e) => {
                                retries.fetch_add(1, Relaxed);
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    for w in 0..8i64 {
        let n: i64 = c.query_row("SELECT n FROM r WHERE id = ?1", [w], |r| r.get(0)).unwrap();
        assert_eq!(n, 150, "row {w}");
    }
    integrity(&c);
    let s = stats(&path).unwrap();
    eprintln!("commits={} rebases={} grouped={} rebase_retries={} unrebasable={} app_retries={}", s.commits, s.rebases, s.rebases_grouped, s.rebase_retries, s.unrebasable, retries.load(Relaxed));
    assert!(s.rebases > 0, "no commit was rebased");
}

/// Two blind updates of the same row: a true conflict, never merged; different rows of one page: rebased.
#[test]
fn rebase_true_conflict_refused() {
    let d = tmpdir("rbtrue");
    let path = path_in(&d, "rbtrue.db");
    let o = Options { rebase: true, params: vec![("mw_rebase_backoff".into(), "0".into())], ..Default::default() };
    let c = conn(&path, &o);
    c.execute_batch("CREATE TABLE r(id INTEGER PRIMARY KEY, n INT); INSERT INTO r VALUES(1, 10), (2, 20);").unwrap();
    let mut c1 = conn(&path, &o);
    let mut c2 = conn(&path, &o);
    {
        let t1 = c1.transaction().unwrap();
        let t2 = c2.transaction().unwrap();
        t1.execute("UPDATE r SET n = n + 1 WHERE id = 1", []).unwrap();
        t2.execute("UPDATE r SET n = n + 100 WHERE id = 1", []).unwrap();
        t1.commit().unwrap();
        let e = t2.commit().unwrap_err();
        assert!(is_busy_snapshot(&e), "{e}");
    }
    {
        let t1 = c1.transaction().unwrap();
        let t2 = c2.transaction().unwrap();
        t1.execute("UPDATE r SET n = n + 1 WHERE id = 1", []).unwrap();
        t2.execute("UPDATE r SET n = n + 1 WHERE id = 2", []).unwrap();
        t1.commit().unwrap();
        t2.commit().expect("disjoint rows must be rebased");
    }
    let a: i64 = c.query_row("SELECT n FROM r WHERE id = 1", [], |r| r.get(0)).unwrap();
    let b: i64 = c.query_row("SELECT n FROM r WHERE id = 2", [], |r| r.get(0)).unwrap();
    assert_eq!((a, b), (12, 21));
    assert_eq!(stats(&path).unwrap().rebases, 1);
}

/// An UPDATE that sets the value a row already has writes no page: not rebased.
#[test]
fn rebase_noop_update_refused() {
    let d = tmpdir("rbnoop");
    let path = path_in(&d, "rbnoop.db");
    let o = Options { rebase: true, params: vec![("mw_rebase_backoff".into(), "0".into())], ..Default::default() };
    let c = conn(&path, &o);
    c.execute_batch("CREATE TABLE r(id INTEGER PRIMARY KEY, n INT); INSERT INTO r VALUES(3, 1000), (4, 1), (5, 5);").unwrap();
    let mut c1 = conn(&path, &o);
    let mut c2 = conn(&path, &o);
    let ta = c1.transaction().unwrap();
    let tb = c2.transaction().unwrap();
    ta.execute("UPDATE r SET n = 1000 WHERE id = 3", []).unwrap(); // no-op: it "wrote" row 3
    ta.execute("UPDATE r SET n = n + 1 WHERE id = 4", []).unwrap();
    tb.execute("UPDATE r SET n = 7 WHERE id = 3", []).unwrap();
    tb.commit().unwrap();
    let e = ta.commit().unwrap_err();
    assert!(is_busy_snapshot(&e), "{e}");
}

/// Many writers on one page with the rebase, inserts with explicit keys, deletes and updates, against a model.
#[test]
fn rebase_mixed_model() {
    let d = tmpdir("rbmix");
    let path = path_in(&d, "rbmix.db");
    let c = conn(&path, &rebase_opts());
    c.execute_batch("CREATE TABLE kv(k INTEGER PRIMARY KEY, owner INT, v INT, s TEXT)").unwrap();
    let hs: Vec<_> = (0..6i64)
        .map(|w| {
            let path = path.clone();
            std::thread::spawn(move || {
                let mut c = conn(&path, &rebase_opts());
                let mut m: HashMap<i64, i64> = HashMap::new();
                let mut next = w * 100_000;
                for i in 0..100 {
                    if i % 5 == 3 && !m.is_empty() {
                        let k = *m.keys().next().unwrap();
                        run_tx(&mut c, |tx| tx.execute("DELETE FROM kv WHERE k = ?1", [k])).unwrap();
                        m.remove(&k);
                    } else if i % 5 == 4 && !m.is_empty() {
                        let k = *m.keys().next().unwrap();
                        run_tx(&mut c, |tx| tx.execute("UPDATE kv SET v = v + 1 WHERE k = ?1", [k])).unwrap();
                        *m.get_mut(&k).unwrap() += 1;
                    } else {
                        next += 1;
                        run_tx(&mut c, |tx| tx.execute("INSERT INTO kv VALUES(?1, ?2, 0, 'abc')", (next, w))).unwrap();
                        m.insert(next, 0);
                    }
                }
                let mut st = c.prepare("SELECT k, v FROM kv WHERE owner = ?1").unwrap();
                let got: HashMap<i64, i64> = st.query_map([w], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|x| x.unwrap()).collect();
                assert_eq!(got, m, "owner {w}");
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    integrity(&c);
    let s = stats(&path).unwrap();
    eprintln!("commits={} aborts={} rebases={} grouped={} unrebasable={}", s.commits, s.aborts, s.rebases, s.rebases_grouped, s.unrebasable);
}
