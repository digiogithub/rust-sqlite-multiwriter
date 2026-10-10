#![allow(dead_code)]
use rusqlite::{Connection, OpenFlags};
use sqlite_multiwriter::{open, Options};
use std::path::PathBuf;

pub fn tmpdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("mwtest-{}-{}-{}", name, std::process::id(), rand::random::<u32>()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub fn path_in(d: &PathBuf, f: &str) -> String {
    d.join(f).to_string_lossy().into_owned()
}

pub fn conn(path: &str, o: &Options) -> Connection {
    open(path, o).unwrap()
}

pub fn stock(path: &str) -> Connection {
    Connection::open_with_flags(path, OpenFlags::default()).unwrap()
}

pub fn integrity(c: &Connection) {
    let s: String = c.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(s, "ok");
}

pub fn sidecars(path: &str) -> Vec<String> {
    let p = std::path::Path::new(path);
    let name = p.file_name().unwrap().to_string_lossy().into_owned();
    std::fs::read_dir(p.parent().unwrap())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&format!("{name}-mw")))
        .collect()
}
