//! Positional file I/O and durability, per platform.

use std::fs::File;
use std::io;

#[cfg(unix)]
pub fn pread(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    let mut got = 0;
    while got < buf.len() {
        match f.read_at(&mut buf[got..], off + got as u64) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(got)
}

#[cfg(unix)]
pub fn pwrite(f: &File, buf: &[u8], off: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.write_all_at(buf, off)
}

#[cfg(windows)]
pub fn pread(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    let mut got = 0;
    while got < buf.len() {
        match f.seek_read(&mut buf[got..], off + got as u64) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => return Err(e),
        }
    }
    Ok(got)
}

#[cfg(windows)]
pub fn pwrite(f: &File, buf: &[u8], off: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut done = 0;
    while done < buf.len() {
        done += f.seek_write(&buf[done..], off + done as u64)?;
    }
    Ok(())
}

/// Flushes a file whose size did not change (the log is written ahead of its end).
pub fn datasync(f: &File) -> io::Result<()> {
    f.sync_data()
}

pub fn fsync(f: &File) -> io::Result<()> {
    f.sync_all()
}

/// Flushes the directory of path: a file that was created is in its directory only once it is.
pub fn sync_dir(path: &str) {
    #[cfg(unix)]
    {
        let dir = std::path::Path::new(path).parent().map(|p| p.to_path_buf()).unwrap_or_else(|| ".".into());
        let dir = if dir.as_os_str().is_empty() { std::path::PathBuf::from(".") } else { dir };
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    #[cfg(not(unix))]
    let _ = path;
}
