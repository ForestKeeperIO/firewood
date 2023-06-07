// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.

// Copied from CedrusDB

use std::fs::OpenOptions;
use std::os::fd::IntoRawFd;
pub(crate) use std::os::unix::io::RawFd as Fd;
use std::path::{Path, PathBuf};
use std::{io::ErrorKind, os::unix::prelude::OpenOptionsExt};

use nix::unistd::close;

pub struct File {
    fd: Fd,
}

impl File {
    pub async fn new<P: AsRef<Path>>(
        fid: u64,
        _flen: u64,
        rootdir: P,
    ) -> Result<Self, std::io::Error> {
        let filepath = rootdir.as_ref().with_file_name(format!("{fid:08x}.fw"));

        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true).mode(0o600);

        let fd = open_options
            .open(&filepath)
            .or_else(|e| match e.kind() {
                ErrorKind::NotFound => open_options.create(true).open(&filepath),
                _ => Err(e),
            })
            .map(|file| file.into_raw_fd())?;

        Ok(File { fd })
    }

    pub fn fd(&self) -> Fd {
        self.fd
    }
}

impl Drop for File {
    fn drop(&mut self) {
        close(self.fd).unwrap();
    }
}

pub fn touch_dir(dirname: &str, rootdir: &Path) -> Result<PathBuf, std::io::Error> {
    let path = rootdir.join(dirname);
    if let Err(e) = std::fs::create_dir(&path) {
        // ignore already-exists error
        if e.kind() != ErrorKind::AlreadyExists {
            return Err(e);
        }
    }
    Ok(path)
}

pub fn open_dir<P: AsRef<Path>>(
    path: P,
    truncate: bool,
) -> Result<(PathBuf, bool), std::io::Error> {
    let mut reset_header = truncate;
    if truncate {
        let _ = std::fs::remove_dir_all(path.as_ref());
    }
    match std::fs::create_dir(path.as_ref()) {
        Err(e) => {
            if truncate || e.kind() != ErrorKind::AlreadyExists {
                return Err(e);
            }
        }
        Ok(_) => {
            // the DB did not exist
            reset_header = true
        }
    }
    Ok((PathBuf::from(path.as_ref()), reset_header))
}
