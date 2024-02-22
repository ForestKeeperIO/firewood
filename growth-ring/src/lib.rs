// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.
//
//! Simple and modular write-ahead-logging implementation.
//!
//! # Examples
//!
//! ```no_run
//! use growthring::{WalStoreImpl, wal::WalLoader};
//! use futures::executor::block_on;
//! let mut loader = WalLoader::new();
//! loader.file_nbit(9).block_nbit(8);
//!
//!
//! // Start with empty WAL (truncate = true).
//! let store = WalStoreImpl::new("/tmp/walfiles", true).unwrap();
//! let mut wal = block_on(loader.load(store, |_, _| {Ok(())}, 0)).unwrap();
//! // Write a vector of records to WAL.
//! for f in wal.grow(vec!["record1(foo)", "record2(bar)", "record3(foobar)"]).into_iter() {
//!     let ring_id = block_on(f).unwrap().1;
//!     println!("WAL recorded record to {:?}", ring_id);
//! }
//!
//!
//! // Load from WAL (truncate = false).
//! let store = WalStoreImpl::new("/tmp/walfiles", false).unwrap();
//! let mut wal = block_on(loader.load(store, |payload, ringid| {
//!     // redo the operations in your application
//!     println!("recover(payload={}, ringid={:?})",
//!              std::str::from_utf8(&payload).unwrap(),
//!              ringid);
//!     Ok(())
//! }, 0)).unwrap();
//! // We saw some log playback, even there is no failure.
//! // Let's try to grow the WAL to create many files.
//! let ring_ids = wal.grow((1..100).into_iter().map(|i| "a".repeat(i)).collect::<Vec<_>>())
//!                   .into_iter().map(|f| block_on(f).unwrap().1).collect::<Vec<_>>();
//! // Then assume all these records are not longer needed. We can tell WalWriter by the `peel`
//! // method.
//! block_on(wal.peel(ring_ids, 0)).unwrap();
//! // There will only be one remaining file in /tmp/walfiles.
//!
//! let store = WalStoreImpl::new("/tmp/walfiles", false).unwrap();
//! let wal = block_on(loader.load(store, |payload, _| {
//!     println!("payload.len() = {}", payload.len());
//!     Ok(())
//! }, 0)).unwrap();
//! // After each recovery, the /tmp/walfiles is empty.
//! ```

pub mod wal;
pub mod walerror;

use async_trait::async_trait;
use std::fs;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tokio_uring::buf::IoBuf;
use tokio_uring::fs::{File, OpenOptions};

use wal::{WalBytes, WalFile, WalPos, WalStore};
use walerror::WalError;

struct RawWalFile {
    file: File,
}

impl RawWalFile {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .mode(0o600)
            .open(path)
            .await?;
        Ok(RawWalFile { file })
    }
}

pub struct WalFileImpl {
    file_mutex: Mutex<RawWalFile>,
    path: PathBuf,
}

#[async_trait(?Send)]
impl WalFile for WalFileImpl {
    async fn allocate(&self, offset: WalPos, length: usize) -> Result<(), WalError> {
        let zero_vec = vec![0; length];
        let _ = self
            .file_mutex
            .lock()
            .await
            .file
            .write_at(zero_vec, offset)
            .await;
        Ok(())
    }

    async fn truncate(&self) -> Result<(), WalError> {
        let path = &self.path;
        let file = &mut self.file_mutex.lock().await.file;

        let f2 = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .mode(0o600)
            .open(path)
            .await?;

        *file = f2;

        Ok(())
    }

    async fn write(&self, offset: WalPos, data: WalBytes) -> Result<(), WalError> {
        let file = &mut self.file_mutex.lock().await.file;

        //file.seek(SeekFrom::Start(offset)).await?;

        // Ok(file.write_all(&data).await?)

        let mut n = 0;
        let len = data.len();
        let mut buf = data;
        while n < len {
            let o = offset + n as u64;
            let res = file.write_at(buf.slice(n..), o).await;
            match res {
                (Ok(0), _) => return Err(WalError::WalDirExists),
                (Ok(m), slice) => {
                    n += m;
                    buf = slice.into_inner();
                }

                // This match on an EINTR error is not performed because this
                // crate's design ensures we are not calling the 'wait' option
                // in the ENTER syscall. Only an Enter with 'wait' can generate
                // an EINTR according to the io_uring man pages.
                // (Err(ref e), slice) if e.kind() == std::io::ErrorKind::Interrupted => {
                //     buf = slice.into_inner();
                // },
                //(Err(e), slice) => return Err(e),
                (Err(_), _) => return Err(WalError::WalDirExists),
            }
        }

        Ok(())
    }

    async fn read(&self, offset: WalPos, length: usize) -> Result<Option<WalBytes>, WalError> {
        let (result, bytes_read) = {
            let result = Vec::with_capacity(length);
            let file = &mut self.file_mutex.lock().await.file;
            //file.seek(SeekFrom::Start(offset)).await?;
            //let bytes_read = file.read_buf(&mut result).await?;
            let (res, result) = file.read_at(result, offset).await;
            let bytes_read = res?;
            (result, bytes_read)
        };

        let result = Some(result).filter(|_| bytes_read == length);

        Ok(result)
    }
}

pub struct WalStoreImpl {
    root_dir: PathBuf,
}

impl WalStoreImpl {
    pub fn new<P: AsRef<Path>>(wal_dir: P, truncate: bool) -> Result<Self, WalError> {
        if truncate {
            if let Err(e) = fs::remove_dir_all(&wal_dir) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(From::from(e));
                }
            }
            fs::create_dir(&wal_dir)?;
        } else if !wal_dir.as_ref().exists() {
            // create Wal dir
            fs::create_dir(&wal_dir)?;
        }

        Ok(WalStoreImpl {
            root_dir: wal_dir.as_ref().to_path_buf(),
        })
    }
}

#[async_trait(?Send)]
impl WalStore<WalFileImpl> for WalStoreImpl {
    type FileNameIter = std::vec::IntoIter<PathBuf>;

    async fn open_file(&self, filename: &str, _touch: bool) -> Result<WalFileImpl, WalError> {
        let path = self.root_dir.join(filename);

        let file = RawWalFile::open(path.clone()).await?;

        let file_mutex = Mutex::new(file);

        Ok(WalFileImpl { file_mutex, path })
    }

    async fn remove_file(&self, filename: String) -> Result<(), WalError> {
        let file_to_remove = self.root_dir.join(filename);
        fs::remove_file(file_to_remove).map_err(From::from)
    }

    fn enumerate_files(&self) -> Result<Self::FileNameIter, WalError> {
        let mut filenames = Vec::new();
        #[allow(clippy::unwrap_used)]
        for path in fs::read_dir(&self.root_dir)?.filter_map(|entry| entry.ok()) {
            filenames.push(path.path());
        }
        Ok(filenames.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn truncation_makes_a_file_smaller() {
    //     tokio_uring::start(async {
    //         const HALF_LENGTH: usize = 512;

    //         let walfile_path = get_temp_walfile_path(file!(), line!());

    //         tokio::fs::remove_file(&walfile_path).await.ok();

    //         #[allow(clippy::unwrap_used)]
    //         let walfile = RawWalFile::open(walfile_path.clone()).await.unwrap();

    //         let walfile_impl = WalFileImpl {
    //             file_mutex: Mutex::new(walfile),
    //             path: walfile_path,
    //         };

    //         let first_half = vec![1u8; HALF_LENGTH];
    //         let second_half = vec![2u8; HALF_LENGTH];

    //         let data = first_half
    //             .iter()
    //             .copied()
    //             .chain(second_half.iter().copied())
    //             .collect();

    //         #[allow(clippy::unwrap_used)]
    //         walfile_impl.write(0, data).await.unwrap();
    //         #[allow(clippy::unwrap_used)]
    //         walfile_impl.truncate(HALF_LENGTH).await.unwrap();

    //         #[allow(clippy::unwrap_used)]
    //         let result = walfile_impl.read(0, HALF_LENGTH).await.unwrap();

    //         assert_eq!(result, None)
    //     })
    // }

    // #[test]
    // fn truncation_extends_a_file_with_zeros() {
    //     tokio_uring::start(async {
    //         const LENGTH: usize = 512;

    //         let walfile_path = get_temp_walfile_path(file!(), line!());

    //         tokio::fs::remove_file(&walfile_path).await.ok();

    //         #[allow(clippy::unwrap_used)]
    //         let walfile = RawWalFile::open(walfile_path.clone()).await.unwrap();

    //         let walfile_impl = WalFileImpl {
    //             file_mutex: Mutex::new(walfile),
    //             path: walfile_path,
    //         };

    //         #[allow(clippy::unwrap_used)]
    //         walfile_impl
    //             .write(0, vec![1u8; LENGTH].into())
    //             .await
    //             .unwrap();

    //         #[allow(clippy::unwrap_used)]
    //         walfile_impl.truncate(2 * LENGTH).await.unwrap();

    //         #[allow(clippy::unwrap_used)]
    //         let result = walfile_impl.read(LENGTH as u64, LENGTH).await.unwrap();

    //         assert_eq!(result, Some(vec![0u8; LENGTH].into()))
    //     })
    // }
    #[test]
    fn write_and_read_full() {
        tokio_uring::start(async {
            let walfile_path = get_temp_walfile_path(file!(), line!());
            let walfile = {
                tokio::fs::remove_file(&walfile_path).await.ok();
                #[allow(clippy::unwrap_used)]
                RawWalFile::open(walfile_path.clone()).await.unwrap()
            };

            let walfile_impl = WalFileImpl {
                file_mutex: Mutex::new(walfile),
                path: walfile_path,
            };

            let data: Vec<u8> = (0..=u8::MAX).collect();

            #[allow(clippy::unwrap_used)]
            walfile_impl.write(0, data.clone().into()).await.unwrap();

            #[allow(clippy::unwrap_used)]
            let result = walfile_impl.read(0, data.len()).await.unwrap();

            assert_eq!(result, Some(data.into()));
        })
    }

    #[test]
    fn write_and_read_subset() {
        tokio_uring::start(async {
            let walfile_path = get_temp_walfile_path(file!(), line!());
            let walfile = {
                tokio::fs::remove_file(&walfile_path).await.ok();
                #[allow(clippy::unwrap_used)]
                RawWalFile::open(walfile_path.clone()).await.unwrap()
            };

            let walfile_impl = WalFileImpl {
                file_mutex: Mutex::new(walfile),
                path: walfile_path,
            };

            let data: Vec<u8> = (0..=u8::MAX).collect();
            #[allow(clippy::unwrap_used)]
            walfile_impl.write(0, data.clone().into()).await.unwrap();

            let mid = data.len() / 2;
            let (start, end) = data.split_at(mid);
            #[allow(clippy::unwrap_used)]
            let read_start_result = walfile_impl.read(0, mid).await.unwrap();
            #[allow(clippy::unwrap_used)]
            let read_end_result = walfile_impl.read(mid as u64, mid).await.unwrap();

            assert_eq!(read_start_result, Some(start.into()));
            assert_eq!(read_end_result, Some(end.into()));
        })
    }

    #[test]
    fn write_and_read_beyond_len() {
        tokio_uring::start(async {
            let walfile_path = get_temp_walfile_path(file!(), line!());
            let walfile = {
                tokio::fs::remove_file(&walfile_path).await.ok();
                #[allow(clippy::unwrap_used)]
                RawWalFile::open(walfile_path.clone()).await.unwrap()
            };

            let walfile_impl = WalFileImpl {
                file_mutex: Mutex::new(walfile),
                path: walfile_path,
            };

            let data: Vec<u8> = (0..=u8::MAX).collect();

            #[allow(clippy::unwrap_used)]
            walfile_impl.write(0, data.clone().into()).await.unwrap();

            #[allow(clippy::unwrap_used)]
            let result = walfile_impl
                .read((data.len() / 2) as u64, data.len())
                .await
                .unwrap();

            assert_eq!(result, None);
        })
    }

    #[test]
    fn write_at_offset() {
        tokio_uring::start(async {
            const OFFSET: u64 = 2;

            let walfile_path = get_temp_walfile_path(file!(), line!());
            let walfile = {
                tokio::fs::remove_file(&walfile_path).await.ok();
                #[allow(clippy::unwrap_used)]
                RawWalFile::open(walfile_path.clone()).await.unwrap()
            };

            let walfile_impl = WalFileImpl {
                file_mutex: Mutex::new(walfile),
                path: walfile_path,
            };

            let data: Vec<u8> = (0..=u8::MAX).collect();

            #[allow(clippy::unwrap_used)]
            walfile_impl
                .write(OFFSET, data.clone().into())
                .await
                .unwrap();

            #[allow(clippy::unwrap_used)]
            let result = walfile_impl
                .read(0, data.len() + OFFSET as usize)
                .await
                .unwrap();

            let data: Vec<_> = std::iter::repeat(0)
                .take(OFFSET as usize)
                .chain(data)
                .collect();

            assert_eq!(result, Some(data.into()));
        });
    }

    #[allow(clippy::unwrap_used)]
    fn get_temp_walfile_path(file: &str, line: u32) -> PathBuf {
        let path = option_env!("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or(std::env::temp_dir());
        path.join(format!("{}_{}", file.replace('/', "-"), line))
    }
}
