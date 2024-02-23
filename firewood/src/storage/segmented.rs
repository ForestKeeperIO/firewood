use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    pin::Pin, task::Poll,
};

use futures::Future as _;
use lru::LruCache;
use tokio::{
    fs::File, io::{AsyncRead, AsyncSeek, AsyncWrite}, sync::Mutex, task::{spawn_blocking, JoinHandle}
};

macro_rules! ready {
    ($e:expr $(,)?) => {
        match $e {
            std::task::Poll::Ready(t) => t,
            std::task::Poll::Pending => return std::task::Poll::Pending,
        }
    };
}

/// segmented file implementation
/// Given a base directory, this struct implements reading and writing to different "segments" based on a size limit

/// The ID of the segment of a file
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct SegmentId(u32);

enum State {
    Idle,
    Busy(JoinHandle<Operation>),
}

#[derive(Debug)]
enum Operation {
    Open(Result<std::fs::File, std::io::Error>),
}

pub struct SegmentedFile {
    base_dir: PathBuf,
    per_file_size: u64,
    offset: u64,
    inner: Mutex<Inner>,
}

struct Inner {
    open_files: LruCache<SegmentId, (u64, tokio::fs::File)>,
    state: State,
}

impl SegmentedFile {
    const fn segment_from_offset(&self, offset: u64) -> SegmentId {
        SegmentId((offset / self.per_file_size) as u32)
    }

    const fn offset_within_segment(&self, offset: u64) -> u64 {
        offset % self.per_file_size
    }

    fn path_from_segment(base_dir: PathBuf, segment: SegmentId) -> PathBuf {
        [&base_dir, &PathBuf::from(format!("{:08}.fw", segment.0))]
            .iter()
            .collect()
    }

    pub fn open<T: AsRef<Path>>(base_dir: T, per_file_size: u64, cap: NonZeroUsize) -> Self {
        SegmentedFile {
            base_dir: PathBuf::from(base_dir.as_ref()),
            inner: Mutex::new(Inner {
                open_files: LruCache::new(cap),
                state: State::Idle,
            }),
            per_file_size,
            offset: 0,
        }
    }
}

impl AsyncRead for SegmentedFile {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let segment = self.segment_from_offset(self.offset);
        let segment_offset = self.offset_within_segment(self.offset);
        let me = self.get_mut();
        let inner = me.inner.get_mut();
        loop {
            match inner.state {
                State::Busy(ref mut rx) => {
                    let op = ready!(Pin::new(rx).poll(cx))?;
                    match op {
                        Operation::Open(std) => {
                            inner.open_files.put(segment, (0, File::from_std(std?)));
                            inner.state = State::Idle;
                        }
                    }
                }
                State::Idle => match inner.open_files.get_mut(&segment) {
                    Some((offset, ref mut handle)) => {
                        if *offset != segment_offset {
                            todo!("seek required");
                        }
                        let result = ready!(Pin::new(handle).poll_read(cx, buf));
                        return Poll::Ready(result);
                    }
                    None => {
                        let path = SegmentedFile::path_from_segment(me.base_dir.clone(), segment);
                        inner.state = State::Busy(spawn_blocking(move || {
                            //let res = std::fs::File::open(path);
                            let res = std::fs::File::options().create(true).write(true).read(true).open(path);
                            Operation::Open(res)
                        }))
                    }
                },
            }
        }
    }
}

impl AsyncWrite for SegmentedFile {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let segment = self.segment_from_offset(self.offset);
        let segment_offset = self.offset_within_segment(self.offset);
        let me = self.get_mut();
        let inner = me.inner.get_mut();
        loop {
            match inner.state {
                State::Busy(ref mut rx) => {
                    let op = ready!(Pin::new(rx).poll(cx))?;
                    match op {
                        Operation::Open(std) => {
                            inner.open_files.put(segment, (0, File::from_std(std?)));
                            inner.state = State::Idle;
                        }
                    }
                }
                State::Idle => match inner.open_files.get_mut(&segment) {
                    Some((offset, ref mut handle)) => {
                        if *offset != segment_offset {
                            todo!("seek required");
                        }
                        let result = ready!(Pin::new(handle).poll_write(cx, buf));
                        return Poll::Ready(result);
                    }
                    None => {
                        let path = SegmentedFile::path_from_segment(me.base_dir.clone(), segment);
                        inner.state = State::Busy(spawn_blocking(move || {
                            //let res = std::fs::File::open(path);
                            let res = std::fs::File::options().create(true).write(true).read(true).open(path);
                            Operation::Open(res)
                        }))
                    }
                },
            }
        }
    }

    /// flush all the files
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        todo!()
    }
}
impl AsyncSeek for SegmentedFile {
    fn start_seek(
        self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let segment = self.segment_from_offset(self.offset);
        let segment_offset = self.offset_within_segment(self.offset);
        let me = self.get_mut();
        let inner = me.inner.get_mut();
        loop {
            match inner.state {
                State::Busy(ref mut rx) => {
                    let op = ready!(Pin::new(rx).poll(cx))?;
                    match op {
                        Operation::Open(std) => {
                            inner.open_files.put(segment, (0, File::from_std(std?)));
                            inner.state = State::Idle;
                        }
                    }
                }
                State::Idle => match inner.open_files.get_mut(&segment) {
                    Some((offset, ref mut handle)) => {
                        if *offset != segment_offset {
                            todo!("seek required");
                        }
                        let result = std::pin::pin!(handle).start_seek(position);
                        return Poll::Ready(result);
                    }
                    None => {
                        let path = SegmentedFile::path_from_segment(me.base_dir.clone(), segment);
                        inner.state = State::Busy(spawn_blocking(move || {
                            //let res = std::fs::File::open(path);
                            let res = std::fs::File::options().create(true).write(true).read(true).open(path);
                            Operation::Open(res)
                        }))
                    }
                },
            }
        }
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        todo!()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn open_test() -> Result<(), Box<dyn std::error::Error>> {
        let mut sf = SegmentedFile::open("/tmp", 2048, NonZeroUsize::new(16).unwrap());
        let buf = [0u8; 32];
        sf.write_all(&buf).await.unwrap();
        sf.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut buf2 = [1u8; 32];
        sf.read_exact(&mut buf2).await.unwrap();
        assert_eq!(buf, buf2);
        Ok(())
    }
}
