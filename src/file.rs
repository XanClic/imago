//! Use a plain as storage.

use crate::io_buffers::{IoVector, IoVectorMut};
use crate::{Storage, StorageOpenOptions};
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::{self, Seek, SeekFrom};
#[cfg(target_os = "macos")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(all(unix, not(target_os = "macos")))]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::RwLock;

/// Use a plain file as storage objects.
#[derive(Debug)]
pub struct File {
    /// The file.
    file: RwLock<fs::File>,

    /// Whether we are using direct I/O.
    direct_io: bool,

    /// For debug purposes.
    filename: PathBuf,
}

impl From<fs::File> for File {
    fn from(file: fs::File) -> Self {
        File {
            file: RwLock::new(file),
            // TODO: Find out, or better yet, drop `direct_io` and just probe the alignment.
            direct_io: false,
            filename: PathBuf::new(),
        }
    }
}

impl Storage for File {
    async fn open(opts: StorageOpenOptions) -> io::Result<Self> {
        let Some(filename) = opts.filename else {
            return Err(io::Error::other("Filename required"));
        };

        let mut file_opts = fs::OpenOptions::new();
        file_opts.read(true).write(opts.writable);
        #[cfg(not(target_os = "macos"))]
        if opts.direct {
            file_opts.custom_flags(
                #[cfg(unix)]
                libc::O_DIRECT,
                #[cfg(windows)]
                windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING,
            );
        }

        let filename_owned = filename.to_owned();
        let file = file_opts.open(filename)?;

        #[cfg(target_os = "macos")]
        if opts.direct {
            // Safe: We check the return value.
            let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
            if ret < 0 {
                let err = io::Error::last_os_error();
                return Err(io::Error::new(
                    err.kind(),
                    format!("Failed to disable host cache: {err}"),
                ));
            }
        }

        Ok(File {
            file: RwLock::new(file),
            direct_io: opts.direct,
            filename: filename_owned,
        })
    }

    fn mem_align(&self) -> usize {
        // TODO: Probe
        if self.direct_io {
            4096
        } else {
            1
        }
    }

    fn req_align(&self) -> usize {
        // TODO: Probe
        if self.direct_io {
            4096
        } else {
            1
        }
    }

    fn size(&self) -> io::Result<u64> {
        let mut file = self.file.write().unwrap();
        file.seek(SeekFrom::End(0))
    }

    #[cfg(unix)]
    async fn readv(&self, bufv: IoVectorMut<'_>, mut offset: u64) -> io::Result<()> {
        // TODO: Use `read_vectored_at()` once `unix_file_vectored_at` is stable
        for mut buffer in bufv.into_inner() {
            let next_offset = offset
                .checked_add(buffer.len() as u64)
                .ok_or_else(|| io::Error::other("Read offset overflow"))?;
            self.file
                .read()
                .unwrap()
                .read_exact_at(&mut buffer, offset)?;
            offset = next_offset;
        }
        Ok(())
    }

    #[cfg(windows)]
    async fn readv(&self, bufv: IoVectorMut<'_>, mut offset: u64) -> io::Result<()> {
        for mut buffer in bufv.into_inner() {
            let mut buffer: &mut [u8] = &mut buffer;
            while !buffer.is_empty() {
                let len = self.file.write().unwrap().seek_read(buffer, offset)?;
                offset = offset
                    .checked_add(len as u64)
                    .ok_or_else(|| io::Error::other("Read offset overflow"))?;
                buffer = buffer.split_at_mut(len).1;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn writev(&self, bufv: IoVector<'_>, mut offset: u64) -> io::Result<()> {
        // TODO: Use `write_vectored_at()` once `unix_file_vectored_at` is stable
        for buffer in bufv.into_inner() {
            let next_offset = offset
                .checked_add(buffer.len() as u64)
                .ok_or_else(|| io::Error::other("Write offset overflow"))?;
            self.file.read().unwrap().write_all_at(&buffer, offset)?;
            offset = next_offset;
        }
        Ok(())
    }

    #[cfg(windows)]
    async fn writev(&self, bufv: IoVector<'_>, mut offset: u64) -> io::Result<()> {
        for buffer in bufv.into_inner() {
            let mut buffer: &[u8] = &buffer;
            while !buffer.is_empty() {
                let len = self.file.write().unwrap().seek_write(buffer, offset)?;
                offset = offset
                    .checked_add(len as u64)
                    .ok_or_else(|| io::Error::other("Write offset overflow"))?;
                buffer = buffer.split_at(len).1;
            }
        }
        Ok(())
    }
}

impl Display for File {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "file:{}", self.filename.to_string_lossy())
    }
}
