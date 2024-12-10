//! Use a plain as storage.

#[cfg(unix)]
use crate::io_buffers::IoBuffer;
use crate::io_buffers::{IoVector, IoVectorMut};
#[cfg(unix)]
use crate::misc_helpers::while_eintr;
use crate::misc_helpers::ResultErrorContext;
use crate::storage::drivers::CommonStorageHelper;
use crate::storage::ext::write_full_zeroes;
use crate::storage::PreallocateMode;
use crate::{Storage, StorageOpenOptions};
use std::cmp;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::{self, Seek, SeekFrom, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;
#[cfg(all(unix, not(target_os = "macos")))]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::{FileExt, OpenOptionsExt};
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
#[cfg(unix)]
use tracing::{debug, warn};
#[cfg(windows)]
use windows_sys::Win32::System::Ioctl::{FILE_ZERO_DATA_INFORMATION, FSCTL_SET_ZERO_DATA};
#[cfg(windows)]
use windows_sys::Win32::System::IO::DeviceIoControl;

/// Use a plain file as storage objects.
#[derive(Debug)]
pub struct File {
    /// The file.
    file: RwLock<fs::File>,

    /// For debug purposes, and to resolve relative filenames.
    filename: Option<PathBuf>,

    /// Minimal I/O alignment for requests.
    req_align: usize,

    /// Minimal memory buffer alignment.
    mem_align: usize,

    /// Cached file length.
    ///
    /// Third parties changing the length concurrently is pretty certain to break things anyway.
    size: AtomicU64,

    /// Storage helper.
    common_storage_helper: CommonStorageHelper,
}

impl TryFrom<fs::File> for File {
    type Error = io::Error;

    /// Use the given existing `std::fs::File`.
    ///
    /// Convert the given existing `std::fs::File` object into an imago storage object.
    ///
    /// When using this, the resulting object will not know its own filename.  That makes it
    /// impossible to auto-resolve relative paths to it, e.g. qcow2 backing file names.
    fn try_from(file: fs::File) -> io::Result<Self> {
        Self::new(file, None, false)
    }
}

impl Storage for File {
    async fn open(opts: StorageOpenOptions) -> io::Result<Self> {
        Self::do_open_sync(opts)
    }

    #[cfg(feature = "sync-wrappers")]
    fn open_sync(opts: StorageOpenOptions) -> io::Result<Self> {
        Self::do_open_sync(opts)
    }

    fn mem_align(&self) -> usize {
        self.mem_align
    }

    fn req_align(&self) -> usize {
        self.req_align
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.size.load(Ordering::Relaxed))
    }

    fn resolve_relative_path<P: AsRef<Path>>(&self, relative: P) -> io::Result<PathBuf> {
        let relative = relative.as_ref();

        if relative.is_absolute() {
            return Ok(relative.to_path_buf());
        }

        let filename = self
            .filename
            .as_ref()
            .ok_or_else(|| io::Error::other("No filename set for base image"))?;

        let dirname = filename
            .parent()
            .ok_or_else(|| io::Error::other("Invalid base image filename set"))?;

        Ok(dirname.join(relative))
    }

    fn get_filename(&self) -> Option<PathBuf> {
        self.filename.as_ref().cloned()
    }

    #[cfg(unix)]
    async unsafe fn pure_readv(
        &self,
        mut bufv: IoVectorMut<'_>,
        mut offset: u64,
    ) -> io::Result<()> {
        while !bufv.is_empty() {
            let iovec = unsafe { bufv.as_iovec() };
            let preadv_offset = offset
                .try_into()
                .map_err(|_| io::Error::other("Read offset overflow"))?;

            let len = while_eintr(|| unsafe {
                libc::preadv(
                    self.file.read().unwrap().as_raw_fd(),
                    iovec.as_ptr(),
                    iovec.len() as libc::c_int,
                    preadv_offset,
                )
            })? as u64;

            if len == 0 {
                // End of file
                bufv.fill(0);
                break;
            }

            bufv = bufv.split_tail_at(len);
            offset = offset
                .checked_add(len)
                .ok_or_else(|| io::Error::other("Read offset overflow"))?;
        }

        Ok(())
    }

    #[cfg(windows)]
    async unsafe fn pure_readv(&self, bufv: IoVectorMut<'_>, mut offset: u64) -> io::Result<()> {
        for mut buffer in bufv.into_inner() {
            let mut buffer: &mut [u8] = &mut buffer;
            while !buffer.is_empty() {
                let len = if offset >= self.size.load(Ordering::Relaxed) {
                    buffer.fill(0);
                    buffer.len()
                } else {
                    self.file.write().unwrap().seek_read(buffer, offset)?
                };
                offset = offset
                    .checked_add(len as u64)
                    .ok_or_else(|| io::Error::other("Read offset overflow"))?;
                buffer = buffer.split_at_mut(len).1;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    async unsafe fn pure_writev(&self, mut bufv: IoVector<'_>, mut offset: u64) -> io::Result<()> {
        while !bufv.is_empty() {
            let iovec = unsafe { bufv.as_iovec() };
            let pwritev_offset = offset
                .try_into()
                .map_err(|_| io::Error::other("Write offset overflow"))?;

            let len = while_eintr(|| unsafe {
                libc::pwritev(
                    self.file.read().unwrap().as_raw_fd(),
                    iovec.as_ptr(),
                    iovec.len() as libc::c_int,
                    pwritev_offset,
                )
            })? as u64;

            if len == 0 {
                // Should not happen, i.e. is an error
                return Err(io::ErrorKind::WriteZero.into());
            }

            bufv = bufv.split_tail_at(len);
            offset = offset
                .checked_add(len)
                .ok_or_else(|| io::Error::other("Write offset overflow"))?;
            self.size.fetch_max(offset, Ordering::Relaxed);
        }

        Ok(())
    }

    #[cfg(windows)]
    async unsafe fn pure_writev(&self, bufv: IoVector<'_>, mut offset: u64) -> io::Result<()> {
        for buffer in bufv.into_inner() {
            let mut buffer: &[u8] = &buffer;
            while !buffer.is_empty() {
                let len = self.file.write().unwrap().seek_write(buffer, offset)?;
                offset = offset
                    .checked_add(len as u64)
                    .ok_or_else(|| io::Error::other("Write offset overflow"))?;
                self.size.fetch_max(offset, Ordering::Relaxed);
                buffer = buffer.split_at(len).1;
            }
        }
        Ok(())
    }

    #[cfg(any(target_os = "linux", windows, target_os = "macos"))]
    async unsafe fn pure_write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        // All of our discard methods also ensure the range reads back as zeroes
        unsafe { self.pure_discard(offset, length) }.await
    }

    // Beware when adding new discard methods: This is called by `pure_write_zeroes()`, so the
    // current expectation is that discarded ranges will read back as zeroes.  If the new method
    // does not guarantee that, you will need to modify `pure_write_zeroes()`.
    #[cfg(target_os = "linux")]
    async unsafe fn pure_discard(&self, offset: u64, length: u64) -> io::Result<()> {
        if self.try_discard_by_truncate(offset, length)? {
            return Ok(());
        }

        // If offset or length are too big, just skip discarding.
        let Ok(offset) = libc::off_t::try_from(offset) else {
            return Ok(());
        };
        let Ok(length) = libc::off_t::try_from(length) else {
            return Ok(());
        };

        let file = self.file.read().unwrap();
        // Safe: File descriptor is valid, and the rest are simple integer parameters.
        while_eintr(|| unsafe {
            libc::fallocate(
                file.as_raw_fd(),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                offset,
                length,
            )
        })?;

        Ok(())
    }

    // Beware when adding new discard methods: This is called by `pure_write_zeroes()`, so the
    // current expectation is that discarded ranges will read back as zeroes.  If the new method
    // does not guarantee that, you will need to modify `pure_write_zeroes()`.
    #[cfg(windows)]
    async unsafe fn pure_discard(&self, offset: u64, length: u64) -> io::Result<()> {
        if self.try_discard_by_truncate(offset, length)? {
            return Ok(());
        }

        // If offset or length are too big, just skip discarding.
        let Ok(offset) = i64::try_from(offset) else {
            return Ok(());
        };
        let Ok(length) = i64::try_from(length) else {
            return Ok(());
        };

        let end = offset.saturating_add(length).saturating_add(1);
        let params = FILE_ZERO_DATA_INFORMATION {
            FileOffset: offset,
            BeyondFinalZero: end,
        };
        let mut _returned = 0;
        let file = self.file.read().unwrap();
        // Safe: File handle is valid, mandatory pointers (input, returned length) are passed and
        // valid, the parameter type matches the call, and the input size matches the object
        // passed.
        let ret = unsafe {
            DeviceIoControl(
                file.as_raw_handle(),
                FSCTL_SET_ZERO_DATA,
                (&params as *const FILE_ZERO_DATA_INFORMATION).cast::<std::ffi::c_void>(),
                size_of_val(&params) as u32,
                std::ptr::null_mut(),
                0,
                &mut _returned,
                std::ptr::null_mut(),
            )
        };
        if ret == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    // Beware when adding new discard methods: This is called by `pure_write_zeroes()`, so the
    // current expectation is that discarded ranges will read back as zeroes.  If the new method
    // does not guarantee that, you will need to modify `pure_write_zeroes()`.
    #[cfg(target_os = "macos")]
    async unsafe fn pure_discard(&self, offset: u64, length: u64) -> io::Result<()> {
        if self.try_discard_by_truncate(offset, length)? {
            return Ok(());
        }

        // If offset or length are too big, just skip discarding.
        let Ok(offset) = libc::off_t::try_from(offset) else {
            return Ok(());
        };
        let Ok(length) = libc::off_t::try_from(length) else {
            return Ok(());
        };

        let params = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: offset,
            fp_length: length,
        };
        let file = self.file.read().unwrap();
        // Safe: FD is valid, passed pointer is valid and its type matches the call.
        while_eintr(|| unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &params) })?;

        Ok(())
    }

    async fn flush(&self) -> io::Result<()> {
        self.file.write().unwrap().flush()
    }

    async fn sync(&self) -> io::Result<()> {
        self.file.write().unwrap().sync_all()
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        // TODO: Figure out what to do.  Generally, `std::fs::File` does not have internal buffers,
        // so we don’t need to invalidate anything; we could close and reopen, but that would still
        // flush, and is difficult to do in a platform-independent way (/proc/self/fd would allow
        // this on Linux).  Using e.g. the filename is not safe.
        // Right now, it’s best not to do anything.
        Ok(())
    }

    fn get_storage_helper(&self) -> &CommonStorageHelper {
        &self.common_storage_helper
    }

    async fn resize(&self, new_size: u64, prealloc_mode: PreallocateMode) -> io::Result<()> {
        let file = self.file.write().unwrap();
        let current_size = self.size.load(Ordering::Relaxed);

        match new_size.cmp(&current_size) {
            std::cmp::Ordering::Equal => return Ok(()),
            std::cmp::Ordering::Less => {
                file.set_len(new_size)?;
                self.size.fetch_min(new_size, Ordering::Relaxed);
                return Ok(());
            }
            std::cmp::Ordering::Greater => (), // handled below
        }

        match prealloc_mode {
            PreallocateMode::None | PreallocateMode::Zero => file.set_len(new_size)?,
            PreallocateMode::Allocate => {
                #[cfg(not(unix))]
                return Err(io::ErrorKind::Unsupported.into());

                #[cfg(all(unix, not(target_os = "macos")))]
                {
                    let ofs = current_size.try_into().map_err(io::Error::other)?;
                    let len = (new_size - current_size)
                        .try_into()
                        .map_err(io::Error::other)?;
                    while_eintr(|| unsafe { libc::fallocate(file.as_raw_fd(), 0, ofs, len) })?;
                }

                #[cfg(target_os = "macos")]
                {
                    // Best-effort.  PEOFPOSMODE allocates from the “physical” EOF, wherever that
                    // may be, but the only alternative would be VOLPOSMODE, which nobody knows the
                    // meaning of.  Also doesn’t change the file length, we need to truncate
                    // afterwards still.
                    let mut params = libc::fstore_t {
                        fst_flags: libc::F_ALLOCATEALL,
                        fst_posmode: libc::F_PEOFPOSMODE,
                        fst_offset: 0,
                        fst_length: (new_size - current_size)
                            .try_into()
                            .map_err(io::Error::other)?,
                        fst_bytesalloc: 0, // output
                    };
                    while_eintr(|| unsafe {
                        libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut params)
                    })?;

                    file.set_len(new_size)?;
                }
            }
            PreallocateMode::WriteData => {
                // FIXME: Keeping the lock would be nice, but resizing concurrently with I/O is
                // pretty risky anyway.
                drop(file);
                write_full_zeroes(self, current_size, new_size - current_size).await?;
            }
        }

        self.size.fetch_max(new_size, Ordering::Relaxed);
        Ok(())
    }
}

impl File {
    /// Central internal function to create a `File` object.
    ///
    /// `direct_io` should be `true` if direct I/O was requested, and can be `false` if that status
    /// is unknown.
    fn new(mut file: fs::File, filename: Option<PathBuf>, direct_io: bool) -> io::Result<Self> {
        let size = file
            .seek(SeekFrom::End(0))
            .err_context(|| "Failed to determine file size")?;

        #[cfg(all(unix, not(target_os = "macos")))]
        let direct_io = direct_io || {
            // Safe: No argument, returns result.
            let res = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
            res > 0 && (res & libc::O_DIRECT) != 0
        };

        let (min_req_align, min_mem_align) = if direct_io {
            #[cfg(unix)]
            {
                (
                    Self::get_min_dio_req_align(&file),
                    Self::get_min_dio_mem_align(&file),
                )
            }

            #[cfg(not(unix))]
            {
                (1, 1)
            } // probe it then
        } else {
            (1, 1)
        };

        let (req_align, mem_align) =
            Self::probe_alignments(&mut file, min_req_align, min_mem_align);
        assert!(req_align.is_power_of_two());
        assert!(mem_align.is_power_of_two());

        Ok(File {
            file: RwLock::new(file),
            filename,
            req_align,
            mem_align,
            size: size.into(),
            common_storage_helper: Default::default(),
        })
    }

    /// Probe minimal request and memory alignments.
    ///
    /// Start at `min_req_align` and `min_mem_align`.
    #[cfg(unix)]
    fn probe_alignments(
        file: &mut fs::File,
        min_req_align: usize,
        min_mem_align: usize,
    ) -> (usize, usize) {
        let mut page_size = page_size::get();
        if !page_size.is_power_of_two() {
            let assume = page_size.checked_next_power_of_two().unwrap_or(4096);
            let assume = cmp::max(4096, assume);
            warn!("Reported page size of {page_size} is not a power of two, assuming {assume}");
            page_size = assume;
        }

        let mut writable = true;

        let max_req_align = 65536;
        let max_mem_align = cmp::max(page_size, max_req_align);

        // Minimum fallbacks in case something goes wrong.
        let safe_req_align = 4096;
        let safe_mem_align = cmp::max(page_size, safe_req_align);

        let mut test_buf = match IoBuffer::new(max_mem_align, max_mem_align) {
            Ok(buf) => buf,
            Err(err) => {
                warn!(
                    "Failed to allocate memory to probe request alignment ({err}), \
                    falling back to {safe_req_align}/{safe_mem_align}"
                );
                return (safe_req_align, safe_mem_align);
            }
        };

        let mut req_align: usize = min_req_align;
        let result = loop {
            assert!(req_align <= max_mem_align);
            match Self::probe_access(
                file,
                test_buf.as_mut_range(0..req_align).into_slice(),
                req_align.try_into().unwrap(),
                &mut writable,
            ) {
                Ok(true) => break Ok(req_align),
                Ok(false) => {
                    if req_align >= max_req_align {
                        break Err(io::Error::other(
                            "Maximum I/O alignment ({max_req_align}) exceeded",
                        ));
                    }
                    // No reason to probe anything between 1 and 512
                    if req_align == min_req_align {
                        req_align = cmp::max(min_req_align << 1, 512);
                    } else {
                        req_align <<= 1;
                    }
                }
                Err(err) => break Err(err),
            }
        };

        let req_align = match result {
            Ok(align) => {
                debug!("Probed request alignment: {align}");
                align
            }
            Err(err) => {
                // Failed to determine request alignment, use a presumably safe value
                let align = cmp::max(req_align, safe_req_align);
                warn!(
                    "Failed to probe request alignment ({}; {}), falling back to {} bytes",
                    err,
                    err.kind(),
                    align
                );
                align
            }
        };

        let mut mem_align: usize = min_mem_align;
        let result = loop {
            assert!(mem_align <= max_mem_align);
            let range = (max_mem_align - mem_align)..max_mem_align;
            match Self::probe_access(
                file,
                test_buf.as_mut_range(range).into_slice(),
                0,
                &mut writable,
            ) {
                Ok(true) => break Ok(mem_align),
                Ok(false) => {
                    // Not aligned
                    if mem_align >= max_mem_align {
                        break Err(io::Error::other(
                            "Maximum memory alignment ({max_mem_align}) exceeded",
                        ));
                    }
                    // No reason to probe anything between 1 and the page size (or 4096 at least)
                    if mem_align == min_mem_align {
                        mem_align = cmp::max(min_mem_align << 1, cmp::min(page_size, 4096));
                    } else {
                        mem_align <<= 1;
                    }
                }
                Err(err) => break Err(err),
            }
        };

        let mem_align = match result {
            Ok(align) => {
                debug!("Probed memory alignment: {align}");
                align
            }
            Err(err) => {
                // Failed to determine memory alignment, use a presumably safe value
                let align = cmp::max(mem_align, safe_mem_align);
                warn!(
                    "Failed to probe memory alignment ({}; {}), falling back to {} bytes",
                    err,
                    err.kind(),
                    align
                );
                align
            }
        };

        (req_align, mem_align)
    }

    /// Do an alignment-probing I/O access.
    ///
    /// Return `Ok(true)` if everything was OK, and `Ok(false)` if the request was reported to be
    /// misaligned.
    ///
    /// `may_write` is a boolean that controls whether this is allowed to write (the same data read
    /// before) to improve reliability.  Is automatically set to `false` if writing is found to not
    /// be possible.
    #[cfg(unix)]
    fn probe_access(
        file: &mut fs::File,
        slice: &mut [u8],
        offset: libc::off_t,
        may_write: &mut bool,
    ) -> io::Result<bool> {
        // Use `libc::pread` so we get well-defined errors.
        // Safe: Passing the slice as the buffer it is.
        let ret = while_eintr(|| unsafe {
            libc::pread(
                file.as_raw_fd(),
                slice.as_mut_ptr() as *mut libc::c_void,
                slice.len(),
                offset,
            )
        });

        if let Err(err) = ret {
            if err.raw_os_error() == Some(libc::EINVAL) {
                return Ok(false);
            } else {
                return Err(err);
            }
        }

        if !*may_write {
            return Ok(true);
        }

        // Safe: Passing the slice as the buffer it is.
        let ret = while_eintr(|| unsafe {
            libc::pwrite(
                file.as_raw_fd(),
                slice.as_ptr() as *const libc::c_void,
                slice.len(),
                offset,
            )
        });

        if let Err(err) = ret {
            if err.raw_os_error() == Some(libc::EINVAL) {
                Ok(false)
            } else if err.raw_os_error() == Some(libc::EBADF) {
                *may_write = false;
                Ok(true)
            } else {
                Err(err)
            }
        } else {
            Ok(true)
        }
    }

    /// Get system-reported minimum request alignment for direct I/O.
    #[cfg(unix)]
    fn get_min_dio_req_align(file: &fs::File) -> usize {
        #[cfg(target_os = "linux")]
        {
            let mut alignment: libc::c_int = 0;
            // Safe: BLKSSZGET wants an int.
            let res = unsafe {
                libc::ioctl(
                    file.as_raw_fd(),
                    libc::BLKSSZGET,
                    ptr::addr_of_mut!(alignment),
                )
            };
            if res >= 0 && alignment > 0 {
                let alignment = alignment as usize;
                if alignment.is_power_of_two() {
                    return alignment;
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            let mut alignment: u32 = 0;
            // Safe: DKIOCGETBLOCKSIZE wants a uint32_t.
            let res = unsafe {
                libc::ioctl(
                    file.as_raw_fd(),
                    0x40046418, // libc::DKIOCGETBLOCKSIZE
                    ptr::addr_of_mut!(alignment),
                )
            };
            if res >= 0 && alignment.is_power_of_two() {
                return alignment as usize;
            }
        }

        #[cfg(target_os = "freebsd")]
        {
            let mut alignment: libc::c_uint = 0;
            // Safe: DIOCGSECTORSIZE wants an unsigned int.
            let res = unsafe {
                libc::ioctl(
                    file.as_raw_fd(),
                    libc::DIOCGSECTORSIZE,
                    ptr::addr_of_mut!(alignment),
                )
            };
            if res >= 0 && alignment.is_power_of_two() {
                return alignment as usize;
            }
        }

        // Then we’ll probe.
        1
    }

    /// Get system-reported minimum memory alignment for direct I/O.
    #[cfg(unix)]
    fn get_min_dio_mem_align(_file: &fs::File) -> usize {
        // I don’t think there’s a reliable way to get this.
        1
    }

    /// Probe minimal request and memory alignments.
    ///
    /// Start at `min_req_align` and `min_mem_align`.
    #[cfg(windows)]
    fn probe_alignments(
        _file: &mut fs::File,
        min_req_align: usize,
        min_mem_align: usize,
    ) -> (usize, usize) {
        // TODO: Need to find out how Windows indicates unaligned I/O
        (cmp::max(min_req_align, 4096), cmp::max(min_mem_align, 4096))
    }

    /// Implementation for [`File::open()`] and [`File::open_sync()`].
    fn do_open_sync(opts: StorageOpenOptions) -> io::Result<Self> {
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
            while_eintr(|| unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) })
                .err_context(|| "Failed to disable host cache")?;
        }

        Self::new(file, Some(filename_owned), opts.direct)
    }

    /// Attempt to discard range by truncating the file.
    ///
    /// If the given range is at the end of the file, discard it by simply truncating the file.
    /// Return `true` on success.
    ///
    /// If the range is not at the end of the file, i.e. another method of discarding is needed,
    /// return `false`.
    fn try_discard_by_truncate(&self, offset: u64, length: u64) -> io::Result<bool> {
        // Prevent modifications to the file length
        #[allow(clippy::readonly_write_lock)]
        let file = self.file.write().unwrap();

        let size = self.size.load(Ordering::Relaxed);
        if offset >= size {
            // Nothing to do
            return Ok(true);
        }

        // If `offset + length` overflows, we can just assume it ends at `size`.  (Anything past
        // `size is irrelevant anyway.)
        let end = offset.checked_add(length).unwrap_or(size);
        if end < size {
            return Ok(false);
        }

        file.set_len(offset)?;
        Ok(true)
    }
}

impl Display for File {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(filename) = self.filename.as_ref() {
            write!(f, "file:{filename:?}")
        } else {
            write!(f, "file:<unknown path>")
        }
    }
}
