//! VMDK implementation.
use crate::format::builder::{FormatDriverBuilder, FormatDriverBuilderBase};
use crate::format::gate::{ImplicitOpenGate, PermissiveImplicitOpenGate};
use crate::format::wrapped::WrappedFormat;
use crate::format::{Format, PreallocateMode};
use crate::misc_helpers::ResultErrorContext;
use crate::raw::Raw;
use crate::{FormatAccess, Storage, StorageOpenOptions};
use std::io;
use std::sync::Arc;

use crate::format::drivers::FormatDriverInstance;
use crate::storage::ext::StorageExt;
use crate::ShallowMapping;
use async_trait::async_trait;
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// As usual, VMDK sector size is 512 bytes as a fixed value
const VMDK_SECTOR_SIZE: u64 = 512;
/// VMDK SPARSE data signature
const VMDK4_MAGIC: u32 = 0x564d444b; // 'KDMV'

/// Represents the backing storage for a VMDK extent
#[derive(Debug, Clone)]
enum VmdkBacking<S: Storage + 'static, F: WrappedFormat<S> + 'static> {
    /// A FLAT extent backing with a RAW file starting from the exact offset
    IndirectFlat(F, u64),
    /// A zero-filled extent
    Zero(),
    /// An unreachable variant used as a Storage marker
    _Unreachable(PhantomData<S>),
}

/// Access type for VMDK extents
#[derive(Debug, Clone, PartialEq)]
enum VmdkAccessType {
    /// Read-write access
    RW,
    /// Read-only access
    RdOnly,
    /// No access
    NoAccess,
}

/// VMDK extent descriptor
#[derive(Debug, Clone)]
struct VmdkExtent<S: Storage + 'static, F: WrappedFormat<S> + 'static> {
    /// Access type (RW, RDONLY, NOACCESS).
    access_type: VmdkAccessType,
    /// Number of sectors.
    sectors: u64,
    /// Backing source
    backing: VmdkBacking<S, F>,

    /// Phantom data to hold the storage type.
    _storage: PhantomData<S>,
}

/// VMDK disk image format implementation.
#[derive(Debug)]
pub struct Vmdk<S: Storage + 'static, F: WrappedFormat<S> + 'static> {
    /// Storage object containing the VMDK metadata.
    metadata: Arc<S>,

    /// Base options to be used for implicitly opened storage objects.
    storage_open_options: StorageOpenOptions,

    /// Virtual disk size in bytes.
    size: AtomicU64,

    /// Parsed VMDK descriptor.
    desc: VmdkDesc,

    /// Storage objects for each extent.
    extents: Vec<VmdkExtent<S, F>>,
}

/// VMDK descriptor information.
#[derive(Debug, Clone)]
pub struct VmdkDesc {
    /// Version number of the VMDK descriptor
    pub version: u32,
    /// Content ID
    pub cid: String,
    /// Content ID of the parent link
    pub parent_cid: String,
    /// Type of virtual disk
    pub create_type: String,
    /// The disk geometry value (sectors)
    pub sectors: u64,
    /// The disk geometry value (heads)
    pub heads: u64,
    /// The disk geometry value (cylinders)
    pub cylinders: u64,
}

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Vmdk<S, F> {
    /// Parse an extent descriptor line.
    async fn parse_extent_line(&self, line: &str) -> io::Result<VmdkExtent<S, F>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        // https://github.com/libyal/libvmdk/blob/main/documentation/VMWare%20Virtual%20Disk%20Format%20(VMDK).asciidoc#221-extent-descriptor
        // At least 3 parts are required for all VMDK extents
        if parts.len() < 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid extent format",
            ));
        }

        let access_type = match parts[0] {
            "RW" => VmdkAccessType::RW,
            "RDONLY" => VmdkAccessType::RdOnly,
            "NOACCESS" => VmdkAccessType::NoAccess,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid access type",
                ))
            }
        };

        let sectors = parts[1]
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid sector count"))?;
        let extent_type = parts[2];

        if extent_type == "ZERO" {
            return Ok(VmdkExtent {
                access_type,
                sectors,
                backing: VmdkBacking::Zero(),
                _storage: PhantomData,
            });
        }
        if extent_type != "FLAT" {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Unsupported extent type {extent_type}"),
            ));
        }

        if parts.len() != 5 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid FLAT extent format",
            ));
        }

        // filename should be in quotes
        let filename = parts[3];
        if !filename.starts_with('"') || !filename.ends_with('"') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Filename must be quoted",
            ));
        }
        let filename = &filename[1..filename.len() - 1];

        let absolute = self
            .metadata
            .resolve_relative_path(filename)
            .err_context(|| format!("Cannot resolve backing file name {filename}"))?;

        let file_opts = self
            .storage_open_options
            .clone()
            .filename(absolute.clone())
            .write(false);

        let mut gate = PermissiveImplicitOpenGate::default();
        let file = gate
            .open_storage(file_opts)
            .await
            .err_context(|| format!("Backing file {absolute:?}"))?;
        let opts = Raw::builder(file).storage_open_options(self.storage_open_options.clone());
        let raw = gate.open_format(opts).await?;

        let offset = parts[4]
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid offset"))?;
        let backing = VmdkBacking::IndirectFlat(F::wrap(FormatAccess::new(raw)), offset);

        Ok(VmdkExtent {
            access_type,
            sectors,
            backing,
            _storage: PhantomData,
        })
    }

    /// Checks if the VMDK version is supported and returns an error if not
    async fn error_out_unsupported_version(&self) -> io::Result<()> {
        let version = self.desc.version;
        if !(1..=3).contains(&version) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("unsupported version {version}"),
            ));
        }
        Ok(())
    }

    /// Parse the VMDK descriptor
    async fn parse_vmdk_descriptor(&mut self, line: &str) -> io::Result<()> {
        let line = line.trim();

        if line.is_empty() || line.starts_with('#') {
            return Ok(());
        }

        // Parse extent descriptors (RW/RDONLY/NOACCESS)
        if line.starts_with("RW ") || line.starts_with("RDONLY ") || line.starts_with("NOACCESS ") {
            let extent = self.parse_extent_line(line).await?;
            self.extents.push(extent);
            return Ok(());
        }

        let v: Vec<_> = line.split("=").map(|a| a.trim()).collect();

        if v.is_empty() || v.len() != 2 {
            return Ok(());
        }

        match v[0] {
            "version" => {
                let version: u32 = v[1].parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid version format")
                })?;

                self.desc.version = version;
                self.error_out_unsupported_version().await?;
                return Ok(());
            }
            "CID" => {
                self.desc.cid = v[1].to_string();
                return Ok(());
            }
            "parentCID" => {
                self.desc.parent_cid = v[1].to_string();
                return Ok(());
            }
            "createType" => {
                let mut stripped = v[1];
                if stripped.starts_with('"') && stripped.ends_with('"') && stripped.len() >= 2 {
                    stripped = &stripped[1..stripped.len() - 1]
                }
                self.desc.create_type = stripped.to_string();
                return Ok(());
            }
            "parentFileNameHint" => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "unsupported VMDK differential image (delta link)",
                ))
            }

            "ddb.geometry.sectors" => {
                let mut stripped = v[1];
                if stripped.starts_with('"') && stripped.ends_with('"') && stripped.len() >= 2 {
                    stripped = &stripped[1..stripped.len() - 1]
                }
                self.desc.sectors = stripped.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid ddb.geometry.sectors")
                })?;
                return Ok(());
            }
            "ddb.geometry.heads" => {
                let mut stripped = v[1];
                if stripped.starts_with('"') && stripped.ends_with('"') && stripped.len() >= 2 {
                    stripped = &stripped[1..stripped.len() - 1]
                }
                self.desc.heads = stripped.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid ddb.geometry.heads")
                })?;
                return Ok(());
            }
            "ddb.geometry.cylinders" => {
                let mut stripped = v[1];
                if stripped.starts_with('"') && stripped.ends_with('"') && stripped.len() >= 2 {
                    stripped = &stripped[1..stripped.len() - 1]
                }
                self.desc.cylinders = stripped.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid ddb.geometry.cylinders")
                })?;
                return Ok(());
            }
            _ => (),
        }

        // Ignore unidentified "ddb." (The Disk Database) items
        if v[0].starts_with("ddb.") {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid VMDK descriptor line",
        ))
    }

    /// Read a single VMDK descriptor line
    async fn read_descriptor_line(
        metadata: &S,
        buffer: &mut Vec<u8>,
        offset: &mut u64,
    ) -> io::Result<bool> {
        buffer.clear();

        loop {
            // Extend buffer if needed
            let old_len = buffer.len();
            buffer.resize(old_len + 65536, 0);

            // Read the chunk
            match metadata
                .read(&mut buffer[old_len..], *offset + old_len as u64)
                .await
            {
                Ok(_) => {
                    // Check for NIL terminator or '\n' in the newly read chunk
                    let new_data = &buffer[old_len..];
                    if let Some(pos) = new_data.iter().position(|&b| b == 0 || b == b'\n') {
                        let eof = buffer[old_len + pos] == 0;

                        buffer.truncate(old_len + pos);
                        *offset += (old_len + pos) as u64 + (!eof as u64);
                        return Ok(eof);
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    /// Internal implementation for opening a VMDK image.
    async fn do_open(metadata: S, storage_open_options: StorageOpenOptions) -> io::Result<Self> {
        let mut q = Vmdk {
            metadata: Arc::new(metadata),
            desc: VmdkDesc {
                version: 0,
                cid: String::new(),
                parent_cid: String::new(),
                create_type: String::new(),
                sectors: 0,
                heads: 0,
                cylinders: 0,
            },
            extents: vec![],
            size: 0.into(),
            storage_open_options,
        };

        // Check if it's a SPARSE format, bail it out now
        let mut magic_buf = vec![0u8; 4];
        q.metadata.read(&mut magic_buf, 0).await?;
        if let Ok(magic) = magic_buf.as_slice().try_into().map(u32::from_le_bytes) {
            if magic == VMDK4_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "unsupported VMDK sparse data file",
                ));
            }
        }

        // Read and parse the VMDK descriptor by reading in lines until we find the end
        let mut line = Vec::new();
        let mut empty = true;
        let mut offset = 0;
        loop {
            let eof = Self::read_descriptor_line(&q.metadata, &mut line, &mut offset).await?;

            if eof {
                break;
            }
            let res: Result<&str, std::io::Error> = match std::str::from_utf8(&line) {
                Ok(l) => Ok(l),
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            q.parse_vmdk_descriptor(res?).await?;
            empty = false
        }

        if empty {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Empty VMDK descriptor",
            ));
        }

        q.size = (q.desc.sectors * q.desc.heads * q.desc.cylinders * VMDK_SECTOR_SIZE).into();
        Ok(q)
    }

    /// Opens a VMDK file.
    pub async fn open_image(metadata: S, writable: bool) -> io::Result<Self> {
        if writable {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported writable VMDK",
            ));
        }
        Self::do_open(metadata, StorageOpenOptions::new()).await
    }

    /// Wrap `inner`, allowing it to be used as a disk image in raw format.
    #[cfg(feature = "sync-wrappers")]
    pub fn open_image_sync(metadata: S, writable: bool) -> io::Result<Self> {
        tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(Self::open_image(metadata, writable))
    }

    /// A pseudo synchronous wrapper. Currently it's just a placeholder
    /// since I'm not sure if VMDK needs this compared to QCOW2.
    #[cfg(feature = "sync-wrappers")]
    pub fn open_implicit_dependencies_sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Display for Vmdk<S, F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "vmdk[{}]", self.metadata)
    }
}

#[async_trait(?Send)]
impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> FormatDriverInstance for Vmdk<S, F> {
    type Storage = S;

    fn format(&self) -> Format {
        Format::Vmdk
    }

    async unsafe fn probe(_storage: &S) -> io::Result<bool>
    where
        Self: Sized,
    {
        Ok(true)
    }

    fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    fn zero_granularity(&self) -> Option<u64> {
        None
    }

    fn collect_storage_dependencies(&self) -> Vec<&S> {
        let mut v = Vec::new();

        for e in &self.extents {
            if let VmdkBacking::IndirectFlat(inner, _) = &e.backing {
                v.append(&mut inner.inner().inner().collect_storage_dependencies())
            }
        }
        v
    }

    fn writable(&self) -> bool {
        false
    }

    async fn get_mapping<'a>(
        &'a self,
        mut offset: u64,
        max_length: u64,
    ) -> io::Result<(ShallowMapping<'a, S>, u64)> {
        let mut remaining = match self.size().checked_sub(offset) {
            None | Some(0) => return Ok((ShallowMapping::Eof {}, 0)),
            Some(remaining) => std::cmp::min(remaining, max_length),
        };

        for e in &self.extents {
            let bytes = e.sectors * VMDK_SECTOR_SIZE;

            if offset > bytes {
                offset -= bytes;
                remaining -= bytes;
                continue;
            }

            if e.access_type == VmdkAccessType::NoAccess {
                return Err(io::Error::other("NOACCESS extent is accessed"));
            }

            match &e.backing {
                VmdkBacking::IndirectFlat(inner, base_offset) => {
                    let offset = offset + base_offset;
                    return Ok((
                        ShallowMapping::Indirect {
                            layer: inner.inner(),
                            offset,
                            writable: false,
                        },
                        std::cmp::min(bytes, remaining),
                    ));
                }
                VmdkBacking::Zero() => {
                    return Ok((
                        ShallowMapping::Zero { explicit: true },
                        std::cmp::min(bytes, remaining),
                    ));
                }
                _ => todo!(),
            }
        }
        Ok((ShallowMapping::Eof {}, 0))
    }

    async fn ensure_data_mapping<'a>(
        &'a self,
        _offset: u64,
        _length: u64,
        _overwrite: bool,
    ) -> io::Result<(&'a S, u64, u64)> {
        Err(io::Error::other("Image is read-only"))
    }

    async fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    async fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    async unsafe fn invalidate_cache(&self) -> io::Result<()> {
        Ok(())
    }

    async fn resize_grow(&self, _new_size: u64, _prealloc_mode: PreallocateMode) -> io::Result<()> {
        Err(io::Error::other("Image is read-only"))
    }

    async fn resize_shrink(&mut self, _new_size: u64) -> io::Result<()> {
        Err(io::Error::other("Image is read-only"))
    }
}

/// Options builder for opening a VMDK image.
pub struct VmdkOpenBuilder<S: Storage + 'static, F: WrappedFormat<S> + 'static = FormatAccess<S>>(
    FormatDriverBuilderBase<S>,
    PhantomData<F>,
);

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> FormatDriverBuilder<S>
    for VmdkOpenBuilder<S, F>
{
    type Format = Vmdk<S, F>;
    const FORMAT: Format = Format::Vmdk;

    fn new(image: S) -> Self {
        VmdkOpenBuilder(FormatDriverBuilderBase::new(image), PhantomData)
    }

    fn new_path<P: AsRef<Path>>(path: P) -> Self {
        VmdkOpenBuilder(FormatDriverBuilderBase::new_path(path), PhantomData)
    }

    fn write(mut self, writable: bool) -> Self {
        self.0.set_write(writable);
        self
    }

    fn storage_open_options(mut self, options: StorageOpenOptions) -> Self {
        self.0.set_storage_open_options(options);
        self
    }

    async fn open<G: ImplicitOpenGate<S>>(self, mut gate: G) -> io::Result<Self::Format> {
        let writable = self.0.get_writable();
        let file = self.0.open_image(&mut gate).await?;
        Vmdk::open_image(file, writable).await
    }

    fn get_image_path(&self) -> Option<PathBuf> {
        self.0.get_image_path()
    }

    fn get_writable(&self) -> bool {
        self.0.get_writable()
    }

    fn get_storage_open_options(&self) -> Option<&StorageOpenOptions> {
        self.0.get_storage_opts()
    }
}
