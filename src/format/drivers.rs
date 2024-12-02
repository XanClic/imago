//! Internal image format driver interface.
//!
//! Provides the internal interface for image format drivers to provide their services, on which
//! the publically visible interface [`FormatAccess`] is built.

use super::Format;
use crate::io_buffers::IoVectorMut;
use crate::{FormatAccess, Storage};
use async_trait::async_trait;
use std::fmt::{Debug, Display};
use std::io;

/// Implementation of a disk image format.
#[async_trait(?Send)]
pub trait FormatDriverInstance: Debug + Display + Send + Sync {
    /// Type of storage used.
    type Storage: Storage;

    /// Return which format this is.
    fn format(&self) -> Format;

    /// Check whether `storage` has this format.
    ///
    /// This is only a rough test and does not guarantee that opening `storage` under this format
    /// will succeed.  Generally, it will only check the magic bytes (if available).  For formats
    /// that do not have distinct features (like raw), this will always return `true`.
    ///
    /// # Safety
    /// Probing is inherently dangerous: Image formats like qcow2 allow referencing external files;
    /// if you use imago to give untrusted parties (like VM guests) access to VM disk image files,
    /// this will give those parties access to data in those files.  Opening images from untrusted
    /// sources can therefore be quite dangerous.  Gating
    /// ([`ImplicitOpenGate`](super::gate::ImplicitOpenGate)) can help mitigate this.
    ///
    /// If you do not know an image’s format, that is a sign it does not come from a trusted
    /// source, and so opening it in a non-raw format may be quite dangerous.
    ///
    /// Perhaps most important to note is that giving an untrusted party (like a VM guest) access
    /// to a raw image file allows that party to modify the whole file.  It may write image headers
    /// into this image file, causing a subsequent probe operation to recognize it as a non-raw
    /// image, referencing arbitrary files on the host filesystem!
    ///
    /// When using imago to give an untrusted third party access to VM disk images, the guidelines
    /// for probing are thus:
    /// - Do not probe.  If at all possible, obtain an image’s format from a trusted side channel.
    /// - If there is no other way, probe each given image only once, before that untrusted third
    ///   party (like a VM guest) had write access to it; remember the probed format, and open the
    ///   image exclusively as that format.
    ///
    /// When working with even potentially untrusted images, you should always use an
    /// [`ImplicitOpenGate`](super::gate::ImplicitOpenGate) to prevent access to files you do not
    /// wish to access.
    async unsafe fn probe(storage: &Self::Storage) -> io::Result<bool>
    where
        Self: Sized;

    /// Size of the disk represented by this image.
    fn size(&self) -> u64;

    /// Recursively collect all storage objects associated with this image.
    ///
    /// “Recursive” means to recurse to other images like e.g. a backing file.
    fn collect_storage_dependencies(&self) -> Vec<&Self::Storage>;

    /// Return whether this image may be modified.
    ///
    /// This state must not change via interior mutability, i.e. as long as this FDI is wrapped in
    /// a `FormatAccess`, its writability must remain constant.
    fn writable(&self) -> bool;

    /// Return the mapping at `offset`.
    ///
    /// Find what `offset` is mapped to, return that mapping information, and the length of that
    /// continuous mapping (from `offset`).
    ///
    /// To determine that continuous mapping length, drivers should not perform additional I/O
    /// beyond what is necessary to get mapping information for `offset` itself.
    ///
    /// `max_length` is a hint how long of a range is required at all, but the returned length may
    /// exceed that value if that simplifies the implementation.
    ///
    /// The returned length must only be 0 if `Mapping::Eof` is returned.
    async fn get_mapping<'a>(
        &'a self,
        offset: u64,
        max_length: u64,
    ) -> io::Result<(Mapping<'a, Self::Storage>, u64)>;

    /// Ensure that `offset` is directly mapped to some storage object, up to a length of `length`.
    ///
    /// Return the storage object, the corresponding offset there, and the continuous length that
    /// the driver was able to map (less than or equal to `length`).
    ///
    /// If the returned length is less than `length`, drivers can expect subsequent calls to
    /// allocate the rest of the original range.  Therefore, if a driver knows in advance that it
    /// is impossible to fully map the given range (e.g. because it lies partially or fully beyond
    /// the end of the disk), it should return an error immediately.
    ///
    /// If `overwrite` is true, the contents in the range are supposed to be overwritten and may be
    /// discarded.  Otherwise, they must be kept.
    async fn ensure_data_mapping<'a>(
        &'a self,
        offset: u64,
        length: u64,
        overwrite: bool,
    ) -> io::Result<(&'a Self::Storage, u64, u64)>;

    /// Read data from a `Mapping::Special` area.
    async fn readv_special(&self, _bufv: IoVectorMut<'_>, _offset: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }

    /// Flush internal buffers.
    ///
    /// Does not need to ensure those buffers are synced to disk (hardware), and does not need to
    /// drop them, i.e. they may still be used on later accesses.
    async fn flush(&self) -> io::Result<()>;

    /// Sync data already written to the storage hardware.
    ///
    /// Does not need to ensure internal buffers are written, i.e. should generally just be passed
    /// through to `Storage::sync()` for all underlying storage objects.
    async fn sync(&self) -> io::Result<()>;

    /// Drop internal buffers.
    ///
    /// Drop all internal buffers, but do not flush them!  All internal data must then be reloaded
    /// from disk.
    ///
    /// # Safety
    /// Not flushing internal buffers may cause image corruption.  The caller must ensure the
    /// on-disk state is consistent.
    async unsafe fn invalidate_cache(&self) -> io::Result<()>;
}

/// Non-recursive mapping information.
///
/// Mapping information as returned by `FormatDriverInstance::get_mapping()`, only looking at that
/// format layer’s information.
pub enum Mapping<'a, S: Storage> {
    /// Raw data.
    Raw {
        /// Storage object where this data is stored.
        storage: &'a S,

        /// Offset in `storage` where this data is stored.
        offset: u64,

        /// Whether this mapping may be written to.
        ///
        /// If `true`, you can directly write to `offset` on `storage` to change the disk image’s
        /// data accordingly.
        ///
        /// If `false`, the disk image format does not allow writing to `offset` on `storage`; a
        /// new mapping must be allocated first.
        writable: bool,
    },

    /// Data lives in a different disk image (e.g. a backing file).
    Indirect {
        /// Format instance where this data can be obtained.
        layer: &'a FormatAccess<S>,

        /// Offset in `layer` where this data can be obtained.
        offset: u64,

        /// Whether this mapping may be written to.
        ///
        /// If `true`, you can directly write to `offset` on `layer` to change the disk image’s
        /// data accordingly.
        ///
        /// If `false`, the disk image format does not allow writing to `offset` on `layer`; a new
        /// mapping must be allocated first.
        writable: bool,
    },

    /// Range is to be read as zeroes.
    Zero,

    /// End of file reached.
    Eof,

    /// Data is encoded in some manner, e.g. compressed or encrypted.
    ///
    /// Such data cannot be accessed directly, but must be interpreted by the image format driver.
    Special {
        /// Original (“guest”) offset to pass to `FormatDriverInstance::readv_special()`.
        offset: u64,
    },
}
