//! Annotating wrapper around storage objects.
//!
//! Wraps other storage objects, adding an arbitrary tag to them.
//!
//! This may be useful when using the “mapping” interface, to identify the storage objects returned
//! in raw mappings.
//!
//! Example:
//! ```
//! # use imago::{FormatAccess, Mapping};
//! # use imago::annotated::Annotated;
//! # use imago::null::Null;
//! # use imago::raw::Raw;
//! # tokio::runtime::Builder::new_current_thread()
//! #   .build()
//! #   .unwrap()
//! #   .block_on(async move {
//! #
//! let disk_size = 16 << 30;
//! let test_offset = 1 << 30;
//! let test_tag = 42;
//!
//! let inner_storage = Null::new(disk_size);
//! let annotated_storage = Annotated::new(inner_storage, test_tag);
//! let image = Raw::open_image(annotated_storage, false).await?;
//! let image = FormatAccess::new(image);
//!
//! let mapping = image.get_mapping(test_offset, 1).await?.0;
//! let Mapping::Raw {
//!     storage,
//!     offset,
//!     writable,
//! } = mapping
//! else {
//!     panic!("Raw mapping expected");
//! };
//! assert_eq!(*storage.tag(), test_tag);
//! assert_eq!(offset, test_offset);
//! #
//! # Ok::<(), std::io::Error>(())
//! # }).unwrap()
//! ```

use crate::io_buffers::{IoVector, IoVectorMut};
use crate::{Storage, StorageOpenOptions};
use std::fmt::{self, Debug, Display, Formatter};
use std::io;
use std::ops::{Deref, DerefMut};

/// Annotating wrapper around storage objects.
///
/// Wraps other storage objects, adding an arbitrary tag to them.
// TODO: Remove the `Default` requirement.  We want to implement `Storage::open()` if `Default` is
// implemented, though, but return an error if it is not.  Doing that probably requires
// specialization, though.
#[derive(Debug)]
pub struct Annotated<Tag: Debug + Default + Display + Send + Sync, S: Storage> {
    /// Wrapped storage object.
    inner: S,

    /// Tag.
    tag: Tag,
}

impl<T: Debug + Default + Display + Send + Sync, S: Storage> Annotated<T, S> {
    /// Wrap `storage`, adding the tag `tag`.
    pub fn new(storage: S, tag: T) -> Self {
        Annotated {
            inner: storage,
            tag,
        }
    }

    /// Get the tag.
    pub fn tag(&self) -> &T {
        &self.tag
    }

    /// Allow modifying or changing the tag.
    pub fn tag_mut(&mut self) -> &mut T {
        &mut self.tag
    }
}

impl<T: Debug + Default + Display + Send + Sync, S: Storage> From<S> for Annotated<T, S> {
    fn from(storage: S) -> Self {
        Self::new(storage, T::default())
    }
}

impl<T: Debug + Default + Display + Send + Sync, S: Storage> Storage for Annotated<T, S> {
    async fn open(opts: StorageOpenOptions) -> io::Result<Self> {
        Ok(S::open(opts).await?.into())
    }

    fn mem_align(&self) -> usize {
        self.inner.mem_align()
    }

    fn req_align(&self) -> usize {
        self.inner.req_align()
    }

    fn size(&self) -> io::Result<u64> {
        self.inner.size()
    }

    async fn readv(&self, bufv: IoVectorMut<'_>, offset: u64) -> io::Result<()> {
        self.inner.readv(bufv, offset).await
    }

    async fn writev(&self, bufv: IoVector<'_>, offset: u64) -> io::Result<()> {
        self.inner.writev(bufv, offset).await
    }

    async fn write_zeroes(&self, offset: u64, length: u64) -> io::Result<()> {
        self.inner.write_zeroes(offset, length).await
    }

    async fn flush(&self) -> io::Result<()> {
        self.inner.flush().await
    }

    async fn sync(&self) -> io::Result<()> {
        self.inner.sync().await
    }
}

impl<T: Debug + Default + Display + Send + Sync, S: Storage> Deref for Annotated<T, S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.inner
    }
}

impl<T: Debug + Default + Display + Send + Sync, S: Storage> DerefMut for Annotated<T, S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<T: Debug + Default + Display + Send + Sync, S: Storage> Display for Annotated<T, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "annotated({})[{}]", self.tag, self.inner)
    }
}
