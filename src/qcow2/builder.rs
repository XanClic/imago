//! Builder for defining open options for qcow2 images.

use super::*;
use crate::format::builder::{FormatDriverBuilderBase, FormatOrBuilder, StorageOrPath};
use std::path::PathBuf;

/// Options builder for opening a qcow2 image.
///
/// Allows setting various options one by one to open a qcow2 image.
pub struct Qcow2OpenBuilder<S: Storage + 'static, F: WrappedFormat<S> + 'static = FormatAccess<S>> {
    /// Basic options.
    base: FormatDriverBuilderBase<S>,

    /// Backing image
    ///
    /// `None` to open the image as specified by the image header, `Some(None)` to not open any
    /// backing image, and `Some(Some(_))` to use that backing image.
    backing: Option<Option<FormatOrBuilder<S, F>>>,

    /// External data file
    ///
    /// `None` to open the file as specified by the image header, `Some(None)` to not open any data
    /// file, and `Some(Some(_))` to use that data file.
    data_file: Option<Option<StorageOrPath<S>>>,
}

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> Qcow2OpenBuilder<S, F> {
    /// Create a new instance.
    fn with_base(base: FormatDriverBuilderBase<S>) -> Self {
        Qcow2OpenBuilder {
            base,
            backing: None,
            data_file: None,
        }
    }

    /// Set a backing image.
    ///
    /// This overrides the implicit backing image given in the image header.  Passing `None` means
    /// not to use any backing image (regardless of whether the image header defines a backing
    /// image).
    pub fn backing(mut self, backing: Option<F>) -> Self {
        self.backing = Some(backing.map(FormatOrBuilder::Format));
        self
    }

    /// Declare a backing image by path.
    ///
    /// Let imago open the given path as an image with the given format.
    ///
    /// Use with caution, as the given image will be opened with default options.
    /// [`Qcow2OpenBuilder::backing()`] is preferable, as it allows you control over how the
    /// backing image is opened.
    pub fn backing_path<P: AsRef<Path>>(mut self, backing: P, format: Format) -> Self {
        self.backing = Some(Some(FormatOrBuilder::new_builder(format, backing)));
        self
    }

    /// Set an external data file.
    ///
    /// This overrides the implicit external data file given in the image header.  Passing `None`
    /// means not to use any external data file (regardless of whether the image header defines an
    /// external data file, and regardless of whether the image header says the image has an
    /// external data file).
    ///
    /// Similarly, passing a data file will then always use that data file, regardless of whether
    /// the image header says the image has an external data file.
    ///
    /// Note that it is wrong to set a data file for an image that does not have one, and it is
    /// wrong to enforce not using a data file for an image that has one.  There is no way to know
    /// whether the image needs an external data file until it is opened.
    ///
    /// If you want to open a specific data file if and only if the image needs it, call
    /// `Qcow2OpenBuilder::data_file(None)` to prevent any data file from being automatically
    /// opened; open the image, then check [`Qcow2::requires_external_data_file()`], and, if true,
    /// invoke [`Qcow2::set_data_file()`].
    pub fn data_file(mut self, data_file: Option<S>) -> Self {
        self.data_file = Some(data_file.map(StorageOrPath::Storage));
        self
    }
}

impl<S: Storage + 'static, F: WrappedFormat<S> + 'static> FormatDriverBuilder<S>
    for Qcow2OpenBuilder<S, F>
{
    type Format = Qcow2<S, F>;
    const FORMAT: Format = Format::Qcow2;

    fn new(image: S) -> Self {
        Self::with_base(FormatDriverBuilderBase::new(image))
    }

    fn new_path<P: AsRef<Path>>(path: P) -> Self {
        Self::with_base(FormatDriverBuilderBase::new_path(path))
    }

    fn write(mut self, write: bool) -> Self {
        self.base.set_write(write);
        self
    }

    fn storage_open_options(mut self, options: StorageOpenOptions) -> Self {
        self.base.set_storage_open_options(options);
        self
    }

    async fn open<G: ImplicitOpenGate<S>>(self, mut gate: G) -> io::Result<Self::Format> {
        let writable = self.base.get_writable();
        let storage_opts = self.base.make_storage_opts();
        let metadata = self.base.open_image(&mut gate).await?;

        let mut qcow2 = Qcow2::<S, F>::do_open(metadata, writable, storage_opts.clone()).await?;

        if let Some(backing) = self.backing {
            let backing = match backing {
                None => None,
                Some(backing) => Some(
                    backing
                        .open_format(storage_opts.clone().write(false), &mut gate)
                        .await
                        .err_context(|| "Backing file")?,
                ),
            };
            qcow2.set_backing(backing);
        }

        if let Some(data_file) = self.data_file {
            let data_file = match data_file {
                None => None,
                Some(data_file) => Some(
                    data_file
                        .open_storage(storage_opts, &mut gate)
                        .await
                        .err_context(|| "External data file")?,
                ),
            };
            qcow2.set_data_file(data_file);
        }

        qcow2.open_implicit_dependencies_gated(gate).await?;

        Ok(qcow2)
    }

    fn get_image_path(&self) -> Option<PathBuf> {
        self.base.get_image_path()
    }

    fn get_writable(&self) -> bool {
        self.base.get_writable()
    }

    fn get_storage_open_options(&self) -> Option<&StorageOpenOptions> {
        self.base.get_storage_opts()
    }
}
