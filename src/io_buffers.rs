//! Types for I/O buffers.
//!
//! This module provides:
//! - buffer types that can be allocated with arbitrary alignment,
//! - references to buffers that more or less ensure the content is read only once (because it can
//!   change for buffers owned by VM guests),
//! - buffer vector types.

use futures::io::{IoSlice, IoSliceMut};
use std::alloc::{self, GlobalAlloc};
use std::fmt::{self, Debug, Formatter};
use std::io;
use std::marker::PhantomData;
#[cfg(unix)]
use std::mem;
use std::mem::{size_of, size_of_val};
use std::ops::Range;
use std::{cmp, ptr, slice};

/// Owned memory buffer.
pub struct IoBuffer {
    /// Raw pointer to the start of the buffer.
    pointer: *mut u8,

    /// Size in bytes.
    size: usize,

    /// Allocation layout.  `None` only for null buffers.
    layout: Option<alloc::Layout>,
}

/// Reference to any immutable memory buffer.
pub struct IoBufferRef<'a> {
    /// Raw pointer to the start of the buffer.
    pointer: *const u8,

    /// Size in bytes.
    size: usize,

    /// Lifetime marker.
    _lifetime: PhantomData<&'a [u8]>,
}

/// Reference to any mutable memory buffer.
pub struct IoBufferMut<'a> {
    /// Raw pointer to the start of the buffer.
    pointer: *mut u8,

    /// Size in bytes.
    size: usize,

    /// Lifetime marker.
    _lifetime: PhantomData<&'a mut [u8]>,
}

// Blocked because of the pointer, but we want this to be usable across threads
unsafe impl Send for IoBuffer {}
unsafe impl Sync for IoBuffer {}
unsafe impl<'a> Send for IoBufferRef<'a> {}
unsafe impl<'a> Sync for IoBufferRef<'a> {}
unsafe impl<'a> Send for IoBufferMut<'a> {}
unsafe impl<'a> Sync for IoBufferMut<'a> {}

impl IoBuffer {
    /// Create a new owned buffer, containing uninitialized data.
    ///
    /// Do note that the returned buffer contains uninitialized data, which however is perfectly
    /// fine for an I/O buffer.
    pub fn new(size: usize, alignment: usize) -> io::Result<Self> {
        let layout = alloc::Layout::from_size_align(size, alignment).map_err(io::Error::other)?;
        Self::new_with_layout(layout)
    }

    /// Create a new owned buffer, containing uninitialized data, with the given `layout`.
    pub fn new_with_layout(layout: alloc::Layout) -> io::Result<Self> {
        if layout.size() == 0 {
            return Ok(IoBuffer {
                pointer: ptr::null_mut(),
                size: 0,
                layout: None,
            });
        }

        // We guarantee the size not to be 0 and do not care about the memory being uninitialized,
        // so this is safe
        let pointer = unsafe { alloc::System.alloc(layout) };

        if pointer.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "Failed to allocate memory (size={}, alignment={})",
                    layout.size(),
                    layout.align(),
                ),
            ));
        }

        Ok(IoBuffer {
            pointer,
            size: layout.size(),
            layout: Some(layout),
        })
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Whether this is a null buffer (length is 0).
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Generate an immutable reference.
    pub fn as_ref(&self) -> IoBufferRef<'_> {
        IoBufferRef {
            pointer: self.pointer as *const u8,
            size: self.size,
            _lifetime: PhantomData,
        }
    }

    /// Generate an immutable reference to a sub-range.
    pub fn as_ref_range(&self, range: Range<usize>) -> IoBufferRef<'_> {
        IoBufferRef::from_slice(&self.as_ref().into_slice()[range])
    }

    /// Generate a mutable reference.
    pub fn as_mut(&mut self) -> IoBufferMut<'_> {
        IoBufferMut {
            pointer: self.pointer,
            size: self.size,
            _lifetime: PhantomData,
        }
    }

    /// Generate a mutable reference to a sub-range.
    pub fn as_mut_range(&mut self, range: Range<usize>) -> IoBufferMut<'_> {
        (&mut self.as_mut().into_slice()[range]).into()
    }
}

impl Drop for IoBuffer {
    /// Free this buffer.
    fn drop(&mut self) {
        if let Some(layout) = self.layout {
            // Safe because we have allocated this buffer using `alloc::System`
            unsafe {
                alloc::System.dealloc(self.pointer, layout);
            }
        }
    }
}

impl<'a> IoBufferRef<'a> {
    /// Create a reference to a slice.
    pub fn from_slice<T: Sized>(slice: &'a [T]) -> Self {
        IoBufferRef {
            pointer: slice.as_ptr() as *const u8,
            size: size_of_val(slice),
            _lifetime: PhantomData,
        }
    }

    /// Create an owned [`IoBuffer`] with the same data (copied).
    pub fn try_into_owned(self, alignment: usize) -> io::Result<IoBuffer> {
        let mut new_buf = IoBuffer::new(self.size, alignment)?;
        new_buf
            .as_mut()
            .into_slice()
            .copy_from_slice(self.into_slice());
        Ok(new_buf)
    }

    /// Size in bytes.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Whether the length is 0.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Return the pointer to the start of the buffer.
    pub fn as_ptr(&self) -> *const u8 {
        self.pointer
    }

    /// Turn this reference into a slice.
    ///
    /// References to `IoBuffer`s must not be copied/cloned (so they can only be accessed once;
    /// they are considered volatile due to potential VM guest accesses), so this consumes the
    /// object.
    pub fn into_slice(self) -> &'a [u8] {
        // Alignment requirement is always met, resulting data is pure binary data
        unsafe { self.into_typed_slice::<u8>() }
    }

    /// Turn this reference into a slice with the given element type.
    ///
    /// # Safety
    /// Caller must ensure that alignment and length requirements are met and that the resulting
    /// data is valid.
    pub unsafe fn into_typed_slice<T: Copy + Sized>(self) -> &'a [T] {
        // Safety ensured by the caller; we ensure that nothing outside of this buffer will be part
        // of the slice
        unsafe { slice::from_raw_parts(self.pointer as *const T, self.size / size_of::<T>()) }
    }

    /// Split the buffer at `mid`.
    ///
    /// Return `&self[..mid]` and `&self[mid..]`.
    ///
    /// If `mid > self.len()`, return `&self[..]` and `[]`.
    pub fn split_at(self, mid: usize) -> (IoBufferRef<'a>, IoBufferRef<'a>) {
        let head_len = cmp::min(mid, self.size);

        (
            IoBufferRef {
                pointer: self.pointer,
                size: head_len,
                _lifetime: PhantomData,
            },
            IoBufferRef {
                // Safe because we have limited this to `self.size`
                pointer: unsafe { self.pointer.add(head_len) },
                size: self.size - head_len,
                _lifetime: PhantomData,
            },
        )
    }
}

impl<'a> From<IoSlice<'a>> for IoBufferRef<'a> {
    fn from(slice: IoSlice<'a>) -> Self {
        IoBufferRef {
            pointer: slice.as_ptr(),
            size: slice.len(),
            _lifetime: PhantomData,
        }
    }
}

impl<'a> From<IoBufferRef<'a>> for IoSlice<'a> {
    fn from(buf: IoBufferRef<'a>) -> Self {
        IoSlice::new(buf.into_slice())
    }
}

impl<'a> IoBufferMut<'a> {
    /// Size in bytes.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Whether the length is 0.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Return the pointer to the start of the buffer.
    pub fn as_ptr(&self) -> *const u8 {
        self.pointer
    }

    /// Turn this reference into a slice.
    ///
    /// References to `IoBuffer`s must not be copied/cloned (so they can only be accessed once;
    /// they are considered volatile due to potential VM guest accesses), so this consumes the
    /// object.
    pub fn into_slice(self) -> &'a mut [u8] {
        // Alignment requirement is always meant, resulting data is pure binary data
        unsafe { self.into_typed_slice::<u8>() }
    }

    /// Turn this reference into a slice with the given element type.
    ///
    /// # Safety
    /// Caller must ensure that alignment and length requirements are met and that the resulting
    /// data is valid.
    pub unsafe fn into_typed_slice<T: Copy + Sized>(self) -> &'a mut [T] {
        // Safety ensured by the caller; we ensure that nothing outside of this buffer will be part
        // of the slice
        unsafe { slice::from_raw_parts_mut(self.pointer as *mut T, self.size / size_of::<T>()) }
    }

    /// Make this reference immutable.
    pub fn into_ref(self) -> IoBufferRef<'a> {
        IoBufferRef {
            pointer: self.pointer,
            size: self.size,
            _lifetime: PhantomData,
        }
    }

    /// Split the buffer at `mid`.
    ///
    /// Return `&mut self[..mid]` and `&mut self[mid..]`.
    ///
    /// If `mid > self.len()`, return `&mut self[..]` and `[]`.
    pub fn split_at(self, mid: usize) -> (IoBufferMut<'a>, IoBufferMut<'a>) {
        let head_len = cmp::min(mid, self.size);

        (
            IoBufferMut {
                pointer: self.pointer,
                size: head_len,
                _lifetime: PhantomData,
            },
            IoBufferMut {
                // Safe because we have limited this to `self.size`
                pointer: unsafe { self.pointer.add(head_len) },
                size: self.size - head_len,
                _lifetime: PhantomData,
            },
        )
    }
}

impl<'a, T: Sized> From<&'a mut [T]> for IoBufferMut<'a> {
    fn from(slice: &'a mut [T]) -> Self {
        IoBufferMut {
            pointer: slice.as_mut_ptr() as *mut u8,
            size: size_of_val(slice),
            _lifetime: PhantomData,
        }
    }
}

impl<'a> From<IoSliceMut<'a>> for IoBufferMut<'a> {
    fn from(mut slice: IoSliceMut<'a>) -> Self {
        IoBufferMut {
            pointer: slice.as_mut_ptr(),
            size: slice.len(),
            _lifetime: PhantomData,
        }
    }
}

impl<'a> From<IoBufferMut<'a>> for IoSliceMut<'a> {
    fn from(buf: IoBufferMut<'a>) -> Self {
        IoSliceMut::new(buf.into_slice())
    }
}

/// Internal helper for memory alignment adherence.
///
/// Collects bounce buffers that are created when enforcing minimum memory alignment requirements
/// on I/O vectors.
///
/// For read requests, dropping this object will automatically copy the data back to the original
/// guest buffers.
#[derive(Default)]
pub struct IoVectorBounceBuffers<'a> {
    /// Collection of bounce buffers; references to these are put into the re-aligned IoVector*
    /// object.
    buffers: Vec<IoBuffer>,

    /// For read requests (hence the IoSliceMut type): Collection of unaligned buffers (which have
    /// been replaced by bounce buffers in the re-aligned IoVectorMut), to which we need to return
    /// the data from the bounce buffers once the request is done (i.e., e.g., when this object is
    /// dropped).
    copy_back_into: Option<Vec<IoSliceMut<'a>>>,
}

macro_rules! impl_io_vector {
    ($type:tt, $inner_type:tt, $buffer_type:tt, $slice_type:ty, $slice_type_lifetime_b:ty) => {
        /// Vector of memory buffers.
        pub struct $type<'a> {
            /// Buffer list.
            vector: Vec<$inner_type<'a>>,

            /// Complete size in bytes.
            total_size: u64,
        }

        impl<'a> $type<'a> {
            /// Create an empty vector.
            pub fn new() -> Self {
                Self::default()
            }

            /// Create an empty vector, pre-allocating space for `cap` buffers.
            ///
            /// This does not allocate an memory buffer, only space in the buffer vector.
            pub fn with_capacity(cap: usize) -> Self {
                $type {
                    vector: Vec::with_capacity(cap),
                    total_size: 0,
                }
            }

            /// Append a slice.
            pub fn push(&mut self, slice: $slice_type) {
                debug_assert!(!slice.is_empty());
                self.total_size += slice.len() as u64;
                self.vector.push($inner_type::new(slice));
            }

            /// Append a slice.
            fn push_ioslice(&mut self, ioslice: $inner_type<'a>) {
                debug_assert!(!ioslice.is_empty());
                self.total_size += ioslice.len() as u64;
                self.vector.push(ioslice);
            }

            /// Same as [`Self::push()`], but takes ownership of `self`.
            ///
            /// By taking ownership of `self` and returning it, this method allows reducing the
            /// lifetime of `self` to that of `slice`, if necessary.
            pub fn with_pushed<'b>(self, slice: $slice_type_lifetime_b) -> $type<'b>
            where
                'a: 'b,
            {
                let mut vec: $type<'b> = self;
                vec.push(slice);
                vec
            }

            /// Insert a slice at the given `index` in the buffer vector.
            pub fn insert(&mut self, index: usize, slice: $slice_type) {
                debug_assert!(!slice.is_empty());
                self.total_size += slice.len() as u64;
                self.vector.insert(index, $inner_type::new(slice));
            }

            /// Same as [`Self::insert()`], but takes ownership of `self.`
            ///
            /// By taking ownership of `self` and returning it, this method allows reducing the
            /// lifetime of `self` to that of `slice`, if necessary.
            pub fn with_inserted<'b>(self, index: usize, slice: $slice_type_lifetime_b) -> $type<'b>
            where
                'a: 'b,
            {
                let mut vec: $type<'b> = self;
                vec.insert(index, slice);
                vec
            }

            /// Return the sum total length in bytes of all buffers in this vector.
            pub fn len(&self) -> u64 {
                self.total_size
            }

            /// Return the number of buffers in this vector.
            pub fn buffer_count(&self) -> usize {
                self.vector.len()
            }

            /// Return `true` if and only if this vector’s length is zero.
            ///
            /// Synonymous with whether this vector’s buffer count is zero.
            pub fn is_empty(&self) -> bool {
                debug_assert!((self.total_size == 0) == self.vector.is_empty());
                self.total_size == 0
            }

            /// Append all buffers from the given other vector to this vector.
            pub fn append(&mut self, mut other: $type<'a>) {
                self.total_size += other.total_size;
                self.vector.append(&mut other.vector);
            }

            /// Implementation for [`Self::split_at()`] and [`Self::split_tail_at()`].
            ///
            /// If `keep_head` is true, both head and tail are returned ([`Self::split_at()`]).
            /// Otherwise, the head is discarded ([`Self::split_tail_at()`]).
            fn do_split_at(mut self, mid: u64, keep_head: bool) -> (Option<$type<'a>>, $type<'a>) {
                if mid >= self.total_size {
                    // Special case: Empty tail
                    return (
                        keep_head.then_some(self),
                        $type {
                            vector: Vec::new(),
                            total_size: 0,
                        },
                    );
                }

                let mut i = 0; // Current element index
                let mut offset = 0u64; // Current element offset
                let (vec_head, vec_tail) = loop {
                    if offset == mid {
                        // Clean split: `i` is fully behind `mid`, the rest is fully ahead
                        if keep_head {
                            let mut vec_head = self.vector;
                            let vec_tail = vec_head.split_off(i);
                            break (Some(vec_head), vec_tail);
                        } else {
                            break (None, self.vector.split_off(i));
                        }
                    }

                    let post_elm_offset = offset + self.vector[i].len() as u64;

                    if post_elm_offset > mid {
                        // Not so clean split: The beginning of this element was before `mid`, the end is
                        // behind it, so we must split this element between head and tail
                        let mut vec_head = self.vector;
                        let mut tail_iter = vec_head.drain(i..);

                        // This is the current element (at `i`), which must be present
                        let mid_elm = tail_iter.next().unwrap();
                        let mid_elm: $buffer_type<'a> = mid_elm.into();

                        // Each element's length is of type usize, so this must fit into usize
                        let mid_elm_head_len: usize = (mid - offset).try_into().unwrap();
                        let (mid_head, mid_tail) = mid_elm.split_at(mid_elm_head_len);

                        let mut vec_tail: Vec<$inner_type<'a>> = vec![mid_tail.into()];
                        vec_tail.extend(tail_iter);

                        if keep_head {
                            vec_head.push(mid_head.into());
                            break (Some(vec_head), vec_tail);
                        } else {
                            break (None, vec_tail);
                        }
                    }

                    offset = post_elm_offset;

                    i += 1;
                    // We know that `mid < self.total_size`, so we must encounter `mid before the end of
                    // the vector
                    assert!(i < self.vector.len());
                };

                let head = keep_head.then(|| $type {
                    vector: vec_head.unwrap(),
                    total_size: mid,
                });
                let tail = $type {
                    vector: vec_tail,
                    total_size: self.total_size - mid,
                };

                (head, tail)
            }

            /// Split the vector into two.
            ///
            /// The first returned vector contains the bytes in the `[..mid]` range, and the second
            /// one covers the `[mid..]` range.
            pub fn split_at(self, mid: u64) -> ($type<'a>, $type<'a>) {
                let (head, tail) = self.do_split_at(mid, true);
                (head.unwrap(), tail)
            }

            /// Like [`Self::split_at()`], but discards the head, only returning the tail.
            ///
            /// More efficient than to use `self.split_at(mid).1` because the former requires
            /// creating a new `Vec` object for the head, which this version skips.
            pub fn split_tail_at(self, mid: u64) -> $type<'a> {
                self.do_split_at(mid, false).1
            }

            /// Copy the data from `self` into `slice`.
            ///
            /// Both must have the same length.
            pub fn copy_into_slice(&self, slice: &mut [u8]) {
                if slice.len() as u64 != self.total_size {
                    panic!("IoVector*::copy_into_slice() called on a slice of different length from the vector");
                }

                assert!(self.total_size <= usize::MAX as u64);

                let mut offset = 0usize;
                for elem in self.vector.iter() {
                    let next_offset = offset + elem.len();
                    slice[offset..next_offset].copy_from_slice(&elem[..]);
                    offset = next_offset;
                }
            }

            /// Create a single owned [`IoBuffer`] with the same data (copied).
            pub fn try_into_owned(self, alignment: usize) -> io::Result<IoBuffer> {
                let size = self.total_size.try_into().map_err(|_| {
                    io::Error::other(format!("Buffer is too big ({})", self.total_size))
                })?;
                let mut new_buf = IoBuffer::new(size, alignment)?;
                self.copy_into_slice(new_buf.as_mut().into_slice());
                Ok(new_buf)
            }

            /// Return a corresponding `&[libc::iovec]`.
            ///
            /// # Safety
            /// `iovec` has no lifetime information.  Callers must ensure no elements in the
            /// returned slice are used beyond the lifetime `'a`.
            #[cfg(unix)]
            pub unsafe fn as_iovec(&'a self) -> &'a [libc::iovec] {
                // IoSlice and IoSliceMut are defined to have the same representation in memory as
                // libc::iovec does
                unsafe {
                    mem::transmute::<&'a [$inner_type<'a>], &'a [libc::iovec]>(&self.vector[..])
                }
            }

            /// Check whether `self` is aligned.
            ///
            /// Each buffer must be aligned to `mem_alignment`, and each buffer’s length must be
            /// aligned to both `mem_alignment` and `req_alignment` (the I/O request offset/size
            /// alignment).
            pub fn is_aligned(&self, mem_alignment: usize, req_alignment: usize) -> bool {
                // Trivial case
                if mem_alignment == 1 && req_alignment == 1 {
                    return true;
                }

                debug_assert!(mem_alignment.is_power_of_two() && req_alignment.is_power_of_two());
                let base_align_mask = mem_alignment - 1;
                let len_align_mask = base_align_mask | (req_alignment - 1);

                self.vector.iter().all(|buf| {
                    buf.as_ptr() as usize & base_align_mask == 0 &&
                        buf.len() & len_align_mask == 0
                })
            }

            /// Consume `self`, returning an I/O vector that fulfills the given alignment
            /// requirements.
            ///
            /// All bounce buffers that are created for this purpose are stored into
            /// `bounce_buffers` (which must have been created just for this function, i.e. must be
            /// empty).
            ///
            /// If `copy_into` is true, the bounce buffers are initialized with data from the input
            /// vector.
            ///
            /// If `copy_back` is true, all unaligned buffers are collected in a `Vec` (instead of
            /// discarding them) and returned as the second element of the tuple.  The caller
            /// should store this `Vec` in the same `IoVectorBounceBuffers` that holds
            /// `bounce_buffers`, so that the data is copied back from the bounce buffers once the
            /// `IoVectorBounceBuffers` object is dropped.  (Implementation detail: This function
            /// cannot operate on `IoVectorBounceBuffers` objects directly, because its
            /// `copy_back_into` field holds `IoSliceMut`s, which may not be what `$inner_type` is
            /// in the implementing macro.)
            ///
            /// This function has the returned vector’s lifetime be limited to how long the
            /// `bounce_buffers` object lives.
            fn create_aligned_buffer<'b>(
                self,
                mem_alignment: usize,
                req_alignment: usize,
                bounce_buffers: &'b mut Vec<IoBuffer>,
                copy_into: bool,
                copy_back: bool,
            ) -> io::Result<($type<'b>, Option<Vec<$inner_type<'a>>>)>
            where
                'a: 'b,
            {
                debug_assert!(copy_into || copy_back);
                debug_assert!(mem_alignment.is_power_of_two() && req_alignment.is_power_of_two());
                let base_align_mask = mem_alignment - 1;
                let len_align_mask = base_align_mask | (req_alignment - 1);

                // First, create all bounce buffers as necessary and put them into
                // `bounce_buffers`.  Thus, `bounce_buffers` no longer needs to be mutable after
                // this loop, which allows us to take `$inner_type<b>` references from those
                // buffers while they are in the `Vec<IoBuffer>`.

                let mut unaligned_length_collection: Option<usize> = None;
                for buffer in &self.vector {
                    let base = buffer.as_ptr() as usize;
                    let len = buffer.len();

                    if base & base_align_mask != 0 || len & len_align_mask != 0 || unaligned_length_collection.is_some() {
                        let unaligned_len =
                            unaligned_length_collection
                            .unwrap_or(0)
                            .checked_add(len)
                            .ok_or_else(|| io::Error::other("I/O vector length overflow"))?;
                        unaligned_length_collection = Some(unaligned_len);

                        if unaligned_len & len_align_mask == 0 {
                            bounce_buffers.push(IoBuffer::new(unaligned_len, mem_alignment)?);
                            unaligned_length_collection = None;
                        }
                    }
                }

                // Second, create the I/O vector that is returned: Interleave already aligned
                // vector buffers with references to the newly created buffers (which have the
                // proper lifetime `'b`), and copy data into those new buffers if `copy_into`.  If
                // `copy_back`, collect the replaced buffers in `copy_back_vector`.

                let mut realigned_vector = Vec::<$inner_type<'b>>::new();
                let mut unaligned_collection: Option<Self> = None;
                let mut buffer_iter = bounce_buffers.iter_mut();
                let mut copy_back_vector = copy_back.then(|| Vec::<$inner_type<'a>>::new());

                for buffer in self.vector {
                    let base = buffer.as_ptr() as usize;
                    let len = buffer.len();

                    if base & base_align_mask != 0 || len & len_align_mask != 0 || unaligned_collection.is_some() {
                        let collection = unaligned_collection.get_or_insert_with(|| Self::new());
                        collection.push_ioslice(buffer);

                        let unaligned_len = collection.len();
                        if unaligned_len & len_align_mask as u64 == 0 {
                            let new_buf: &'b mut IoBuffer = buffer_iter.next().unwrap();
                            if copy_into {
                                collection.copy_into_slice(new_buf.as_mut().into_slice());
                            }

                            // Drop the collection
                            let mut collection = unaligned_collection.take().unwrap();
                            if let Some(copy_back_vector) = copy_back_vector.as_mut() {
                                copy_back_vector.append(&mut collection.vector);
                            }

                            // Get a reference from `bounce_buffers` to ensure the lifetime
                            realigned_vector.push($inner_type::new(new_buf.as_mut().into_slice()));
                        }
                    } else {
                        realigned_vector.push(buffer);
                    }
                }

                Ok((
                    $type {
                        vector: realigned_vector,
                        total_size: self.total_size,
                    },
                    copy_back_vector,
                ))
            }

            /// Return the internal vector of `IoSlice` objects.
            pub fn into_inner(self) -> Vec<$inner_type<'a>> {
                self.vector
            }
        }

        impl<'a> From<Vec<$inner_type<'a>>> for $type<'a> {
            fn from(vector: Vec<$inner_type<'a>>) -> Self {
                let total_size = vector
                    .iter()
                    .map(|e| e.len())
                    .fold(0u64, |sum, e| sum + e as u64);

                $type { vector, total_size }
            }
        }

        impl<'a> From<$buffer_type<'a>> for $type<'a> {
            fn from(buffer: $buffer_type<'a>) -> Self {
                let total_size = buffer.len() as u64;
                if total_size > 0 {
                    $type {
                        vector: vec![buffer.into()],
                        total_size,
                    }
                } else {
                    $type {
                        vector: Vec::new(),
                        total_size: 0,
                    }
                }
            }
        }

        impl<'a> From<$slice_type> for $type<'a> {
            fn from(slice: $slice_type) -> Self {
                let total_size = slice.len() as u64;
                if total_size > 0 {
                    $type {
                        vector: vec![$inner_type::new(slice)],
                        total_size,
                    }
                } else {
                    $type {
                        vector: Vec::new(),
                        total_size: 0,
                    }
                }
            }
        }

        impl<'a> Default for $type<'a> {
            fn default() -> Self {
                $type {
                    vector: Vec::new(),
                    total_size: 0,
                }
            }
        }

        impl Debug for $type<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.debug_struct(std::stringify!($type))
                    .field("vector.len()", &self.vector.len())
                    .field("total_size", &self.total_size)
                    .finish()
            }
        }
    };
}

impl_io_vector!(IoVector, IoSlice, IoBufferRef, &'a [u8], &'b [u8]);
impl_io_vector!(
    IoVectorMut,
    IoSliceMut,
    IoBufferMut,
    &'a mut [u8],
    &'b mut [u8]
);

impl<'a> IoVector<'a> {
    /// Ensure that all buffers in the vector adhere to the given alignment.
    ///
    /// Buffers’ addresses must be aligned to `mem_alignment`, and their lengths must be aligned to
    /// both `mem_alignment` and `req_alignment`.
    ///
    /// To align everything, bounce buffers are created and filled with data from the
    /// buffers in the vector (which is why this is only for writes).  These bounce buffers
    /// are stored in `bounce_buffers`, and the lifetime `'b` ensures this object will
    /// outlive the returned new vector.
    ///
    /// `bounce_buffers` must have been created specifically for this single function call through
    /// `IoVectorBounceBuffers::default()`.
    pub fn enforce_alignment_for_write<'b>(
        self,
        mem_alignment: usize,
        req_alignment: usize,
        bounce_buffers: &'b mut IoVectorBounceBuffers<'static>,
    ) -> io::Result<IoVector<'b>>
    where
        'a: 'b,
    {
        debug_assert!(bounce_buffers.is_empty());

        let (aligned, copy_back_buffers) = self.create_aligned_buffer(
            mem_alignment,
            req_alignment,
            &mut bounce_buffers.buffers,
            true,
            false,
        )?;
        debug_assert!(copy_back_buffers.is_none());
        Ok(aligned)
    }
}

impl<'a> IoVectorMut<'a> {
    /// Fill all buffers in the vector with the given byte pattern.
    pub fn fill(&mut self, value: u8) {
        for slice in self.vector.iter_mut() {
            slice.fill(value);
        }
    }

    /// Copy data from `slice` into the buffers in this vector.
    ///
    /// The vector and the slice must have the same total length.
    pub fn copy_from_slice(&mut self, slice: &[u8]) {
        if slice.len() as u64 != self.total_size {
            panic!("IoVectorMut::copy_from_slice() called on a slice of different length from the vector");
        }

        assert!(self.total_size <= usize::MAX as u64);

        let mut offset = 0usize;
        for elem in self.vector.iter_mut() {
            let next_offset = offset + elem.len();
            elem.copy_from_slice(&slice[offset..next_offset]);
            offset = next_offset;
        }
    }

    /// Ensure that all buffers in the vector adhere to the given alignment.
    ///
    /// Buffers’ addresses must be aligned to `mem_alignment`, and their lengths must be aligned to
    /// both `mem_alignment` and `req_alignment`.
    ///
    /// To align everything, bounce buffers are created without initializing them (which is
    /// why this is only for reads).  These bounce buffers are stored in `bounce_buffers`,
    /// and the lifetime `'b` ensures this object will outlive the returned new vector.
    /// When `bounce_buffers` is dropped, the data in those bounce buffers (filled by the read
    /// operation) will automatically be copied back into the original guest buffers.
    ///
    /// `bounce_buffers` must have been created specifically for this single function call through
    /// `IoVectorBounceBuffers::default()`.
    pub fn enforce_alignment_for_read<'b>(
        self,
        mem_alignment: usize,
        req_alignment: usize,
        bounce_buffers: &'b mut IoVectorBounceBuffers<'a>,
    ) -> io::Result<IoVectorMut<'b>>
    where
        'a: 'b,
    {
        debug_assert!(bounce_buffers.is_empty());

        let (aligned, copy_back_buffers) = self.create_aligned_buffer(
            mem_alignment,
            req_alignment,
            &mut bounce_buffers.buffers,
            false,
            true,
        )?;
        bounce_buffers.copy_back_into = copy_back_buffers;
        Ok(aligned)
    }
}

impl<'a> From<&'a Vec<u8>> for IoVector<'a> {
    fn from(vec: &'a Vec<u8>) -> Self {
        vec.as_slice().into()
    }
}

impl<'a> From<&'a IoBuffer> for IoVector<'a> {
    fn from(buf: &'a IoBuffer) -> Self {
        buf.as_ref().into_slice().into()
    }
}

impl<'a> From<&'a mut Vec<u8>> for IoVectorMut<'a> {
    fn from(vec: &'a mut Vec<u8>) -> Self {
        vec.as_mut_slice().into()
    }
}

impl<'a> From<&'a mut IoBuffer> for IoVectorMut<'a> {
    fn from(buf: &'a mut IoBuffer) -> Self {
        buf.as_mut().into_slice().into()
    }
}

impl IoVectorBounceBuffers<'_> {
    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty() && self.copy_back_into.is_none()
    }
}

impl Drop for IoVectorBounceBuffers<'_> {
    /// If the data in the bounce buffers is to be copied back into the original guest buffers (for
    /// read operations), do so when the bounce buffers are dropped.
    fn drop(&mut self) {
        if let Some(copy_back_into) = self.copy_back_into.take() {
            let input_buffer_count = self.buffers.len();
            let mut input_i = 0;
            let mut input_offset = 0;

            for mut target_buffer in copy_back_into {
                let next_input_offset = input_offset + target_buffer.len();
                let input_buffer = self.buffers[input_i].as_ref().into_slice();
                target_buffer.copy_from_slice(&input_buffer[input_offset..next_input_offset]);
                input_offset = next_input_offset;

                debug_assert!(input_offset <= input_buffer.len());
                if input_offset == input_buffer.len() {
                    input_i += 1;
                    input_offset = 0;
                }
            }

            debug_assert!(input_i == input_buffer_count);
        }
    }
}
