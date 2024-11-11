//! Miscellaneous helper functions.

use std::io;
use std::ops::Range;

/// Checks whether something overlaps with something else.
pub(crate) trait Overlaps {
    /// Does this overlap with `other`?
    fn overlaps(&self, other: &Self) -> bool;
}

impl<I: Ord> Overlaps for Range<I> {
    fn overlaps(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}

/// Prepend `Error` messages by context.
///
/// Trait for `Error` objects that allows prepending their error messages by something that gives
/// context.
pub(crate) trait ErrorContext {
    /// Prepend the error by `context`.
    fn context<C: std::fmt::Display>(self, context: C) -> Self;
}

impl ErrorContext for io::Error {
    fn context<C: std::fmt::Display>(self, context: C) -> Self {
        io::Error::new(self.kind(), format!("{context}: {self}"))
    }
}

/// Give results context in case of error.
///
/// Lifts the `ErrorContext` trait to `Result` types.
pub(crate) trait ResultErrorContext {
    /// Give context if `self` is an error.
    ///
    /// If `self` is an error, prepend the given `context`.
    fn err_context<C: std::fmt::Display, F: FnOnce() -> C>(self, context: F) -> Self;
}

impl<V, E: ErrorContext> ResultErrorContext for Result<V, E> {
    fn err_context<C: std::fmt::Display, F: FnOnce() -> C>(self, context: F) -> Self {
        self.map_err(|err| err.context(context()))
    }
}
