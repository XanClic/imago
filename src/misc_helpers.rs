//! Miscellaneous helper functions.

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
