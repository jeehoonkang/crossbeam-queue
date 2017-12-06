//! The tagged usize
//!
//! FIXME

use core::sync::atomic::{AtomicUsize, Ordering};

/// A tagged usize.
///
/// Internally, the data is represented as an integer that wraps around at some unspecified point
/// and a flag that represents whether it is tagged or not.
#[derive(Copy, Clone, Default, Debug, Eq, PartialEq)]
pub struct TaggedUsize {
    /// The least significant bit is set if tagged. The rest of the bits hold the data.
    pub(crate) data: usize,
}

impl TaggedUsize {
    /// FIXME
    #[inline]
    pub fn new(data: usize, tag: bool) -> Self {
        Self { data: data << 1 + (if tag { 1 } else { 0 }) }
    }

    /// Returns the starting epoch in unpinned state.
    #[inline]
    pub fn starting() -> Self {
        Self::default()
    }

    /// Returns the number of epochs `self` is ahead of `rhs`.
    ///
    /// Internally, epochs are represented as numbers in the range `(isize::MIN / 2) .. (isize::MAX
    /// / 2)`, so the returned distance will be in the same interval.
    pub fn wrapping_sub(self, rhs: Self) -> isize {
        // The result is the same with `(self.data & !1).wrapping_sub(rhs.data & !1) as isize >> 1`,
        // because the possible difference of LSB in `(self.data & !1).wrapping_sub(rhs.data & !1)`
        // will be ignored in the shift operation.
        self.data.wrapping_sub(rhs.data & !1) as isize >> 1
    }

    /// Returns `true` if the epoch is marked as pinned.
    #[inline]
    pub fn is_pinned(self) -> bool {
        (self.data & 1) == 1
    }

    /// Returns the same epoch, but marked as pinned.
    #[inline]
    pub fn pinned(self) -> TaggedUsize {
        TaggedUsize { data: self.data | 1 }
    }

    /// Returns the same epoch, but marked as unpinned.
    #[inline]
    pub fn unpinned(self) -> TaggedUsize {
        TaggedUsize { data: self.data & !1 }
    }

    /// Returns the successor epoch.
    ///
    /// The returned epoch will be marked as pinned only if the previous one was as well.
    #[inline]
    pub fn successor(self) -> TaggedUsize {
        TaggedUsize { data: self.data.wrapping_add(2) }
    }
}

/// An atomic value that holds an `TaggedUsize`.
#[derive(Default, Debug)]
pub struct AtomicTaggedUsize {
    /// Since `TaggedUsize` is just a wrapper around `usize`, an `AtomicTaggedUsize` is similarly represented
    /// using an `AtomicUsize`.
    data: AtomicUsize,
}

impl AtomicTaggedUsize {
    /// Creates a new atomic epoch.
    #[inline]
    pub fn new(epoch: TaggedUsize) -> Self {
        let data = AtomicUsize::new(epoch.data);
        AtomicTaggedUsize { data }
    }

    /// Loads a value from the atomic epoch.
    #[inline]
    pub fn load(&self, ord: Ordering) -> TaggedUsize {
        TaggedUsize { data: self.data.load(ord) }
    }

    /// Stores a value into the atomic epoch.
    #[inline]
    pub fn store(&self, epoch: TaggedUsize, ord: Ordering) {
        self.data.store(epoch.data, ord);
    }

    /// Stores a value into the atomic epoch if the current value is the same as `current`.
    ///
    /// The return value is always the previous value. If it is equal to `current`, then the value
    /// is updated.
    ///
    /// The `Ordering` argument describes the memory ordering of this operation.
    #[inline]
    pub fn compare_and_swap(&self, current: TaggedUsize, new: TaggedUsize, ord: Ordering) -> TaggedUsize {
        let data = self.data.compare_and_swap(current.data, new.data, ord);
        TaggedUsize { data }
    }
}
