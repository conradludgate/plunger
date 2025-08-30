//! Unique IDs.

use core::fmt::Debug;

/// A marker trait for any type that functions as an ID.
///
/// # Safety
///
/// It must not be possible to create an arbitrary ID that is equal to one that already exists
/// without cloning that exact ID.
pub unsafe trait Id: Sized + Copy + PartialEq + Eq + Debug {}

/// A wrapper around an ID that asserts it is unique.
///
/// This takes away the implementation of [`Copy`] and [`Clone`] for an ID and forbids access to
/// the underlying ID.
#[derive(Debug, PartialEq, Eq)]
pub struct Unique<I: Id> {
    id: I,
}

impl<I: Id> Unique<I> {
    /// Create a new `Unique`, asserting that the given ID contained within is unique.
    ///
    /// # Safety
    ///
    /// The given `id` must be unique at the time of calling this function.
    pub const unsafe fn new(id: I) -> Self {
        Self { id }
    }

    /// Take the inner ID out of this [`Unique`], taking away the uniqueness guarantee.
    #[must_use]
    pub const fn into_inner(self) -> I {
        self.id
    }
}

// SAFETY: `Unique<I>` functions as a `SyncWrapper`
unsafe impl<I: Id> Sync for Unique<I> {}

#[cfg(debug_assertions)]
#[cfg(target_has_atomic = "64")]
mod checked {
    use super::Id;
    use super::Unique;
    use core::num::NonZeroU64;
    use core::sync::atomic;
    use core::sync::atomic::AtomicU64;

    /// An allocator of IDs that uses a global atomic `u64` counter.
    ///
    /// This type is only available on platforms with 64-bit atomics.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Checked(NonZeroU64);

    impl Checked {
        /// Allocate a new ID.
        #[must_use]
        pub fn new() -> Unique<Self> {
            static COUNTER: AtomicU64 = AtomicU64::new(1);
            const MAX_ID: u64 = u64::MAX >> 1;

            // Use Relaxed because there is no data that depends on this counter.
            let id = COUNTER.fetch_add(1, atomic::Ordering::Relaxed);

            // Ensure overflows don't happen. Abort instead of panicking so it can't be recovered
            // from.
            if id >= MAX_ID {
                std::process::abort();
            }

            // SAFETY: `COUNTER` starts at one and the above `assert!` ensures that it never
            // overflows.
            let id = Self(unsafe { NonZeroU64::new_unchecked(id) });

            // SAFETY: The counter only increments and never overflows.
            unsafe { Unique::new(id) }
        }
    }

    // SAFETY: `new` can never return two `u64`s with the same value.
    unsafe impl Id for Checked {}
}

#[cfg(target_has_atomic = "64")]
mod debug_checked {
    use super::Id;
    use super::Unique;
    #[cfg(debug_assertions)]
    use crate::pin_list::id;

    /// Equivalent to [`id::Checked`] when `debug_assertions` are enabled, but equivalent to
    /// [`id::Unchecked`] in release.
    ///
    /// This type is only available on platforms with 64-bit atomics.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DebugChecked {
        #[cfg(debug_assertions)]
        checked: id::checked::Checked,
    }

    impl DebugChecked {
        /// Create a new [`DebugChecked`]. With `debug_assertions` enabled, this will increment a
        /// global atomic counter. In release, this is a no-op.
        ///
        /// # Safety
        ///
        /// The returned ID must not be compared against any other `DebugChecked` IDs that
        /// originated from a different call to this function.
        ///
        /// Note that this function is completely safe to use when `debug_assertions` are enabled,
        /// although it still requires `unsafe` due to the behaviour in release.
        #[must_use]
        pub unsafe fn new() -> Unique<Self> {
            let this = Self {
                #[cfg(debug_assertions)]
                checked: id::checked::Checked::new().into_inner(),
            };
            // SAFETY: Ensured by callera
            unsafe { Unique::new(this) }
        }
    }

    // SAFETY: Ensured by caller in `DebugChecked::new`
    unsafe impl Id for DebugChecked {}
}
#[cfg(target_has_atomic = "64")]
pub use debug_checked::DebugChecked;
