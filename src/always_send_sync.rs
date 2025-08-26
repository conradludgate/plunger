//! Inspired by <https://docs.rs/always_send/latest/always_send/index.html>.

use std::marker::PhantomData;

#[derive(Clone)]
pub struct AlwaysSendSync<T> {
    /// The inner value is publicly accessible, and there is no [`Drop`] implementation
    /// so you can have full access to it.
    ///
    /// For this reasons, we also don't provides any getter methods, or `.into_inner()`.
    ///
    /// Another (private) field in this struct enforces invariance and prevents construction
    /// other than through methods such as [`AlwaysSend::new`].
    pub inner: T,
    marker: PhantomData<fn() -> *mut T>,
}

/// This is the main feature, an implementation of `Send` *without* reqiring `T: Send`.
unsafe impl<T> Send for AlwaysSendSync<T> {}
/// This is the main feature, an implementation of `Sync` *without* reqiring `T: Sync`.
unsafe impl<T> Sync for AlwaysSendSync<T> {}

// this is because all constructors do require `T: Send + Sync`
// and invariance ensures there is no way in which
// the contained type `T` could change later
impl<T: Send + Sync> AlwaysSendSync<T> {
    /// Wraps sendable type in the [`AlwaysSendSync<T>`] wrapper.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            marker: PhantomData,
        }
    }
}
