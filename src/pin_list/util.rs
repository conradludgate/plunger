//! Shared internal utilities.

use core::mem::ManuallyDrop;

pub(crate) fn run_on_drop<F: FnOnce()>(f: F) -> impl Drop {
    RunOnDrop(ManuallyDrop::new(f))
}

struct RunOnDrop<F: FnOnce()>(ManuallyDrop<F>);
impl<F: FnOnce()> Drop for RunOnDrop<F> {
    fn drop(&mut self) {
        (unsafe { ManuallyDrop::take(&mut self.0) })();
    }
}

macro_rules! debug_unreachable {
    ($($tt:tt)*) => {
        if cfg!(debug_assertions) {
            unreachable!($($tt)*)
        } else {
            core::hint::unreachable_unchecked()
        }
    }
}
pub(crate) use debug_unreachable;

/// Polyfill for `Option::unwrap_unchecked`, since this crate's MSRV is older than the
/// stabilization of that function.
pub(crate) unsafe fn unwrap_unchecked<T>(opt: Option<T>) -> T {
    match opt {
        Some(val) => val,
        // SAFETY: The caller ensures the `Option` is not `None`.
        None => unsafe { debug_unreachable!() },
    }
}
