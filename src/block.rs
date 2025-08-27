//! pollster but possibly aware of the tokio runtime.

use std::sync::Arc;

use futures_util::task::ArcWake;
use parking_lot::{Condvar, MutexGuard};

pub struct Signal {
    cond: Condvar,
}

fn block_in_place<R>(f: impl FnOnce() -> R) -> R {
    #[cfg(feature = "tokio")]
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        log("block_in_place");
        return tokio::task::block_in_place(f);
    }

    log("blocking");
    f()
}

fn log(mode: &str) {
    tracing::warn!(mode, "blocking due to cancellation");
}

impl Signal {
    pub fn new() -> Self {
        Self {
            cond: Condvar::new(),
        }
    }

    pub fn wait<T>(&self, state: &mut MutexGuard<T>) {
        block_in_place(|| {
            self.cond.wait(state);
        });
    }

    fn notify(&self) {
        self.cond.notify_one();
    }
}

impl ArcWake for Signal {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.notify();
    }
}
