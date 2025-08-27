use std::{sync::Arc, task::Poll};

use parking_lot::lock_api::MutexGuard;

use crate::{Inner, Shutdown};

pub struct Worker<Ctx> {
    inner: Arc<Inner<Ctx>>,
}

impl<Ctx> Clone for Worker<Ctx> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        inner.queue.lock().workers += 1;
        Self { inner }
    }
}

impl<Ctx> Drop for Worker<Ctx> {
    fn drop(&mut self) {
        self.inner.queue.lock().workers -= 1;
    }
}

impl<Ctx> Worker<Ctx> {
    pub(super) fn new(inner: &Arc<Inner<Ctx>>) -> Self {
        Self {
            inner: inner.clone(),
        }
    }

    /// Run a worker thread.
    pub fn run(self, mut ctx: Ctx) {
        let mut guard = self.inner.queue.lock();
        let shutdown = loop {
            let Ok((mut job, node)) = guard.list.acquire_front(false) else {
                if let Some(shutdown) = &guard.shutdown {
                    break shutdown.clone();
                }

                self.inner.notify.wait(&mut guard);
                continue;
            };

            let result = MutexGuard::unlocked(&mut guard, || job.run(&mut ctx));

            match result {
                Poll::Pending => {
                    if *node.acquired(&guard.list) {
                        node.unprotected(&guard.list).notify();
                    }
                    node.insert_after(&mut guard.list.cursor_back_mut(), job);
                }
                Poll::Ready(result) => {
                    guard.len -= 1;
                    node.unprotected(&guard.list).notify();
                    node.release_current(&mut guard.list, result);
                }
            }
        };

        drop(guard);
        drop(self);

        if let Shutdown::Steal(tx) = shutdown {
            _ = tx.inner.send(ctx);
        }
    }
}
