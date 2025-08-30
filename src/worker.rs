use std::sync::Arc;

use crate::{Inner, Shutdown};

/// The worker state for a [`Plunger`](crate::Plunger)
pub struct Worker<Ctx = ()> {
    inner: Arc<Inner<Ctx>>,
}

impl<Ctx> Clone for Worker<Ctx> {
    fn clone(&self) -> Self {
        Worker::new(&self.inner)
    }
}

impl<Ctx> Drop for Worker<Ctx> {
    fn drop(&mut self) {
        self.inner.queue.lock().workers -= 1;
    }
}

impl<Ctx> Worker<Ctx> {
    pub(super) fn new(inner: &Arc<Inner<Ctx>>) -> Self {
        let inner = inner.clone();
        inner.queue.lock().workers += 1;
        Self { inner }
    }

    /// Run a worker thread.
    pub fn run(self, mut ctx: Ctx) {
        let mut guard = self.inner.queue.lock();
        let shutdown = loop {
            let Ok((job, node)) = guard.list.acquire_front(false) else {
                if let Some(shutdown) = &guard.shutdown {
                    break shutdown.clone();
                }

                self.inner.notify.wait(&mut guard);
                continue;
            };

            job.run_job(node, &mut guard, &mut ctx);
        };

        drop(guard);
        drop(self);

        if let Shutdown::Steal(tx) = shutdown {
            _ = tx.inner.send(ctx);
        }
    }
}
