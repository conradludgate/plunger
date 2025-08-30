use std::{
    cell::UnsafeCell,
    panic::UnwindSafe,
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll},
};

use diatomic_waker::DiatomicWaker;
use futures_util::future::Either;
use futures_util::task::waker_ref;
use pinned_aliasable::Aliasable;

use crate::{
    Plunger, Types, block, job,
    pin_list::{Node, NodeData},
};

pin_project_lite::pin_project!(
    /// A task spawned into a [`Plunger`]
    pub struct Task<'a, J: job::Job<Ctx>, Ctx = ()> {
        #[pin]
        state: Aliasable<UnsafeCell<J>>,

        #[pin]
        node: Node<Types<Ctx>>,

        plunger: Option<&'a Plunger<Ctx>>,
    }

    impl<Ctx, J: job::Job<Ctx>> PinnedDrop for Task<'_, J, Ctx> {
        fn drop(mut this: Pin<&mut Self>) {
            this.drop_inner();
        }
    }
);

#[cfg(feature = "nightly_async_drop")]
impl<Ctx, J: job::Job<Ctx>> std::future::AsyncDrop for Task<'_, J, Ctx> {
    fn drop(mut self: Pin<&mut Self>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |cx| self.as_mut().poll_drop(cx).map(|_| {}))
    }
}

unsafe impl<Ctx, J: job::Job<Ctx> + Send> Send for Task<'_, J, Ctx> {}

impl<Ctx, J: job::Job<Ctx>> UnwindSafe for Task<'_, J, Ctx> {}

impl<'a, Ctx, J: job::Job<Ctx>> Task<'a, J, Ctx> {
    #[inline(always)]
    pub(super) fn new(plunger: &'a Plunger<Ctx>, job: J) -> Self
    where
        Ctx: UnwindSafe,
    {
        Self {
            state: Aliasable::new(UnsafeCell::new(job)),
            node: Node::new(DiatomicWaker::new()),
            plunger: Some(plunger),
        }
    }
}

impl<Ctx, J: job::Job<Ctx> + Send + 'static> Future for Task<'_, J, Ctx> {
    type Output = J::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let Some(plunger) = *this.plunger else {
            panic!("polled after completion");
        };

        let Some(node) = this.node.as_mut().initialized_mut() else {
            let job = unsafe { job::RawDynJob::new(this.state.as_ref()) };

            let mut guard = plunger.inner.queue.lock();
            guard.len += 1;
            let node = guard.list.push_back(this.node, job);

            unsafe { node.unprotected().register(cx.waker()) };
            drop(guard);

            plunger.inner.notify.notify_one();

            return Poll::Pending;
        };

        let guard = plunger.inner.queue.lock();

        if node.protected(&guard.list).is_some() || node.acquired(&guard.list).is_some() {
            unsafe { node.unprotected().register(cx.waker()) };
            drop(guard);

            return Poll::Pending;
        }

        drop(guard);

        *this.plunger = None;
        let completion = unsafe { node.take_removed_unchecked() };

        // safety: no longer shared
        let task_state = unsafe { &mut *this.state.as_ref().get().get() };

        match completion {
            job::JobComplete::Success => Poll::Ready(unsafe { task_state.output() }),
            job::JobComplete::Panic => unsafe { task_state.resume() },
        }
    }
}

impl<Ctx, J: job::Job<Ctx> + Send + 'static> Task<'_, J, Ctx> {
    /// Run the task to completion, or until the cancellation future completes.
    ///
    /// If the cancellation future completes, we might not be able to cancel the job immediately as it might
    /// in the middle of execution. You should not rely on this completing as soon as the cancellation future
    /// completes.
    pub async fn or_cancelled<C>(self, cancel: impl Future<Output = C>) -> Result<J::Output, C> {
        match futures_util::future::select(pin!(self), pin!(cancel)).await {
            Either::Left((output, _)) => Ok(output),
            Either::Right((cancelled, this)) => this.cancel().await.map_err(|()| cancelled),
        }
    }
}

impl<Ctx, J: job::Job<Ctx>> Task<'_, J, Ctx> {
    /// Cancel the task as soon as possible, returning only when it is fully cancelled.
    pub async fn cancel(mut self: Pin<&mut Self>) -> Result<J::Output, ()> {
        std::future::poll_fn(|cx| self.as_mut().poll_drop(cx)).await
    }

    #[inline]
    fn poll_drop(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<J::Output, ()>> {
        let mut this = self.project();
        let Some(plunger) = *this.plunger else {
            return Poll::Ready(Err(()));
        };

        let Some(mut node) = this.node.as_mut().initialized_mut() else {
            // safety: state is Init, so we own this.
            let task_state = unsafe { &mut *this.state.as_ref().get().get() };
            unsafe { task_state.discard_init() }
            return Poll::Ready(Err(()));
        };

        let mut guard = plunger.inner.queue.lock();

        let data = match node.reset(&mut guard.list) {
            Ok(data) => data,
            // currently running, cannot be removed
            Err(err_node) => {
                node = err_node;

                // register our intent to cancel.
                *node.acquired_mut(&mut guard.list).unwrap() = true;

                unsafe { node.unprotected().register(cx.waker()) };
                return Poll::Pending;
            }
        };

        drop(guard);

        *this.plunger = None;
        // safety: no longer shared
        let task_state = unsafe { &mut *this.state.as_ref().get().get() };

        match data {
            // safety: we were linked in the queue, so the queued value must be initialised
            NodeData::Linked(_) => unsafe {
                task_state.discard_init();
                Poll::Ready(Err(()))
            },
            NodeData::Released(job::JobComplete::Success) => unsafe {
                Poll::Ready(Ok(task_state.output()))
            },
            NodeData::Released(job::JobComplete::Panic) => unsafe { task_state.resume() },
        }
    }

    #[inline]
    fn drop_inner(self: Pin<&mut Self>) {
        let mut this = self.project();
        let Some(plunger) = *this.plunger else {
            return;
        };

        let Some(mut node) = this.node.as_mut().initialized_mut() else {
            // safety: state is Init, so we own this.
            let task_state = unsafe { &mut *this.state.as_ref().get().get() };
            unsafe { task_state.discard_init() }
            return;
        };

        let mut guard = plunger.inner.queue.lock();

        let mut signal = None;
        let data = loop {
            match node.reset(&mut guard.list) {
                Ok(data) => break data,
                // currently running, cannot be removed
                Err(err_node) => {
                    node = err_node;

                    // register our intent to cancel.
                    *node.acquired_mut(&mut guard.list).unwrap() = true;

                    let signal = signal.get_or_insert_with(|| Arc::new(block::Signal::new()));
                    unsafe { node.unprotected().register(&waker_ref(signal)) };

                    signal.wait(&mut guard);
                }
            }
        };

        drop(guard);

        *this.plunger = None;
        // safety: no longer shared
        let task_state = unsafe { &mut *this.state.as_ref().get().get() };

        match data {
            // safety: we were linked in the queue, so the queued value must be initialised
            NodeData::Linked(_) => unsafe { task_state.discard_init() },
            NodeData::Released(job::JobComplete::Success) => unsafe {
                task_state.output();
            },
            NodeData::Released(job::JobComplete::Panic) => unsafe { task_state.discard_unwind() },
        }
    }
}
