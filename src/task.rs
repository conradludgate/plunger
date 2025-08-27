use std::{
    cell::UnsafeCell,
    panic::UnwindSafe,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use diatomic_waker::DiatomicWaker;
use futures_util::task::waker_ref;
use pinned_aliasable::Aliasable;

use crate::{
    JobComplete, Plunger, RawDynJob, Types, block,
    job::Job,
    pin_list::{Node, NodeData},
};

pin_project_lite::pin_project!(
    pub(super) struct Task<'a, Ctx, J: Job<Ctx>> {
        #[pin]
        state: Aliasable<UnsafeCell<J>>,

        #[pin]
        node: Node<Types<Ctx>>,

        plunger: Option<&'a Plunger<Ctx>>,
    }

    impl<Ctx, J: Job<Ctx>> PinnedDrop for Task<'_, Ctx, J> {
        fn drop(mut this: Pin<&mut Self>) {
            this.drop_inner();
        }
    }
);

unsafe impl<Ctx, J: Job<Ctx> + Send> Send for Task<'_, Ctx, J> {}

impl<Ctx, J: Job<Ctx>> UnwindSafe for Task<'_, Ctx, J> {}

impl<'a, Ctx, J: Job<Ctx>> Task<'a, Ctx, J> {
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

impl<Ctx, J: Job<Ctx> + Send + 'static> Future for Task<'_, Ctx, J> {
    type Output = J::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let Some(plunger) = *this.plunger else {
            panic!("polled after completion");
        };

        let Some(node) = this.node.as_mut().initialized_mut() else {
            let job = unsafe { RawDynJob::new(this.state.as_ref()) };

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
            JobComplete::Success => Poll::Ready(unsafe { task_state.output() }),
            JobComplete::Panic => unsafe { task_state.resume() },
        }
    }
}

impl<Ctx, J: Job<Ctx>> Task<'_, Ctx, J> {
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

        // safety: no longer shared
        let task_state = unsafe { &mut *this.state.as_ref().get().get() };

        match data {
            // safety: we were linked in the queue, so the queued value must be initialised
            NodeData::Linked(_) => unsafe { task_state.discard_init() },
            NodeData::Released(JobComplete::Success) => unsafe {
                task_state.output();
            },
            NodeData::Released(JobComplete::Panic) => unsafe { task_state.discard_unwind() },
        }
    }
}
