use std::{
    cell::UnsafeCell,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, UnwindSafe},
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll},
};

use diatomic_waker::DiatomicWaker;
use futures_util::task::waker_ref;
use pinned_aliasable::Aliasable;

use crate::{
    JobComplete, Plunger, Types, block,
    pin_list::{Node, NodeData},
};

pin_project_lite::pin_project!(
    pub(super) struct Task<'a, Ctx, F, R> {
        #[pin]
        state: Aliasable<UnsafeCell<Value<F, R>>>,

        #[pin]
        node: Node<Types<Ctx>>,

        job: unsafe fn(inout: *mut (), ctx: &mut Ctx) -> Poll<JobComplete>,

        plunger: Option<&'a Plunger<Ctx>>,
    }

    impl<Ctx, F, R> PinnedDrop for Task<'_, Ctx, F, R> {
        fn drop(mut this: Pin<&mut Self>) {
            this.drop_inner();
        }
    }
);

unsafe impl<Ctx, F: Send, R: Send> Send for Task<'_, Ctx, F, R> {}

impl<Ctx, F, R> UnwindSafe for Task<'_, Ctx, F, R> {}

impl<'a, Ctx, F, R> Task<'a, Ctx, F, R> {
    #[inline(always)]
    pub(super) fn once(plunger: &'a Plunger<Ctx>, task: F) -> impl Future<Output = R>
    where
        Ctx: UnwindSafe,
        F: FnOnce(&mut Ctx) -> R + Send + 'static,
        R: Send + 'static,
    {
        Self {
            state: Aliasable::new(UnsafeCell::new(Value {
                queued: ManuallyDrop::new(task),
            })),
            node: Node::new(),
            job: run_once::<Ctx, F, R>,
            plunger: Some(plunger),
        }
    }

    #[inline(always)]
    pub(super) fn until(plunger: &'a Plunger<Ctx>, task: F) -> impl Future<Output = R>
    where
        Ctx: UnwindSafe,
        F: FnMut(&mut Ctx) -> Poll<R> + Send + 'static,
        R: Send + 'static,
    {
        Self {
            state: Aliasable::new(UnsafeCell::new(Value {
                queued: ManuallyDrop::new(task),
            })),
            node: Node::new(),
            job: run_repeat::<Ctx, F, R>,
            plunger: Some(plunger),
        }
    }
}

impl<Ctx, F, R> Future for Task<'_, Ctx, F, R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let Some(plunger) = *this.plunger else {
            panic!("polled after completion");
        };

        let Some(node) = this.node.as_mut().initialized_mut() else {
            // get our shared pointer
            // this is a bit awkward to preserve the borrow stack.
            let data = NonNull::from(this.state.as_ref().get()).as_ptr().cast();

            let job = Job {
                job: *this.job,
                data,
            };

            let mut guard = plunger.inner.queue.lock();
            guard.len += 1;
            let node = guard.list.push_back(this.node, job, DiatomicWaker::new());

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

        let (completion, _) = unsafe { node.take_removed_unchecked() };

        drop(guard);

        // safety: no longer shared
        let task_state = unsafe { &mut *this.state.as_ref().get().get() };

        match completion {
            JobComplete::Success => {
                let output = unsafe { ManuallyDrop::take(&mut task_state.completed) };

                *this.plunger = None;
                Poll::Ready(output)
            }
            JobComplete::Panic => {
                let output = unsafe { ManuallyDrop::take(&mut task_state.panicked) };

                *this.plunger = None;
                std::panic::resume_unwind(output)
            }
        }
    }
}

impl<Ctx, F, R> Task<'_, Ctx, F, R> {
    #[inline]
    fn drop_inner(self: Pin<&mut Self>) {
        let mut this = self.project();
        let Some(plunger) = *this.plunger else {
            return;
        };

        let Some(mut node) = this.node.as_mut().initialized_mut() else {
            // safety: state is Init, so we own this.
            let task_state = unsafe { &mut *this.state.as_ref().get().get() };
            unsafe { ManuallyDrop::drop(&mut task_state.queued) };
            return;
        };

        let mut guard = plunger.inner.queue.lock();

        let mut signal = None;
        let data = loop {
            match node.reset(&mut guard.list) {
                Ok((data, _)) => break data,
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
            NodeData::Linked(_) => unsafe { ManuallyDrop::drop(&mut task_state.queued) },
            NodeData::Released(JobComplete::Success) => unsafe {
                ManuallyDrop::drop(&mut task_state.completed)
            },
            NodeData::Released(JobComplete::Panic) => unsafe {
                ManuallyDrop::drop(&mut task_state.panicked)
            },
        }
    }
}

// todo: how can we store a `&mut dyn FnMut(&mut Ctx) -> Poll<()>` inside of pin_list :think:
pub(super) struct Job<Ctx> {
    job: unsafe fn(inout: *mut (), ctx: &mut Ctx) -> Poll<JobComplete>,
    data: *mut (),
}

unsafe impl<Ctx> Send for Job<Ctx> {}

impl<Ctx> Job<Ctx> {
    pub(super) fn run(&mut self, ctx: &mut Ctx) -> Poll<JobComplete> {
        unsafe { (self.job)(self.data, ctx) }
    }
}

union Value<F, R> {
    queued: ManuallyDrop<F>,
    completed: ManuallyDrop<R>,
    panicked: ManuallyDrop<Box<dyn core::any::Any + Send + 'static>>,
}

/// # Safety:
/// `task` must be from a `*mut PlungerTask<Ctx, F, R>` and it must be safe to deref.
/// `task.shared.state` must be `Running`
unsafe fn task_state<'a, F, R>(task_state: *mut ()) -> &'a mut Value<F, R> {
    let task_state = task_state.cast::<UnsafeCell<Value<F, R>>>();

    // safety: caller ensures that state is Running, so we own this.
    unsafe { &mut *(*task_state).get() }
}

/// # Safety:
/// `task` must be from a `*mut PlungerTask<Ctx, F, R>` and it must be safe to deref.
/// `task.shared.state` must be `Running`
unsafe fn run_once<Ctx, F, R>(state: *mut (), ctx: &mut Ctx) -> Poll<JobComplete>
where
    F: FnOnce(&mut Ctx) -> R + 'static,
    Ctx: UnwindSafe,
{
    // safety: caller ensures that state is Running, so we own this.
    let task_state = unsafe { task_state::<F, R>(state) };

    let func = unsafe { ManuallyDrop::take(&mut task_state.queued) };
    match std::panic::catch_unwind(AssertUnwindSafe(|| func(ctx))) {
        Ok(output) => {
            task_state.completed = ManuallyDrop::new(output);
            Poll::Ready(JobComplete::Success)
        }
        Err(panic) => {
            task_state.panicked = ManuallyDrop::new(panic);
            Poll::Ready(JobComplete::Panic)
        }
    }
}

/// # Safety:
/// `task` must be from a `*mut PlungerTask<Ctx, F, R>` and it must be safe to deref.
/// `task.shared.state` must be `Running`
unsafe fn run_repeat<Ctx, F, R>(state: *mut (), ctx: &mut Ctx) -> Poll<JobComplete>
where
    F: FnMut(&mut Ctx) -> Poll<R> + 'static,
    Ctx: UnwindSafe,
{
    // safety: caller ensures that state is Running, so we own this.
    let task_state = unsafe { task_state::<F, R>(state) };

    let func = unsafe { &mut task_state.queued };
    match std::panic::catch_unwind(AssertUnwindSafe(|| func(ctx))) {
        Ok(Poll::Pending) => Poll::Pending,
        Ok(Poll::Ready(output)) => {
            unsafe { ManuallyDrop::drop(&mut task_state.queued) };
            task_state.completed = ManuallyDrop::new(output);
            Poll::Ready(JobComplete::Success)
        }
        Err(panic) => {
            unsafe { ManuallyDrop::drop(&mut task_state.queued) };
            task_state.panicked = ManuallyDrop::new(panic);
            Poll::Ready(JobComplete::Panic)
        }
    }
}
