use std::{
    cell::UnsafeCell,
    mem::ManuallyDrop,
    panic::AssertUnwindSafe,
    pin::Pin,
    ptr::NonNull,
    task::{Poll, ready},
};

use parking_lot::MutexGuard;
use pinned_aliasable::Aliasable;

use crate::{PlungerQueue, Types, pin_list::AcquiredNode};

pub(super) struct RawDynJob<Ctx> {
    job: *mut (dyn JobErased<Ctx> + Send),
}

// like a `&'static mut dyn JobErased<Ctx>`
unsafe impl<Ctx> Send for RawDynJob<Ctx> {}

impl<Ctx> RawDynJob<Ctx> {
    pub unsafe fn new<F>(job: Pin<&Aliasable<UnsafeCell<F>>>) -> Self
    where
        F: JobErased<Ctx> + Send,
    {
        let job = NonNull::from(job.get()).as_ptr().cast::<F>();
        Self { job: job as _ }
    }

    pub(super) fn run_job(
        self,
        node: AcquiredNode<Types<Ctx>>,
        guard: &mut MutexGuard<PlungerQueue<Ctx>>,
        ctx: &mut Ctx,
    ) {
        // safety: job must be initialised.
        let result = MutexGuard::unlocked(guard, || unsafe { &mut *self.job }.run(ctx));

        match result {
            Poll::Pending => {
                if *node.acquired(&guard.list) {
                    node.unprotected(&guard.list).notify();
                }
                // safety: job is still initialised
                node.insert_after(&mut guard.list.cursor_back_mut(), self);
            }
            Poll::Ready(result) => {
                guard.len -= 1;
                node.unprotected(&guard.list).notify();
                node.release_current(&mut guard.list, result);
            }
        }
    }
}

pub enum JobComplete {
    Success,
    Panic,
}

pub trait JobErased<Ctx> {
    /// # Safety
    /// After returning Ready or panicking, this should never be called again.
    unsafe fn poll(&mut self, ctx: &mut Ctx) -> Poll<()>;

    /// Store the panic from the last poll
    fn panic(&mut self, panic: Box<dyn std::any::Any + Send + 'static>);

    fn run(&mut self, ctx: &mut Ctx) -> Poll<JobComplete> {
        let res = std::panic::catch_unwind(AssertUnwindSafe(|| unsafe { self.poll(ctx) }));
        match res {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) => Poll::Ready(JobComplete::Success),
            Err(panic) => {
                self.panic(panic);
                Poll::Ready(JobComplete::Panic)
            }
        }
    }
}

pub trait Job<Ctx>: JobErased<Ctx> {
    type Output;

    unsafe fn discard_init(&mut self);
    unsafe fn output(&mut self) -> Self::Output;
    unsafe fn resume(&mut self) -> !;
    unsafe fn discard_unwind(&mut self);
}

pub struct Once<F, R>(Value<F, R>);
pub struct Until<F, R>(Value<F, R>);

impl<F, R> Once<F, R> {
    pub fn new(f: F) -> Self {
        Self(Value {
            queued: ManuallyDrop::new(f),
        })
    }
}
impl<F, R> Until<F, R> {
    pub fn new(f: F) -> Self {
        Self(Value {
            queued: ManuallyDrop::new(f),
        })
    }
}

impl<Ctx, F, R> JobErased<Ctx> for Once<F, R>
where
    F: FnOnce(&mut Ctx) -> R,
{
    unsafe fn poll(&mut self, ctx: &mut Ctx) -> Poll<()> {
        let func = unsafe { ManuallyDrop::take(&mut self.0.queued) };

        let r = func(ctx);
        self.0.completed = ManuallyDrop::new(r);

        Poll::Ready(())
    }

    fn panic(&mut self, panic: Box<dyn std::any::Any + Send + 'static>) {
        self.0.panicked = ManuallyDrop::new(panic);
    }
}

impl<Ctx, F, R> JobErased<Ctx> for Until<F, R>
where
    F: FnMut(&mut Ctx) -> Poll<R>,
{
    unsafe fn poll(&mut self, ctx: &mut Ctx) -> Poll<()> {
        let func = unsafe { &mut self.0.queued };

        let r = ready!(func(ctx));
        unsafe { ManuallyDrop::drop(&mut self.0.queued) };
        self.0.completed = ManuallyDrop::new(r);

        Poll::Ready(())
    }

    fn panic(&mut self, panic: Box<dyn std::any::Any + Send + 'static>) {
        unsafe { ManuallyDrop::drop(&mut self.0.queued) };
        self.0.panicked = ManuallyDrop::new(panic);
    }
}

impl<Ctx, F, R> Job<Ctx> for Once<F, R>
where
    F: FnOnce(&mut Ctx) -> R,
{
    type Output = R;

    unsafe fn discard_init(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.0.queued) };
    }

    unsafe fn output(&mut self) -> Self::Output {
        unsafe { ManuallyDrop::take(&mut self.0.completed) }
    }

    unsafe fn resume(&mut self) -> ! {
        let output = unsafe { ManuallyDrop::take(&mut self.0.panicked) };
        std::panic::resume_unwind(output)
    }

    unsafe fn discard_unwind(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.0.panicked) };
    }
}

impl<Ctx, F, R> Job<Ctx> for Until<F, R>
where
    F: FnMut(&mut Ctx) -> Poll<R>,
{
    type Output = R;

    unsafe fn discard_init(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.0.queued) };
    }

    unsafe fn output(&mut self) -> Self::Output {
        unsafe { ManuallyDrop::take(&mut self.0.completed) }
    }

    unsafe fn resume(&mut self) -> ! {
        let output = unsafe { ManuallyDrop::take(&mut self.0.panicked) };
        std::panic::resume_unwind(output)
    }

    unsafe fn discard_unwind(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.0.panicked) };
    }
}

// We currently use a union because:
// 1. There's already enough discriminants to know which state.
// 2. We want the `completed` value to be at the start of our task.
union Value<F, R> {
    // Initialised if our task node is uninitalised, or if the task is linked.
    queued: ManuallyDrop<F>,
    // Initialised if our task is removed.
    completed: ManuallyDrop<R>,
    // Initialised if our task is removed.
    panicked: ManuallyDrop<Box<dyn core::any::Any + Send + 'static>>,
}

unsafe impl<F: Send, R: Send> Send for Value<F, R> {}
