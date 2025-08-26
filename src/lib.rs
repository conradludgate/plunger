//! [`Plunger`] quickly unblocks your async tasks.
//!
//! ## Example
//!
//! ```
//! #[tokio::main]
//! async fn main() {
//!     let plunger = plunger::Plunger::new();
//!
//!     let hash = "$argon2i$v=19$m=65536,t=1,p=1$c29tZXNhbHQAAAAAAAAAAA$+r0d29hqEB0yasKr55ZgICsQGSkl0v0kgwhd+U3wyRo";
//!     let password = "password";
//!
//!     plunger
//!         .unblock(move || password_auth::verify_password(password, hash))
//!         .await
//!         .unwrap();
//! }
//! ```
//!
//! ## Important notes
//!
//! While the intent is to unblock the async runtime, this API might have to defensively block the runtime if
//! cancellation occurs while the task is running.
//!
//! We assume the following:
//! 1. Cancellation is rare
//! 2. Tasks run in the range of 100us to 1ms
//! 3. We can use block_in_place to reduce the impact of blocking when the tokio feature is enabled and using a multithreaded runtime.

use core::{
    cell::UnsafeCell,
    pin::Pin,
    task::{Context, Poll},
};

use std::{
    future::poll_fn,
    mem::{ManuallyDrop, offset_of},
    num::NonZero,
    panic::{AssertUnwindSafe, UnwindSafe},
    ptr::NonNull,
    sync::Arc,
    task::Waker,
};

use parking_lot::{Condvar, Mutex};
use pinned_aliasable::Aliasable;

pub struct Plunger<Ctx = ()> {
    // TODO: use our own Arc, with strong_count = spawners and weak_count = workers.
    inner: Arc<Inner<Ctx>>,
}

impl<Ctx> Clone for Plunger<Ctx> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        inner.queue.lock().spawners += 1;
        Self { inner }
    }
}

impl<Ctx> Drop for Plunger<Ctx> {
    fn drop(&mut self) {
        let mut guard = self.inner.queue.lock();
        guard.spawners -= 1;
        if guard.spawners == 0 {
            self.inner.notify.notify_all();
        }
    }
}

struct Worker<Ctx> {
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

struct Inner<Ctx> {
    queue: Mutex<PlungerQueue<Ctx>>,
    notify: Condvar,
}

unsafe impl<Ctx> Send for Inner<Ctx> {}
unsafe impl<Ctx> Sync for Inner<Ctx> {}

impl Plunger {
    pub fn new() -> Self {
        let t = std::thread::available_parallelism().unwrap_or(const { NonZero::new(4).unwrap() });
        Self::with_threads(t)
    }

    pub fn with_threads(threads: NonZero<usize>) -> Self {
        Self::with_ctx(std::iter::repeat_n(|| {}, threads.get()))
    }

    #[inline(always)]
    pub fn unblock<F, R>(&self, task: F) -> impl Future<Output = R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.unblock_ctx(|&mut ()| task())
    }
}

impl Default for Plunger {
    fn default() -> Self {
        Self::new()
    }
}

impl<Ctx> Plunger<Ctx> {
    pub fn with_ctx(ctx: impl IntoIterator<Item = impl FnOnce() -> Ctx + Send + 'static>) -> Self
    where
        Ctx: Send + 'static,
    {
        let inner = Arc::new(Inner {
            queue: Mutex::new(PlungerQueue {
                head: None,
                tail: None,
                len: 0,
                workers: 0,
                spawners: 1,
            }),
            notify: Condvar::new(),
        });

        let worker = Worker {
            inner: inner.clone(),
        };

        let mut threads = 0;
        for ctx in ctx {
            threads += 1;
            let worker = worker.clone();
            std::thread::Builder::new()
                .name("plunger-worker".to_owned())
                .spawn(move || worker.worker(ctx))
                .unwrap();
        }

        assert!(threads > 0, "no threads spawned");

        Self { inner }
    }

    #[inline(always)]
    pub fn unblock_ctx<F, R>(&self, task: F) -> impl Future<Output = R>
    where
        Ctx: UnwindSafe,
        F: FnOnce(&mut Ctx) -> R + Send + 'static,
        R: Send + 'static,
    {
        Task::<'_, Ctx, F, R> {
            plunger: Some(self),
            inner: Aliasable::new(PlungerTask {
                footer: UnsafeCell::new(PlungerTaskHeader {
                    state: State::Init,
                    next: None,
                    prev: None,
                    f: run::<Ctx, F, R>,
                    waker: Waker::noop().clone(),
                }),
                state: UnsafeCell::new(Value {
                    queued: ManuallyDrop::new(task),
                }),
            }),
        }
    }

    pub fn workers(&self) -> usize {
        self.inner.queue.lock().workers
    }

    pub fn len(&self) -> usize {
        self.inner.queue.lock().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<Ctx> Worker<Ctx> {
    fn worker(&self, ctx: impl FnOnce() -> Ctx) {
        let mut ctx = ctx();

        let mut queue = self.inner.queue.lock();
        queue.workers += 1;

        loop {
            let Some(head) = queue.head else {
                if queue.spawners == 0 {
                    break;
                }

                self.inner.notify.wait(&mut queue);
                continue;
            };

            let f = {
                let header = unsafe { &mut *head.as_ptr() };
                debug_assert!(header.prev.is_none());

                // unlink from the queue
                queue.head = header.next;
                if let Some(next) = header.next {
                    unsafe { &mut *next.as_ptr() }.prev = None;
                } else {
                    queue.tail = None;
                }
                queue.len -= 1;

                header.state = State::Running;

                header.f
            };

            drop(queue);
            let state = unsafe { f(head.as_ptr(), &mut ctx) };
            queue = self.inner.queue.lock();

            let waker = {
                let header = unsafe { &mut *head.as_ptr() };
                header.state = state;
                core::mem::replace(&mut header.waker, Waker::noop().clone())
            };

            waker.wake();
        }
    }
}

struct PlungerQueue<Ctx> {
    head: Option<NonNull<PlungerTaskHeader<Ctx>>>,
    tail: Option<NonNull<PlungerTaskHeader<Ctx>>>,

    len: usize,
    workers: usize,
    spawners: usize,
}

#[repr(C)]
struct PlungerTask<Ctx, F, R> {
    // if header.state == Init, Completed, or Panicked, then this is owned by the Task.
    // if header.state == Queued, or Running, then this is owned by one of the worker threads.
    state: UnsafeCell<Value<F, R>>,

    // can only be read/mutated while holding the queue lock.
    footer: UnsafeCell<PlungerTaskHeader<Ctx>>,
}

struct PlungerTaskHeader<Ctx> {
    state: State,

    // next/prev are only used if state == Queued.
    next: Option<NonNull<PlungerTaskHeader<Ctx>>>,
    prev: Option<NonNull<PlungerTaskHeader<Ctx>>>,

    waker: Waker,

    f: unsafe fn(inout: *mut PlungerTaskHeader<Ctx>, ctx: &mut Ctx) -> State,
}

#[derive(PartialEq, Debug, Clone, Copy)]
enum State {
    Init,
    Queued,
    Running,
    Completed,
    Panicked,
}

union Value<F, R> {
    queued: ManuallyDrop<F>,
    running: (),
    completed: ManuallyDrop<R>,
    panicked: ManuallyDrop<Box<dyn core::any::Any + Send + 'static>>,
}

/// # Safety:
/// `task` must be from a `*mut PlungerTask<Ctx, F, R>` and it must be safe to deref.
/// `task.header.state` must be `Running`
unsafe fn run<Ctx, F, R>(task: *mut PlungerTaskHeader<Ctx>, ctx: &mut Ctx) -> State
where
    F: FnOnce(&mut Ctx) -> R + 'static,
    Ctx: UnwindSafe,
{
    let offset = offset_of!(PlungerTask<Ctx, F, R>, footer);

    // safety: caller ensures this is a valid PlungerTask
    let task = unsafe { &mut *task.byte_sub(offset).cast::<PlungerTask<Ctx, F, R>>() };

    // safety: caller ensures that state is Running, so we own this.
    let task_state = unsafe { &mut *task.state.get() };

    let func = unsafe { ManuallyDrop::take(&mut task_state.queued) };
    task_state.running = ();

    match std::panic::catch_unwind(AssertUnwindSafe(|| func(ctx))) {
        Ok(output) => {
            task_state.completed = ManuallyDrop::new(output);
            State::Completed
        }
        Err(panic) => {
            task_state.panicked = ManuallyDrop::new(panic);
            State::Panicked
        }
    }
}

pin_project_lite::pin_project!(
    struct Task<'a, Ctx, F, R> {
        #[pin]
        inner: Aliasable<PlungerTask<Ctx, F, R>>,

        plunger: Option<&'a Plunger<Ctx>>,
    }

    impl<Ctx, F, R> PinnedDrop for Task<'_, Ctx, F, R> {
        fn drop(mut this: Pin<&mut Self>) {
            while this.plunger.is_some() {
                let fut = poll_fn(|cx| this.as_mut().poll_inner(cx, true));

                #[cfg(feature = "tokio")]
                if let Ok(handle) = tokio::runtime::Handle::try_current()
                    && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
                {
                    let _ = tokio::task::block_in_place(|| pollster::block_on(fut));
                    continue;
                }

                let _ = pollster::block_on(fut);
            }
        }
    }
);

unsafe impl<Ctx, F: Send, R: Send> Send for Task<'_, Ctx, F, R> {}

impl<Ctx, F, R> UnwindSafe for Task<'_, Ctx, F, R> {}

impl<Ctx, F, R> Task<'_, Ctx, F, R> {
    #[inline]
    fn poll_inner(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        unlink: bool,
    ) -> Poll<std::thread::Result<Option<R>>> {
        let this = self.project();
        let Some(plunger) = *this.plunger else {
            panic!("polled after completion");
        };

        let task = this.inner.as_ref().get();

        let mut queue = plunger.inner.queue.lock();

        // we can only touch the inner header while the queue is locked.
        let header = unsafe { &mut *task.footer.get() };
        let notify = match header.state {
            // exit immediately.
            State::Init if unlink => {
                // safety: state is Init, so we own this.
                let task_state = unsafe { &mut *task.state.get() };
                unsafe { ManuallyDrop::drop(&mut task_state.queued) };

                *this.plunger = None;
                return Poll::Ready(Ok(None));
            }
            // unlink before exiting.
            State::Queued if unlink => {
                // unlink from the queue
                if let Some(prev) = header.prev {
                    unsafe { &mut *prev.as_ptr() }.next = header.next;
                } else {
                    queue.head = header.next;
                }

                if let Some(next) = header.next {
                    unsafe { &mut *next.as_ptr() }.prev = header.prev;
                } else {
                    queue.tail = header.prev;
                }

                queue.len -= 1;

                header.state = State::Init;

                // safety: state is Init, so we own this.
                let task_state = unsafe { &mut *task.state.get() };
                unsafe { ManuallyDrop::drop(&mut task_state.queued) };

                *this.plunger = None;
                return Poll::Ready(Ok(None));
            }
            // we need to enqueue ourselves.
            State::Init => {
                header.waker.clone_from(cx.waker());

                // get our header pointer
                let ptr = NonNull::new(task.footer.get());

                // link into the queue
                header.prev = core::mem::replace(&mut queue.tail, ptr);
                if let Some(prev) = header.prev {
                    unsafe { &mut *prev.as_ptr() }.next = ptr;
                } else {
                    queue.head = ptr;
                }
                queue.len += 1;

                // mark as linked.
                header.state = State::Queued;

                // notify that a task is now ready.
                true
            }
            // keep waiting.
            State::Queued | State::Running => {
                header.waker.clone_from(cx.waker());

                // do not notify about a new task.
                false
            }
            // get the value
            State::Completed => {
                // safety: state is Completed, so we own this.
                let task_state = unsafe { &mut *task.state.get() };
                // safety: state is Completed.
                let output = unsafe { ManuallyDrop::take(&mut task_state.completed) };

                *this.plunger = None;
                return Poll::Ready(Ok(Some(output)));
            }
            State::Panicked => {
                // safety: state is Panicked, so we own this.
                let task_state = unsafe { &mut *task.state.get() };
                // safety: state is Panicked.
                let output = unsafe { ManuallyDrop::take(&mut task_state.panicked) };

                *this.plunger = None;
                return Poll::Ready(Err(output));
            }
        };

        drop(queue);
        if notify {
            plunger.inner.notify.notify_one();
        }

        Poll::Pending
    }
}

impl<Ctx, F, R> Future for Task<'_, Ctx, F, R>
where
    F: FnOnce(&mut Ctx) -> R + 'static,
{
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.poll_inner(cx, false) {
            Poll::Ready(Ok(Some(output))) => Poll::Ready(output),
            Poll::Ready(Ok(None)) => unreachable!(),
            Poll::Ready(Err(output)) => std::panic::resume_unwind(output),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        hint::black_box,
        num::NonZero,
        pin::pin,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use crossbeam_utils::sync::WaitGroup;
    use futures::{FutureExt, future::Either};
    use pbkdf2::pbkdf2_hmac_array;
    use tokio::task::JoinSet;

    use crate::Plunger;

    #[tokio::test]
    async fn basic() {
        let plunger = Plunger::with_threads(NonZero::new(1).unwrap());

        let lhs_res = String::from("lhs");
        let rhs_res = String::from("rhs");

        let lhs = pin!(plunger.unblock(|| {
            std::thread::sleep(Duration::from_secs(2));
            lhs_res
        }));
        let rhs = pin!(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            rhs_res
        });

        let (lhs_res, rhs_res) = match futures::future::select(lhs, rhs).await {
            Either::Left(_) => {
                panic!("rhs should complete first.")
            }
            Either::Right((rhs_res, lhs)) => (lhs.await, rhs_res),
        };

        assert_eq!(&*lhs_res, "lhs");
        assert_eq!(&*rhs_res, "rhs");
    }

    #[ignore = "slow"]
    #[tokio::test]
    async fn smoke() {
        const N: u32 = 1000;
        const M: u32 = 1000;

        // warmup 0

        for _ in 0..M {
            black_box(pbkdf2_hmac_array::<sha2::Sha256, 32>(
                black_box(b"hunter2"),
                black_box(b"mysupersecuresalt"),
                400,
            ));
        }

        // warmup 1

        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    for _ in 0..M {
                        black_box(pbkdf2_hmac_array::<sha2::Sha256, 32>(
                            black_box(b"hunter2"),
                            black_box(b"mysupersecuresalt"),
                            400,
                        ));
                    }
                });
            }
        });
        let expected_dur = start.elapsed() / M / 4;

        // proper run

        let plunger = Plunger::with_threads(NonZero::new(4).unwrap());

        let start = Instant::now();
        let mut join_set = JoinSet::new();
        for _ in 0..N {
            let plunger = plunger.clone();
            join_set.spawn(async move {
                for _ in 0..M {
                    let res = plunger
                        .unblock(move || {
                            pbkdf2_hmac_array::<sha2::Sha256, 32>(
                                black_box(b"hunter2"),
                                black_box(b"mysupersecuresalt"),
                                400,
                            )
                        })
                        .await;

                    black_box(res);
                }
            });
        }
        join_set.join_all().await;

        let dur = start.elapsed() / N / M;

        dbg!(dur, expected_dur);
    }

    #[tokio::test]
    async fn panic() {
        let plunger = Plunger::with_threads(NonZero::new(1).unwrap());

        let mut drop_check = Arc::new(());
        let drop_check2 = drop_check.clone();

        let panic = plunger
            .unblock(|| {
                if true {
                    panic!("panic!")
                }
                drop_check2
            })
            .catch_unwind()
            .await
            .unwrap_err();

        assert_eq!(panic.downcast_ref::<&str>(), Some(&"panic!"));

        Arc::get_mut(&mut drop_check).expect("the arc should be unique");
    }

    #[tokio::test]
    async fn cancellation() {
        let plunger = Plunger::with_threads(NonZero::new(1).unwrap());

        let first_state = Arc::new(Mutex::new(0));
        let second_state = Arc::new(Mutex::new(0));

        {
            let first_state = first_state.clone();
            let mut first = pin!(plunger.unblock(move || {
                *first_state.lock().unwrap() += 1;
                std::thread::sleep(Duration::from_secs(2));
                *first_state.lock().unwrap() += 1;
            }));

            // poll once to enqueue
            first.as_mut().now_or_never();

            {
                let second_state = second_state.clone();
                let mut second = pin!(plunger.unblock(move || {
                    *second_state.lock().unwrap() += 1;
                    std::thread::sleep(Duration::from_secs(2));
                    *second_state.lock().unwrap() += 1;
                }));

                // poll once to enqueue
                second.as_mut().now_or_never();

                // allow some time for the worker thread to wake up.
                tokio::time::sleep(Duration::from_millis(10)).await;

                // drop second, it should not wait.
            }
            assert_eq!(*second_state.lock().unwrap(), 0);

            // drop first, it should wait.
        }
        assert_eq!(*first_state.lock().unwrap(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn cancellation_mt() {
        let plunger = Plunger::with_threads(NonZero::new(1).unwrap());

        let first_state = Arc::new(Mutex::new(0));
        let second_state = Arc::new(Mutex::new(0));

        let block_check = tokio::spawn({
            let first_state = first_state.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(1)).await;

                if cfg!(feature = "tokio") {
                    // check that the tokio worker thread was not blocked.
                    assert_eq!(*first_state.lock().unwrap(), 1);
                } else {
                    // check that the tokio worker thread was blocked.
                    assert_eq!(*first_state.lock().unwrap(), 2);
                }
                first_state
            }
        });

        tokio::spawn(async move {
            let first_state = first_state.clone();
            let mut first = pin!(plunger.unblock(move || {
                *first_state.lock().unwrap() += 1;
                std::thread::sleep(Duration::from_secs(2));
                *first_state.lock().unwrap() += 1;
            }));

            // poll once to enqueue
            first.as_mut().now_or_never();

            {
                let second_state = second_state.clone();
                let mut second = pin!(plunger.unblock(move || {
                    *second_state.lock().unwrap() += 1;
                    std::thread::sleep(Duration::from_secs(2));
                    *second_state.lock().unwrap() += 1;
                }));

                // poll once to enqueue
                second.as_mut().now_or_never();

                // allow some time for the worker thread to wake up.
                tokio::time::sleep(Duration::from_millis(10)).await;

                // drop second, it should not wait.
            }
            assert_eq!(*second_state.lock().unwrap(), 0);

            // drop first, it should wait.
        })
        .await
        .unwrap();

        let first_state = block_check.await.unwrap();
        assert_eq!(*first_state.lock().unwrap(), 2);
    }

    #[test]
    fn shutdown() {
        let wg = WaitGroup::new();
        let plunger = Plunger::with_ctx(
            std::iter::from_fn(|| Some(wg.clone()))
                .map(|wg| move || wg)
                .take(8),
        );

        let plunger2 = plunger.clone();

        drop(plunger);
        drop(plunger2);

        wg.wait();
    }
}
