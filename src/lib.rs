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

mod always_send_sync;
mod block;
mod pin_list;

use core::{
    cell::UnsafeCell,
    pin::Pin,
    task::{Context, Poll},
};

use std::{
    future::poll_fn,
    mem::ManuallyDrop,
    num::NonZero,
    panic::{AssertUnwindSafe, UnwindSafe},
    ptr::NonNull,
    sync::Arc,
};

use diatomic_waker::DiatomicWaker;
use parking_lot::{Condvar, Mutex, MutexGuard};
use pinned_aliasable::Aliasable;

use crate::pin_list::{Node, NodeData, PinList};

/// `Plunger` quickly unblocks your async tasks.
pub struct Plunger<Ctx = ()> {
    // TODO: use our own Arc, with strong_count = spawners and weak_count = workers.
    inner: Arc<Inner<Ctx>>,
}

impl<Ctx> Drop for Plunger<Ctx> {
    fn drop(&mut self) {
        let mut guard = self.inner.queue.lock();
        if guard.shutdown.is_none() {
            guard.shutdown = Some(Shutdown::Drop);
            self.inner.notify.notify_all();
        }
    }
}

struct Inner<Ctx> {
    queue: Mutex<PlungerQueue<Ctx>>,
    notify: Condvar,
}

unsafe impl<Ctx> Send for Inner<Ctx> {}
unsafe impl<Ctx> Sync for Inner<Ctx> {}

impl Plunger {
    /// Create a new `Plunger` with the number of threads tuned according to the [`std::thread::available_parallelism`].
    pub fn new() -> Self {
        let t = std::thread::available_parallelism().unwrap_or(const { NonZero::new(4).unwrap() });
        Self::with_threads(t)
    }

    /// Create a new `Plunger` with the given number of threads.
    pub fn with_threads(threads: NonZero<usize>) -> Self {
        Self::with_ctx(std::iter::repeat_n(|| {}, threads.get()))
    }
}

impl Default for Plunger {
    fn default() -> Self {
        Self::new()
    }
}

impl<Ctx> Plunger<Ctx> {
    pub fn create() -> (Self, Worker<Ctx>) {
        let inner = Arc::new(Inner {
            queue: Mutex::new(PlungerQueue {
                list: PinList::new(pin_list::id::Checked::new()),
                len: 0,
                workers: 1,
                shutdown: None,
            }),
            notify: Condvar::new(),
        });

        let worker = Worker {
            inner: inner.clone(),
        };

        (Self { inner }, worker)
    }

    /// Create a new `Plunger`, using the iterator to define the threads.
    ///
    /// ## Context
    ///
    /// Worker threads can have a thread local context that can be used for some arbitrary purpose by
    /// the tasks spawned into the worker. This could be just a scratch space, or it could be a cache,
    /// or it could be some metrics. The world is your oyster.
    pub fn with_ctx(ctx: impl IntoIterator<Item = impl FnOnce() -> Ctx + Send + 'static>) -> Self
    where
        Ctx: Send + 'static,
    {
        let (this, worker) = Self::create();

        for ctx in ctx {
            let worker = worker.clone();
            std::thread::Builder::new()
                .name("plunger-worker".to_owned())
                .spawn(move || worker.run(ctx()))
                .unwrap();
        }

        this
    }

    /// Steal the contexts from the workers in an arbitrary order and shutdown the thread pool.
    pub fn steal_contexts(self) -> Vec<Ctx>
    where
        Ctx: Send,
    {
        let mut queue = self.inner.queue.lock();

        let w = queue.workers;
        let (tx, rx) = std::sync::mpsc::sync_channel(w);

        queue.shutdown = Some(Shutdown::Steal(always_send_sync::AlwaysSendSync::new(tx)));
        self.inner.notify.notify_all();

        drop(queue);
        drop(self);

        let mut ctx = Vec::with_capacity(w);
        ctx.extend(rx.iter());
        ctx
    }

    /// Run the CPU intensive code in the thread pool, avoiding blocking the async runtime.
    ///
    /// ## Cancellation
    ///
    /// If this task is cancelled before completion, it might block the runtime.
    /// This is because we allocate the task in the current stack, and not on the heap,
    /// so we need to keep the stack allocation initialised and wait until it is free.
    ///
    /// We use [`tokio::task::block_in_place`] if available. If the task is only in the queue,
    /// no blocking will occur.
    ///
    /// For this reason, we recommend that you ensure cancellation is rare,
    /// and that tasks run for 100us-1ms to reduce the blocking duration.
    #[inline(always)]
    pub fn unblock<F, R>(&self, task: F) -> impl Future<Output = R>
    where
        Ctx: UnwindSafe,
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.unblock_ctx(|&mut _| task())
    }

    /// Run the CPU intensive code in the thread pool, avoiding blocking the async runtime.
    ///
    /// This additionally grants access to the thread local context. Use it as you wish.
    ///
    /// ## Cancellation
    ///
    /// If this task is cancelled before completion, it might block the runtime.
    /// This is because we allocate the task in the current stack, and not on the heap,
    /// so we need to keep the stack allocation initialised and wait until it is free.
    ///
    /// We use [`tokio::task::block_in_place`] if available. If the task is only in the queue,
    /// no blocking will occur.
    ///
    /// For this reason, we recommend that you ensure cancellation is rare,
    /// and that tasks run for 100us-1ms to reduce the blocking duration.
    #[inline(always)]
    pub fn unblock_ctx<F, R>(&self, task: F) -> impl Future<Output = R>
    where
        Ctx: UnwindSafe,
        F: FnOnce(&mut Ctx) -> R + Send + 'static,
        R: Send + 'static,
    {
        Task::<'_, Ctx, F, R> {
            state: Aliasable::new(UnsafeCell::new(Value {
                queued: ManuallyDrop::new(task),
            })),
            node: Node::new(),
            job: run_once::<Ctx, F, R>,
            plunger: Some(self),
        }
    }

    /// Run the CPU intensive code in the thread pool, avoiding blocking the async runtime.
    /// The CPU intensive code can return [`Poll::Pending`] to efficiently allow yielding
    /// of the task if some fairness is desired.
    ///
    /// This additionally grants access to the thread local context. Use it as you wish.
    ///
    /// ## Cancellation
    ///
    /// If this task is cancelled before completion, it might block the runtime.
    /// This is because we allocate the task in the current stack, and not on the heap,
    /// so we need to keep the stack allocation initialised and wait until it is free.
    ///
    /// We use [`tokio::task::block_in_place`] if available. If the task is only in the queue,
    /// no blocking will occur.
    ///
    /// For this reason, we recommend that you ensure cancellation is rare,
    /// and that tasks run for 100us-1ms to reduce the blocking duration.
    #[inline(always)]
    pub fn unblock_repeat_ctx<F, R>(&self, task: F) -> impl Future<Output = R>
    where
        Ctx: UnwindSafe,
        F: FnMut(&mut Ctx) -> Poll<R> + Send + 'static,
        R: Send + 'static,
    {
        Task::<'_, Ctx, F, R> {
            state: Aliasable::new(UnsafeCell::new(Value {
                queued: ManuallyDrop::new(task),
            })),
            node: Node::new(),
            job: run_repeat::<Ctx, F, R>,
            plunger: Some(self),
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
    /// Run a worker thread.
    pub fn run(self, mut ctx: Ctx) {
        let mut guard = self.inner.queue.lock();
        let shutdown = loop {
            let mut cursor = guard.list.cursor_front_mut();
            let Ok((job, node)) = cursor.acquire_current(false) else {
                if let Some(shutdown) = &guard.shutdown {
                    break shutdown.clone();
                }

                self.inner.notify.wait(&mut guard);
                continue;
            };

            let result =
                MutexGuard::unlocked(&mut guard, || unsafe { (job.job)(job.data, &mut ctx) });

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

// /// helper to ensure the shared state is tied to the lifetime of the guard.
// unsafe fn access_shared<'a, 'b, Ctx>(
//     guard: &'a mut MutexGuard<'b, PlungerQueue<Ctx>>,
//     ptr: *mut UnsafeCell<PlungerTaskShared<Ctx>>,
// ) -> (
//     &'a mut MutexGuard<'b, PlungerQueue<Ctx>>,
//     &'a mut PlungerTaskShared<Ctx>,
// ) {
//     (guard, unsafe { &mut *(*ptr).get() })
// }

struct PlungerQueue<Ctx> {
    list: PinList<Types<Ctx>>,

    // head: Option<NonNull<UnsafeCell<PlungerTaskShared<Ctx>>>>,
    // tail: Option<NonNull<UnsafeCell<PlungerTaskShared<Ctx>>>>,
    len: usize,
    workers: usize,
    shutdown: Option<Shutdown<Ctx>>,
}

type Types<Ctx> = dyn pin_list::Types<
        Id = pin_list::id::Checked,
        Protected = Job<Ctx>,
        Acquired = bool,
        Released = JobComplete,
        Unprotected = DiatomicWaker,
    >;

enum JobComplete {
    Success,
    Panic,
}

// todo: how can we store a `&mut dyn FnMut(&mut Ctx) -> Poll<()>` inside of pin_list :think:
struct Job<Ctx> {
    job: unsafe fn(inout: *mut (), ctx: &mut Ctx) -> Poll<JobComplete>,
    data: *mut (),
}

enum Shutdown<Ctx> {
    Drop,
    Steal(always_send_sync::AlwaysSendSync<std::sync::mpsc::SyncSender<Ctx>>),
}

impl<Ctx> Clone for Shutdown<Ctx> {
    fn clone(&self) -> Self {
        match self {
            Self::Drop => Self::Drop,
            Self::Steal(tx) => Self::Steal(tx.clone()),
        }
    }
}

// #[repr(C)]
// struct PlungerTask<Ctx, F, R> {
//     // if shared.state == Init, Completed, or Panicked, then this is owned by the Task.
//     // if shared.state == Queued, or Running, then this is owned by one of the worker threads.
//     state: UnsafeCell<Value<F, R>>,

//     // can only be read/mutated while holding the queue lock.
//     shared: UnsafeCell<PlungerTaskShared<Ctx>>,
// }

// struct PlungerTaskShared<Ctx> {
//     state: State,

//     // next/prev are only used if state == Queued.
//     next: Option<NonNull<UnsafeCell<PlungerTaskShared<Ctx>>>>,
//     prev: Option<NonNull<UnsafeCell<PlungerTaskShared<Ctx>>>>,

//     shutdown: bool,
//     waker: Waker,

//     f: unsafe fn(inout: *mut UnsafeCell<PlungerTaskShared<Ctx>>, ctx: &mut Ctx) -> State,
// }

// impl<Ctx> PlungerTaskShared<Ctx> {
//     unsafe fn link(ptr: NonNull<UnsafeCell<Self>>, queue: &mut MutexGuard<'_, PlungerQueue<Ctx>>) {
//         let (queue, shared) = unsafe { access_shared(queue, ptr.as_ptr()) };

//         // link into the queue
//         shared.prev = queue.tail.replace(ptr);
//         if let Some(prev) = shared.prev {
//             unsafe { access_shared(queue, prev.as_ptr()) }.1.next = Some(ptr);
//         } else {
//             queue.head = Some(ptr);
//         }
//         queue.len += 1;

//         // mark as linked.
//         shared.state = State::Queued;
//     }

//     unsafe fn unlink(&mut self, queue: &mut MutexGuard<'_, PlungerQueue<Ctx>>) {
//         // unlink from the queue
//         if let Some(prev) = self.prev {
//             unsafe { access_shared(queue, prev.as_ptr()) }.1.next = self.next;
//         } else {
//             queue.head = self.next;
//         }

//         if let Some(next) = self.next {
//             unsafe { access_shared(queue, next.as_ptr()) }.1.prev = self.prev;
//         } else {
//             queue.tail = self.prev;
//         }

//         queue.len -= 1;
//     }
// }

// #[derive(PartialEq, Debug, Clone, Copy)]
// enum State {
//     Init,
//     Queued,
//     Running,
//     Completed,
//     Panicked,
// }

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

pin_project_lite::pin_project!(
    struct Task<'a, Ctx, F, R> {
        #[pin]
        state: Aliasable<UnsafeCell<Value<F, R>>>,

        #[pin]
        node: Node<Types<Ctx>>,

        job: unsafe fn(inout: *mut (), ctx: &mut Ctx) -> Poll<JobComplete>,

        plunger: Option<&'a Plunger<Ctx>>,
    }

    impl<Ctx, F, R> PinnedDrop for Task<'_, Ctx, F, R> {
        fn drop(mut this: Pin<&mut Self>) {
            while this.plunger.is_some() {
                let _ = block::block_on(poll_fn(|cx| this.as_mut().poll_inner(cx, true)));
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
        let mut this = self.project();
        let Some(plunger) = *this.plunger else {
            panic!("polled after completion");
        };

        let node = match this.node.as_mut().initialized_mut() {
            Some(node) => node,
            None if unlink => {
                // safety: state is Init, so we own this.
                let task_state = unsafe { &mut *this.state.as_ref().get().get() };
                unsafe { ManuallyDrop::drop(&mut task_state.queued) };

                *this.plunger = None;
                return Poll::Ready(Ok(None));
            }
            None => {
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
            }
        };

        let mut guard = plunger.inner.queue.lock();

        let (completion, _) = if unlink {
            match node.reset(&mut guard.list) {
                // currently running, cannot be removed
                Err(node) => {
                    // register our intent to cancel.
                    *node.acquired_mut(&mut guard.list).unwrap() = true;
                    unsafe { node.unprotected().register(cx.waker()) };
                    return Poll::Pending;
                }
                // was in the queue
                Ok((NodeData::Linked(_), _)) => {
                    drop(guard);

                    // safety: no longer shared
                    let task_state = unsafe { &mut *this.state.as_ref().get().get() };

                    // safety: we were linked in the queue, so the queued value must be initialised
                    unsafe { ManuallyDrop::drop(&mut task_state.queued) };

                    *this.plunger = None;
                    return Poll::Ready(Ok(None));
                }
                // had finished already
                Ok((NodeData::Released(completion), waker)) => (completion, waker),
            }
        } else if node.protected(&guard.list).is_some() || node.acquired(&guard.list).is_some() {
            unsafe { node.unprotected().register(cx.waker()) };
            drop(guard);

            return Poll::Pending;
        } else {
            unsafe { node.take_removed_unchecked() }
        };

        drop(guard);

        // safety: no longer shared
        let task_state = unsafe { &mut *this.state.as_ref().get().get() };

        match completion {
            JobComplete::Success => {
                let output = unsafe { ManuallyDrop::take(&mut task_state.completed) };

                *this.plunger = None;
                Poll::Ready(Ok(Some(output)))
            }
            JobComplete::Panic => {
                let output = unsafe { ManuallyDrop::take(&mut task_state.panicked) };

                *this.plunger = None;
                Poll::Ready(Err(output))
            }
        }
    }
}

impl<Ctx, F, R> Future for Task<'_, Ctx, F, R> {
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

    #[test]
    fn basic() {
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

        let (lhs_res, rhs_res) = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                match futures::future::select(lhs, rhs).await {
                    Either::Left(_) => {
                        panic!("rhs should complete first.")
                    }
                    Either::Right((rhs_res, lhs)) => (lhs.await, rhs_res),
                }
            });

        assert_eq!(&*lhs_res, "lhs");
        assert_eq!(&*rhs_res, "rhs");
    }

    #[ignore = "slow"]
    #[test]
    fn smoke() {
        let body = async {
            const N: u32 = 10;
            const M: u32 = 10;
            let plunger = Arc::new(Plunger::with_threads(NonZero::new(4).unwrap()));
            let mut join_set = JoinSet::new();
            for _ in 0..N {
                let plunger = plunger.clone();
                join_set.spawn(async move {
                    for m in 0..M {
                        let mut i = 0;
                        let mut salt = [0; 32];
                        let pw = black_box(b"hunter2");

                        let job = plunger.unblock_repeat_ctx(move |()| {
                            i += 1;

                            let res = pbkdf2_hmac_array::<sha2::Sha256, 32>(pw, &salt, 1);

                            if i >= 2 {
                                std::task::Poll::Ready(res)
                            } else {
                                salt = res;
                                std::task::Poll::Pending
                            }
                        });

                        if m % 5 == 4 {
                            _ = black_box(
                                tokio::time::timeout(Duration::from_micros(50), job).await,
                            );
                        } else {
                            black_box(job.await);
                        }
                    }
                });
            }
            join_set.join_all().await;
        };

        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("Failed building the Runtime")
            .block_on(body);
    }

    #[ignore = "slow"]
    #[tokio::test]
    async fn perforamnce_sanity_check() {
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

        let plunger = Arc::new(Plunger::with_threads(NonZero::new(4).unwrap()));

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

        drop(plunger);

        wg.wait();
    }

    #[test]
    fn take_context() {
        let plunger = Plunger::with_ctx([|| 1, || 2, || 3, || 4, || 5, || 6, || 7, || 8]);
        let mut ctx = plunger.steal_contexts();

        ctx.sort_unstable();
        assert_eq!(ctx, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }
}
