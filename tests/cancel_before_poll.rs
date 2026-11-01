use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct CountDrop(Arc<AtomicUsize>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn cancelling_before_first_poll_drops_the_job_once() {
    let pool = plunger::Plunger::<()>::build();
    let drops = Arc::new(AtomicUsize::new(0));
    let value = Box::new(CountDrop(drops.clone()));
    let mut task = Box::pin(pool.unblock(move || drop(value)));

    assert!(futures::executor::block_on(task.as_mut().cancel()).is_err());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert!(futures::executor::block_on(task.as_mut().cancel()).is_err());
    drop(task);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn cancelling_before_first_poll_is_complete_if_the_job_destructor_panics() {
    struct PanicOnDrop(Arc<AtomicUsize>);

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
            panic!("job destructor panicked");
        }
    }

    let pool = plunger::Plunger::<()>::build();
    let drops = Arc::new(AtomicUsize::new(0));
    let value = Box::new(PanicOnDrop(drops.clone()));
    let mut task = Box::pin(pool.unblock(move || drop(value)));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = futures::executor::block_on(task.as_mut().cancel());
    }));
    assert!(result.is_err());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert!(futures::executor::block_on(task.as_mut().cancel()).is_err());
    drop(task);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
