use std::{
    future::Future,
    task::{Context, Waker},
};

#[test]
fn cancelling_a_queued_task_updates_the_count_once() {
    let pool = plunger::Plunger::<()>::build();
    let mut task = Box::pin(pool.unblock(|| 1));
    assert!(
        task.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(pool.len(), 1);

    assert!(futures::executor::block_on(task.as_mut().cancel()).is_err());
    assert_eq!(pool.len(), 0);
    assert!(pool.is_empty());
    assert!(futures::executor::block_on(task.as_mut().cancel()).is_err());
    drop(task);
    assert_eq!(pool.len(), 0);
}

#[test]
fn dropping_a_queued_task_keeps_the_other_task_counted() {
    let pool = plunger::Plunger::<()>::build();
    let mut first = Box::pin(pool.unblock(|| 1));
    let mut next = Box::pin(pool.unblock(|| 2));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(next.as_mut().poll(&mut cx).is_pending());
    assert_eq!(pool.len(), 2);

    drop(first);
    assert_eq!(pool.len(), 1);
    assert!(!pool.is_empty());
    drop(next);
    assert!(pool.is_empty());
}
