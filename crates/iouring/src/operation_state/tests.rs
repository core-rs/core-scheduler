// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::block_on;
use std::{
    future::Future,
    sync::atomic::AtomicUsize,
    task::{Context, Wake, Waker},
};

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn close_unregisters_abandoned_waiters_and_can_wait_again() {
    let mut state = OperationState::default();
    block_on(state.close());
    let guard = state.begin(()).unwrap();
    assert!(state.check_idle().is_err());
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    let owners = Arc::strong_count(&wakes);
    let mut closing = Box::pin(state.close());
    assert!(closing.as_mut().poll(&mut context).is_pending());
    assert_eq!(Arc::strong_count(&wakes), owners + 1);
    drop(closing);
    assert_eq!(Arc::strong_count(&wakes), owners);
    let mut closing = Box::pin(state.close());
    assert!(closing.as_mut().poll(&mut context).is_pending());
    std::thread::spawn(move || drop(guard)).join().unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(closing.as_mut().poll(&mut context).is_ready());
    drop(closing);
    assert_eq!(Arc::strong_count(&wakes), owners);
    state.check_idle().unwrap();
    block_on(state.close());
}

#[test]
fn retirement_releases_the_owned_value_before_waking_close() {
    struct Owner(Arc<AtomicBool>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    struct CheckRetired(Arc<AtomicBool>);
    impl Wake for CheckRetired {
        fn wake(self: Arc<Self>) {
            assert!(self.0.load(Ordering::Acquire));
        }
    }
    let mut state = OperationState::default();
    let retired = Arc::new(AtomicBool::new(false));
    let guard = state.begin(Owner(Arc::clone(&retired))).ok().unwrap();
    let waker = Waker::from(Arc::new(CheckRetired(Arc::clone(&retired))));
    let mut context = Context::from_waker(&waker);
    let mut closing = Box::pin(state.close());
    assert!(closing.as_mut().poll(&mut context).is_pending());
    drop(guard);
    assert!(closing.as_mut().poll(&mut context).is_ready());
    assert!(retired.load(Ordering::Acquire));
}

#[test]
fn retirement_racing_close_cannot_lose_its_wakeup() {
    for _ in 0..128 {
        let mut state = OperationState::default();
        let guard = state.begin(()).unwrap();
        let retiring = std::thread::spawn(move || drop(guard));
        block_on(state.close());
        retiring.join().unwrap();
        state.check_idle().unwrap();
    }
}
