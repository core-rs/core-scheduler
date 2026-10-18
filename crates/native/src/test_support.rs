// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::mpsc,
    time::Duration,
};
pub(crate) fn run(test: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(catch_unwind(AssertUnwindSafe(test)));
    });
    if let Err(payload) = completion
        .recv_timeout(Duration::from_secs(15))
        .expect("native test deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}
pub(crate) fn scheduler() -> crate::NativeScheduler {
    crate::NativeScheduler::with_options(crate::NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 4,
        ..Default::default()
    })
    .unwrap()
}

pub(crate) struct Gate {
    entered: mpsc::Sender<()>,
    released: std::sync::Mutex<mpsc::Receiver<()>>,
}
pub(crate) struct Release(mpsc::Sender<()>);
impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
impl Gate {
    pub(crate) fn new() -> (std::sync::Arc<Self>, mpsc::Receiver<()>, Release) {
        let (entered, started) = mpsc::channel();
        let (release, released) = mpsc::channel();
        (
            std::sync::Arc::new(Self {
                entered,
                released: std::sync::Mutex::new(released),
            }),
            started,
            Release(release),
        )
    }
    pub(crate) fn wait(&self) {
        self.entered.send(()).unwrap();
        self.released
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .expect("native operation gate deadline exceeded");
    }
}

#[test]
fn watchdog_propagates_assertion_panics() {
    assert!(catch_unwind(|| run(|| panic!("test assertion"))).is_err());
}
