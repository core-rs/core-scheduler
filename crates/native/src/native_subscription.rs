// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::signal_service::Shared;
use std::sync::Weak;

/// An owning Unix signal subscription. Drop stops notification admission without joining callbacks. Native signal dispositions follow signal-hook's process-wide handler policy and are not restored on unsubscribe.
#[must_use = "retain the subscription while notifications are wanted"]
pub struct NativeSubscription {
    pub(crate) shared: Weak<Shared>,
    pub(crate) id: usize,
}
impl Drop for NativeSubscription {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            let removed = shared.state.lock().unwrap().targets.remove(&self.id);
            // A target destructor may reenter the service.
            drop(removed);
        }
    }
}
