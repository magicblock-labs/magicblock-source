use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Shared startup-readiness flag for the gRPC service.
///
/// The service starts in the "not ready" state. Once startup
/// preflight has verified all required dependencies, the owner of the
/// state must call [`ServiceReadiness::mark_ready`]. The Ping handler
/// reads the state to decide whether to advertise the service as ready
/// to clients.
#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct ServiceReadiness {
    inner: Arc<AtomicBool>,
}

impl ServiceReadiness {
    /// Construct a new readiness flag in the "not ready" state.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Test-only constructor that starts in the "ready" state. Used by
    /// unit tests that want to bypass the preflight gate.
    #[cfg(test)]
    pub fn ready_for_test() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Flip the flag to `ready`. Idempotent; safe to call repeatedly.
    #[allow(dead_code)]
    pub fn mark_ready(&self) {
        self.inner.store(true, Ordering::Release);
    }

    /// Return whether the service has finished startup preflight.
    #[allow(dead_code)]
    pub fn is_ready(&self) -> bool {
        self.inner.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceReadiness;

    #[test]
    fn test_new_is_not_ready() {
        let r = ServiceReadiness::new();
        assert!(!r.is_ready());
    }

    #[test]
    fn test_mark_ready_sets_flag() {
        let r = ServiceReadiness::new();
        r.mark_ready();
        assert!(r.is_ready());
    }

    #[test]
    fn test_clones_share_state() {
        let r = ServiceReadiness::new();
        let r2 = r.clone();
        assert!(!r2.is_ready());
        r.mark_ready();
        assert!(r2.is_ready());
    }

    #[test]
    fn test_ready_for_test_is_ready() {
        assert!(ServiceReadiness::ready_for_test().is_ready());
    }
}
