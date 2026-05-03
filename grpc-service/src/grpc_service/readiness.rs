use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
struct ServiceReadinessInner {
    preflight_ready: AtomicBool,
    kafka_ready: AtomicBool,
}

/// Shared startup-readiness state for the gRPC service.
///
/// The service starts in the "not ready" state. It becomes ready only
/// after startup preflight has verified all required dependencies and
/// the Kafka consumer has received a partition assignment.
#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub struct ServiceReadiness {
    inner: Arc<ServiceReadinessInner>,
}

impl ServiceReadiness {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ServiceReadinessInner::default()),
        }
    }

    #[cfg(test)]
    pub fn ready_for_test() -> Self {
        let readiness = Self::new();
        readiness.mark_preflight_ready();
        readiness.mark_kafka_ready();
        readiness
    }

    #[allow(dead_code)]
    pub fn mark_preflight_ready(&self) {
        self.inner.preflight_ready.store(true, Ordering::Release);
    }

    #[allow(dead_code)]
    pub fn mark_kafka_ready(&self) {
        self.inner.kafka_ready.store(true, Ordering::Release);
    }

    #[allow(dead_code)]
    pub fn mark_kafka_not_ready(&self) {
        self.inner.kafka_ready.store(false, Ordering::Release);
    }

    #[allow(dead_code)]
    pub fn is_ready(&self) -> bool {
        self.inner.preflight_ready.load(Ordering::Acquire)
            && self.inner.kafka_ready.load(Ordering::Acquire)
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
    fn test_preflight_alone_does_not_make_service_ready() {
        let r = ServiceReadiness::new();
        r.mark_preflight_ready();
        assert!(!r.is_ready());
    }

    #[test]
    fn test_kafka_alone_does_not_make_service_ready() {
        let r = ServiceReadiness::new();
        r.mark_kafka_ready();
        assert!(!r.is_ready());
    }

    #[test]
    fn test_service_becomes_ready_only_after_both_flags() {
        let r = ServiceReadiness::new();
        r.mark_preflight_ready();
        r.mark_kafka_ready();
        assert!(r.is_ready());
    }

    #[test]
    fn test_mark_kafka_not_ready_clears_readiness() {
        let r = ServiceReadiness::new();
        r.mark_preflight_ready();
        r.mark_kafka_ready();
        assert!(r.is_ready());

        r.mark_kafka_not_ready();
        assert!(!r.is_ready());
    }

    #[test]
    fn test_clones_share_state() {
        let r = ServiceReadiness::new();
        let r2 = r.clone();
        r.mark_preflight_ready();
        r.mark_kafka_ready();
        assert!(r2.is_ready());

        r2.mark_kafka_not_ready();
        assert!(!r.is_ready());
    }

    #[test]
    fn test_ready_for_test_is_ready() {
        assert!(ServiceReadiness::ready_for_test().is_ready());
    }
}
