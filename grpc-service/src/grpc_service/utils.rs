use std::time::{Duration, Instant};

pub(super) fn millis_since(
    now: Instant,
    start: Option<Instant>,
) -> Option<u64> {
    start.map(|start| duration_to_millis(now.duration_since(start)))
}

fn duration_to_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
