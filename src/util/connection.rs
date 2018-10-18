use std::sync::atomic::{AtomicUsize, Ordering};
use std::usize;

pub type ConnectionId = usize;

#[derive(Debug)]
pub struct ConnectionIdGenerator {
    next_id: AtomicUsize,
}

impl ConnectionIdGenerator {
    const MIN_ID: ConnectionId = usize::MIN + Self::NEXT_ID_ADD;
    const MAX_ID: ConnectionId = usize::MAX;
    const NEXT_ID_ADD: ConnectionId = 1;

    #[cfg_attr(feature = "cargo-clippy", allow(absurd_extreme_comparisons))]
    pub fn generate_id(&self) -> ConnectionId {
        debug_assert!(Self::NEXT_ID_ADD > 0);
        loop {
            let id = self.next_id.fetch_add(Self::NEXT_ID_ADD, Ordering::Relaxed);
            debug_assert!(id <= Self::MAX_ID);
            if id >= Self::MIN_ID {
                return id;
            }
            // continue after overflow
        }
    }
}

impl Default for ConnectionIdGenerator {
    fn default() -> Self {
        Self {
            next_id: AtomicUsize::new(Self::MIN_ID),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_id() {
        assert_eq!(
            ConnectionIdGenerator::MIN_ID,
            ConnectionIdGenerator::default().generate_id()
        );
    }
}
