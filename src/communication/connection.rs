use std::sync::atomic::{AtomicUsize, Ordering};

pub type ConnectionId = usize;

#[derive(Debug)]
pub struct ConnectionIdGenerator {
    next_connection_id: AtomicUsize,
}

impl ConnectionIdGenerator {
    const MIN_CONNECTION_ID: ConnectionId = 1;

    pub fn generate_id(&self) -> ConnectionId {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        debug_assert!(connection_id >= Self::MIN_CONNECTION_ID);
        connection_id
    }
}

impl Default for ConnectionIdGenerator {
    fn default() -> Self {
        Self {
            next_connection_id: AtomicUsize::new(Self::MIN_CONNECTION_ID),
        }
    }
}

pub trait NewConnectionContext: Clone {
    type Instance;

    fn new_connection_context(&self) -> Self::Instance;
}
