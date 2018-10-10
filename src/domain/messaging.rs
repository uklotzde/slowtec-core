use super::*;

use std::cell::Cell;
use std::usize;

#[derive(Debug, Clone)]
pub enum MessagePayload {
    Text(String),
    Binary(Vec<u8>),
}

pub trait PushMessageChannel {
    fn push_message(&mut self, message_payload: MessagePayload) -> Fallible<()>;
}

pub type MessageId = usize;

#[derive(Debug)]
pub struct MessageIdGenerator {
    next_id: Cell<MessageId>,
}

impl MessageIdGenerator {
    const NEXT_ID_ADD: MessageId = 1;
    const MIN_ID: MessageId = usize::MIN + Self::NEXT_ID_ADD;
    const MAX_ID: MessageId = usize::MAX;

    #[cfg_attr(feature = "cargo-clippy", allow(absurd_extreme_comparisons))]
    pub fn generate_id(&self) -> MessageId {
        let id = self.next_id.get();
        debug_assert!(id >= Self::MIN_ID);
        debug_assert!(id <= Self::MAX_ID);
        debug_assert!(Self::NEXT_ID_ADD > 0);
        if id <= Self::MAX_ID - Self::NEXT_ID_ADD {
            self.next_id.set(id + Self::NEXT_ID_ADD);
        } else {
            // overflow -> start over again
            self.next_id.set(Self::MIN_ID);
        }
        id
    }
}

impl Default for MessageIdGenerator {
    fn default() -> Self {
        Self {
            next_id: Cell::new(Self::MIN_ID),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_id() {
        assert_eq!(
            MessageIdGenerator::MIN_ID,
            MessageIdGenerator::default().generate_id()
        );
    }
}
