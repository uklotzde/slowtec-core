use std::cell::Cell;

pub type MessageId = usize;

#[derive(Debug)]
pub struct MessageIdGenerator {
    next_message_id: Cell<MessageId>,
}

impl MessageIdGenerator {
    const MIN_MESSAGE_ID: MessageId = 1;

    pub fn generate_id(&self) -> MessageId {
        let message_id = self.next_message_id.get();
        debug_assert!(message_id >= Self::MIN_MESSAGE_ID);
        self.next_message_id.set(message_id + 1);
        message_id
    }
}

impl Default for MessageIdGenerator {
    fn default() -> Self {
        Self {
            next_message_id: Cell::new(Self::MIN_MESSAGE_ID),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessagePayload {
    Text(String),
    Binary(Vec<u8>),
}
