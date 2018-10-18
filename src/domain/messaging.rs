use super::*;

#[derive(Debug, Clone)]
pub enum MessagePayload {
    Text(String),
    Binary(Vec<u8>),
}

pub trait PushMessageChannel {
    fn push_message(&mut self, message_payload: MessagePayload) -> Fallible<()>;
}
