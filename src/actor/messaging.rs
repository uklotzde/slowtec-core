use super::*;

use crate::domain::messaging::{MessagePayload, PushMessageChannel};
use crate::util::connection::ConnectionId;
use crate::util::messaging::MessageId;

use futures::{Future, Stream};

use std::collections::HashMap;
use std::ops::DerefMut;

#[derive(Debug, Clone, Copy)]
pub enum PushConnectionSignal {}

pub enum PushConnectionCommand {
    Connect(ConnectionId, Box<dyn PushMessageChannel + Send>),
    Disconnect(ConnectionId),
    Reply(ConnectionId, MessageId, MessagePayload),
    Notify(ConnectionId, MessagePayload),
    BroadcastOthers(ConnectionId, MessagePayload),
    BroadcastAll(MessagePayload),
}

#[derive(Debug, Clone, Copy)]
pub enum PushConnectionQuery {}

pub type PushConnectionAction =
    Action<PushConnectionSignal, PushConnectionCommand, PushConnectionQuery>;

pub type PushConnectionActionSender = ActionSender<PushConnectionAction>;
type PushConnectionActionReceiver = ActionReceiver<PushConnectionAction>;

pub enum PushConnectionNotification {
    Connected(ConnectionId),
    Disconnected(ConnectionId, Box<dyn PushMessageChannel + Send>),
}

type PushConnectionNotificationSender = NotificationSender<PushConnectionNotification>;
pub type PushConnectionNotificationReceiver = NotificationReceiver<PushConnectionNotification>;

pub struct PushConnectionActor {
    connections: HashMap<ConnectionId, Box<dyn PushMessageChannel + Send>>,
    notification_tx: PushConnectionNotificationSender,
}

impl PushConnectionActor {
    fn new(notification_tx: PushConnectionNotificationSender) -> Self {
        Self {
            connections: HashMap::default(),
            notification_tx,
        }
    }

    fn connect(
        &mut self,
        connection_id: ConnectionId,
        connection: Box<dyn PushMessageChannel + Send>,
    ) {
        debug_assert!(!self.connections.contains_key(&connection_id));
        self.connections.insert(connection_id, connection);
    }

    fn disconnect(
        &mut self,
        connection_id: ConnectionId,
    ) -> Option<Box<dyn PushMessageChannel + Send>> {
        debug_assert!(self.connections.contains_key(&connection_id));
        self.connections.remove(&connection_id)
    }

    fn connection(
        &mut self,
        connection_id: ConnectionId,
    ) -> Option<&mut (dyn PushMessageChannel + Send + 'static)> {
        self.connections
            .get_mut(&connection_id)
            .map(DerefMut::deref_mut)
    }

    fn push_response(
        &mut self,
        connection_id: ConnectionId,
        message_id: MessageId,
        response_payload: MessagePayload,
    ) -> Fallible<()> {
        match self.connection(connection_id) {
            Some(connection) => {
                if let Err(err) = connection.push_message(response_payload) {
                    bail!(
                        "Connection {} - Failed to start sending response for message {}: {}",
                        connection_id,
                        message_id,
                        err
                    );
                }
            }
            None => {
                bail!(
                    "Connection {} not found - Cannot send response for message {}",
                    connection_id,
                    message_id
                );
            }
        }
        Ok(())
    }

    fn push_notification(
        &mut self,
        connection_id: ConnectionId,
        notification_payload: MessagePayload,
    ) -> Fallible<()> {
        match self.connection(connection_id) {
            Some(connection) => {
                if let Err(err) = connection.push_message(notification_payload) {
                    bail!(
                        "Connection {} - Failed to start sending notification: {}",
                        connection_id,
                        err
                    );
                }
            }
            None => {
                bail!(
                    "Connection {} not found - Cannot send notification",
                    connection_id,
                );
            }
        }
        Ok(())
    }

    fn push_broadcast_filtered<P>(
        &mut self,
        filter_connection_id: P,
        broadcast_payload: &MessagePayload,
    ) -> Fallible<()>
    where
        P: Fn(ConnectionId) -> bool,
    {
        let mut success_count: usize = 0;
        let mut failure_count: usize = 0;
        for (connection_id, connection) in self
            .connections
            .iter_mut()
            .filter(|(connection_id, _)| filter_connection_id(**connection_id))
        {
            if let Err(err) = connection.push_message(broadcast_payload.clone()) {
                failure_count += 1;
                error!(
                    "Connection {} - Failed to send broadcast message: {}",
                    connection_id, err
                );
            } else {
                success_count += 1;
            }
        }
        if failure_count > 0 {
            let total_count = success_count + failure_count;
            bail!(
                "Failed to send {} of {} broadcast message(s)",
                failure_count,
                total_count,
            )
        }
        Ok(())
    }

    fn push_broadcast_others(
        &mut self,
        connection_id: ConnectionId,
        broadcast_payload: &MessagePayload,
    ) -> Fallible<()> {
        self.push_broadcast_filtered(
            |other_connection_id| connection_id != other_connection_id,
            broadcast_payload,
        )
    }

    fn push_broadcast_all(&mut self, broadcast_payload: &MessagePayload) -> Fallible<()> {
        self.push_broadcast_filtered(|_| true, broadcast_payload)
    }

    fn handle_command(
        &mut self,
        response_tx: CommandResponseSender,
        command: PushConnectionCommand,
    ) {
        let result = match command {
            PushConnectionCommand::Connect(connection_id, connection) => {
                info!("Connection {} - Connecting", connection_id);
                self.connect(connection_id, connection);
                info!("Connection {} - Connected", connection_id);
                notify(
                    &self.notification_tx,
                    PushConnectionNotification::Connected(connection_id),
                );
                Ok(())
            }
            PushConnectionCommand::Disconnect(connection_id) => {
                info!("Connection {} - Disconnecting", connection_id);
                if let Some(connection) = self.disconnect(connection_id) {
                    info!("Connection {} - Disconnected", connection_id);
                    notify(
                        &self.notification_tx,
                        PushConnectionNotification::Disconnected(connection_id, connection),
                    );
                } else {
                    warn!("Connection {} - Not connected", connection_id);
                }
                Ok(())
            }
            PushConnectionCommand::Reply(connection_id, message_id, response_payload) => {
                self.push_response(connection_id, message_id, response_payload)
            }
            PushConnectionCommand::Notify(connection_id, notification_payload) => {
                self.push_notification(connection_id, notification_payload)
            }
            PushConnectionCommand::BroadcastOthers(connection_id, broadcast_payload) => {
                self.push_broadcast_others(connection_id, &broadcast_payload)
            }
            PushConnectionCommand::BroadcastAll(broadcast_payload) => {
                self.push_broadcast_all(&broadcast_payload)
            }
        };
        reply(response_tx, result)
    }

    fn handle_action(&mut self, action: PushConnectionAction) {
        match action {
            Action::Signal(signal) => match signal {},
            Action::Command(response_tx, command) => self.handle_command(response_tx, command),
            Action::Query(query) => match query {},
        }
    }

    fn handle_actions(
        mut self,
        action_rx: PushConnectionActionReceiver,
    ) -> impl Future<Item = (), Error = ()> {
        action_rx.for_each(move |action| {
            self.handle_action(action);
            Ok(())
        })
    }

    pub fn evoke() -> (
        impl Future<Item = (), Error = ()>,
        PushConnectionActionSender,
        PushConnectionNotificationReceiver,
    ) {
        let (action_tx, action_rx) = new_request_channel();
        let (notification_tx, notification_rx) = new_notification_channel();
        let action_handler = Self::new(notification_tx).handle_actions(action_rx);
        (action_handler, action_tx, notification_rx)
    }
}
