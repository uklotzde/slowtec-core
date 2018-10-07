use super::*;

use super::connection::*;
use super::message::*;
use super::subsystem::*;

use futures::{Future, Stream};

use std::collections::HashMap;

pub trait PushConnection {
    fn push_message(&mut self, message_payload: MessagePayload) -> Fallible<()>;
}

#[derive(Debug, Clone, Copy)]
pub enum PushConnectionIncident {}

pub enum PushConnectionCommand {
    Connect(ConnectionId, Box<dyn PushConnection + Send>),
    Disconnect(ConnectionId),
    Reply(ConnectionId, MessageId, MessagePayload),
    Notify(ConnectionId, MessagePayload),
    BroadcastOthers(ConnectionId, MessagePayload),
    BroadcastAll(MessagePayload),
}

#[derive(Debug, Clone, Copy)]
pub enum PushConnectionQuery {}

pub type PushConnectionAction =
    Action<PushConnectionIncident, PushConnectionCommand, PushConnectionQuery>;

pub type PushConnectionActionSender = ActionSender<PushConnectionAction>;
type PushConnectionActionReceiver = ActionReceiver<PushConnectionAction>;

pub enum PushConnectionNotification {
    Connected(ConnectionId),
    Disconnected(ConnectionId, Box<dyn PushConnection + Send>),
}

type PushConnectionNotificationSender = NotificationSender<PushConnectionNotification>;
pub type PushConnectionNotificationReceiver = NotificationReceiver<PushConnectionNotification>;

pub struct PushConnectionController {
    connections: HashMap<ConnectionId, Box<dyn PushConnection + Send>>,
    notification_tx: PushConnectionNotificationSender,
}

impl PushConnectionController {
    fn new(notification_tx: PushConnectionNotificationSender) -> Self {
        Self {
            connections: HashMap::default(),
            notification_tx,
        }
    }

    fn connect(&mut self, connection_id: ConnectionId, connection: Box<dyn PushConnection + Send>) {
        debug_assert!(!self.connections.contains_key(&connection_id));
        self.connections.insert(connection_id, connection);
    }

    fn disconnect(
        &mut self,
        connection_id: ConnectionId,
    ) -> Option<Box<dyn PushConnection + Send>> {
        debug_assert!(self.connections.contains_key(&connection_id));
        self.connections.remove(&connection_id)
    }

    fn connection(
        &mut self,
        connection_id: ConnectionId,
    ) -> Option<&mut Box<dyn PushConnection + Send>> {
        self.connections.get_mut(&connection_id)
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
        mut predicate: P,
        broadcast_payload: &MessagePayload,
    ) -> Fallible<()>
    where
        P: FnMut(ConnectionId) -> bool,
    {
        let mut success_count: usize = 0;
        let mut failure_count: usize = 0;
        for (connection_id, connection) in self
            .connections
            .iter_mut()
            .filter(|(connection_id, _)| predicate(**connection_id))
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
            Action::Incident(incident) => match incident {},
            Action::Command(response_tx, command) => self.handle_command(response_tx, command),
            Action::Query(query) => match query {},
        }
    }

    fn handle_actions(
        mut self,
        action_rx: PushConnectionActionReceiver,
    ) -> impl Future<Item = (), Error = ()> {
        action_rx
            .map(move |action| self.handle_action(action))
            .or_else(|()| {
                error!("Aborted action handling");
                Err(())
            }).for_each(|()| Ok(())) // consume the entire (unlimited) stream
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
