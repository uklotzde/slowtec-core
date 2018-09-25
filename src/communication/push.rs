use super::*;

use super::connection::*;
use super::message::*;
use super::subsystem::*;

use futures::{Future, Stream};

use std::collections::HashMap;

pub trait PushConnection {
    fn push_message(&mut self, message_payload: MessagePayload) -> ErrorResult<()>;
}

#[derive(Debug, Clone, Copy)]
pub enum PushConnectionIncident {}

pub enum PushConnectionCommand {
    Connect(ConnectionId, Box<dyn PushConnection + Send>),
    Disconnect(ConnectionId),
    SendResponse(ConnectionId, MessageId, MessagePayload),
    BroadcastOthers(ConnectionId, MessagePayload),
    BroadcastAll(MessagePayload),
}

#[derive(Debug, Clone, Copy)]
pub enum PushConnectionQuery {}

pub enum PushConnectionAction {
    Incident(PushConnectionIncident),
    Command(CommandResponseSender, PushConnectionCommand),
    Query(PushConnectionQuery),
}

pub type PushConnectionActionSender = ActionSender<PushConnectionAction>;
type PushConnectionActionReceiver = ActionReceiver<PushConnectionAction>;

pub enum PushConnectionNotification {
    Connected(ConnectionId),
    Disconnected(ConnectionId, Box<dyn PushConnection + Send>),
}

type PushConnectionNotificationSender = NotificationSender<PushConnectionNotification>;
pub type PushConnectionNotificationReceiver = NotificationReceiver<PushConnectionNotification>;

pub struct PushConnectionManager {
    connections: HashMap<ConnectionId, Box<dyn PushConnection + Send>>,
    notification_tx: PushConnectionNotificationSender,
}

impl PushConnectionManager {
    pub fn new(notification_tx: PushConnectionNotificationSender) -> Self {
        Self {
            connections: HashMap::default(),
            notification_tx,
        }
    }

    pub fn connect(
        &mut self,
        connection_id: ConnectionId,
        connection: Box<dyn PushConnection + Send>,
    ) {
        debug_assert!(!self.connections.contains_key(&connection_id));
        self.connections.insert(connection_id, connection);
    }

    pub fn disconnect(
        &mut self,
        connection_id: ConnectionId,
    ) -> Option<Box<dyn PushConnection + Send>> {
        debug_assert!(self.connections.contains_key(&connection_id));
        self.connections.remove(&connection_id)
    }

    pub fn connection(
        &mut self,
        connection_id: ConnectionId,
    ) -> Option<&mut Box<dyn PushConnection + Send>> {
        self.connections.get_mut(&connection_id)
    }

    pub fn push_response(
        &mut self,
        connection_id: ConnectionId,
        message_id: MessageId,
        response_payload: MessagePayload,
    ) -> ErrorResult<()> {
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

    pub fn push_broadcast_filtered<P>(
        &mut self,
        mut predicate: P,
        broadcast_payload: &MessagePayload,
    ) -> ErrorResult<()>
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

    pub fn push_broadcast_others(
        &mut self,
        connection_id: ConnectionId,
        broadcast_payload: &MessagePayload,
    ) -> ErrorResult<()> {
        self.push_broadcast_filtered(
            |other_connection_id| connection_id != other_connection_id,
            broadcast_payload,
        )
    }

    pub fn push_broadcast_all(&mut self, broadcast_payload: &MessagePayload) -> ErrorResult<()> {
        self.push_broadcast_filtered(|_| true, broadcast_payload)
    }

    pub fn handle_command(
        &mut self,
        response_tx: CommandResponseSender,
        command: PushConnectionCommand,
    ) {
        match command {
            PushConnectionCommand::Connect(connection_id, connection) => {
                self.connect(connection_id, connection);
                info!("Registered new connection {}", connection_id);
                reply(response_tx, Ok(()));
                notify(
                    &self.notification_tx,
                    PushConnectionNotification::Connected(connection_id),
                );
            }
            PushConnectionCommand::Disconnect(connection_id) => {
                if let Some(connection) = self.disconnect(connection_id) {
                    info!("Deregistered connection {}", connection_id);
                    reply(response_tx, Ok(()));
                    notify(
                        &self.notification_tx,
                        PushConnectionNotification::Disconnected(connection_id, connection),
                    );
                }
            }
            PushConnectionCommand::SendResponse(connection_id, message_id, response_payload) => {
                let result = self.push_response(connection_id, message_id, response_payload);
                reply(response_tx, result);
            }
            PushConnectionCommand::BroadcastOthers(connection_id, broadcast_payload) => {
                let result = self.push_broadcast_others(connection_id, &broadcast_payload);
                reply(response_tx, result);
            }
            PushConnectionCommand::BroadcastAll(broadcast_payload) => {
                let result = self.push_broadcast_all(&broadcast_payload);
                reply(response_tx, result);
            }
        }
    }

    pub fn handle_action(
        &mut self,
        action: PushConnectionAction,
    ) -> impl Future<Item = (), Error = ()> {
        match action {
            PushConnectionAction::Incident(_incident) => unreachable!(),
            PushConnectionAction::Command(response_tx, command) => {
                self.handle_command(response_tx, command)
            }
            PushConnectionAction::Query(_query) => unreachable!(),
        }
        futures::future::ok(())
    }

    pub fn handle_actions(
        mut self,
        action_rx: PushConnectionActionReceiver,
    ) -> impl Future<Item = (), Error = ()> {
        action_rx
            .and_then(move |action| self.handle_action(action))
            .or_else(|()| {
                // unreachable
                warn!("Keep on running after error in action handler");
                Ok(())
            }).for_each(|()| Ok(()))
    }

    pub fn new_action_handler() -> (
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
