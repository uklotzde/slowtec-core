use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum SocketConnection {
    Disconnected,
    Connecting(SocketAddr),
    Connected(SocketAddr),
    Disconnecting(SocketAddr),
}

impl SocketConnection {
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        match self {
            SocketConnection::Disconnected => None,
            SocketConnection::Connecting(socket_addr) => Some(*socket_addr),
            SocketConnection::Connected(socket_addr) => Some(*socket_addr),
            SocketConnection::Disconnecting(socket_addr) => Some(*socket_addr),
        }
    }

    pub fn is_connected(&self) -> bool {
        match self {
            SocketConnection::Connected(_) => true,
            _ => false,
        }
    }
}

impl Default for SocketConnection {
    fn default() -> Self {
        SocketConnection::Disconnected
    }
}
