use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Serialize, de::DeserializeOwned};

/// Shared netcode protocol identifier; client and server must use the same value.
pub const PROTOCOL_ID: u64 = 7;

/// Default UDP bind address for the headless server (reachable across Docker port forwarding).
pub const DEFAULT_SERVER_BIND_ADDR: &str = "0.0.0.0:5000";

/// Default UDP server address the local GUI client connects to.
pub const DEFAULT_CLIENT_CONNECT_ADDR: &str = "127.0.0.1:5000";

/// Fixed loop tick rate for the headless server schedule runner.
pub const SERVER_TICK_RATE_HZ: f64 = 60.0;

/// Maximum simultaneous client connections accepted by the server.
pub const MAX_CLIENTS: usize = 64;

/// Messages sent from the client to the server over `DefaultChannel::ReliableOrdered`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClientMessage {
    Ping { seq: u64 },
}

/// Messages sent from the server to the client over `DefaultChannel::ReliableOrdered`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ServerMessage {
    Pong { seq: u64 },
}

/// Serializes a protocol message to UTF-8 JSON bytes for transmission over Renet.
///
/// # Arguments
/// * `message` - Protocol message value implementing [`Serialize`].
///
/// # Example
/// ```
/// use bevy_server_in_docker_poc::protocol::{ClientMessage, encode_message};
///
/// let bytes = encode_message(&ClientMessage::Ping { seq: 1 }).unwrap();
/// assert!(!bytes.is_empty());
/// ```
pub fn encode_message<T: Serialize>(message: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(message)
}

/// Deserializes a protocol message from bytes received over Renet.
///
/// # Arguments
/// * `bytes` - Raw message payload received from a Renet channel.
///
/// # Example
/// ```
/// use bevy_server_in_docker_poc::protocol::{ServerMessage, decode_message, encode_message};
///
/// let bytes = encode_message(&ServerMessage::Pong { seq: 42 }).unwrap();
/// let decoded: ServerMessage = decode_message(&bytes).unwrap();
/// assert_eq!(decoded, ServerMessage::Pong { seq: 42 });
/// ```
pub fn decode_message<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Returns the current system time as a [`Duration`] since `UNIX_EPOCH` for netcode initialization.
///
/// # Example
/// ```
/// use bevy_server_in_docker_poc::protocol::unix_timestamp;
///
/// assert!(unix_timestamp().as_secs() > 0);
/// ```
pub fn unix_timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX_EPOCH")
}

#[cfg(test)]
#[path = "protocol_test.rs"]
mod tests;
