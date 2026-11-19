use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::Duration,
};

use bevy::{app::ScheduleRunnerPlugin, log::LogPlugin, prelude::*};
use bevy_renet::{
    RenetServer, RenetServerEvent, RenetServerPlugin,
    netcode::{
        NetcodeErrorEvent, NetcodeServerPlugin, NetcodeServerTransport, ServerAuthentication,
        ServerConfig,
    },
    renet::{ConnectionConfig, DefaultChannel, ServerEvent},
};

use crate::protocol::{
    ClientMessage, MAX_CLIENTS, PROTOCOL_ID, SERVER_TICK_RATE_HZ, ServerMessage, decode_message,
    encode_message, unix_timestamp,
};

/// Builds a headless Bevy server [`App`] bound to `bind_addr` without starting its loop.
///
/// Returning the actual bound [`SocketAddr`] lets callers bind port `0` in tests and discover
/// the ephemeral port assigned by the OS.
///
/// # Arguments
/// * `bind_addr` - Local UDP address to bind the server socket on (e.g. `0.0.0.0:5000`).
/// * `enable_logging` - Whether to attach [`LogPlugin`]; set to `false` when constructing
///   multiple apps inside a single test process.
///
/// # Example
/// ```no_run
/// use std::net::SocketAddr;
/// use bevy_server_in_docker_poc::server::build_server_app;
///
/// let addr: SocketAddr = "0.0.0.0:5000".parse().unwrap();
/// let (mut app, bound_addr) = build_server_app(addr, true).unwrap();
/// println!("Listening on {bound_addr}");
/// app.run();
/// ```
pub fn build_server_app(
    bind_addr: SocketAddr,
    enable_logging: bool,
) -> io::Result<(App, SocketAddr)> {
    let socket = UdpSocket::bind(bind_addr)?;
    let bound_addr = socket.local_addr()?;

    let server_config = ServerConfig {
        current_time: unix_timestamp(),
        max_clients: MAX_CLIENTS,
        protocol_id: PROTOCOL_ID,
        public_addresses: vec![bound_addr],
        authentication: ServerAuthentication::Unsecure,
    };

    let transport = NetcodeServerTransport::new(server_config, socket)?;
    let server = RenetServer::new(ConnectionConfig::default());

    let mut app = App::new();
    let tick_interval = Duration::from_secs_f64(1.0 / SERVER_TICK_RATE_HZ);
    app.add_plugins(MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(tick_interval)));
    if enable_logging {
        app.add_plugins(LogPlugin::default());
    }

    app.add_plugins((RenetServerPlugin, NetcodeServerPlugin))
        .insert_resource(server)
        .insert_resource(transport)
        .add_observer(on_server_event)
        .add_observer(on_netcode_error)
        .add_systems(Update, handle_client_messages);

    Ok((app, bound_addr))
}

/// Starts the console-only Bevy server and blocks until shutdown.
///
/// # Arguments
/// * `bind_addr` - Local UDP address to bind (e.g. `0.0.0.0:5000`).
///
/// # Example
/// ```no_run
/// use std::net::SocketAddr;
/// use bevy_server_in_docker_poc::server;
///
/// let addr: SocketAddr = "0.0.0.0:5000".parse().unwrap();
/// server::run(addr).unwrap();
/// ```
pub fn run(bind_addr: SocketAddr) -> io::Result<()> {
    let (mut app, bound_addr) = build_server_app(bind_addr, true)?;
    info!(
        "Bevy headless server listening on udp://{bound_addr} (tick_rate={SERVER_TICK_RATE_HZ}Hz)"
    );
    app.run();
    Ok(())
}

/// Logs client connection and disconnection lifecycle events emitted by Renet.
///
/// # Arguments
/// * `event` - Observer trigger wrapping the underlying [`RenetServerEvent`].
fn on_server_event(event: On<RenetServerEvent>) {
    match &**event {
        ServerEvent::ClientConnected { client_id } => {
            info!("Client {client_id} connected");
        }
        ServerEvent::ClientDisconnected { client_id, reason } => {
            info!("Client {client_id} disconnected: {reason}");
        }
    }
}

/// Logs transport-level netcode errors without crashing the server loop.
///
/// # Arguments
/// * `error` - Observer trigger wrapping [`NetcodeErrorEvent`].
fn on_netcode_error(error: On<NetcodeErrorEvent>) {
    warn!("Server netcode transport error: {}", **error);
}

/// Reads incoming `Ping` messages from all connected clients and replies with `Pong`.
///
/// # Arguments
/// * `server` - Mutable access to the [`RenetServer`] resource.
fn handle_client_messages(mut server: ResMut<RenetServer>) {
    for client_id in server.clients_id() {
        while let Some(payload) = server.receive_message(client_id, DefaultChannel::ReliableOrdered)
        {
            let message = match decode_message::<ClientMessage>(&payload) {
                Ok(msg) => msg,
                Err(err) => {
                    warn!("Ignoring malformed message from client {client_id}: {err}");
                    continue;
                }
            };

            match message {
                ClientMessage::Ping { seq } => {
                    info!("Received Ping(seq={seq}) from client {client_id}; sending Pong");
                    let reply = ServerMessage::Pong { seq };
                    match encode_message(&reply) {
                        Ok(bytes) => {
                            server.send_message(client_id, DefaultChannel::ReliableOrdered, bytes);
                        }
                        Err(err) => {
                            error!("Failed to encode Pong(seq={seq}): {err}");
                        }
                    }
                }
            }
        }
    }
}
