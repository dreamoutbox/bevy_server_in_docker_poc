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
    renet::{ClientId, ConnectionConfig, DefaultChannel, ServerEvent},
};

use crate::{
    async_task::{AsyncTask, AsyncWorkOutput, simulate_async_work},
    protocol::{
        ClientMessage, MAX_CLIENTS, MAX_SERVER_ASYNC_DELAY_MS, PROTOCOL_ID, SERVER_TICK_RATE_HZ,
        ServerMessage, decode_message, encode_message, unix_timestamp,
    },
};

const STARTUP_WARMUP_DELAY_MS: u64 = 150;

/// Result of an async background job requested by a connected client.
struct ClientAsyncJobOutcome {
    client_id: ClientId,
    job_id: u64,
    output: AsyncWorkOutput,
}

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
        .add_systems(Startup, spawn_startup_async_task)
        .add_systems(
            Update,
            (
                handle_client_messages,
                poll_startup_async_tasks,
                poll_client_async_jobs,
            ),
        );

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

/// Spawns a local background async task on server startup without blocking the 60 Hz tick loop.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] used to spawn the task entity.
fn spawn_startup_async_task(mut commands: Commands) {
    info!("Spawning local server startup async warmup task");
    let delay = Duration::from_millis(STARTUP_WARMUP_DELAY_MS);
    commands.spawn(AsyncTask::spawn(async move {
        simulate_async_work("server-startup-warmup", delay).await
    }));
}

/// Polls local server startup async tasks and logs their completion.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] used to despawn completed task entities.
/// * `tasks` - Query over in-flight local [`AsyncTask<AsyncWorkOutput>`] entities.
fn poll_startup_async_tasks(
    mut commands: Commands,
    mut tasks: Query<(Entity, &mut AsyncTask<AsyncWorkOutput>)>,
) {
    for (entity, mut task) in &mut tasks {
        if let Some(output) = task.try_take() {
            info!(
                "Local server async task finished: {} ({} ms)",
                output.summary, output.elapsed_ms
            );
            commands.entity(entity).despawn();
        }
    }
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

/// Encodes and sends a [`ServerMessage`] to `client_id` over `DefaultChannel::ReliableOrdered`.
///
/// # Arguments
/// * `server` - Mutable access to the [`RenetServer`] resource.
/// * `client_id` - Destination client identifier.
/// * `message` - Protocol message to serialize and send.
fn send_server_message(server: &mut RenetServer, client_id: ClientId, message: &ServerMessage) {
    match encode_message(message) {
        Ok(bytes) => {
            server.send_message(client_id, DefaultChannel::ReliableOrdered, bytes);
        }
        Err(err) => {
            error!("Failed to encode {message:?} for client {client_id}: {err}");
        }
    }
}

/// Reads incoming client messages, replying immediately to `Ping` and offloading `AsyncWorkRequest`
/// onto Bevy's background compute pool so the server tick loop never stalls.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] used to spawn async job task entities.
/// * `server` - Mutable access to the [`RenetServer`] resource.
fn handle_client_messages(mut commands: Commands, mut server: ResMut<RenetServer>) {
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
                    send_server_message(&mut server, client_id, &ServerMessage::Pong { seq });
                }
                ClientMessage::AsyncWorkRequest { job_id, delay_ms } => {
                    let clamped_ms = delay_ms.min(MAX_SERVER_ASYNC_DELAY_MS);
                    info!(
                        "Received AsyncWorkRequest(job_id={job_id}, delay_ms={clamped_ms}) from client {client_id}; spawning async task"
                    );
                    commands.spawn(AsyncTask::spawn(async move {
                        let label = format!("server-job-#{job_id}");
                        let output =
                            simulate_async_work(&label, Duration::from_millis(clamped_ms)).await;
                        ClientAsyncJobOutcome {
                            client_id,
                            job_id,
                            output,
                        }
                    }));
                }
            }
        }
    }
}

/// Polls client-requested server async jobs and sends [`ServerMessage::AsyncWorkResult`] once ready.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] used to despawn completed task entities.
/// * `server` - Mutable access to the [`RenetServer`] resource.
/// * `tasks` - Query over in-flight [`AsyncTask<ClientAsyncJobOutcome>`] entities.
fn poll_client_async_jobs(
    mut commands: Commands,
    mut server: ResMut<RenetServer>,
    mut tasks: Query<(Entity, &mut AsyncTask<ClientAsyncJobOutcome>)>,
) {
    for (entity, mut task) in &mut tasks {
        let Some(outcome) = task.try_take() else {
            continue;
        };
        commands.entity(entity).despawn();

        info!(
            "Completed server async job #{} for client {} in {} ms",
            outcome.job_id, outcome.client_id, outcome.output.elapsed_ms
        );

        if !server.is_connected(outcome.client_id) {
            warn!(
                "Skipping AsyncWorkResult(job_id={}) because client {} disconnected",
                outcome.job_id, outcome.client_id
            );
            continue;
        }

        let reply = ServerMessage::AsyncWorkResult {
            job_id: outcome.job_id,
            elapsed_ms: outcome.output.elapsed_ms,
            summary: outcome.output.summary,
        };
        send_server_message(&mut server, outcome.client_id, &reply);
    }
}
