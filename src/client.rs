use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use bevy::{
    picking::hover::Hovered,
    prelude::*,
    text::FontSize,
    ui::Pressed,
    ui_widgets::{Activate, Button as UiButton},
};
use bevy_renet::{
    RenetClient, RenetClientPlugin,
    netcode::{
        ClientAuthentication, NetcodeClientPlugin, NetcodeClientTransport, NetcodeErrorEvent,
    },
    renet::{ConnectionConfig, DefaultChannel},
};

use crate::{
    async_task::{AsyncTask, AsyncWorkOutput, simulate_async_work},
    protocol::{
        ClientMessage, DEFAULT_LOCAL_ASYNC_DELAY_MS, DEFAULT_SERVER_ASYNC_DELAY_MS, PROTOCOL_ID,
        ServerMessage, decode_message, encode_message, unix_timestamp,
    },
};

const WINDOW_TITLE: &str = "Bevy Server in Docker POC - Client";
const WINDOW_WIDTH: u32 = 580;
const WINDOW_HEIGHT: u32 = 480;

const BG_COLOR: Color = Color::srgb(0.10, 0.11, 0.14);
const TITLE_TEXT_COLOR: Color = Color::srgb(0.95, 0.96, 0.98);
const BODY_TEXT_COLOR: Color = Color::srgb(0.82, 0.85, 0.90);
const BUTTON_TEXT_COLOR: Color = Color::srgb(0.98, 0.98, 0.99);

const BUTTON_NORMAL_COLOR: Color = Color::srgb(0.18, 0.45, 0.78);
const BUTTON_HOVER_COLOR: Color = Color::srgb(0.24, 0.54, 0.88);
const BUTTON_PRESSED_COLOR: Color = Color::srgb(0.12, 0.34, 0.62);
const BUTTON_DISABLED_COLOR: Color = Color::srgb(0.30, 0.32, 0.36);

const TITLE_FONT_SIZE: FontSize = FontSize::Px(26.0);
const STATUS_FONT_SIZE: FontSize = FontSize::Px(17.0);
const BUTTON_FONT_SIZE: FontSize = FontSize::Px(18.0);
const BUTTON_WIDTH_PX: f32 = 240.0;
const BUTTON_HEIGHT_PX: f32 = 46.0;
const BUTTON_BORDER_RADIUS_PX: f32 = 8.0;
const LAYOUT_GAP_PX: f32 = 14.0;
const LAYOUT_PADDING_PX: f32 = 24.0;

/// High-level connection state displayed by the client UI and tracked in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

/// Lifecycle status of a local or server-side async background job tracked by the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncJobStatus {
    Idle,
    Running {
        job_id: u64,
    },
    Completed {
        job_id: u64,
        elapsed_ms: u64,
        summary: String,
    },
}

/// Tracks ping sequence numbers, round-trip timing, and server connection status.
#[derive(Resource, Debug)]
pub struct ClientPingState {
    pub server_addr: SocketAddr,
    pub next_seq: u64,
    pub last_sent: Option<(u64, Instant)>,
    pub last_pong_seq: Option<u64>,
    pub last_rtt_ms: Option<f64>,
    pub pong_count: u64,
    pub connection_status: ConnectionStatus,
    pub last_error: Option<String>,
}

impl ClientPingState {
    /// Creates initial ping tracking state targeting `server_addr`.
    ///
    /// # Arguments
    /// * `server_addr` - UDP address of the server the client connects to.
    ///
    /// # Example
    /// ```
    /// use std::net::SocketAddr;
    /// use bevy_server_in_docker_poc::client::{ClientPingState, ConnectionStatus};
    ///
    /// let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    /// let state = ClientPingState::new(addr);
    /// assert_eq!(state.connection_status, ConnectionStatus::Connecting);
    /// ```
    pub fn new(server_addr: SocketAddr) -> Self {
        Self {
            server_addr,
            next_seq: 1,
            last_sent: None,
            last_pong_seq: None,
            last_rtt_ms: None,
            pong_count: 0,
            connection_status: ConnectionStatus::Connecting,
            last_error: None,
        }
    }
}

/// Tracks both local client async tasks and server-side async RPC requests.
#[derive(Resource, Debug)]
pub struct ClientAsyncState {
    pub next_local_job_id: u64,
    pub local_status: AsyncJobStatus,
    pub next_server_job_id: u64,
    pub server_status: AsyncJobStatus,
}

impl Default for ClientAsyncState {
    fn default() -> Self {
        Self {
            next_local_job_id: 1,
            local_status: AsyncJobStatus::Idle,
            next_server_job_id: 1,
            server_status: AsyncJobStatus::Idle,
        }
    }
}

/// Result produced by a local client background async task.
struct LocalAsyncJobOutcome {
    job_id: u64,
    output: AsyncWorkOutput,
}

/// Headless networking and async-task plugin for the client; separated from [`ClientUiPlugin`]
/// so integration tests can exercise real UDP ping/pong and async jobs without a GUI window.
pub struct ClientNetPlugin {
    pub server_addr: SocketAddr,
}

impl Plugin for ClientNetPlugin {
    fn build(&self, app: &mut App) {
        let (client, transport) = create_renet_client(self.server_addr)
            .expect("failed to initialize client UDP netcode transport");

        app.add_plugins((RenetClientPlugin, NetcodeClientPlugin))
            .insert_resource(ClientPingState::new(self.server_addr))
            .init_resource::<ClientAsyncState>()
            .insert_resource(client)
            .insert_resource(transport)
            .add_observer(on_netcode_error)
            .add_systems(
                Update,
                (
                    sync_connection_status,
                    receive_server_messages,
                    poll_local_async_jobs,
                ),
            );
    }
}

/// GUI plugin rendering connection status, interactive action buttons, and status readouts.
struct ClientUiPlugin;

impl Plugin for ClientUiPlugin {
    fn build(&self, app: &mut App) {
        // `DefaultPlugins` already registers `UiWidgetsPlugins` (including `ButtonPlugin`).
        app.add_systems(Startup, setup_ui)
            .add_systems(Update, (update_ui_text, update_button_visuals));
    }
}

/// Identifies each dynamic text readout in the client UI.
#[derive(Component, Clone, Copy, PartialEq, Eq)]
enum UiLabelKind {
    Status,
    Pong,
    LocalAsync,
    ServerAsync,
}

/// Identifies each interactive action button in the client UI.
#[derive(Component, Clone, Copy, PartialEq, Eq)]
enum ActionButton {
    Ping,
    LocalAsync,
    ServerAsync,
}

impl ActionButton {
    /// Returns whether this button requires an active server connection.
    fn requires_connection(self) -> bool {
        match self {
            Self::Ping | Self::ServerAsync => true,
            Self::LocalAsync => false,
        }
    }
}

/// Creates the [`RenetClient`] and [`NetcodeClientTransport`] targeting `server_addr`.
///
/// Binds `0.0.0.0:0` so the socket can communicate with both localhost and remote/Docker IPs.
///
/// # Arguments
/// * `server_addr` - Destination UDP address of the Bevy server.
fn create_renet_client(
    server_addr: SocketAddr,
) -> io::Result<(RenetClient, NetcodeClientTransport)> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    let current_time = unix_timestamp();
    let client_id = current_time.as_nanos() as u64;

    let authentication = ClientAuthentication::Unsecure {
        client_id,
        protocol_id: PROTOCOL_ID,
        server_addr,
        user_data: None,
    };

    let transport = NetcodeClientTransport::new(current_time, authentication, socket)
        .map_err(|err| io::Error::other(format!("netcode transport init failed: {err}")))?;
    let client = RenetClient::new(ConnectionConfig::default());

    Ok((client, transport))
}

/// Encodes and sends a [`ClientMessage`] over `DefaultChannel::ReliableOrdered` if connected.
///
/// # Arguments
/// * `client` - Mutable access to [`RenetClient`].
/// * `message` - Protocol message to serialize and send.
fn send_client_message(client: &mut RenetClient, message: &ClientMessage) -> bool {
    if !client.is_connected() {
        warn!("Cannot send {message:?} while not connected to server");
        return false;
    }

    match encode_message(message) {
        Ok(bytes) => {
            client.send_message(DefaultChannel::ReliableOrdered, bytes);
            true
        }
        Err(err) => {
            error!("Failed to encode {message:?}: {err}");
            false
        }
    }
}

/// Sends a [`ClientMessage::Ping`] on `DefaultChannel::ReliableOrdered` if connected.
///
/// Returns `true` when the ping was queued for transmission, or `false` if not yet connected.
///
/// # Arguments
/// * `client` - Mutable access to the active [`RenetClient`].
/// * `state` - Mutable ping state where sequence numbers and send timestamps are recorded.
///
/// # Example
/// ```no_run
/// use bevy::prelude::*;
/// use bevy_renet::RenetClient;
/// use bevy_server_in_docker_poc::client::{ClientPingState, send_ping};
///
/// fn my_system(mut client: ResMut<RenetClient>, mut state: ResMut<ClientPingState>) {
///     let _sent = send_ping(&mut client, &mut state);
/// }
/// ```
pub fn send_ping(client: &mut RenetClient, state: &mut ClientPingState) -> bool {
    let seq = state.next_seq;
    if !send_client_message(client, &ClientMessage::Ping { seq }) {
        return false;
    }

    state.next_seq += 1;
    state.last_sent = Some((seq, Instant::now()));
    info!("Sent Ping(seq={seq}) to {}", state.server_addr);
    true
}

/// Spawns a non-blocking local async job on Bevy's background task pool and returns its `job_id`.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] used to attach the spawned [`AsyncTask`] component.
/// * `async_state` - Mutable [`ClientAsyncState`] updated to [`AsyncJobStatus::Running`].
/// * `delay` - Simulated work duration executed off the UI thread.
///
/// # Example
/// ```no_run
/// use std::time::Duration;
/// use bevy::prelude::*;
/// use bevy_server_in_docker_poc::client::{ClientAsyncState, spawn_local_async_job};
///
/// fn trigger_job(mut commands: Commands, mut async_state: ResMut<ClientAsyncState>) {
///     let _job_id = spawn_local_async_job(&mut commands, &mut async_state, Duration::from_millis(200));
/// }
/// ```
pub fn spawn_local_async_job(
    commands: &mut Commands,
    async_state: &mut ClientAsyncState,
    delay: Duration,
) -> u64 {
    let job_id = async_state.next_local_job_id;
    async_state.next_local_job_id += 1;
    async_state.local_status = AsyncJobStatus::Running { job_id };

    info!("Spawning local client async job #{job_id} (delay={delay:?})");
    commands.spawn(AsyncTask::spawn(async move {
        let label = format!("client-local-#{job_id}");
        let output = simulate_async_work(&label, delay).await;
        LocalAsyncJobOutcome { job_id, output }
    }));
    job_id
}

/// Sends a [`ClientMessage::AsyncWorkRequest`] to the server if connected, returning the `job_id`.
///
/// # Arguments
/// * `client` - Mutable access to [`RenetClient`].
/// * `async_state` - Mutable [`ClientAsyncState`] updated to [`AsyncJobStatus::Running`].
/// * `delay_ms` - Requested simulated work duration on the server in milliseconds.
///
/// # Example
/// ```no_run
/// use bevy::prelude::*;
/// use bevy_renet::RenetClient;
/// use bevy_server_in_docker_poc::client::{ClientAsyncState, request_server_async_work};
///
/// fn trigger_rpc(mut client: ResMut<RenetClient>, mut async_state: ResMut<ClientAsyncState>) {
///     let _job_id = request_server_async_work(&mut client, &mut async_state, 500);
/// }
/// ```
pub fn request_server_async_work(
    client: &mut RenetClient,
    async_state: &mut ClientAsyncState,
    delay_ms: u64,
) -> Option<u64> {
    let job_id = async_state.next_server_job_id;
    let message = ClientMessage::AsyncWorkRequest { job_id, delay_ms };
    if !send_client_message(client, &message) {
        return None;
    }

    async_state.next_server_job_id += 1;
    async_state.server_status = AsyncJobStatus::Running { job_id };
    info!("Sent AsyncWorkRequest(job_id={job_id}, delay_ms={delay_ms})");
    Some(job_id)
}

/// Builds and launches the GUI Bevy client connecting to `server_addr`.
///
/// # Arguments
/// * `server_addr` - UDP address of the Bevy server (e.g. `127.0.0.1:5000`).
///
/// # Example
/// ```no_run
/// use std::net::SocketAddr;
/// use bevy_server_in_docker_poc::client;
///
/// let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
/// client::run(addr);
/// ```
pub fn run(server_addr: SocketAddr) {
    App::new()
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: WINDOW_TITLE.to_string(),
                resolution: (WINDOW_WIDTH, WINDOW_HEIGHT).into(),
                ..default()
            }),
            ..default()
        }))
        .add_plugins((ClientNetPlugin { server_addr }, ClientUiPlugin))
        .run();
}

/// Synchronizes [`ClientPingState::connection_status`] with the live [`RenetClient`] state.
///
/// # Arguments
/// * `client` - Read-only access to the [`RenetClient`] resource.
/// * `state` - Mutable [`ClientPingState`] updated only when status changes.
fn sync_connection_status(client: Res<RenetClient>, mut state: ResMut<ClientPingState>) {
    let next_status = if client.is_connected() {
        ConnectionStatus::Connected
    } else if client.is_connecting() {
        ConnectionStatus::Connecting
    } else {
        ConnectionStatus::Disconnected
    };

    if state.connection_status != next_status {
        info!(
            "Client connection status changed: {:?} -> {:?}",
            state.connection_status, next_status
        );
        state.connection_status = next_status;
    }
}

/// Drains incoming server messages, updating ping RTT counters and server async RPC state.
///
/// # Arguments
/// * `client` - Mutable access to [`RenetClient`] for receiving messages.
/// * `ping_state` - Mutable [`ClientPingState`] storing the latest pong metadata.
/// * `async_state` - Mutable [`ClientAsyncState`] storing server async RPC results.
fn receive_server_messages(
    mut client: ResMut<RenetClient>,
    mut ping_state: ResMut<ClientPingState>,
    mut async_state: ResMut<ClientAsyncState>,
) {
    while let Some(payload) = client.receive_message(DefaultChannel::ReliableOrdered) {
        let message = match decode_message::<ServerMessage>(&payload) {
            Ok(msg) => msg,
            Err(err) => {
                warn!("Ignoring malformed message from server: {err}");
                continue;
            }
        };

        match message {
            ServerMessage::Pong { seq } => {
                let rtt_ms = match ping_state.last_sent {
                    Some((sent_seq, sent_at)) if sent_seq == seq => {
                        Some(sent_at.elapsed().as_secs_f64() * 1000.0)
                    }
                    _ => None,
                };
                ping_state.last_pong_seq = Some(seq);
                ping_state.last_rtt_ms = rtt_ms;
                ping_state.pong_count += 1;
                match rtt_ms {
                    Some(ms) => info!("Received Pong(seq={seq}) in {ms:.2} ms"),
                    None => info!("Received Pong(seq={seq})"),
                }
            }
            ServerMessage::AsyncWorkResult {
                job_id,
                elapsed_ms,
                summary,
            } => {
                info!(
                    "Received AsyncWorkResult(job_id={job_id}, elapsed_ms={elapsed_ms}): {summary}"
                );
                async_state.server_status = AsyncJobStatus::Completed {
                    job_id,
                    elapsed_ms,
                    summary,
                };
            }
        }
    }
}

/// Polls local client async tasks without blocking UI rendering and records finished results.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] used to despawn completed task entities.
/// * `async_state` - Mutable [`ClientAsyncState`] updated upon task completion.
/// * `tasks` - Query over in-flight local [`AsyncTask<LocalAsyncJobOutcome>`] entities.
fn poll_local_async_jobs(
    mut commands: Commands,
    mut async_state: ResMut<ClientAsyncState>,
    mut tasks: Query<(Entity, &mut AsyncTask<LocalAsyncJobOutcome>)>,
) {
    for (entity, mut task) in &mut tasks {
        let Some(outcome) = task.try_take() else {
            continue;
        };
        commands.entity(entity).despawn();

        info!(
            "Local client async job #{} finished in {} ms: {}",
            outcome.job_id, outcome.output.elapsed_ms, outcome.output.summary
        );
        async_state.local_status = AsyncJobStatus::Completed {
            job_id: outcome.job_id,
            elapsed_ms: outcome.output.elapsed_ms,
            summary: outcome.output.summary,
        };
    }
}

/// Records netcode transport errors in [`ClientPingState`] for UI feedback and logging.
///
/// # Arguments
/// * `error` - Observer trigger wrapping [`NetcodeErrorEvent`].
/// * `state` - Mutable [`ClientPingState`] where the latest error text is stored.
fn on_netcode_error(error: On<NetcodeErrorEvent>, mut state: ResMut<ClientPingState>) {
    let msg = error.to_string();
    warn!("Client netcode transport error: {msg}");
    state.last_error = Some(msg);
}

/// Observer dispatching button clicks across all [`ActionButton`] variants.
///
/// # Arguments
/// * `activate` - Activation event emitted by [`bevy::ui_widgets::ButtonPlugin`].
/// * `commands` - Bevy [`Commands`] used to spawn local async tasks.
/// * `buttons` - Query resolving which [`ActionButton`] entity was activated.
/// * `client` - Mutable [`RenetClient`] used for server requests.
/// * `ping_state` - Mutable [`ClientPingState`] for ping tracking.
/// * `async_state` - Mutable [`ClientAsyncState`] for local and server async jobs.
fn on_action_button_activate(
    activate: On<Activate>,
    mut commands: Commands,
    buttons: Query<&ActionButton>,
    mut client: ResMut<RenetClient>,
    mut ping_state: ResMut<ClientPingState>,
    mut async_state: ResMut<ClientAsyncState>,
) {
    let Ok(&action) = buttons.get(activate.entity) else {
        return;
    };

    match action {
        ActionButton::Ping => {
            send_ping(&mut client, &mut ping_state);
        }
        ActionButton::LocalAsync => {
            spawn_local_async_job(
                &mut commands,
                &mut async_state,
                Duration::from_millis(DEFAULT_LOCAL_ASYNC_DELAY_MS),
            );
        }
        ActionButton::ServerAsync => {
            request_server_async_work(&mut client, &mut async_state, DEFAULT_SERVER_ASYNC_DELAY_MS);
        }
    }
}

/// Spawns a styled interactive button entity wired to [`on_action_button_activate`].
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] buffer.
/// * `label` - Visible button text.
/// * `action` - [`ActionButton`] variant identifying what clicking this button triggers.
fn spawn_action_button(commands: &mut Commands, label: &str, action: ActionButton) -> Entity {
    let button_label = commands
        .spawn((
            Text::new(label),
            TextFont {
                font_size: BUTTON_FONT_SIZE,
                ..default()
            },
            TextColor(BUTTON_TEXT_COLOR),
        ))
        .id();

    let initial_bg = if action.requires_connection() {
        BUTTON_DISABLED_COLOR
    } else {
        BUTTON_NORMAL_COLOR
    };

    commands
        .spawn((
            action,
            UiButton,
            Hovered::default(),
            Node {
                width: Val::Px(BUTTON_WIDTH_PX),
                height: Val::Px(BUTTON_HEIGHT_PX),
                align_items: AlignItems::Center,
                justify_content: JustifyContent::Center,
                border_radius: BorderRadius::all(Val::Px(BUTTON_BORDER_RADIUS_PX)),
                ..default()
            },
            BackgroundColor(initial_bg),
        ))
        .add_child(button_label)
        .observe(on_action_button_activate)
        .id()
}

/// Spawns a dynamic readout text entity tagged with [`UiLabelKind`].
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] buffer.
/// * `kind` - [`UiLabelKind`] identifying which state field drives this label.
/// * `initial_text` - Initial display text rendered on the first frame.
fn spawn_status_label(commands: &mut Commands, kind: UiLabelKind, initial_text: String) -> Entity {
    commands
        .spawn((
            kind,
            Text::new(initial_text),
            TextFont {
                font_size: STATUS_FONT_SIZE,
                ..default()
            },
            TextColor(BODY_TEXT_COLOR),
        ))
        .id()
}

/// Spawns the 2D UI camera, layout container, status labels, and interactive buttons.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] buffer used to spawn UI entities.
/// * `ping_state` - Initial [`ClientPingState`] used to populate initial status labels.
/// * `async_state` - Initial [`ClientAsyncState`] used to populate async status labels.
fn setup_ui(
    mut commands: Commands,
    ping_state: Res<ClientPingState>,
    async_state: Res<ClientAsyncState>,
) {
    commands.spawn(Camera2d);

    let root = commands
        .spawn((
            Node {
                width: Val::Percent(100.0),
                height: Val::Percent(100.0),
                flex_direction: FlexDirection::Column,
                align_items: AlignItems::Center,
                justify_content: JustifyContent::Center,
                row_gap: Val::Px(LAYOUT_GAP_PX),
                padding: UiRect::all(Val::Px(LAYOUT_PADDING_PX)),
                ..default()
            },
            BackgroundColor(BG_COLOR),
        ))
        .id();

    let title = commands
        .spawn((
            Text::new("Bevy Renet Ping / Async POC"),
            TextFont {
                font_size: TITLE_FONT_SIZE,
                ..default()
            },
            TextColor(TITLE_TEXT_COLOR),
        ))
        .id();

    let status = spawn_status_label(
        &mut commands,
        UiLabelKind::Status,
        format_status_text(&ping_state),
    );
    let ping_button = spawn_action_button(&mut commands, "Ping Server", ActionButton::Ping);
    let pong = spawn_status_label(
        &mut commands,
        UiLabelKind::Pong,
        format_pong_text(&ping_state),
    );
    let local_async_button =
        spawn_action_button(&mut commands, "Run Local Async", ActionButton::LocalAsync);
    let local_async = spawn_status_label(
        &mut commands,
        UiLabelKind::LocalAsync,
        format_async_status_text("Local Async", &async_state.local_status),
    );
    let server_async_button =
        spawn_action_button(&mut commands, "Run Server Async", ActionButton::ServerAsync);
    let server_async = spawn_status_label(
        &mut commands,
        UiLabelKind::ServerAsync,
        format_async_status_text("Server Async", &async_state.server_status),
    );

    commands.entity(root).add_children(&[
        title,
        status,
        ping_button,
        pong,
        local_async_button,
        local_async,
        server_async_button,
        server_async,
    ]);
}

/// Refreshes all UI labels whenever [`ClientPingState`] or [`ClientAsyncState`] changes.
///
/// # Arguments
/// * `ping_state` - Shared [`ClientPingState`] resource.
/// * `async_state` - Shared [`ClientAsyncState`] resource.
/// * `labels` - Query over all [`UiLabelKind`] text entities.
fn update_ui_text(
    ping_state: Res<ClientPingState>,
    async_state: Res<ClientAsyncState>,
    mut labels: Query<(&UiLabelKind, &mut Text)>,
) {
    if !ping_state.is_changed() && !async_state.is_changed() {
        return;
    }

    for (kind, mut text) in &mut labels {
        text.0 = match kind {
            UiLabelKind::Status => format_status_text(&ping_state),
            UiLabelKind::Pong => format_pong_text(&ping_state),
            UiLabelKind::LocalAsync => {
                format_async_status_text("Local Async", &async_state.local_status)
            }
            UiLabelKind::ServerAsync => {
                format_async_status_text("Server Async", &async_state.server_status)
            }
        };
    }
}

/// Updates action button background colors to reflect connection, hover, and press state.
///
/// # Arguments
/// * `state` - Shared [`ClientPingState`] to check whether the client is connected.
/// * `buttons` - Query over every [`ActionButton`]'s hover, press, and background components.
fn update_button_visuals(
    state: Res<ClientPingState>,
    mut buttons: Query<(&ActionButton, &Hovered, Has<Pressed>, &mut BackgroundColor)>,
) {
    let connected = state.connection_status == ConnectionStatus::Connected;

    for (action, hovered, pressed, mut bg) in &mut buttons {
        let color = if action.requires_connection() && !connected {
            BUTTON_DISABLED_COLOR
        } else if pressed {
            BUTTON_PRESSED_COLOR
        } else if hovered.get() {
            BUTTON_HOVER_COLOR
        } else {
            BUTTON_NORMAL_COLOR
        };

        if bg.0 != color {
            bg.0 = color;
        }
    }
}

/// Formats the connection status string shown at the top of the UI.
///
/// # Arguments
/// * `state` - Current [`ClientPingState`].
fn format_status_text(state: &ClientPingState) -> String {
    let base = match state.connection_status {
        ConnectionStatus::Connecting => format!("Status: Connecting to {}...", state.server_addr),
        ConnectionStatus::Connected => format!("Status: Connected ({})", state.server_addr),
        ConnectionStatus::Disconnected => format!("Status: Disconnected ({})", state.server_addr),
    };

    match &state.last_error {
        Some(err) if state.connection_status != ConnectionStatus::Connected => {
            format!("{base}\nError: {err}")
        }
        _ => base,
    }
}

/// Formats the latest Pong response and cumulative count shown below the Ping button.
///
/// # Arguments
/// * `state` - Current [`ClientPingState`].
fn format_pong_text(state: &ClientPingState) -> String {
    match (state.last_pong_seq, state.last_rtt_ms) {
        (Some(seq), Some(rtt_ms)) => {
            format!(
                "Last Pong: #{seq} ({rtt_ms:.2} ms) | Total: {}",
                state.pong_count
            )
        }
        (Some(seq), None) => {
            format!("Last Pong: #{seq} | Total: {}", state.pong_count)
        }
        (None, _) => "Last Pong: none yet".to_string(),
    }
}

/// Formats a local or server async job status line.
///
/// # Arguments
/// * `prefix` - Label prefix (e.g. `"Local Async"` or `"Server Async"`).
/// * `status` - Current [`AsyncJobStatus`] to render.
fn format_async_status_text(prefix: &str, status: &AsyncJobStatus) -> String {
    match status {
        AsyncJobStatus::Idle => format!("{prefix}: idle"),
        AsyncJobStatus::Running { job_id } => format!("{prefix}: running #{job_id}..."),
        AsyncJobStatus::Completed {
            job_id,
            elapsed_ms,
            summary,
        } => format!("{prefix}: #{job_id} {summary} ({elapsed_ms} ms)"),
    }
}
