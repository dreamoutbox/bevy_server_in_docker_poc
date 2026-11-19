use std::{
    io,
    net::{SocketAddr, UdpSocket},
    time::Instant,
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

use crate::protocol::{
    ClientMessage, PROTOCOL_ID, ServerMessage, decode_message, encode_message, unix_timestamp,
};

const WINDOW_TITLE: &str = "Bevy Server in Docker POC - Client";
const WINDOW_WIDTH: u32 = 520;
const WINDOW_HEIGHT: u32 = 340;

const BG_COLOR: Color = Color::srgb(0.10, 0.11, 0.14);
const TITLE_TEXT_COLOR: Color = Color::srgb(0.95, 0.96, 0.98);
const BODY_TEXT_COLOR: Color = Color::srgb(0.82, 0.85, 0.90);
const BUTTON_TEXT_COLOR: Color = Color::srgb(0.98, 0.98, 0.99);

const BUTTON_NORMAL_COLOR: Color = Color::srgb(0.18, 0.45, 0.78);
const BUTTON_HOVER_COLOR: Color = Color::srgb(0.24, 0.54, 0.88);
const BUTTON_PRESSED_COLOR: Color = Color::srgb(0.12, 0.34, 0.62);
const BUTTON_DISABLED_COLOR: Color = Color::srgb(0.30, 0.32, 0.36);

const TITLE_FONT_SIZE: FontSize = FontSize::Px(26.0);
const STATUS_FONT_SIZE: FontSize = FontSize::Px(18.0);
const BUTTON_FONT_SIZE: FontSize = FontSize::Px(20.0);
const BUTTON_WIDTH_PX: f32 = 220.0;
const BUTTON_HEIGHT_PX: f32 = 54.0;
const BUTTON_BORDER_RADIUS_PX: f32 = 8.0;
const LAYOUT_GAP_PX: f32 = 18.0;
const LAYOUT_PADDING_PX: f32 = 24.0;

/// High-level connection state displayed by the client UI and tracked in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
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

/// Headless networking plugin for the client; separated from [`ClientUiPlugin`] so integration
/// tests can exercise real UDP ping/pong without opening a GUI window.
pub struct ClientNetPlugin {
    pub server_addr: SocketAddr,
}

impl Plugin for ClientNetPlugin {
    fn build(&self, app: &mut App) {
        let (client, transport) = create_renet_client(self.server_addr)
            .expect("failed to initialize client UDP netcode transport");

        app.add_plugins((RenetClientPlugin, NetcodeClientPlugin))
            .insert_resource(ClientPingState::new(self.server_addr))
            .insert_resource(client)
            .insert_resource(transport)
            .add_observer(on_netcode_error)
            .add_systems(Update, (sync_connection_status, receive_server_messages));
    }
}

/// GUI plugin rendering connection status, the Ping button, and Pong responses.
struct ClientUiPlugin;

impl Plugin for ClientUiPlugin {
    fn build(&self, app: &mut App) {
        // `DefaultPlugins` already registers `UiWidgetsPlugins` (including `ButtonPlugin`).
        app.add_systems(Startup, setup_ui)
            .add_systems(Update, (update_ui_text, update_button_visuals));
    }
}

#[derive(Component)]
struct StatusText;

#[derive(Component)]
struct PongText;

#[derive(Component)]
struct PingButton;

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
    if !client.is_connected() {
        warn!(
            "Cannot send Ping while not connected to {}",
            state.server_addr
        );
        return false;
    }

    let seq = state.next_seq;
    state.next_seq += 1;
    state.last_sent = Some((seq, Instant::now()));

    match encode_message(&ClientMessage::Ping { seq }) {
        Ok(bytes) => {
            client.send_message(DefaultChannel::ReliableOrdered, bytes);
            info!("Sent Ping(seq={seq}) to {}", state.server_addr);
            true
        }
        Err(err) => {
            error!("Failed to encode Ping(seq={seq}): {err}");
            false
        }
    }
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

/// Drains incoming server messages and updates pong counters and RTT measurement.
///
/// # Arguments
/// * `client` - Mutable access to [`RenetClient`] for receiving messages.
/// * `state` - Mutable [`ClientPingState`] storing the latest pong metadata.
fn receive_server_messages(mut client: ResMut<RenetClient>, mut state: ResMut<ClientPingState>) {
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
                let rtt_ms = match state.last_sent {
                    Some((sent_seq, sent_at)) if sent_seq == seq => {
                        Some(sent_at.elapsed().as_secs_f64() * 1000.0)
                    }
                    _ => None,
                };
                state.last_pong_seq = Some(seq);
                state.last_rtt_ms = rtt_ms;
                state.pong_count += 1;
                match rtt_ms {
                    Some(ms) => info!("Received Pong(seq={seq}) in {ms:.2} ms"),
                    None => info!("Received Pong(seq={seq})"),
                }
            }
        }
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

/// Observer invoked when the user clicks or activates the Ping button.
///
/// # Arguments
/// * `_activate` - Activation event emitted by [`bevy::ui_widgets::ButtonPlugin`].
/// * `client` - Mutable [`RenetClient`] used to queue the ping message.
/// * `state` - Mutable [`ClientPingState`] tracking sequence numbers and send time.
fn on_ping_button_activate(
    _activate: On<Activate>,
    mut client: ResMut<RenetClient>,
    mut state: ResMut<ClientPingState>,
) {
    send_ping(&mut client, &mut state);
}

/// Spawns the 2D UI camera, layout container, status labels, and interactive Ping button.
///
/// # Arguments
/// * `commands` - Bevy [`Commands`] buffer used to spawn UI entities.
/// * `state` - Initial [`ClientPingState`] used to populate initial label text.
fn setup_ui(mut commands: Commands, state: Res<ClientPingState>) {
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
            Text::new("Bevy Renet Ping / Pong POC"),
            TextFont {
                font_size: TITLE_FONT_SIZE,
                ..default()
            },
            TextColor(TITLE_TEXT_COLOR),
        ))
        .id();

    let status = commands
        .spawn((
            StatusText,
            Text::new(format_status_text(&state)),
            TextFont {
                font_size: STATUS_FONT_SIZE,
                ..default()
            },
            TextColor(BODY_TEXT_COLOR),
        ))
        .id();

    let button_label = commands
        .spawn((
            Text::new("Ping Server"),
            TextFont {
                font_size: BUTTON_FONT_SIZE,
                ..default()
            },
            TextColor(BUTTON_TEXT_COLOR),
        ))
        .id();

    let button = commands
        .spawn((
            PingButton,
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
            BackgroundColor(BUTTON_DISABLED_COLOR),
        ))
        .add_child(button_label)
        .observe(on_ping_button_activate)
        .id();

    let pong = commands
        .spawn((
            PongText,
            Text::new(format_pong_text(&state)),
            TextFont {
                font_size: STATUS_FONT_SIZE,
                ..default()
            },
            TextColor(BODY_TEXT_COLOR),
        ))
        .id();

    commands
        .entity(root)
        .add_children(&[title, status, button, pong]);
}

/// Refreshes the status and pong readout labels whenever [`ClientPingState`] changes.
///
/// # Arguments
/// * `state` - Shared [`ClientPingState`] resource.
/// * `status_query` - Query targeting the connection status text entity.
/// * `pong_query` - Query targeting the pong response text entity.
fn update_ui_text(
    state: Res<ClientPingState>,
    mut status_query: Query<&mut Text, (With<StatusText>, Without<PongText>)>,
    mut pong_query: Query<&mut Text, (With<PongText>, Without<StatusText>)>,
) {
    if !state.is_changed() {
        return;
    }

    if let Ok(mut text) = status_query.single_mut() {
        text.0 = format_status_text(&state);
    }
    if let Ok(mut text) = pong_query.single_mut() {
        text.0 = format_pong_text(&state);
    }
}

/// Updates the Ping button background color to reflect connection, hover, and press state.
///
/// # Arguments
/// * `state` - Shared [`ClientPingState`] to check whether the client is connected.
/// * `button_query` - Query over the Ping button's hover, press, and background components.
fn update_button_visuals(
    state: Res<ClientPingState>,
    mut button_query: Query<(&Hovered, Has<Pressed>, &mut BackgroundColor), With<PingButton>>,
) {
    let Ok((hovered, pressed, mut bg)) = button_query.single_mut() else {
        return;
    };

    let color = if state.connection_status != ConnectionStatus::Connected {
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

/// Formats the connection status string shown above the Ping button.
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
