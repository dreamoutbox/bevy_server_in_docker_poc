#![cfg(all(feature = "server", feature = "client"))]

use std::{
    net::SocketAddr,
    thread,
    time::{Duration, Instant},
};

use bevy::prelude::*;
use bevy_renet::RenetClient;
use bevy_server_in_docker_poc::{
    client::{ClientNetPlugin, ClientPingState, ConnectionStatus, send_ping},
    protocol::DEFAULT_CLIENT_CONNECT_ADDR,
    server::build_server_app,
};

const STEP_INTERVAL: Duration = Duration::from_millis(16);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PONG_TIMEOUT: Duration = Duration::from_secs(5);

/// Builds a headless client [`App`] that runs networking without opening a window.
///
/// # Arguments
/// * `server_addr` - Target UDP address of the Bevy server.
fn build_headless_client_app(server_addr: SocketAddr) -> App {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .add_plugins(ClientNetPlugin { server_addr });
    app
}

#[test]
fn ping_pong_round_trip_between_headless_server_and_client() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (mut server_app, server_addr) =
        build_server_app(bind_addr, false).expect("server must bind ephemeral port");
    let mut client_app = build_headless_client_app(server_addr);

    let connect_deadline = Instant::now() + CONNECT_TIMEOUT;
    while Instant::now() < connect_deadline {
        server_app.update();
        client_app.update();
        let state = client_app.world().resource::<ClientPingState>();
        if state.connection_status == ConnectionStatus::Connected {
            break;
        }
        thread::sleep(STEP_INTERVAL);
    }

    assert_eq!(
        client_app
            .world()
            .resource::<ClientPingState>()
            .connection_status,
        ConnectionStatus::Connected,
        "client did not connect to headless server within {CONNECT_TIMEOUT:?}"
    );

    for expected_seq in 1..=2 {
        let sent = {
            let world = client_app.world_mut();
            world.resource_scope(|world, mut client: Mut<RenetClient>| {
                let mut state = world.resource_mut::<ClientPingState>();
                send_ping(&mut client, &mut state)
            })
        };
        assert!(sent, "send_ping must succeed when connected");

        let pong_deadline = Instant::now() + PONG_TIMEOUT;
        while Instant::now() < pong_deadline {
            client_app.update();
            server_app.update();
            let state = client_app.world().resource::<ClientPingState>();
            if state.pong_count == expected_seq {
                break;
            }
            thread::sleep(STEP_INTERVAL);
        }

        let state = client_app.world().resource::<ClientPingState>();
        assert_eq!(state.pong_count, expected_seq);
        assert_eq!(state.last_pong_seq, Some(expected_seq));
        assert!(state.last_rtt_ms.is_some());
    }
}

/// Live smoke test against an already-running server (e.g. `docker compose up -d`).
///
/// Run with:
/// `cargo test --test ping_pong ping_live_server -- --ignored --nocapture`
#[test]
#[ignore = "requires a running server (e.g. docker compose up -d)"]
fn ping_live_server() {
    let addr_str = std::env::var("LIVE_SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_CLIENT_CONNECT_ADDR.to_string());
    let server_addr: SocketAddr = addr_str.parse().expect("LIVE_SERVER_ADDR must be valid");
    let mut client_app = build_headless_client_app(server_addr);

    let connect_deadline = Instant::now() + CONNECT_TIMEOUT;
    while Instant::now() < connect_deadline {
        client_app.update();
        if client_app
            .world()
            .resource::<ClientPingState>()
            .connection_status
            == ConnectionStatus::Connected
        {
            break;
        }
        thread::sleep(STEP_INTERVAL);
    }

    assert_eq!(
        client_app
            .world()
            .resource::<ClientPingState>()
            .connection_status,
        ConnectionStatus::Connected,
        "client did not connect to live server at {server_addr}"
    );

    let sent = {
        let world = client_app.world_mut();
        world.resource_scope(|world, mut client: Mut<RenetClient>| {
            let mut state = world.resource_mut::<ClientPingState>();
            send_ping(&mut client, &mut state)
        })
    };
    assert!(sent, "send_ping must succeed when connected");

    let pong_deadline = Instant::now() + PONG_TIMEOUT;
    while Instant::now() < pong_deadline {
        client_app.update();
        if client_app.world().resource::<ClientPingState>().pong_count >= 1 {
            break;
        }
        thread::sleep(STEP_INTERVAL);
    }

    let state = client_app.world().resource::<ClientPingState>();
    assert_eq!(state.last_pong_seq, Some(1));
    assert!(state.last_rtt_ms.is_some());
    println!(
        "Live server at {server_addr} responded with Pong #1 in {:.2} ms",
        state.last_rtt_ms.unwrap()
    );
}
