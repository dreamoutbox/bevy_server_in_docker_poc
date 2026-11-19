#![cfg(all(feature = "server", feature = "client"))]

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use bevy::{ecs::system::RunSystemOnce, prelude::*};
use bevy_renet::RenetClient;
use bevy_server_in_docker_poc::{
    client::{
        AsyncJobStatus, ClientAsyncState, ClientNetPlugin, ClientPingState, ConnectionStatus,
        HttpJobStatus, request_server_async_work, request_server_http_fetch, send_ping,
        spawn_local_async_job, spawn_local_http_fetch,
    },
    protocol::DEFAULT_CLIENT_CONNECT_ADDR,
    server::build_server_app,
};

const STEP_INTERVAL: Duration = Duration::from_millis(16);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Builds a headless client [`App`] that runs networking and async tasks without opening a window.
///
/// # Arguments
/// * `server_addr` - Target UDP address of the Bevy server.
fn build_headless_client_app(server_addr: SocketAddr) -> App {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .add_plugins(ClientNetPlugin { server_addr });
    app
}

/// Steps `server_app` and `client_app` until the client connects or `CONNECT_TIMEOUT` expires.
///
/// # Arguments
/// * `server_app` - Headless server Bevy [`App`].
/// * `client_app` - Headless client Bevy [`App`].
fn drive_until_connected(server_app: &mut App, client_app: &mut App) {
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    while Instant::now() < deadline {
        server_app.update();
        client_app.update();
        if client_app
            .world()
            .resource::<ClientPingState>()
            .connection_status
            == ConnectionStatus::Connected
        {
            return;
        }
        thread::sleep(STEP_INTERVAL);
    }

    panic!("client did not connect to headless server within {CONNECT_TIMEOUT:?}");
}

#[test]
fn ping_pong_round_trip_between_headless_server_and_client() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (mut server_app, server_addr) =
        build_server_app(bind_addr, false).expect("server must bind ephemeral port");
    let mut client_app = build_headless_client_app(server_addr);

    drive_until_connected(&mut server_app, &mut client_app);

    for expected_seq in 1..=2 {
        let sent = {
            let world = client_app.world_mut();
            world.resource_scope(|world, mut client: Mut<RenetClient>| {
                let mut state = world.resource_mut::<ClientPingState>();
                send_ping(&mut client, &mut state)
            })
        };
        assert!(sent, "send_ping must succeed when connected");

        let deadline = Instant::now() + RESPONSE_TIMEOUT;
        while Instant::now() < deadline {
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

#[test]
fn async_work_on_client_and_server_does_not_block_ping() {
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (mut server_app, server_addr) =
        build_server_app(bind_addr, false).expect("server must bind ephemeral port");
    let mut client_app = build_headless_client_app(server_addr);

    drive_until_connected(&mut server_app, &mut client_app);

    // Kick off a local client async job, a server async RPC, and an immediate Ping simultaneously.
    let (local_job_id, server_job_id, ping_sent) = client_app
        .world_mut()
        .run_system_once(
            |mut commands: Commands,
             mut client: ResMut<RenetClient>,
             mut ping_state: ResMut<ClientPingState>,
             mut async_state: ResMut<ClientAsyncState>| {
                let local_job_id = spawn_local_async_job(
                    &mut commands,
                    &mut async_state,
                    Duration::from_millis(40),
                );
                let server_job_id = request_server_async_work(&mut client, &mut async_state, 60);
                let ping_sent = send_ping(&mut client, &mut ping_state);
                (local_job_id, server_job_id, ping_sent)
            },
        )
        .expect("trigger system must execute");

    assert_eq!(local_job_id, 1);
    assert_eq!(server_job_id, Some(1));
    assert!(ping_sent);

    let deadline = Instant::now() + RESPONSE_TIMEOUT;
    while Instant::now() < deadline {
        client_app.update();
        server_app.update();

        let ping_done = client_app.world().resource::<ClientPingState>().pong_count == 1;
        let async_state = client_app.world().resource::<ClientAsyncState>();
        let local_done = matches!(async_state.local_status, AsyncJobStatus::Completed { .. });
        let server_done = matches!(async_state.server_status, AsyncJobStatus::Completed { .. });
        if ping_done && local_done && server_done {
            break;
        }
        thread::sleep(STEP_INTERVAL);
    }

    assert_eq!(
        client_app
            .world()
            .resource::<ClientPingState>()
            .last_pong_seq,
        Some(1)
    );

    let async_state = client_app.world().resource::<ClientAsyncState>();
    match &async_state.local_status {
        AsyncJobStatus::Completed {
            job_id,
            elapsed_ms,
            summary,
        } => {
            assert_eq!(*job_id, 1);
            assert!(*elapsed_ms >= 30);
            assert!(summary.contains("client-local-#1"));
        }
        other => panic!("expected completed local async job, got {other:?}"),
    }

    match &async_state.server_status {
        AsyncJobStatus::Completed {
            job_id,
            elapsed_ms,
            summary,
        } => {
            assert_eq!(*job_id, 1);
            assert!(*elapsed_ms >= 50);
            assert!(summary.contains("server-job-#1"));
        }
        other => panic!("expected completed server async job, got {other:?}"),
    }
}

/// Spawns a local mock HTTP/1.1 server handling `expected_requests` sequential GETs.
fn spawn_mock_http_server(expected_requests: usize, body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("mock HTTP server must bind");
    let addr = listener.local_addr().unwrap();

    thread::spawn(move || {
        for _ in 0..expected_requests {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut req_buf = [0u8; 1024];
                let _ = stream.read(&mut req_buf);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes());
            }
        }
    });

    format!("http://{addr}/mock")
}

#[test]
fn http_fetch_on_client_and_server() {
    let mock_url = spawn_mock_http_server(2, r#"{"ok":true,"source":"local-mock"}"#);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (mut server_app, server_addr) =
        build_server_app(bind_addr, false).expect("server must bind ephemeral port");
    let mut client_app = build_headless_client_app(server_addr);

    drive_until_connected(&mut server_app, &mut client_app);

    let local_url = mock_url.clone();
    let server_url = mock_url.clone();
    let (local_job_id, server_job_id) = client_app
        .world_mut()
        .run_system_once(
            move |mut commands: Commands,
                  mut client: ResMut<RenetClient>,
                  mut async_state: ResMut<ClientAsyncState>| {
                let local_job_id =
                    spawn_local_http_fetch(&mut commands, &mut async_state, local_url.clone());
                let server_job_id = request_server_http_fetch(
                    &mut client,
                    &mut async_state,
                    Some(server_url.clone()),
                );
                (local_job_id, server_job_id)
            },
        )
        .expect("trigger HTTP fetch system must execute");

    assert_eq!(local_job_id, 1);
    assert_eq!(server_job_id, Some(1));

    let deadline = Instant::now() + RESPONSE_TIMEOUT;
    while Instant::now() < deadline {
        client_app.update();
        server_app.update();

        let async_state = client_app.world().resource::<ClientAsyncState>();
        let local_done = matches!(
            async_state.local_http_status,
            HttpJobStatus::Completed { .. }
        );
        let server_done = matches!(
            async_state.server_http_status,
            HttpJobStatus::Completed { .. }
        );
        if local_done && server_done {
            break;
        }
        thread::sleep(STEP_INTERVAL);
    }

    let async_state = client_app.world().resource::<ClientAsyncState>();
    match &async_state.local_http_status {
        HttpJobStatus::Completed {
            job_id,
            status_code,
            body_preview,
            error,
            ..
        } => {
            assert_eq!(*job_id, 1);
            assert_eq!(*status_code, Some(200));
            assert!(error.is_none());
            assert!(body_preview.contains("local-mock"));
        }
        other => panic!("expected completed local HTTP fetch, got {other:?}"),
    }

    match &async_state.server_http_status {
        HttpJobStatus::Completed {
            job_id,
            status_code,
            body_preview,
            error,
            ..
        } => {
            assert_eq!(*job_id, 1);
            assert_eq!(*status_code, Some(200));
            assert!(error.is_none());
            assert!(body_preview.contains("local-mock"));
        }
        other => panic!("expected completed server HTTP fetch, got {other:?}"),
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

    let pong_deadline = Instant::now() + RESPONSE_TIMEOUT;
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
