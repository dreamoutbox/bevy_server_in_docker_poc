use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};

use bevy::prelude::*;

use super::*;
use crate::tokio_task::TokioTask;

#[derive(Resource, Default)]
struct CapturedHttpOutcome(Option<HttpFetchOutcome>);

fn poll_test_http_task(
    mut commands: Commands,
    mut query: Query<(Entity, &mut TokioTask<HttpFetchOutcome>)>,
    mut captured: ResMut<CapturedHttpOutcome>,
) {
    for (entity, mut task) in &mut query {
        if let Some(result) = task.try_take() {
            captured.0 = Some(result.expect("Tokio task must not panic"));
            commands.entity(entity).despawn();
        }
    }
}

/// Spawns a single-request local HTTP/1.1 mock server on an ephemeral port and returns its URL.
fn spawn_mock_http_server(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("mock HTTP server must bind");
    let addr = listener.local_addr().unwrap();

    thread::spawn(move || {
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
    });

    format!("http://{addr}/test")
}

#[test]
fn tokio_task_fetches_mock_http_endpoint_in_bevy_app() {
    let url = spawn_mock_http_server(r#"{"ok": true, "message": "hello from mock"}"#);

    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .init_resource::<CapturedHttpOutcome>()
        .add_systems(Update, poll_test_http_task);

    let url_clone = url.clone();
    app.world_mut()
        .spawn(TokioTask::spawn(async move { fetch_url(&url_clone).await }));

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        app.update();
        if app.world().resource::<CapturedHttpOutcome>().0.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    let outcome = app
        .world()
        .resource::<CapturedHttpOutcome>()
        .0
        .clone()
        .expect("HTTP task must complete before deadline");
    assert_eq!(outcome.url, url);
    assert_eq!(outcome.status_code, Some(200));
    assert!(outcome.error.is_none());
    assert!(outcome.body_preview.contains("hello from mock"));
}

#[test]
fn fetch_url_reports_invalid_url_as_error() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins)
        .init_resource::<CapturedHttpOutcome>()
        .add_systems(Update, poll_test_http_task);

    app.world_mut().spawn(TokioTask::spawn(async {
        fetch_url("://invalid-url").await
    }));

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        app.update();
        if app.world().resource::<CapturedHttpOutcome>().0.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    let outcome = app
        .world()
        .resource::<CapturedHttpOutcome>()
        .0
        .clone()
        .expect("HTTP error task must complete");
    assert_eq!(outcome.status_code, None);
    assert!(outcome.error.is_some());
}
