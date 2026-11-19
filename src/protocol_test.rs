use super::*;

#[test]
fn client_ping_round_trip() {
    let original = ClientMessage::Ping { seq: 7 };
    let encoded = encode_message(&original).expect("encoding Ping must succeed");
    let decoded: ClientMessage = decode_message(&encoded).expect("decoding Ping must succeed");
    assert_eq!(decoded, original);
}

#[test]
fn server_pong_round_trip() {
    let original = ServerMessage::Pong { seq: 99 };
    let encoded = encode_message(&original).expect("encoding Pong must succeed");
    let decoded: ServerMessage = decode_message(&encoded).expect("decoding Pong must succeed");
    assert_eq!(decoded, original);
}

#[test]
fn invalid_payload_returns_error() {
    let result = decode_message::<ClientMessage>(b"not-valid-json");
    assert!(result.is_err());
}

#[test]
fn client_async_work_request_round_trip() {
    let original = ClientMessage::AsyncWorkRequest {
        job_id: 3,
        delay_ms: 250,
    };
    let encoded = encode_message(&original).expect("encoding AsyncWorkRequest must succeed");
    let decoded: ClientMessage =
        decode_message(&encoded).expect("decoding AsyncWorkRequest must succeed");
    assert_eq!(decoded, original);
}

#[test]
fn server_async_work_result_round_trip() {
    let original = ServerMessage::AsyncWorkResult {
        job_id: 3,
        elapsed_ms: 252,
        summary: "server-job-#3 completed".to_string(),
    };
    let encoded = encode_message(&original).expect("encoding AsyncWorkResult must succeed");
    let decoded: ServerMessage =
        decode_message(&encoded).expect("decoding AsyncWorkResult must succeed");
    assert_eq!(decoded, original);
}

#[test]
fn client_http_fetch_request_round_trip() {
    let original = ClientMessage::HttpFetchRequest {
        job_id: 5,
        url: Some("https://httpbin.org/get".to_string()),
    };
    let encoded = encode_message(&original).expect("encoding HttpFetchRequest must succeed");
    let decoded: ClientMessage =
        decode_message(&encoded).expect("decoding HttpFetchRequest must succeed");
    assert_eq!(decoded, original);
}

#[test]
fn server_http_fetch_result_round_trip() {
    let original = ServerMessage::HttpFetchResult {
        job_id: 5,
        url: "https://httpbin.org/get".to_string(),
        status_code: Some(200),
        elapsed_ms: 145,
        body_preview: "{\"ok\":true}".to_string(),
        error: None,
    };
    let encoded = encode_message(&original).expect("encoding HttpFetchResult must succeed");
    let decoded: ServerMessage =
        decode_message(&encoded).expect("decoding HttpFetchResult must succeed");
    assert_eq!(decoded, original);
}
