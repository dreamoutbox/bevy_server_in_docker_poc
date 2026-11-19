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
