# Bevy Server in Docker POC

Single-codebase Rust + Bevy (`0.20`) client/server proof of concept using `bevy_renet` (`6.0`, UDP netcode):

- **Server**: Console-only headless Bevy app running in Docker (`--no-default-features --features server`, zero GUI/audio/display dependencies).
- **Client**: Native Bevy GUI app with a **Ping Server** button that sends `Ping` and displays the server's `Pong` reply and RTT.

## Quickstart (Server in Docker + GUI Client on Host)

1. Build and start the headless Bevy server in Docker:
   ```bash
   docker compose up --build
   ```

2. In another terminal, launch the GUI client:
   ```bash
   cargo run -- --mode client
   ```

3. Click **Ping Server** in the client window. The client displays the `Pong` sequence number and RTT, while the Docker container logs each received `Ping` and outgoing `Pong`.

## Running Locally Without Docker

Start the headless server:
```bash
cargo run --no-default-features --features server -- --mode server
```

Start the GUI client (optionally overriding the target address):
```bash
cargo run -- --mode client --addr 127.0.0.1:5000
```

## Verification Commands

```bash
# Unit tests + headless UDP ping/pong integration test
cargo test

# Verify headless server builds without GUI dependencies
cargo build --no-default-features --features server

# Live smoke test against running Docker server (`docker compose up -d`)
cargo test --test ping_pong ping_live_server -- --ignored --nocapture
```
