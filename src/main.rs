use std::{net::SocketAddr, process::ExitCode};

use clap::{Parser, ValueEnum};

use bevy_server_in_docker_poc::protocol::{DEFAULT_CLIENT_CONNECT_ADDR, DEFAULT_SERVER_BIND_ADDR};

/// Runtime mode selecting whether this binary launches the headless server or GUI client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum RunMode {
    Server,
    Client,
}

/// Command-line configuration for launching either the server or client from one codebase.
#[derive(Parser, Debug)]
#[command(name = "bevy_server_in_docker_poc")]
#[command(about = "Shared-codebase Bevy server (console/Docker) and client (GUI) ping/pong POC")]
struct Cli {
    /// Execution mode: console-only `server` or GUI `client`.
    #[arg(long, value_enum, env = "APP_MODE")]
    mode: RunMode,

    /// UDP socket address to bind (server) or connect to (client).
    #[arg(long, env = "SERVER_ADDR")]
    addr: Option<SocketAddr>,
}

/// Resolves the UDP address for `mode`, falling back to mode-specific defaults when omitted.
///
/// # Arguments
/// * `mode` - Selected [`RunMode`] (`Server` or `Client`).
/// * `override_addr` - Optional address supplied via `--addr` or `SERVER_ADDR`.
fn resolve_addr(mode: RunMode, override_addr: Option<SocketAddr>) -> SocketAddr {
    if let Some(addr) = override_addr {
        return addr;
    }

    let default_str = match mode {
        RunMode::Server => DEFAULT_SERVER_BIND_ADDR,
        RunMode::Client => DEFAULT_CLIENT_CONNECT_ADDR,
    };

    default_str
        .parse()
        .expect("built-in default socket address must parse")
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let addr = resolve_addr(cli.mode, cli.addr);

    match cli.mode {
        RunMode::Server => run_server(addr),
        RunMode::Client => run_client(addr),
    }
}

/// Dispatches server execution when compiled with the `server` Cargo feature.
///
/// # Arguments
/// * `addr` - Local UDP address to bind the server socket on.
fn run_server(addr: SocketAddr) -> ExitCode {
    #[cfg(feature = "server")]
    {
        if let Err(err) = bevy_server_in_docker_poc::server::run(addr) {
            eprintln!("Server failed to start on {addr}: {err}");
            return ExitCode::FAILURE;
        }
        ExitCode::SUCCESS
    }

    #[cfg(not(feature = "server"))]
    {
        let _ = addr;
        eprintln!(
            "This binary was built without the `server` feature. Rebuild with `--features server`."
        );
        ExitCode::FAILURE
    }
}

/// Dispatches GUI client execution when compiled with the `client` Cargo feature.
///
/// # Arguments
/// * `addr` - Target UDP address of the Bevy server.
fn run_client(addr: SocketAddr) -> ExitCode {
    #[cfg(feature = "client")]
    {
        bevy_server_in_docker_poc::client::run(addr);
        ExitCode::SUCCESS
    }

    #[cfg(not(feature = "client"))]
    {
        let _ = addr;
        eprintln!(
            "This binary was built without the `client` feature. Rebuild with `--features client`."
        );
        ExitCode::FAILURE
    }
}
