# syntax=docker/dockerfile:1

FROM lukemathwalker/cargo-chef:latest-rust-1-slim-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --no-default-features --features server --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --no-default-features --features server --bin bevy_server_in_docker_poc

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --shell /usr/sbin/nologin app

COPY --from=builder /app/target/release/bevy_server_in_docker_poc /usr/local/bin/bevy_server_in_docker_poc

USER app
EXPOSE 5000/udp

ENTRYPOINT ["/usr/local/bin/bevy_server_in_docker_poc"]
CMD ["--mode", "server"]
