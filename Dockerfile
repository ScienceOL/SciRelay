# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.94
FROM rust:${RUST_VERSION}-slim AS chef
WORKDIR /app
RUN cargo install cargo-chef --locked --version 0.1.71

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG BIN
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --bin liyanlabs-${BIN} --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin liyanlabs-${BIN}

FROM debian:bookworm-slim AS runtime
ARG BIN
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 liyanlabs \
    && useradd --system --uid 1000 --gid 1000 --shell /usr/sbin/nologin liyanlabs
COPY --from=builder /app/target/release/liyanlabs-${BIN} /usr/local/bin/liyanlabs-server
USER liyanlabs
ENTRYPOINT ["/usr/local/bin/liyanlabs-server"]
