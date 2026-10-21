# pi-host-ops-server: dispatcher HTTP + SQLite. Worker stays on the host
# (needs host `pi_svc` / project dirs) and is NOT part of this image.
# Database file lives on a mounted volume; migrations run at startup.

FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
# Build only the server binary (worker / host-ops ship via Release assets).
RUN cargo build --locked --release --bin pi-host-ops-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY LICENSE /app/LICENSE
COPY --from=build /src/target/release/pi-host-ops-server /app/pi-host-ops-server
# SQLite data dir: created here so the anonymous/named volume inherits ownership.
RUN mkdir -p /data && chown 65534:65534 /data
ENV BIND=0.0.0.0:8787
ENV DATABASE_URL=sqlite:/data/host-ops.db
EXPOSE 8787
USER 65534:65534
ENTRYPOINT ["/app/pi-host-ops-server"]
