# ====== stage 1: cargo-chef planner ======
FROM rust:1.90.0-alpine3.21 AS chef
RUN apk add --no-cache build-base openssl-dev sqlite-dev pkgconfig
RUN cargo install cargo-chef --locked
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ====== stage 2: builder ======
FROM rust:1.90.0-alpine3.21 AS builder
RUN apk add --no-cache build-base openssl-dev sqlite-dev pkgconfig
RUN cargo install cargo-chef --locked
WORKDIR /app
COPY --from=chef /app/recipe.json recipe.json
# build only dependencies (cache)
RUN cargo chef cook --release --recipe-path recipe.json
# now copy actual source and build
COPY . .
RUN cargo build --release

# ====== stage 3: runtime ======
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 libssl3 ca-certificates tzdata wget curl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/imgpuller-metric-export /usr/local/bin/imgpuller
VOLUME ["/app/data"]
ENV APP_PORT=5555
EXPOSE 5555
HEALTHCHECK --interval=15s --timeout=5s --retries=5 --start-period=20s \
  CMD curl -fsS http://127.0.0.1:${APP_PORT}/health || exit 1
ENTRYPOINT ["/usr/local/bin/imgpuller"]
