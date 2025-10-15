FROM rust:1.82 as builder
WORKDIR /app
RUN apt-get update && apt-get install -y build-essential pkg-config libssl-dev
COPY . .
RUN cargo install cargo-watch
