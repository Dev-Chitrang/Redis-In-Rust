# Stage 1: Build the Rust application
FROM rust:1 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Stage 2: Runtime image
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/redis-clone /usr/local/bin/rust-redis

RUN mkdir -p /data \
    && useradd --system --no-create-home redisuser \
    && chown -R redisuser:redisuser /data

USER redisuser

EXPOSE 6379

CMD ["rust-redis"]
