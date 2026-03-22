# FoxKV Dockerfile
# Multi-stage build for optimized image size

# Stage 1: Builder
FROM rust:1.91.1-slim AS builder

WORKDIR /usr/src/foxkv

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build release binary
RUN cargo build --release --bin foxkv

# Stage 2: Runtime
FROM debian:bookworm-slim

# Create foxkv user for security
RUN groupadd -r foxkv && useradd -r -g foxkv foxkv

# Create data directory
RUN mkdir -p /data && chown foxkv:foxkv /data

# Copy binary from builder
COPY --from=builder /usr/src/foxkv/target/release/foxkv /usr/local/bin/foxkv

# Copy default config (use redis.conf)
COPY redis.conf /etc/foxkv/redis.conf

# Copy entrypoint script
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set working directory
WORKDIR /data

# Expose FoxKV port
EXPOSE 6379

# Switch to non-root user
USER foxkv

# Volume for persistent data
VOLUME ["/data"]

# Entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command
CMD ["foxkv", "--config", "/etc/foxkv/redis.conf"]
