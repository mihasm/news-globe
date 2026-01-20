# Multi-stage build for location service
FROM rust:latest AS builder

WORKDIR /app
COPY Cargo.toml .
COPY src/ src/

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install curl for downloading zip files
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /app/target/release/geodb /usr/local/bin/geodb

# Create directories
RUN mkdir -p /app/db

# Set working directory
WORKDIR /app

# Copy entrypoint script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8787

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8787/health || exit 1

# Run entrypoint script
CMD ["/app/entrypoint.sh"]