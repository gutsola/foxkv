#!/bin/bash
set -e

# FoxKV Docker Entrypoint Script

# Function to log messages
log() {
    echo "[FoxKV Entrypoint] $1"
}

# Set default configuration values
FOXKV_PORT=${FOXKV_PORT:-6379}

# Default config file (use redis.conf)
CONFIG_FILE="/etc/foxkv/redis.conf"

# Handle password configuration
if [ -n "$FOXKV_PASSWORD" ]; then
    log "Setting up authentication..."
    echo "requirepass $FOXKV_PASSWORD" >> "$CONFIG_FILE"
fi

# Handle memory limit
if [ -n "$FOXKV_MAXMEMORY" ]; then
    log "Setting maxmemory to $FOXKV_MAXMEMORY"
    echo "maxmemory $FOXKV_MAXMEMORY" >> "$CONFIG_FILE"
fi

# Handle AOF configuration
if [ "$FOXKV_APPENDONLY" = "no" ]; then
    log "Disabling AOF persistence..."
    sed -i 's/^appendonly yes/appendonly no/' "$CONFIG_FILE"
fi

# Handle custom configuration file
if [ -n "$FOXKV_CONFIG_FILE" ] && [ -f "$FOXKV_CONFIG_FILE" ]; then
    log "Using custom configuration file: $FOXKV_CONFIG_FILE"
    CONFIG_FILE="$FOXKV_CONFIG_FILE"
fi

# Ensure data directory exists and is writable
if [ ! -d "/data" ]; then
    log "Creating data directory..."
    mkdir -p /data
fi

# Print startup information
log "Starting FoxKV..."
log "Port: $FOXKV_PORT"
log "Data Directory: /data"
log "Config File: $CONFIG_FILE"

# Execute the main command
if [ "$1" = "foxkv" ]; then
    exec foxkv --config "$CONFIG_FILE"
else
    exec "$@"
fi
