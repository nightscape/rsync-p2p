#!/bin/bash

set -e

MODE="${1:-help}"
QUIET="${QUIET:-false}"

# Check for --quiet flag in any position
for arg in "$@"; do
    if [ "$arg" = "--quiet" ]; then
        QUIET=true
    fi
done

show_help() {
    cat <<EOF
rsync-p2p - Rsync with Malai P2P tunneling

Usage:
  docker run rsync-p2p server [PORT] [--quiet]    - Run rsync daemon with Malai TCP exposure
  docker run rsync-p2p client <ID> <SRC> <DEST>  - Connect via Malai bridge and sync

Modes:
  server [PORT] [--quiet]  Run rsync in daemon mode and expose via Malai
                          PORT defaults to 873 (rsync default)
                          --quiet outputs only MALAI_ID=<id> for machine parsing

  client <ID> <SRC> <DEST>  Connect to rsync daemon via Malai TCP bridge
                          ID: Malai identifier from server
                          SRC: Source path
                          DEST: Destination path

Examples:
  # On server (human-friendly):
  docker run -v /data:/data rsync-p2p server

  # On server (machine-readable):
  docker run -e QUIET=true -v /data:/data rsync-p2p server

  # On client (after getting ID from server):
  docker run -v /local:/local rsync-p2p client <id52> /files/ /local/

Environment Variables:
  RSYNC_PORT      Port for rsync daemon (default: 873)
  BRIDGE_PORT     Local port for Malai bridge (default: 8873)
  QUIET           Set to 'true' for machine-readable output (default: false)

EOF
}

run_server() {
    local port="${1:-${RSYNC_PORT:-873}}"

    # Generate Malai identity if not provided
    if [ -z "$KULFI_SECRET_KEY" ]; then
        [ "$QUIET" != "true" ] && echo "🔑 Generating Malai identity..." >&2
        export KULFI_SECRET_KEY=$(malai keygen 2>&1 | grep -v "Generated Public Key")
    fi

    [ "$QUIET" != "true" ] && echo "🚀 Starting rsync daemon on port ${port}..." >&2

    # Create minimal rsyncd.conf
    cat > /tmp/rsyncd.conf <<EOF
[data]
    path = /data
    read only = false
    uid = root
    gid = root
    comment = Rsync data share
EOF

    # Start rsync daemon in background
    rsync --daemon --no-detach --port="${port}" --config=/tmp/rsyncd.conf &
    RSYNC_PID=$!

    # Give rsync a moment to start
    sleep 2

    if ! kill -0 $RSYNC_PID 2>/dev/null; then
        echo "❌ Error: rsync daemon failed to start" >&2
        exit 1
    fi

    [ "$QUIET" != "true" ] && echo "✅ Rsync daemon started (PID: $RSYNC_PID)" >&2
    [ "$QUIET" != "true" ] && echo "🌐 Exposing port ${port} via Malai P2P tunnel..." >&2
    [ "$QUIET" != "true" ] && echo "" >&2

    # Run malai tcp in background and monitor output
    malai tcp "${port}" --public > /tmp/malai-output.log 2>&1 &
    MALAI_PID=$!

    # Wait for Malai ID to appear and extract it (can take 30-60 seconds for P2P setup)
    MALAI_ID=""
    for i in {1..60}; do
        if [ -f /tmp/malai-output.log ]; then
            # Look for the ID in "Run malai tcp-bridge <ID>" line
            MALAI_ID=$(grep "tcp-bridge" /tmp/malai-output.log 2>/dev/null | awk '{print $4}' | head -1)
            if [ -n "$MALAI_ID" ]; then
                break
            fi
        fi
        sleep 1
    done

    if [ -z "$MALAI_ID" ]; then
        echo "❌ Error: Failed to get Malai ID" >&2
        cat /tmp/malai-output.log >&2
        exit 1
    fi

    if [ "$QUIET" = "true" ]; then
        # Machine-readable output
        echo "MALAI_ID=$MALAI_ID"
        echo "PORT=$port"
    else
        # Human-friendly output
        echo "" >&2
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" >&2
        echo "📡 Server is ready!" >&2
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" >&2
        echo "" >&2
        echo "Malai ID: $MALAI_ID" >&2
        echo "Port: $port" >&2
        echo "" >&2
        echo "To sync files from a client, run:" >&2
        echo "" >&2
        echo "  docker run --rm -v /path/to/local:/local rsync-p2p client \\" >&2
        echo "    $MALAI_ID \\" >&2
        echo "    /              \\" >&2
        echo "    /local/" >&2
        echo "" >&2
        echo "Replace /path/to/local with your local directory." >&2
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" >&2
        echo "" >&2
    fi

    # Continue tailing malai output if not in quiet mode
    if [ "$QUIET" != "true" ]; then
        tail -f /tmp/malai-output.log >&2
    else
        # In quiet mode, just wait for malai process
        wait $MALAI_PID
    fi
}

run_client() {
    local malai_id="$1"
    local src="$2"
    local dest="$3"
    local bridge_port="${BRIDGE_PORT:-8873}"

    if [ -z "$malai_id" ] || [ -z "$src" ] || [ -z "$dest" ]; then
        echo "❌ Error: client mode requires <ID> <SRC> <DEST>" >&2
        show_help
        exit 1
    fi

    # Generate Malai identity if not provided
    if [ -z "$KULFI_SECRET_KEY" ]; then
        [ "$QUIET" != "true" ] && echo "🔑 Generating Malai identity..." >&2
        export KULFI_SECRET_KEY=$(malai keygen 2>&1 | grep -v "Generated Public Key")
    fi

    [ "$QUIET" != "true" ] && echo "🌐 Connecting to server ${malai_id}..." >&2
    [ "$QUIET" != "true" ] && echo "🔌 Setting up Malai TCP bridge on port ${bridge_port}..." >&2

    # Start Malai bridge in background and suppress output
    malai tcp-bridge "${malai_id}" "${bridge_port}" > /tmp/bridge.log 2>&1 &
    BRIDGE_PID=$!

    # Give bridge time to establish
    sleep 3

    if ! kill -0 $BRIDGE_PID 2>/dev/null; then
        echo "❌ Error: Malai TCP bridge failed to start" >&2
        cat /tmp/bridge.log >&2
        exit 1
    fi

    [ "$QUIET" != "true" ] && echo "✅ Bridge established!" >&2
    [ "$QUIET" != "true" ] && echo "📦 Syncing files from ${src} to ${dest}..." >&2
    [ "$QUIET" != "true" ] && echo "" >&2

    # Run rsync through the bridge
    if [ "$QUIET" = "true" ]; then
        rsync -az --port="${bridge_port}" "rsync://localhost/data${src}" "${dest}"
    else
        rsync -avz --port="${bridge_port}" "rsync://localhost/data${src}" "${dest}"
    fi
    RSYNC_EXIT=$?

    # Cleanup
    kill $BRIDGE_PID 2>/dev/null || true

    if [ "$QUIET" != "true" ]; then
        echo "" >&2
        if [ $RSYNC_EXIT -eq 0 ]; then
            echo "✅ Sync complete!" >&2
        else
            echo "❌ Sync failed with exit code $RSYNC_EXIT" >&2
        fi
    fi

    exit $RSYNC_EXIT
}

case "$MODE" in
    server)
        run_server "$2"
        ;;
    client)
        run_client "$2" "$3" "$4"
        ;;
    help|--help|-h)
        show_help
        exit 0
        ;;
    *)
        echo "Error: Unknown mode '$MODE'" >&2
        show_help
        exit 1
        ;;
esac
