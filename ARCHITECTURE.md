# Architecture

## Overview

transfer-p2p is a modular, extensible framework for peer-to-peer data transfer through NAT/firewall boundaries using Malai tunneling.

## Design Principles

1. **Modularity**: Each transfer method (rsync, HTTP, etc.) is self-contained
2. **Common Infrastructure**: All methods share Malai's P2P tunneling capabilities
3. **Extensibility**: New transfer methods can be added without modifying existing code
4. **Simplicity**: Container-based deployment with minimal configuration

## Components

### Malai Tunneling Layer

[Malai](https://malai.sh) provides the core P2P tunneling infrastructure:
- **malai tcp --public**: Creates a public tunnel endpoint and generates a unique ID
- **malai tcp-bridge**: Connects to a remote tunnel and creates a local proxy
- Handles NAT traversal and firewall bypass automatically
- No port forwarding or VPN required

### Transfer Methods

Each transfer method consists of:
- **Server mode**: Runs the protocol daemon and exposes it via Malai
- **Client mode**: Connects to remote daemon via Malai bridge

#### rsync (Currently Implemented)

Server:
1. Creates rsyncd.conf with `/data` share
2. Starts rsync daemon on specified port (default: 873)
3. Exposes port via `malai tcp --public`
4. Returns Malai ID for clients

Client:
1. Starts `malai tcp-bridge` to remote ID
2. Creates local proxy on specified port (default: 8873)
3. Runs rsync client through the bridge
4. Syncs files as if connecting to localhost

#### Future Transfer Methods

The architecture supports adding new methods like:
- **HTTP server**: Simple file serving with web interface
- **WebDAV**: Bidirectional file sync with standard clients
- **Custom protocols**: Any TCP-based protocol can be tunneled

## Entrypoint Router

The `entrypoint.sh` script routes requests to the appropriate transfer method:

```bash
transfer-p2p <METHOD> <MODE> [ARGS...]
```

- `METHOD`: Transfer method (rsync, http, etc.)
- `MODE`: server or client
- `ARGS`: Method-specific arguments

### Adding a New Transfer Method

1. Add package dependencies to Dockerfile if needed
2. Implement handler functions in entrypoint.sh:
   - `run_<method>_server()`: Server-side logic
   - `run_<method>_client()`: Client-side logic
3. Add case branch in main router
4. Update help text
5. Document in README.md and CLAUDE.md

Example structure:
```bash
run_http_server() {
    local port="${1:-8080}"

    # Start HTTP server in background
    python3 -m http.server "$port" --directory /data &
    SERVER_PID=$!

    # Expose via Malai
    malai tcp "$port" --public > /tmp/malai.log 2>&1 &

    # Extract and display Malai ID
    # ... (similar to rsync implementation)
}

run_http_client() {
    local malai_id="$1"
    local bridge_port="${2:-8080}"

    # Start Malai bridge
    malai tcp-bridge "$malai_id" "$bridge_port" &

    # Download files via local proxy
    wget -r "http://localhost:$bridge_port/"
}
```

## Data Flow

### Server Side
```
Local Files → Protocol Daemon → Malai TCP Tunnel → P2P Network
    /data         (rsync, HTTP)     malai tcp        (Internet)
```

### Client Side
```
P2P Network → Malai TCP Bridge → Local Proxy → Protocol Client → Local Files
(Internet)    malai tcp-bridge     localhost      (rsync, wget)     /local
```

## Environment Variables

Common:
- `QUIET`: Machine-readable output (default: false)

Method-specific:
- `RSYNC_PORT`: Server rsync port (default: 873)
- `BRIDGE_PORT`: Client bridge port (default: 8873)

## Security Considerations

- Malai handles encryption and authentication
- No credentials stored in container
- Each Malai instance generates unique identity
- P2P connections are ephemeral
- Server exposes only what's mounted at `/data`

## Future Enhancements

1. **Multi-protocol support**: Run multiple transfer methods simultaneously
2. **Authentication layer**: Add optional password/key authentication
3. **Compression**: Protocol-specific compression options
4. **Progress tracking**: Unified progress reporting across methods
5. **Discovery service**: Find available servers by tag/name
