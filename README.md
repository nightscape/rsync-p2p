# rsync-p2p

Peer-to-peer rsync over NAT/firewall using Malai tunneling.

## Overview

This Docker container enables rsync file synchronization between machines behind NAT/firewalls without requiring port forwarding or VPN setup. It uses [Malai](https://malai.sh) to create secure tunnels for rsync daemon connections.

## Quick Start

### 1. Build the image

```bash
docker build -t rsync-p2p .
```

### 2. Run server (machine with files to share)

```bash
docker run -v /path/to/share:/data rsync-p2p server
```

This will output a Malai ID like:
```
Malai: Sharing port 873
Run malai tcp-bridge id52abc <some-port>
to connect to it from any machine.
```

### 3. Run client (machine receiving files)

```bash
docker run -v /local/path:/local rsync-p2p client id52abc /source/ /local/dest/
```

Replace `id52abc` with the actual ID from the server output.

## Usage

### Server Mode

```bash
docker run -v <host-dir>:/data rsync-p2p server [PORT] [--quiet]
```

- `<host-dir>`: Directory to share via rsync
- `PORT`: Optional, rsync port (default: 873)
- `--quiet`: Optional flag for machine-readable output

### Client Mode

```bash
docker run -v <host-dir>:/local rsync-p2p client <MALAI-ID> <SRC> <DEST>
```

- `<host-dir>`: Local directory for syncing
- `<MALAI-ID>`: ID from server output
- `<SRC>`: Source path on server (e.g., `/files/`)
- `<DEST>`: Destination path in container (e.g., `/local/backup/`)

## Examples

### Share entire directory

**Server:**
```bash
docker run -v /home/user/documents:/data rsync-p2p server
# Note the Malai ID from output
```

**Client:**
```bash
docker run -v /home/user/backup:/local rsync-p2p client id52abc / /local/docs/
```

### Sync specific subdirectory

**Client:**
```bash
docker run -v /home/user/backup:/local rsync-p2p client id52abc /photos/ /local/photos/
```

## Environment Variables

- `RSYNC_PORT`: Server rsync port (default: 873)
- `BRIDGE_PORT`: Client bridge port (default: 8873)
- `QUIET`: Set to `true` for machine-readable output (default: false)

```bash
# Custom ports example
docker run -e RSYNC_PORT=8873 -v /data:/data rsync-p2p server
docker run -e BRIDGE_PORT=9000 -v /local:/local rsync-p2p client id52abc /files/ /local/
```

### Quiet Mode

Quiet mode provides machine-readable output suitable for scripting and automation. It can be enabled via the `QUIET` environment variable or the `--quiet` flag.

**Human-friendly output (default):**
```bash
docker run -v /data:/data rsync-p2p server
```
Output includes status messages, progress indicators, and formatted instructions.

**Machine-readable output:**
```bash
docker run -e QUIET=true -v /data:/data rsync-p2p server
# or
docker run -v /data:/data rsync-p2p server --quiet
```
Output format:
```
MALAI_ID=id52abc
PORT=873
```

This output can be easily parsed in scripts:
```bash
eval $(docker run -e QUIET=true -v /data:/data rsync-p2p server)
echo "Server ID: $MALAI_ID"
```

## How It Works

1. **Server** runs rsync in daemon mode and exposes it via `malai tcp --public`
2. **Malai** creates a secure tunnel and generates a unique ID
3. **Client** uses `malai tcp-bridge` to create a local proxy to the remote rsync daemon
4. **rsync** syncs files through the tunnel as if it were a local connection

## Standalone Malai Installation

To install Malai on your host system without Docker:

```bash
./install.sh
malai --help
```

Supports macOS (arm64) and Linux (x86_64, aarch64).

## License

MIT
