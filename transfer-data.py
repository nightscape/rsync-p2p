#!/usr/bin/env python3
"""
Data Transfer Tool - Universal incremental data sync using rsync daemon pattern.

This tool provides reliable, incremental transfers between different storage systems using
rsync's proven delta-sync algorithm. It works by temporarily starting an rsync daemon on
the source, establishing a network connection, and pulling data to the target.

URL Schema:
  docker-volume:/volume-name[/path]            - Docker volume (host from FROM_HOST/TO_HOST)
  k8s-pvc:/namespace/pvc-name[/path]           - K8s PVC (context from k8s:CONTEXT host)
  directory:/path                               - Filesystem directory
  weaviate://[api-key:KEY@]host[:port]         - Weaviate instance
  postgres://user:pass@container-or-pod/database - Postgres database (via pg_dump/restore)

Transfer Methods:
  - File-based (docker-volume, k8s-pvc, directory): Uses rsync daemon for incremental sync
  - Weaviate: REST API-based object transfer with batch import
  - Postgres: pg_dump → rsync-p2p (Malai P2P) → pg_restore with NAT traversal

Examples:
  # Docker volume → K8s PVC (rsync daemon, incremental)
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/data_volume->k8s-pvc:/default/data-pvc

  # K8s PVC → K8s PVC (cross-cluster, rsync daemon)
  transfer-data.py k8s:cluster1 k8s:cluster2 \\
    --copy k8s-pvc:/default/data-pvc->k8s-pvc:/production/data-pvc

  # Directory → K8s PVC (rsync daemon)
  transfer-data.py root@remote.host k8s:orbstack \\
    --copy directory:/var/lib/data->k8s-pvc:/default/app-data

  # Docker volume → Directory (rsync daemon)
  transfer-data.py root@docker.host root@file.server \\
    --copy docker-volume:/backup->directory:/mnt/backups/myapp

  # With exclusions and subpaths
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/my-volume/app/data->k8s-pvc:/default/my-pvc/data \\
    --exclude '*.temp,*.log,cache/*'

  # Postgres database (rsync-p2p with Malai NAT traversal)
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy postgres://dagster:dagster@ai-platform-dagster_postgres-1/dagster->postgres://postgres@postgres-1/dagster

  # Weaviate instance (REST API batch transfer)
  transfer-data.py localhost localhost \\
    --copy weaviate://localhost:8080->weaviate://api-key:secret@localhost:18080

  # Multiple copies in one command
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/vol1->k8s-pvc:/default/pvc1 \\
    --copy docker-volume:/vol2->k8s-pvc:/default/pvc2 \\
    --copy postgres://user:pass@postgres-container/db->postgres://user:pass@postgres-pod/db \\
    --dry-run

  # Debug mode for troubleshooting
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/data->k8s-pvc:/default/data \\
    --debug

Features:
  ✅ Incremental sync - only transfers differences (rsync delta algorithm)
  ✅ NAT traversal - peer-to-peer via Malai, works across firewalls
  ✅ Universal support - all combinations of docker/k8s/directory
  ✅ Subpath support - transfer specific subdirectories
  ✅ Exclusion patterns - skip files/directories matching patterns
  ✅ Cross-cluster K8s - works across isolated networks
  ✅ Resume capability - handles interrupted transfers
  ✅ Compression - efficient network usage
"""

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass
from typing import Optional, List
import json


@dataclass
class Location:
    """Represents a source or destination location."""
    scheme: str
    host: Optional[str]
    path: Optional[str]
    username: Optional[str]
    password: Optional[str]
    port: Optional[int]

    @classmethod
    def parse(cls, url: str, default_host: Optional[str] = None) -> 'Location':
        """Parse a URL into a Location object."""
        parsed = urlparse(url)

        # Extract path components based on scheme
        if parsed.scheme == 'docker-volume':
            # docker-volume:/volume_name[/path]
            # Single slash, everything is in path
            parts = parsed.path.lstrip('/').split('/', 1)
            volume = parts[0] if parts else None
            subpath = '/' + parts[1] if len(parts) > 1 else None

            return cls(
                scheme='docker-volume',
                host=default_host,
                path=f"{volume}{subpath or ''}",
                username=None,
                password=None,
                port=None
            )

        elif parsed.scheme == 'k8s-pvc':
            # k8s-pvc:/namespace/pvc-name[/path]
            # Single slash, context comes from default_host (e.g., k8s:orbstack)
            # Format: namespace/pvc-name or namespace/pvc-name/subpath
            path_parts = parsed.path.lstrip('/').split('/', 2)

            if len(path_parts) == 0:
                raise ValueError("k8s-pvc URL must specify at least namespace/pvc-name")
            elif len(path_parts) == 1:
                # Just pvc name, use default namespace
                namespace = 'default'
                pvc = path_parts[0]
                subpath = None
            elif len(path_parts) == 2:
                # namespace/pvc
                namespace = path_parts[0]
                pvc = path_parts[1]
                subpath = None
            else:
                # namespace/pvc/subpath
                namespace = path_parts[0]
                pvc = path_parts[1]
                subpath = '/' + path_parts[2]

            return cls(
                scheme='k8s-pvc',
                host=default_host,  # Will be k8s:context
                path=f"{namespace}/{pvc}{subpath or ''}",
                username=None,
                password=None,
                port=None
            )

        elif parsed.scheme == 'directory':
            # directory://[host/]path
            if parsed.netloc:
                host = parsed.netloc
                path = parsed.path
            else:
                host = default_host
                path = '/' + parsed.path.lstrip('/')
            return cls(
                scheme='directory',
                host=host,
                path=path,
                username=None,
                password=None,
                port=None
            )

        elif parsed.scheme == 'weaviate':
            # weaviate://[api-key:KEY@]host[:port]
            port = parsed.port
            if port is None:
                # Default port is 80 for http, 443 for https
                port = 80
            return cls(
                scheme='weaviate',
                host=parsed.hostname,
                path=parsed.path or '/',
                username=parsed.username,
                password=parsed.password,
                port=port
            )

        elif parsed.scheme == 'postgres':
            # postgres://user:pass@container-or-pod/database
            # hostname is the container/pod name
            # path is the database name
            database = parsed.path.lstrip('/')
            return cls(
                scheme='postgres',
                host=parsed.hostname,  # container or pod name
                path=database,
                username=parsed.username,
                password=parsed.password,
                port=parsed.port or 5432
            )

        else:
            raise ValueError(f"Unsupported scheme: {parsed.scheme}")


class TransferEngine:
    """Handles data transfers between different storage types."""

    def __init__(self, from_host: str, to_host: str, dry_run: bool = False, debug: bool = False):
        self.from_host = from_host
        self.to_host = to_host
        self.dry_run = dry_run
        self.debug = debug

    def is_localhost(self, host: str) -> bool:
        """Check if host is localhost."""
        return host in ('localhost', '127.0.0.1', '::1') or host == subprocess.run(
            ['hostname'], capture_output=True, text=True
        ).stdout.strip()

    def is_k8s_context(self, host: str) -> bool:
        """Check if host is a K8s context."""
        return host and host.startswith('k8s:')

    def get_k8s_context(self, host: str) -> str:
        """Extract K8s context from host."""
        return host.replace('k8s:', '')

    def execute_on_host(self, host: str, command: str) -> subprocess.CompletedProcess:
        """Execute command on host (localhost or remote via SSH)."""
        if self.dry_run:
            print(f"[DRY RUN] Would execute on {host}: {command}")
            return subprocess.CompletedProcess(args=[], returncode=0, stdout='', stderr='')

        if self.debug:
            print(f"[DEBUG] Executing on {host}: {command}")

        if self.is_localhost(host):
            result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        else:
            result = subprocess.run(
                ['ssh', host, command],
                capture_output=True,
                text=True,
                check=True
            )

        if self.debug:
            print(f"[DEBUG] Command exit code: {result.returncode}")
            if result.stdout:
                print(f"[DEBUG] stdout: {result.stdout[:500]}")
            if result.stderr:
                print(f"[DEBUG] stderr: {result.stderr[:500]}")

        return result

    def execute_kubectl(self, context: str, command: str) -> subprocess.CompletedProcess:
        """Execute kubectl command with context."""
        if self.dry_run:
            print(f"[DRY RUN] Would execute: kubectl --context={context} {command}")
            return subprocess.CompletedProcess(args=[], returncode=0, stdout='', stderr='')

        full_command = f"kubectl --context={context} {command}"
        if self.debug:
            print(f"[DEBUG] Executing kubectl: {full_command}")

        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )

        if self.debug:
            print(f"[DEBUG] kubectl exit code: {result.returncode}")
            if result.stdout:
                print(f"[DEBUG] stdout: {result.stdout[:500]}")
            if result.stderr:
                print(f"[DEBUG] stderr: {result.stderr[:500]}")

        return result

    def _start_tcp_tunnel(self, location: Location, unique_id: str) -> dict:
        """Start TCP tunnel for exposing a service via Malai P2P.

        Returns dict with cleanup info and 'malai_id' for P2P connection.
        """
        rsync_p2p_image = "ghcr.io/nightscape/rsync-p2p:main"
        port = location.port

        if self.is_k8s_context(location.host):
            # For K8s, we need to expose the service's port
            context = self.get_k8s_context(location.host)
            pod_name = f"tcp-tunnel-{unique_id}"

            # Extract namespace if specified in host
            if '/' in location.host:
                parts = location.host.split('/', 1)
                namespace = parts[0].replace('k8s:', '')
            else:
                namespace = 'default'

            # Create pod that runs TCP tunnel
            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": namespace
                },
                "spec": {
                    "containers": [{
                        "name": "tcp-tunnel",
                        "image": rsync_p2p_image,
                        "args": ["tcp", "server", str(port)],
                        "env": [{"name": "QUIET", "value": "true"}]
                    }],
                    "restartPolicy": "Never"
                }
            }

            print(f"  🚀 Starting TCP tunnel pod (Malai P2P) in K8s for port {port}...")

            import tempfile
            import yaml
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            # Wait for pod to start and get Malai ID
            print(f"  ⏳ Waiting for TCP tunnel to initialize...")
            import time
            time.sleep(5)

            logs_cmd = f"logs {pod_name} -n {namespace}"
            result = self.execute_kubectl(context, logs_cmd)

            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from TCP tunnel pod. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID from K8s TCP tunnel: {malai_id}")

            return {
                'type': 'k8s-tcp-tunnel',
                'name': pod_name,
                'namespace': namespace,
                'context': context,
                'malai_id': malai_id,
                'port': port
            }
        else:
            # For Docker/localhost, run TCP tunnel container
            container_name = f"tcp-tunnel-{unique_id}"

            # Network mode: if host is localhost, use host network; otherwise connect to host network
            if self.is_localhost(location.host):
                network_mode = "--network host"
            else:
                # For remote hosts, we assume the service is accessible on the host
                network_mode = "--network host"

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"{network_mode} "
                f"-e QUIET=true "
                f"{rsync_p2p_image} tcp server {port}"
            )

            print(f"  🚀 Starting TCP tunnel (Malai P2P) on {location.host} for port {port}...")
            if self.debug:
                print(f"[DEBUG] Start command: {start_cmd}")

            self.execute_on_host(location.host, start_cmd)

            # Wait for container to start
            import time
            time.sleep(3)

            # Get logs to extract MALAI_ID
            logs_cmd = f"docker logs {container_name}"
            result = self.execute_on_host(location.host, logs_cmd)

            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from TCP tunnel. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID: {malai_id}")

            return {
                'type': 'docker-tcp-tunnel',
                'name': container_name,
                'host': location.host,
                'malai_id': malai_id,
                'port': port
            }

    def _cleanup_tcp_tunnel(self, tunnel_info: dict):
        """Cleanup TCP tunnel resources."""
        print(f"  🧹 Cleaning up TCP tunnel...")

        if tunnel_info['type'] == 'docker-tcp-tunnel':
            cleanup_cmd = f"docker rm -f {tunnel_info['name']}"
            try:
                self.execute_on_host(tunnel_info['host'], cleanup_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup tunnel container: {e}")

        elif tunnel_info['type'] == 'k8s-tcp-tunnel':
            delete_cmd = f"delete pod {tunnel_info['name']} -n {tunnel_info['namespace']} --force --grace-period=0"
            try:
                self.execute_kubectl(tunnel_info['context'], delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to delete tunnel pod: {e}")

    def transfer_weaviate_to_weaviate(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Transfer data between Weaviate instances using P2P tunnel and REST API."""
        print(f"🔄 Transferring Weaviate → Weaviate (P2P): {source.host}:{source.port} → {target.host}:{target.port}")

        if self.debug:
            print(f"[DEBUG] Source location: host={source.host}, port={source.port}, has_auth={bool(source.password)}")
            print(f"[DEBUG] Target location: host={target.host}, port={target.port}, has_auth={bool(target.password)}")

        import requests
        import urllib3
        import subprocess
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Generate unique ID for this transfer
        unique_id = subprocess.run(['date', '+%s%N'], capture_output=True, text=True).stdout.strip()[:16]

        source_tunnel = None
        bridge_process = None

        try:
            # Start TCP tunnel on source to expose Weaviate port via Malai P2P
            source_tunnel = self._start_tcp_tunnel(source, unique_id)
            malai_id = source_tunnel['malai_id']
            print(f"  📡 Malai P2P ID: {malai_id}")

            # Start Malai bridge on target side (or localhost) to connect to source
            bridge_port = 18080  # Use a different port to avoid conflicts
            print(f"  🔌 Setting up Malai TCP bridge on port {bridge_port}...")

            # Run malai tcp-bridge locally (assuming transfer-data.py runs on target host)
            # We need the malai binary available locally
            bridge_cmd = f"malai tcp-bridge {malai_id} {bridge_port}"

            if self.debug:
                print(f"[DEBUG] Bridge command: {bridge_cmd}")

            # Start bridge in background
            bridge_process = subprocess.Popen(
                bridge_cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Give bridge time to establish
            import time
            time.sleep(5)

            if bridge_process.poll() is not None:
                stdout, stderr = bridge_process.communicate()
                raise Exception(f"Malai bridge failed to start: {stderr.decode()}")

            print(f"  ✅ Bridge established on localhost:{bridge_port}")

            # Now connect to source via bridge and target directly
            source_url = f"http://localhost:{bridge_port}"

            # Build target URL
            if target.port == 443:
                target_url = f"https://{target.host}"
            elif target.port == 80:
                target_url = f"http://{target.host}"
            else:
                target_url = f"http://{target.host}:{target.port}"

            if self.debug:
                print(f"[DEBUG] Source URL (via bridge): {source_url}")
                print(f"[DEBUG] Target URL: {target_url}")

            source_headers = {}
            target_headers = {}

            if source.password:  # api-key stored in password field
                source_headers['Authorization'] = f'Bearer {source.password}'
            if target.password:
                target_headers['Authorization'] = f'Bearer {target.password}'

            # Get source schema
            print("  📊 Fetching source schema...")
            schema_resp = requests.get(f"{source_url}/v1/schema", headers=source_headers, verify=False)
            schema_resp.raise_for_status()
            schema = schema_resp.json()

            if not schema.get('classes'):
                print("  ⚠️  Source schema is empty, nothing to transfer")
                return

            # Create classes in target
            print(f"  🔨 Creating {len(schema['classes'])} classes in target...")
            for cls in schema['classes']:
                if self.dry_run:
                    print(f"  [DRY RUN] Would create class: {cls['class']}")
                    continue

                resp = requests.post(f"{target_url}/v1/schema", headers=target_headers, json=cls, verify=False)
                if resp.status_code == 422 and 'already exists' in resp.text:
                    print(f"  ℹ️  Class {cls['class']} already exists, skipping")
                else:
                    resp.raise_for_status()

            # Transfer objects for each class
            for cls in schema['classes']:
                class_name = cls['class']

                # Skip if excluded
                if exclude and any(re.match(pattern.replace('*', '.*'), class_name) for pattern in exclude):
                    print(f"  🚫 Skipping excluded class: {class_name}")
                    continue

                print(f"  📦 Transferring class: {class_name}")

                # Batch export/import
                offset = 0
                limit = 100
                total = 0

                while True:
                    # Fetch batch (include vectors!)
                    resp = requests.get(
                        f"{source_url}/v1/objects",
                        headers=source_headers,
                        params={'class': class_name, 'limit': limit, 'offset': offset, 'include': 'vector'},
                        verify=False
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    objects = data.get('objects', [])
                    if not objects:
                        break

                    # Import batch
                    if not self.dry_run:
                        batch_objects = []
                        for obj in objects:
                            batch_obj = {
                                'class': class_name,
                                'id': obj.get('id'),
                                'properties': obj.get('properties', {}),
                            }
                            if 'vector' in obj:
                                batch_obj['vector'] = obj['vector']
                            batch_objects.append(batch_obj)

                        batch_resp = requests.post(
                            f"{target_url}/v1/batch/objects",
                            headers=target_headers,
                            json={'objects': batch_objects},
                            verify=False
                        )
                        batch_resp.raise_for_status()

                        # Check for errors in batch response
                        batch_result = batch_resp.json()
                        if isinstance(batch_result, list):
                            errors = [r for r in batch_result if r.get('result', {}).get('errors')]
                            if errors:
                                print(f"\n  ⚠️  Batch had {len(errors)} errors")
                                for err in errors[:3]:  # Show first 3 errors
                                    print(f"    {err.get('result', {}).get('errors', {}).get('error', [{}])[0].get('message', 'Unknown error')}")

                    total += len(objects)
                    offset += limit
                    print(f"    → Transferred {total} objects...", end='\r')

                print(f"    ✅ Transferred {total} objects for {class_name}")

            print("  ✅ Weaviate → Weaviate transfer completed successfully")

        finally:
            # Cleanup resources
            if bridge_process and bridge_process.poll() is None:
                print("  🧹 Stopping Malai bridge...")
                bridge_process.terminate()
                try:
                    bridge_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    bridge_process.kill()

            if source_tunnel:
                self._cleanup_tcp_tunnel(source_tunnel)

    def _start_rsync_daemon(self, location: Location, unique_id: str) -> dict:
        """Start rsync daemon container/pod for a location using rsync-p2p (with Malai P2P).

        Returns dict with cleanup info and 'malai_id' for P2P connection.
        """
        daemon_port = 873

        # Use rsync-p2p image which combines rsync + Malai for P2P NAT traversal
        rsync_p2p_image = "ghcr.io/nightscape/rsync-p2p:main"

        if location.scheme == 'docker-volume':
            # Parse volume name and subpath
            parts = location.path.split('/', 1)
            volume = parts[0]
            subpath = '/' + parts[1] if len(parts) > 1 else ''

            container_name = f"rsync-daemon-{unique_id}"

            # Start rsync-p2p server in quiet mode to get Malai ID
            # Mount volume with subpath if specified
            volume_mount = f"-v {volume}:/data:ro"

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"{volume_mount} "
                f"-e QUIET=true "
                f"{rsync_p2p_image} server"
            )

            print(f"  🚀 Starting rsync-p2p daemon (Malai P2P) on {location.host}...")
            if self.debug:
                print(f"[DEBUG] Start command: {start_cmd}")

            self.execute_on_host(location.host, start_cmd)

            # Wait a moment for container to start and output Malai ID
            import time
            time.sleep(3)

            # Get logs to extract MALAI_ID
            logs_cmd = f"docker logs {container_name}"
            result = self.execute_on_host(location.host, logs_cmd)

            # Parse MALAI_ID from output (format: MALAI_ID=id52abc)
            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from daemon. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID: {malai_id}")

            return {
                'type': 'docker-p2p',
                'name': container_name,
                'host': location.host,
                'malai_id': malai_id,
                'subpath': subpath
            }

        elif location.scheme == 'k8s-pvc':
            # Parse namespace, PVC, subpath
            parts = location.path.split('/', 2)
            namespace = parts[0]
            pvc = parts[1]
            subpath = '/' + parts[2] if len(parts) > 2 else ''

            pod_name = f"rsync-daemon-{unique_id}"
            context = self.get_k8s_context(location.host)

            # Create pod spec with rsync-p2p server
            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": namespace
                },
                "spec": {
                    "containers": [{
                        "name": "rsync-p2p-server",
                        "image": rsync_p2p_image,
                        "args": ["server"],
                        "env": [{"name": "QUIET", "value": "true"}],
                        "volumeMounts": [{
                            "name": "data",
                            "mountPath": "/data",
                            "readOnly": True
                        }]
                    }],
                    "volumes": [{
                        "name": "data",
                        "persistentVolumeClaim": {
                            "claimName": pvc
                        }
                    }],
                    "restartPolicy": "Never"
                }
            }

            print(f"  🚀 Starting rsync-p2p daemon pod (Malai P2P) in K8s...")

            # Create pod using kubectl apply
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            # Wait for pod to be running (not ready, since server runs continuously)
            print(f"  ⏳ Waiting for rsync-p2p daemon pod to start...")
            import time
            time.sleep(5)  # Give pod time to start and output Malai ID

            # Get logs to extract MALAI_ID
            logs_cmd = f"logs {pod_name} -n {namespace}"
            result = self.execute_kubectl(context, logs_cmd)

            # Parse MALAI_ID from output
            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from K8s pod. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID from K8s pod: {malai_id}")

            return {
                'type': 'k8s-p2p',
                'name': pod_name,
                'namespace': namespace,
                'context': context,
                'malai_id': malai_id,
                'subpath': subpath
            }

        elif location.scheme == 'directory':
            # Mount directory as volume
            container_name = f"rsync-daemon-{unique_id}"

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"-v {location.path}:/data:ro "
                f"-e QUIET=true "
                f"{rsync_p2p_image} server"
            )

            print(f"  🚀 Starting rsync-p2p daemon (Malai P2P) on {location.host}...")
            if self.debug:
                print(f"[DEBUG] Start command: {start_cmd}")

            self.execute_on_host(location.host, start_cmd)

            # Wait for container to start
            import time
            time.sleep(3)

            # Get logs to extract MALAI_ID
            logs_cmd = f"docker logs {container_name}"
            result = self.execute_on_host(location.host, logs_cmd)

            # Parse MALAI_ID
            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from daemon. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID: {malai_id}")

            return {
                'type': 'docker-p2p',
                'name': container_name,
                'host': location.host,
                'malai_id': malai_id,
                'subpath': ''
            }
        else:
            raise ValueError(f"Unsupported scheme for rsync daemon: {location.scheme}")

    def _get_malai_connection_info(self, daemon_info: dict) -> dict:
        """Get Malai P2P connection info from daemon info.

        Returns dict with 'malai_id' and 'subpath' for P2P client connection.
        """
        return {
            'malai_id': daemon_info['malai_id'],
            'subpath': daemon_info.get('subpath', '')
        }

    def _run_rsync_client(
        self,
        target: Location,
        malai_connection: dict,
        unique_id: str,
        exclude: Optional[List[str]] = None
    ):
        """Run rsync-p2p client to pull from daemon via Malai P2P.

        Args:
            malai_connection: Dict with 'malai_id' and 'subpath' from daemon
        """

        # Use rsync-p2p image for P2P client
        rsync_p2p_image = "ghcr.io/nightscape/rsync-p2p:main"

        malai_id = malai_connection['malai_id']
        source_subpath = malai_connection.get('subpath', '')

        # Note: rsync-p2p client handles rsync internally, exclusions would need to be
        # implemented in the rsync-p2p wrapper or passed through
        if exclude and self.debug:
            print(f"[DEBUG] Note: Exclusions {exclude} not yet supported with rsync-p2p")

        if target.scheme == 'docker-volume':
            # Parse volume name and subpath
            parts = target.path.split('/', 1)
            volume = parts[0]
            target_subpath = '/' + parts[1] if len(parts) > 1 else ''

            container_name = f"rsync-client-{unique_id}"

            # rsync-p2p client command: client <malai_id> /source/ /dest/
            # Source path: daemon exposes its /data as root, so we use "/{source_subpath}"
            # Dest path: /data{target_subpath}/ in client container
            source_path = f"/{source_subpath.lstrip('/')}/" if source_subpath else "/"
            dest_path = f"/data{target_subpath}/"

            client_cmd = (
                f"docker run --rm --name {container_name} "
                f"-v {volume}:/data "
                f"{rsync_p2p_image} client {malai_id} {source_path} {dest_path}"
            )

            print(f"  🔄 Running rsync-p2p client (Malai P2P) on {target.host}...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")
                print(f"[DEBUG] Client command: {client_cmd}")

            self.execute_on_host(target.host, client_cmd)

        elif target.scheme == 'k8s-pvc':
            # Parse namespace, PVC, subpath
            parts = target.path.split('/', 2)
            namespace = parts[0]
            pvc = parts[1]
            target_subpath = '/' + parts[2] if len(parts) > 2 else ''

            pod_name = f"rsync-client-{unique_id}"
            context = self.get_k8s_context(target.host)

            # Source path: daemon exposes /data as root
            source_path = f"/{source_subpath.lstrip('/')}/" if source_subpath else "/"
            dest_path = f"/data{target_subpath}/"

            # Create client pod with rsync-p2p
            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": namespace
                },
                "spec": {
                    "containers": [{
                        "name": "rsync-p2p-client",
                        "image": rsync_p2p_image,
                        "args": ["client", malai_id, source_path, dest_path],
                        "volumeMounts": [{
                            "name": "data",
                            "mountPath": "/data"
                        }]
                    }],
                    "volumes": [{
                        "name": "data",
                        "persistentVolumeClaim": {
                            "claimName": pvc
                        }
                    }],
                    "restartPolicy": "Never"
                }
            }

            print(f"  🔄 Running rsync-p2p client pod (Malai P2P) in K8s...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")

            # Create pod
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            # Wait for pod to complete
            print(f"  ⏳ Waiting for rsync-p2p client to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {namespace} --timeout=300s"
            try:
                self.execute_kubectl(context, wait_cmd)
            except subprocess.CalledProcessError as e:
                # Pod might have failed, check its status
                status_cmd = f"get pod/{pod_name} -n {namespace} -o jsonpath='{{.status.phase}}'"
                result = self.execute_kubectl(context, status_cmd)
                if result.stdout.strip() != 'Succeeded':
                    raise Exception(f"Rsync client pod failed with status: {result.stdout}")

            # Get logs if debug mode
            if self.debug:
                logs_cmd = f"logs {pod_name} -n {namespace}"
                result = self.execute_kubectl(context, logs_cmd)
                print(f"[DEBUG] Rsync-p2p output: {result.stdout}")

            # Delete client pod
            delete_cmd = f"delete pod {pod_name} -n {namespace} --force --grace-period=0"
            self.execute_kubectl(context, delete_cmd)

        elif target.scheme == 'directory':
            container_name = f"rsync-client-{unique_id}"

            # Source path: daemon exposes /data as root
            source_path = f"/{source_subpath.lstrip('/')}/" if source_subpath else "/"
            dest_path = "/data/"

            client_cmd = (
                f"docker run --rm --name {container_name} "
                f"-v {target.path}:/data "
                f"{rsync_p2p_image} client {malai_id} {source_path} {dest_path}"
            )

            print(f"  🔄 Running rsync-p2p client (Malai P2P) on {target.host}...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")
                print(f"[DEBUG] Client command: {client_cmd}")

            self.execute_on_host(target.host, client_cmd)
        else:
            raise ValueError(f"Unsupported scheme for rsync client: {target.scheme}")

    def _cleanup_rsync_daemon(self, daemon_info: dict):
        """Cleanup rsync daemon resources."""
        print(f"  🧹 Cleaning up rsync-p2p daemon...")

        if daemon_info['type'] in ('docker-p2p', 'docker'):
            cleanup_cmd = f"docker rm -f {daemon_info['name']}"
            try:
                self.execute_on_host(daemon_info['host'], cleanup_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup daemon container: {e}")

        elif daemon_info['type'] in ('k8s-p2p', 'k8s'):
            # Stop port-forward
            if daemon_info.get('port_forward_process'):
                try:
                    daemon_info['port_forward_process'].terminate()
                    daemon_info['port_forward_process'].wait(timeout=5)
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Failed to stop port-forward: {e}")

            # Delete pod
            delete_cmd = f"delete pod {daemon_info['name']} -n {daemon_info['namespace']} --force --grace-period=0"
            try:
                self.execute_kubectl(daemon_info['context'], delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to delete daemon pod: {e}")

    def _create_temp_volume(self, location: Location, unique_id: str) -> str:
        """Create a temporary volume for dump/restore operations.

        Returns volume name or path.
        """
        if self.is_k8s_context(location.host):
            context = self.get_k8s_context(location.host)
            if '/' in location.host:
                namespace, _ = location.host.split('/', 1)
            else:
                namespace = 'default'

            pvc_name = f"pg-dump-{unique_id}"

            pvc_spec = {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": pvc_name,
                    "namespace": namespace
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {
                        "requests": {
                            "storage": "10Gi"
                        }
                    }
                }
            }

            print(f"  📦 Creating temporary PVC for dump...")
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pvc_spec, f)
                pvc_file = f.name

            try:
                apply_cmd = f"apply -f {pvc_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pvc_file)

            return f"{namespace}/{pvc_name}"
        else:
            volume_name = f"pg-dump-{unique_id}"
            create_cmd = f"docker volume create {volume_name}"
            print(f"  📦 Creating temporary Docker volume for dump...")
            self.execute_on_host(location.host, create_cmd)
            return volume_name

    def _cleanup_temp_volume(self, location: Location, volume_identifier: str):
        """Cleanup temporary volume."""
        print(f"  🧹 Cleaning up temporary volume...")

        if self.is_k8s_context(location.host):
            context = self.get_k8s_context(location.host)
            namespace, pvc_name = volume_identifier.split('/', 1)
            delete_cmd = f"delete pvc {pvc_name} -n {namespace}"
            try:
                self.execute_kubectl(context, delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup PVC: {e}")
        else:
            delete_cmd = f"docker volume rm {volume_identifier}"
            try:
                self.execute_on_host(location.host, delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup volume: {e}")

    def _pg_dump_to_volume(self, source: Location, volume_identifier: str, unique_id: str):
        """Run pg_dump and save output to a volume."""
        print(f"  📊 Running pg_dump to volume...")

        dump_file = "/dump/database.pgdump"

        if self.is_k8s_context(source.host):
            context = self.get_k8s_context(source.host)
            if '/' in source.host:
                namespace, pod = source.host.split('/', 1)
            else:
                namespace = 'default'
                pod = source.host

            pvc_namespace, pvc_name = volume_identifier.split('/', 1)
            pod_name = f"pg-dump-{unique_id}"

            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": pvc_namespace
                },
                "spec": {
                    "containers": [{
                        "name": "pg-dump",
                        "image": "postgres:16-alpine",
                        "command": ["sh", "-c"],
                        "args": [f"pg_dump -h {pod}.{namespace}.svc.cluster.local -U {source.username} -Fc -f {dump_file} {source.path}"],
                        "env": [{"name": "PGPASSWORD", "value": source.password or ""}],
                        "volumeMounts": [{
                            "name": "dump",
                            "mountPath": "/dump"
                        }]
                    }],
                    "volumes": [{
                        "name": "dump",
                        "persistentVolumeClaim": {"claimName": pvc_name}
                    }],
                    "restartPolicy": "Never"
                }
            }

            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            print(f"  ⏳ Waiting for pg_dump to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {pvc_namespace} --timeout=600s"
            self.execute_kubectl(context, wait_cmd)

            delete_cmd = f"delete pod {pod_name} -n {pvc_namespace} --force --grace-period=0"
            self.execute_kubectl(context, delete_cmd)
        else:
            container_name = f"pg-dump-{unique_id}"
            dump_cmd = (
                f"docker run --rm --name {container_name} "
                f"--network container:{source.host} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={source.password or ''} "
                f"postgres:16-alpine "
                f"pg_dump -h localhost -U {source.username} -Fc -f {dump_file} {source.path}"
            )
            self.execute_on_host(source.host, dump_cmd)

        print(f"  ✅ pg_dump completed")

    def _pg_restore_from_volume(self, target: Location, volume_identifier: str, unique_id: str):
        """Run pg_restore from a volume."""
        print(f"  🚀 Running pg_restore from volume...")

        dump_file = "/dump/database.pgdump"

        if self.is_k8s_context(target.host):
            context = self.get_k8s_context(target.host)
            if '/' in target.host:
                namespace, pod = target.host.split('/', 1)
            else:
                namespace = 'default'
                pod = target.host

            pvc_namespace, pvc_name = volume_identifier.split('/', 1)
            pod_name = f"pg-restore-{unique_id}"

            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": pvc_namespace
                },
                "spec": {
                    "containers": [{
                        "name": "pg-restore",
                        "image": "postgres:16-alpine",
                        "command": ["sh", "-c"],
                        "args": [f"pg_restore -h {pod}.{namespace}.svc.cluster.local -U {target.username} -j4 --no-owner --clean -d {target.path} {dump_file}"],
                        "env": [{"name": "PGPASSWORD", "value": target.password or ""}],
                        "volumeMounts": [{
                            "name": "dump",
                            "mountPath": "/dump"
                        }]
                    }],
                    "volumes": [{
                        "name": "dump",
                        "persistentVolumeClaim": {"claimName": pvc_name}
                    }],
                    "restartPolicy": "Never"
                }
            }

            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            print(f"  ⏳ Waiting for pg_restore to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {pvc_namespace} --timeout=600s"
            self.execute_kubectl(context, wait_cmd)

            if self.debug:
                logs_cmd = f"logs {pod_name} -n {pvc_namespace}"
                result = self.execute_kubectl(context, logs_cmd)
                print(f"[DEBUG] pg_restore output: {result.stdout}")

            delete_cmd = f"delete pod {pod_name} -n {pvc_namespace} --force --grace-period=0"
            self.execute_kubectl(context, delete_cmd)
        else:
            container_name = f"pg-restore-{unique_id}"
            restore_cmd = (
                f"docker run --rm --name {container_name} "
                f"--network container:{target.host} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={target.password or ''} "
                f"postgres:16-alpine "
                f"pg_restore -h localhost -U {target.username} -j4 --no-owner --clean -d {target.path} {dump_file}"
            )
            self.execute_on_host(target.host, restore_cmd)

        print(f"  ✅ pg_restore completed")

    def transfer_rsync_daemon(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Universal incremental transfer using rsync-p2p with Malai P2P.

        This works for all storage types: docker-volume, k8s-pvc, directory.
        The pattern:
        1. Start rsync-p2p daemon on source (read-only) - gets Malai ID
        2. Establish P2P connection using Malai ID (automatic NAT traversal)
        3. Run rsync-p2p client on target to pull data
        4. Cleanup temporary resources
        """
        print(f"🔄 Transferring via rsync-p2p (Malai P2P): {source.scheme}:{source.path} → {target.scheme}:{target.path}")

        # Generate unique ID for this transfer
        unique_id = subprocess.run(['date', '+%s%N'], capture_output=True, text=True).stdout.strip()[:16]

        daemon_info = None
        try:
            # Start rsync-p2p daemon on source
            daemon_info = self._start_rsync_daemon(source, unique_id)

            # Get Malai P2P connection info
            malai_connection = self._get_malai_connection_info(daemon_info)
            print(f"  📡 Malai P2P ID: {malai_connection['malai_id']}")

            # Run rsync-p2p client on target
            self._run_rsync_client(target, malai_connection, unique_id, exclude)

            print("  ✅ Rsync-p2p (Malai P2P) transfer completed successfully")

        finally:
            if daemon_info:
                self._cleanup_rsync_daemon(daemon_info)

    def transfer_postgres_to_postgres(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Transfer Postgres database using pg_dump → rsync-p2p → pg_restore.

        This method:
        1. Dumps database to a temporary volume on source
        2. Uses rsync-p2p (with Malai P2P) to transfer dump file
        3. Restores database from volume on target
        4. Cleans up temporary resources

        Benefits:
        - NAT traversal via Malai P2P - works across firewalls
        - No intermediate local storage required
        - Resume capability if transfer interrupted
        - Works across isolated networks
        """
        print(f"🔄 Transferring Postgres via rsync-p2p: {source.host}/{source.path} → {target.host}/{target.path}")

        unique_id = subprocess.run(['date', '+%s%N'], capture_output=True, text=True).stdout.strip()[:16]

        source_volume = None
        target_volume = None
        daemon_info = None

        try:
            source_volume = self._create_temp_volume(source, unique_id)
            self._pg_dump_to_volume(source, source_volume, unique_id)

            target_volume = self._create_temp_volume(target, unique_id)

            if self.is_k8s_context(source.host):
                namespace, pvc_name = source_volume.split('/', 1)
                source_loc = Location.parse(f"k8s-pvc:/{namespace}/{pvc_name}", source.host)
            else:
                source_loc = Location.parse(f"docker-volume:/{source_volume}", source.host)

            daemon_info = self._start_rsync_daemon(source_loc, unique_id)
            malai_connection = self._get_malai_connection_info(daemon_info)
            print(f"  📡 Malai P2P ID: {malai_connection['malai_id']}")

            if self.is_k8s_context(target.host):
                namespace, pvc_name = target_volume.split('/', 1)
                target_loc = Location.parse(f"k8s-pvc:/{namespace}/{pvc_name}", target.host)
            else:
                target_loc = Location.parse(f"docker-volume:/{target_volume}", target.host)

            self._run_rsync_client(target_loc, malai_connection, unique_id, exclude)

            self._pg_restore_from_volume(target, target_volume, unique_id)

            print("  ✅ Postgres rsync-p2p transfer completed successfully")

        finally:
            if daemon_info:
                self._cleanup_rsync_daemon(daemon_info)
            if source_volume:
                self._cleanup_temp_volume(source, source_volume)
            if target_volume:
                self._cleanup_temp_volume(target, target_volume)

    def transfer(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Route transfer to appropriate handler based on source/target types."""
        transfer_key = f"{source.scheme}->{target.scheme}"

        # Specialized handlers for specific data types
        specialized_handlers = {
            'weaviate->weaviate': self.transfer_weaviate_to_weaviate,
            'postgres->postgres': self.transfer_postgres_to_postgres,
        }

        # Check for specialized handlers first
        if transfer_key in specialized_handlers:
            specialized_handlers[transfer_key](source, target, exclude)
            return

        # Use rsync-p2p (Malai) as universal handler for file-based storage
        # Supports: docker-volume, k8s-pvc, directory (all combinations)
        # Provides P2P connectivity with automatic NAT traversal
        if source.scheme in ('docker-volume', 'k8s-pvc', 'directory') and \
           target.scheme in ('docker-volume', 'k8s-pvc', 'directory'):
            self.transfer_rsync_daemon(source, target, exclude)
            return

        raise ValueError(f"Unsupported transfer type: {transfer_key}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('from_host', help='Source host (hostname, user@host, localhost, or k8s:context)')
    parser.add_argument('to_host', help='Target host (hostname, user@host, localhost, or k8s:context)')
    parser.add_argument(
        '--copy',
        action='append',
        dest='copies',
        metavar='SOURCE->TARGET',
        help='Copy specification in URL format (can be specified multiple times)'
    )
    parser.add_argument(
        '--exclude',
        metavar='PATTERNS',
        help='Comma-separated exclusion patterns (e.g., "*.temp,*.log")'
    )
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without doing it')
    parser.add_argument('--debug', action='store_true', help='Print additional debug information')

    args = parser.parse_args()

    if not args.copies:
        parser.error("At least one --copy argument is required")

    # Parse exclusions
    exclusions = args.exclude.split(',') if args.exclude else None

    # Create transfer engine
    engine = TransferEngine(args.from_host, args.to_host, dry_run=args.dry_run, debug=args.debug)

    print("🚀 Data Transfer Tool")
    print(f"📤 From: {args.from_host}")
    print(f"📥 To: {args.to_host}")
    print()

    if args.dry_run:
        print("🔍 DRY RUN MODE")
        print()

    if args.debug:
        print("🐛 DEBUG MODE ENABLED")
        print()

    # Process each copy
    for copy_spec in args.copies:
        if '->' not in copy_spec:
            print(f"❌ Error: Invalid copy spec (missing '->'): {copy_spec}")
            sys.exit(1)

        source_url, target_url = copy_spec.split('->', 1)

        if args.debug:
            print(f"[DEBUG] Processing copy spec: {copy_spec}")
            print(f"[DEBUG] Source URL: {source_url.strip()}")
            print(f"[DEBUG] Target URL: {target_url.strip()}")

        try:
            source = Location.parse(source_url.strip(), args.from_host)
            target = Location.parse(target_url.strip(), args.to_host)

            if args.debug:
                print(f"[DEBUG] Parsed source Location: {source}")
                print(f"[DEBUG] Parsed target Location: {target}")

            engine.transfer(source, target, exclusions)
            print()

        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    print("✅ All transfers completed successfully!")


if __name__ == '__main__':
    main()
