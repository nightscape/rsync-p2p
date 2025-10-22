#!/usr/bin/env python3
"""
Weaviate to Weaviate copying script for use in transfer-p2p container.
Runs on the target host and copies data from source (via P2P bridge) to local target.
"""
import sys
import requests
import urllib3
import time
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def wait_for_endpoint(url, name, timeout_seconds=90, poll_interval=5):
    """Poll an endpoint until it responds or timeout."""
    print(f"  ⏳ Waiting for {name} to be ready...", file=sys.stderr)
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            resp = requests.get(f"{url}/v1/meta", timeout=3, verify=False)
            if resp.status_code < 500:
                print(f"  ✅ {name} is ready", file=sys.stderr)
                return True
        except (requests.exceptions.ConnectionError,
               requests.exceptions.Timeout,
               requests.exceptions.ReadTimeout):
            pass
        time.sleep(poll_interval)
    return False

def copy_weaviate(source_url, target_url, source_auth=None, target_auth=None, exclude_patterns=None):
    """Copy all data from source Weaviate to target Weaviate."""
    print(f"🔄 Copying Weaviate data", file=sys.stderr)
    print(f"  📤 Source: {source_url}", file=sys.stderr)
    print(f"  📥 Target: {target_url}", file=sys.stderr)

    # Wait for both endpoints to be ready
    if not wait_for_endpoint(source_url, "Source"):
        raise Exception(f"Source Weaviate at {source_url} did not become ready")

    if not wait_for_endpoint(target_url, "Target"):
        raise Exception(f"Target Weaviate at {target_url} did not become ready")

    source_headers = {}
    target_headers = {}

    if source_auth:
        source_headers['Authorization'] = f'Bearer {source_auth}'
    if target_auth:
        target_headers['Authorization'] = f'Bearer {target_auth}'

    # Get source schema
    print("  📊 Fetching source schema...", file=sys.stderr)
    schema_resp = requests.get(f"{source_url}/v1/schema", headers=source_headers, verify=False)
    schema_resp.raise_for_status()
    schema = schema_resp.json()

    if not schema.get('classes'):
        print("  ⚠️  Source schema is empty, nothing to transfer", file=sys.stderr)
        return

    # Create classes in target
    print(f"  🔨 Creating {len(schema['classes'])} classes in target...", file=sys.stderr)
    for cls in schema['classes']:
        resp = requests.post(f"{target_url}/v1/schema", headers=target_headers, json=cls, verify=False)
        if resp.status_code == 422 and 'already exists' in resp.text:
            print(f"  ℹ️  Class {cls['class']} already exists, skipping", file=sys.stderr)
        else:
            resp.raise_for_status()

    # Transfer objects for each class
    for cls in schema['classes']:
        class_name = cls['class']

        # Skip if excluded
        if exclude_patterns:
            if any(re.match(pattern.replace('*', '.*'), class_name) for pattern in exclude_patterns):
                print(f"  🚫 Skipping excluded class: {class_name}", file=sys.stderr)
                continue

        print(f"  📦 Transferring class: {class_name}", file=sys.stderr)

        # Batch export/import using cursor-based pagination
        limit = 100
        total = 0
        after_id = None

        while True:
            # Fetch batch (include vectors!)
            params = {'class': class_name, 'limit': limit, 'include': 'vector'}
            if after_id:
                params['after'] = after_id

            resp = requests.get(
                f"{source_url}/v1/objects",
                headers=source_headers,
                params=params,
                verify=False
            )
            resp.raise_for_status()
            data = resp.json()

            objects = data.get('objects', [])
            if not objects:
                break

            # Set cursor for next iteration
            after_id = objects[-1].get('id')

            # Import batch
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
                    print(f"\n  ⚠️  Batch had {len(errors)} errors", file=sys.stderr)
                    for err in errors[:3]:
                        print(f"    {err.get('result', {}).get('errors', {}).get('error', [{}])[0].get('message', 'Unknown error')}", file=sys.stderr)

            total += len(objects)
            print(f"    → Transferred {total} objects...", end='\r', file=sys.stderr)

        print(f"    ✅ Transferred {total} objects for {class_name}              ", file=sys.stderr)

    print("  ✅ Weaviate copy completed successfully", file=sys.stderr)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Copy data from one Weaviate instance to another')
    parser.add_argument('--source-url', required=True, help='Source Weaviate URL (e.g., http://localhost:8080)')
    parser.add_argument('--target-url', required=True, help='Target Weaviate URL (e.g., http://localhost:8081)')
    parser.add_argument('--source-auth', help='Source API key (optional)')
    parser.add_argument('--target-auth', help='Target API key (optional)')
    parser.add_argument('--exclude', help='Comma-separated exclusion patterns')

    args = parser.parse_args()

    exclude_patterns = args.exclude.split(',') if args.exclude else None

    try:
        copy_weaviate(
            args.source_url,
            args.target_url,
            args.source_auth,
            args.target_auth,
            exclude_patterns
        )
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
