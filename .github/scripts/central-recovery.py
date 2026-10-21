#!/usr/bin/env python3
"""Resume one reviewed Central deployment without a second upload."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.request
import uuid

BASE = 'https://central.sonatype.com/api/v1/publisher'


def save(path, state):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('x') as handle:
        json.dump(state, handle, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def request(endpoint, body=b'', content_type='application/json'):
    token = base64.b64encode((os.environ['CENTRAL_TOKEN_USERNAME'] + ':' +
                             os.environ['CENTRAL_TOKEN_PASSWORD']).encode()).decode()
    req = urllib.request.Request(BASE + endpoint, data=body, method='POST', headers={
        'Authorization': 'Bearer ' + token, 'Content-Type': content_type})
    # Refuse redirects: never forward authentication to another origin.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    with urllib.request.build_opener(NoRedirect).open(req, timeout=30) as response:
        return response.read()


def recover(bundle, receipt, state_path, allow_upload=False, publish=False,
            call=request, sleep=time.sleep, clock=time.monotonic, timeout=600, prepare_upload=False,
            approval_reference=None):
    bundle_bytes = bundle.read_bytes()
    receipt_bytes = receipt.read_bytes()
    identity = json.loads(receipt_bytes)
    bundle_hash = hashlib.sha256(bundle_bytes).hexdigest()
    if identity['bundle_sha256'] != bundle_hash:
        raise ValueError('Bundle differs from validation receipt')
    required = {'coordinate', 'entries', 'signer_fingerprint', 'bundle_sha256'}
    if not required <= identity.keys() or len(identity['entries']) != 24:
        raise ValueError('Incomplete offline validation receipt')
    receipt_hash = hashlib.sha256(receipt_bytes).hexdigest()
    expected = {'bundle_sha256': bundle_hash, 'receipt_sha256': receipt_hash,
                'coordinate': identity['coordinate']}
    if prepare_upload:
        if state_path.exists() or allow_upload or publish:
            raise ValueError('Prepare upload only once, without network flags')
        state = {**expected, 'phase': 'upload-prepared', 'deployment_id': None}
        if approval_reference:
            state['upload_approval_reference'] = approval_reference
        save(state_path, state)
        return state
    if state_path.exists():
        state = json.loads(state_path.read_text())
        if any(state.get(key) != value for key, value in expected.items()):
            raise ValueError('Saved deployment identity differs from reviewed input')
        if not state.get('deployment_id') and not (allow_upload and state.get('phase') == 'upload-prepared'):
            raise ValueError('Upload outcome unknown: inspect Central; never upload automatically again')
    else:
        if not allow_upload:
            raise ValueError('No saved deployment: explicit --allow-upload required')
        state = {**expected, 'phase': 'upload-prepared', 'deployment_id': None}
    if not state.get('deployment_id'):
        state['phase'] = 'upload-started'
        if approval_reference:
            state['upload_approval_reference'] = approval_reference
        # Durable intent prevents automatic re-upload after an ambiguous response.
        save(state_path, state)
        boundary = 'central-' + uuid.uuid4().hex
        body = (f'--{boundary}\r\nContent-Disposition: form-data; name="bundle"; filename="central.zip"\r\n'
                'Content-Type: application/zip\r\n\r\n').encode() + bundle_bytes + f'\r\n--{boundary}--\r\n'.encode()
        deployment_id = call('/upload?publishingType=USER_MANAGED', body,
                             'multipart/form-data; boundary=' + boundary).decode().strip()
        if str(uuid.UUID(deployment_id)) != deployment_id:
            raise ValueError('Upload returned invalid deployment ID; inspect Central manually')
        state.update(deployment_id=deployment_id, phase='uploaded')
        save(state_path, state)  # Before the first status request.
    deployment_id = state['deployment_id']
    if str(uuid.UUID(deployment_id)) != deployment_id:
        raise ValueError('Invalid saved deployment ID')
    deadline = clock() + timeout
    while clock() < deadline:
        status = json.loads(call('/status?id=' + deployment_id))
        if status.get('deploymentId') != deployment_id:
            raise ValueError('Status deployment ID mismatch')
        phase = status.get('deploymentState')
        if phase not in ('PENDING', 'VALIDATING', 'VALIDATED', 'PUBLISHING', 'PUBLISHED', 'FAILED'):
            raise ValueError('Unknown Central deployment state')
        state.update(phase=phase, last_status=status)
        save(state_path, state)
        if phase == 'FAILED':
            raise ValueError('Central rejected this deployment; preserve receipt and investigate')
        if phase == 'PUBLISHED':
            return state
        if phase == 'VALIDATED':
            if not publish:
                return state
            if state.get('publish_attempted'):
                raise ValueError('Publish outcome uncertain: inspect same deployment before retrying')
            state['publish_attempted'] = True
            if approval_reference:
                state['publish_approval_reference'] = approval_reference
            save(state_path, state)
            call('/deployment/' + deployment_id)
        sleep(5)
    raise TimeoutError('Polling deadline reached; resume using the same saved deployment')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--allow-upload', action='store_true')
    parser.add_argument('--publish', action='store_true')
    parser.add_argument('--prepare-upload', action='store_true')
    parser.add_argument('--approval-reference', help='Non-secret audit reference; never a substitute for actual approval')
    args = parser.parse_args()
    # Serialize access to the exact state file; stale locks require operator review.
    lock = args.state.with_suffix(args.state.suffix + '.lock')
    with lock.open('x') as handle:
        handle.write(str(os.getpid()))
    try:
        recover(args.bundle, args.receipt, args.state, args.allow_upload, args.publish,
                prepare_upload=args.prepare_upload, approval_reference=args.approval_reference)
    finally:
        lock.unlink()


if __name__ == '__main__':
    main()
