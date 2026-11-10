#!/usr/bin/env python3
"""Upload one reviewed, unsigned X.Y.Z-SNAPSHOT to Central's snapshot repository, once.

The candidate is data: its four payloads are checked against the unsigned receipt
and uploaded byte for byte under one timestamped snapshot version, with the
checksums and Maven metadata a Maven deploy would write, then read back. Nothing
in the candidate is executed. The state file is written before the first request
and after the upload, so a failed run shows what it wrote.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

REPOSITORY = 'https://central.sonatype.com/repository/maven-snapshots/'
NS = {'p': 'http://maven.apache.org/POM/4.0.0'}
# Suffix of each payload after artifact-version, and its (classifier, extension).
PAYLOADS = {'.pom': ('', 'pom'), '.jar': ('', 'jar'),
            '-sources.jar': ('sources', 'jar'), '-javadoc.jar': ('javadoc', 'jar')}
CHECKSUMS = ('md5', 'sha1')


def save(path, state):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('x') as handle:
        json.dump(state, handle, indent=2)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def request(method, url, body=None):
    """One authenticated request to the snapshot repository; returns (status, bytes)."""
    if not url.startswith(REPOSITORY):
        raise ValueError('Requests go to the snapshot repository only')
    token = base64.b64encode((os.environ['CENTRAL_TOKEN_USERNAME'] + ':' +
                             os.environ['CENTRAL_TOKEN_PASSWORD']).encode()).decode()
    headers = {'Authorization': 'Basic ' + token}
    if body is not None:
        headers['Content-Type'] = 'application/octet-stream'
    req = urllib.request.Request(url, data=body, method=method, headers=headers)

    # Refuse redirects: never forward authentication to another origin.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, b''


def candidate_payloads(candidate, version, commit, run):
    """Check the unsigned candidate and return (group, artifact, {suffix: bytes})."""
    if not re.fullmatch(r'\d+\.\d+\.\d+-SNAPSHOT', version):
        raise ValueError('Snapshot upload requires an X.Y.Z-SNAPSHOT version')
    if not re.fullmatch(r'[a-f0-9]{40}', commit) or not re.fullmatch(r'[1-9][0-9]*', run):
        raise ValueError('Exact source commit and candidate run required')
    record = json.loads((candidate / 'provenance.json').read_text())
    receipt = json.loads((candidate / 'unsigned.json').read_text())
    if (record.get('source_commit') != commit or record.get('version') != version
            or receipt.get('source_commit') != commit or receipt.get('version') != version
            or receipt.get('candidate_run') != run
            or receipt.get('qualification') != 'UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE'):
        raise ValueError('Unsigned snapshot candidate identity mismatch')
    group, artifact = record.get('group', ''), record.get('artifact', '')
    if (not re.fullmatch(r'[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+', group)
            or not re.fullmatch(r'[A-Za-z0-9_-]+', artifact)):
        raise ValueError('Invalid snapshot coordinate')
    directory = candidate / 'unsigned'
    names = {artifact + '-' + version + suffix: suffix for suffix in PAYLOADS}
    present = {path.name for path in directory.iterdir()}
    if present != set(names) or set(receipt.get('payload_sha256', {})) != set(names):
        raise ValueError('Snapshot candidate must hold exactly its four payloads')
    payloads = {}
    for name, suffix in names.items():
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValueError('Snapshot payload must be a regular file')
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != receipt['payload_sha256'][name]:
            raise ValueError('Snapshot payload differs from the unsigned receipt: ' + name)
        payloads[suffix] = data
    pom = ET.fromstring(payloads['.pom'])
    if any(pom.findtext('p:' + key, namespaces=NS) != value
           for key, value in (('groupId', group), ('artifactId', artifact), ('version', version))):
        raise ValueError('Snapshot POM coordinate mismatch')
    return group, artifact, payloads


def next_build(metadata):
    """The build number after the one in existing version metadata (1 without any)."""
    if metadata is None:
        return 1
    number = ET.fromstring(metadata).findtext('versioning/snapshot/buildNumber')
    if not number or not re.fullmatch(r'[1-9][0-9]*', number):
        raise ValueError('Existing snapshot metadata has no build number; refusing to guess')
    return int(number) + 1


def version_metadata(group, artifact, version, timestamp, build, updated):
    base = version.removesuffix('-SNAPSHOT')
    value = f'{base}-{timestamp}-{build}'
    entries = ''.join(
        '<snapshotVersion>' + (f'<classifier>{classifier}</classifier>' if classifier else '')
        + f'<extension>{extension}</extension><value>{value}</value><updated>{updated}</updated>'
        '</snapshotVersion>'
        for classifier, extension in PAYLOADS.values())
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n<metadata modelVersion="1.1.0">'
            f'<groupId>{group}</groupId><artifactId>{artifact}</artifactId><version>{version}</version>'
            f'<versioning><snapshot><timestamp>{timestamp}</timestamp><buildNumber>{build}</buildNumber></snapshot>'
            f'<lastUpdated>{updated}</lastUpdated><snapshotVersions>{entries}</snapshotVersions></versioning>'
            '</metadata>\n').encode()


def artifact_metadata(existing, group, artifact, version, updated):
    versions = []
    if existing is not None:
        root = ET.fromstring(existing)
        if root.findtext('groupId') != group or root.findtext('artifactId') != artifact:
            raise ValueError('Existing artifact metadata belongs to another coordinate')
        versions = [element.text for element in root.findall('versioning/versions/version') if element.text]
    if version not in versions:
        versions.append(version)
    listed = ''.join(f'<version>{item}</version>' for item in versions)
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n<metadata>'
            f'<groupId>{group}</groupId><artifactId>{artifact}</artifactId>'
            f'<versioning><latest>{version}</latest><versions>{listed}</versions>'
            f'<lastUpdated>{updated}</lastUpdated></versioning></metadata>\n').encode()


def upload(candidate, version, commit, run, state_path, call=request, now=None, sleep=time.sleep):
    group, artifact, payloads = candidate_payloads(candidate, version, commit, run)
    if state_path.exists():
        raise ValueError('This run already has snapshot state; never upload again')
    moment = now or datetime.datetime.now(datetime.timezone.utc)
    timestamp, updated = moment.strftime('%Y%m%d.%H%M%S'), moment.strftime('%Y%m%d%H%M%S')
    artifact_url = REPOSITORY + group.replace('.', '/') + '/' + artifact + '/'
    version_url = artifact_url + version + '/'
    state = {'coordinate': f'{group}:{artifact}:{version}', 'source_commit': commit, 'candidate_run': run,
             'phase': 'upload-prepared', 'snapshot_version': None, 'files': {}}
    save(state_path, state)

    def get(url):
        status, body = call('GET', url)
        if status == 200:
            return body
        if status == 404:
            return None
        raise RuntimeError(f'GET {url} answered {status}')

    def put(url, data):
        for name, body in [(url, data)] + [(url + '.' + algorithm, hashlib.new(algorithm, data).hexdigest().encode())
                                           for algorithm in CHECKSUMS]:
            status, _ = call('PUT', name, body)
            if status not in (200, 201, 204):
                raise RuntimeError(f'PUT {name} answered {status}')

    build = next_build(get(version_url + 'maven-metadata.xml'))
    value = f"{version.removesuffix('-SNAPSHOT')}-{timestamp}-{build}"
    state.update(phase='uploading', snapshot_version=value)
    save(state_path, state)
    for suffix, data in payloads.items():
        name = artifact + '-' + value + suffix
        put(version_url + name, data)
        state['files'][name] = hashlib.sha256(data).hexdigest()
        save(state_path, state)
    put(version_url + 'maven-metadata.xml', version_metadata(group, artifact, version, timestamp, build, updated))
    put(artifact_url + 'maven-metadata.xml',
        artifact_metadata(get(artifact_url + 'maven-metadata.xml'), group, artifact, version, updated))
    state['phase'] = 'uploaded'
    save(state_path, state)
    # Read the exact bytes back; the repository may need a moment to serve them.
    for name, digest in state['files'].items():
        for attempt in range(6):
            body = get(version_url + name)
            if body is not None:
                break
            sleep(10)
        if body is None or hashlib.sha256(body).hexdigest() != digest:
            raise RuntimeError('Snapshot readback does not match the uploaded bytes: ' + name)
    state['phase'] = 'verified'
    save(state_path, state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--candidate-run', required=True)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--check-only', action='store_true', help='Check the candidate; make no request')
    args = parser.parse_args()
    if args.check_only:
        candidate_payloads(args.candidate, args.version, args.source_commit, args.candidate_run)
        return
    state = upload(args.candidate, args.version, args.source_commit, args.candidate_run, args.state)
    print(json.dumps({key: state[key] for key in ('coordinate', 'snapshot_version', 'phase')}))


if __name__ == '__main__':
    main()
