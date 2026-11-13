#!/usr/bin/env python3
"""Offline Central bundle validation. No build, upload, or release side effects."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ALGORITHMS = ('md5', 'sha1', 'sha256', 'sha512')
LIMIT = 64 * 1024 * 1024


def coordinate(group, artifact, version):
    if not re.fullmatch(r'[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+', group):
        raise ValueError('Invalid group')
    if not re.fullmatch(r'[A-Za-z0-9_-]+', artifact):
        raise ValueError('Invalid artifact')
    if not re.fullmatch(r'\d+\.\d+\.\d+', version):
        raise ValueError('Only stable numeric versions are accepted')
    root = group.replace('.', '/') + '/' + artifact
    prefix = f'{root}/{version}/{artifact}-{version}'
    return root, tuple(prefix + suffix for suffix in ('.pom', '.jar', '-sources.jar', '-javadoc.jar'))


def digest(data, algorithm='sha256'):
    return hashlib.new(algorithm, data).hexdigest()


def entries_digest(entries):
    """Stable identity of every signed bundle entry, reproducible from public bytes."""
    return digest(json.dumps(entries, sort_keys=True, separators=(',', ':')).encode())


def verify_signature(payload, signature, fingerprint):
    """Verify against the operator-provisioned GPG keyring and pinned signer."""
    if not re.fullmatch(r'[A-F0-9]{40}', fingerprint):
        raise ValueError('Expected full uppercase signer fingerprint')
    with tempfile.TemporaryDirectory(prefix='central-signature-') as directory:
        data = Path(directory) / 'payload'
        sig = Path(directory) / 'payload.asc'
        data.write_bytes(payload)
        sig.write_bytes(signature)
        result = subprocess.run(
            ['gpg', '--no-options', '--batch', '--no-auto-key-retrieve', '--status-fd', '1', '--verify', str(sig), str(data)],
            capture_output=True, text=True, timeout=30, check=False)
        valid = [line.split() for line in result.stdout.splitlines()
                 if line.startswith('[GNUPG:] VALIDSIG ')]
        rejected = any(token in result.stdout for token in
                       ('[GNUPG:] REVKEYSIG ', '[GNUPG:] EXPKEYSIG ', '[GNUPG:] EXPSIG '))
        if result.returncode or rejected or len(valid) != 1 or fingerprint not in (valid[0][2], valid[0][-1]):
            raise ValueError('Detached signature or expected signer verification failed')


def validate(path, group, artifact, version, fingerprint, signature_verifier=verify_signature):
    root, payloads = coordinate(group, artifact, version)
    expected = {name + suffix for name in payloads for suffix in ('', '.asc', *('.' + a for a in ALGORITHMS))}
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries) != len(expected) or {item.filename for item in entries} != expected:
            raise ValueError('Bundle must contain exactly the four signed artifacts and their checksums')
        if sum(item.file_size for item in entries) > LIMIT:
            raise ValueError('Bundle exceeds expanded size limit')
        for item in entries:
            if item.flag_bits & 1 or item.is_dir() or (item.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError('Encrypted, directory, or symbolic-link entry rejected')
        blobs = {item.filename: archive.read(item) for item in entries}
    pom = ET.fromstring(blobs[payloads[0]])
    ns = {'p': 'http://maven.apache.org/POM/4.0.0'}
    if pom.tag != '{http://maven.apache.org/POM/4.0.0}project':
        raise ValueError('Expected Maven POM')
    for key, value in (('groupId', group), ('artifactId', artifact), ('version', version)):
        if pom.findtext('p:' + key, namespaces=ns) != value:
            raise ValueError('POM coordinate mismatch')
    for name in payloads:
        if not blobs[name]:
            raise ValueError('Empty artifact')
        for algorithm in ALGORITHMS:
            if blobs[name + '.' + algorithm].decode('ascii').strip() != digest(blobs[name], algorithm):
                raise ValueError('Artifact checksum mismatch')
        signature_verifier(blobs[name], blobs[name + '.asc'], fingerprint)
        if name.endswith('.jar'):
            import io
            with zipfile.ZipFile(io.BytesIO(blobs[name])) as jar:
                infos = jar.infolist()
                if (not infos or len({item.filename for item in infos}) != len(infos)
                        or sum(item.file_size for item in infos) > LIMIT):
                    raise ValueError('Empty, duplicate, or oversized JAR contents')
                if jar.testzip() is not None:
                    raise ValueError('Invalid or empty JAR')
                if name == payloads[1]:
                    embedded = f'META-INF/maven/{group}/{artifact}/pom.properties'
                    properties = dict(line.split('=', 1) for line in jar.read(embedded).decode().splitlines()
                                      if '=' in line and not line.startswith('#'))
                    if any(properties.get(key) != value for key, value in
                           (('groupId', group), ('artifactId', artifact), ('version', version))):
                        raise ValueError('Runtime JAR coordinate mismatch')
    entries = {name: digest(data) for name, data in sorted(blobs.items())}
    return {'coordinate': f'{group}:{artifact}:{version}', 'bundle_sha256': digest(Path(path).read_bytes()),
            'signer_fingerprint': fingerprint, 'entries': entries, 'entries_sha256': entries_digest(entries)}


def clean(source, output, group, artifact, version):
    """Strip only known Maven local bookkeeping; never repair release payloads."""
    root, _ = coordinate(group, artifact, version)
    allowed = {root + '/maven-metadata-local.xml', root + f'/{version}/_remote.repositories'}
    allowed |= {root + f'/{version}/_remote.repositories.{algorithm}' for algorithm in ALGORITHMS}
    if source.resolve() == output.resolve() or output.exists():
        raise ValueError('Output must be a new, separate file')
    with zipfile.ZipFile(source) as original:
        infos = original.infolist()
        if len({item.filename for item in infos}) != len(infos) or sum(i.file_size for i in infos) > LIMIT:
            raise ValueError('Duplicate entries or expanded size limit exceeded')
        with zipfile.ZipFile(output, 'x') as target:
            for item in infos:
                if item.filename not in allowed:
                    target.writestr(copy.copy(item), original.read(item))
    # Validation is mandatory after cleaning; this operation alone is not approval.


def assemble(directory, output, group, artifact, version):
    """Build a clean bundle from four signed artifacts, without Maven install/deploy."""
    _, payloads = coordinate(group, artifact, version)
    if output.exists():
        raise ValueError('Output must be a new file')
    blobs = {}
    for name in payloads:
        source = directory / Path(name).name
        signature = directory / (Path(name).name + '.asc')
        if source.is_symlink() or signature.is_symlink():
            raise ValueError('Symbolic-link input rejected')
        if source.stat().st_size + signature.stat().st_size > LIMIT:
            raise ValueError('Signed artifact exceeds size limit')
        blobs[name] = source.read_bytes()
        blobs[name + '.asc'] = signature.read_bytes()
        for algorithm in ALGORITHMS:
            blobs[name + '.' + algorithm] = digest(blobs[name], algorithm).encode()
    if sum(map(len, blobs.values())) > LIMIT:
        raise ValueError('Bundle exceeds expanded size limit')
    with zipfile.ZipFile(output, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(blobs.items()):
            archive.writestr(name, data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bundle', type=Path)
    parser.add_argument('--group', required=True)
    parser.add_argument('--artifact', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--signer', required=True)
    parser.add_argument('--clean-to', type=Path)
    parser.add_argument('--from-directory', type=Path)
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    if args.clean_to and args.from_directory:
        parser.error('--clean-to and --from-directory are mutually exclusive')
    candidate = args.bundle
    if args.from_directory:
        assemble(args.from_directory, candidate, args.group, args.artifact, args.version)
    if args.clean_to:
        clean(candidate, args.clean_to, args.group, args.artifact, args.version)
        candidate = args.clean_to
    receipt = validate(candidate, args.group, args.artifact, args.version, args.signer)
    with args.receipt.open('x') as handle:
        json.dump(receipt, handle, indent=2)
        handle.write('\n')


if __name__ == '__main__':
    main()
