#!/usr/bin/env python3
"""Trusted offline signer for a reviewed unsigned Central candidate.

Run with ``python3 -I`` from a clean checkout of reviewed ``main`` on the release
workstation. The downloaded candidate directory is data only: nothing from it is
executed, imported or used as tooling. Signing uses the operator's own gpg-agent
and pinentry; this tool never accepts a passphrase. It produces the signed bundle
and validation receipt consumed by ``central-recovery.py``.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import sys
import xml.etree.ElementTree as ET

# Keep the trusted checkout clean: no bytecode caches beside the tools.
sys.dont_write_bytecode = True
TOOLS = Path(__file__).resolve().parent
CHECKOUT = TOOLS.parents[1]
MARKER = 'UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE'
EVIDENCE = ('provenance.json', 'effective-pom.xml', 'release-notes.md', 'unsigned.json')
RECEIPT_KEYS = {'source_commit', 'version', 'qualification', 'payload_sha256', 'candidate_run'}
SUFFIXES = ('.pom', '.jar', '-sources.jar', '-javadoc.jar')
POM_NS = {'p': 'http://maven.apache.org/POM/4.0.0'}
SIGN_TIMEOUT = 300  # Allows an operator to answer pinentry.

_spec = importlib.util.spec_from_file_location('central_bundle', TOOLS / 'central-bundle.py')
bundle = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bundle)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def check_inputs(version, commit, run, signer=None):
    if not re.fullmatch(r'\d+\.\d+\.\d+', version):
        raise ValueError('Explicit stable numeric version required')
    if not re.fullmatch(r'[a-f0-9]{40}', commit):
        raise ValueError('Exact 40-character source commit required')
    if not re.fullmatch(r'[1-9][0-9]*', run):
        raise ValueError('Exact candidate workflow run ID required')
    if signer is not None and not re.fullmatch(r'[A-F0-9]{40}', signer):
        raise ValueError('Full uppercase 40-character signer fingerprint required')


def trusted_coordinate(pom):
    """Read group/artifact from the reviewed checkout's POM, never the candidate."""
    project = ET.parse(pom).getroot()
    group = project.findtext('p:groupId', namespaces=POM_NS)
    artifact = project.findtext('p:artifactId', namespaces=POM_NS)
    bundle.coordinate(group or '', artifact or '', '0.0.0')
    return group, artifact


def payload_names(artifact, version):
    return tuple(f'{artifact}-{version}{suffix}' for suffix in SUFFIXES)


def inside(path, parent):
    return path == parent or parent in path.parents


def inventory(directory, names):
    """Require exactly the retained artifact layout: regular files, no links."""
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('Candidate directory must be a real directory')
    expected_files = set(EVIDENCE) | {'unsigned/' + name for name in names}
    found = set()
    pending = [(directory, '')]
    while pending:
        current, prefix = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                relative = prefix + entry.name
                mode = entry.stat(follow_symlinks=False).st_mode
                if stat.S_ISLNK(mode):
                    raise ValueError('Symbolic link rejected in candidate: ' + relative)
                if stat.S_ISDIR(mode):
                    if relative != 'unsigned':
                        raise ValueError('Unexpected candidate directory: ' + relative)
                    pending.append((Path(entry.path), relative + '/'))
                elif stat.S_ISREG(mode):
                    if relative not in expected_files:
                        raise ValueError('Unexpected candidate file: ' + relative)
                    found.add(relative)
                else:
                    raise ValueError('Special file rejected in candidate: ' + relative)
    missing = expected_files - found
    if missing:
        raise ValueError('Missing candidate files: ' + ', '.join(sorted(missing)))


def read_limited(path, limit=bundle.LIMIT):
    # O_NOFOLLOW: a file swapped for a link after the inventory is still refused.
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    except OSError as error:
        raise ValueError('Candidate file is not a readable regular file: ' + path.name) from error
    with os.fdopen(descriptor, 'rb') as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError('Candidate file is not a regular file: ' + path.name)
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError('Candidate file exceeds size limit: ' + path.name)
    return data


def verify_candidate(directory, version, commit, run, pom):
    """Return verified payload bytes and identity; never sign or execute anything."""
    check_inputs(version, commit, run)
    group, artifact = trusted_coordinate(pom)
    names = payload_names(artifact, version)
    directory = Path(directory)
    inventory(directory, names)
    receipt_bytes = read_limited(directory / 'unsigned.json', 1024 * 1024)
    receipt = json.loads(receipt_bytes)
    if not isinstance(receipt, dict) or set(receipt) != RECEIPT_KEYS:
        raise ValueError('Unsigned receipt fields differ from the expected identity record')
    if receipt['qualification'] != MARKER:
        raise ValueError('Unsigned receipt identity marker missing')
    if (receipt['version'], receipt['source_commit'], receipt['candidate_run']) != (version, commit, run):
        raise ValueError('Unsigned receipt version/source commit/candidate run mismatch')
    hashes = receipt['payload_sha256']
    if (not isinstance(hashes, dict) or set(hashes) != set(names)
            or not all(isinstance(value, str) and re.fullmatch(r'[a-f0-9]{64}', value) for value in hashes.values())):
        raise ValueError('Unsigned receipt must pin exactly the four expected payload names')
    provenance = json.loads(read_limited(directory / 'provenance.json', 4 * 1024 * 1024))
    if any(provenance.get(key) != value for key, value in
           (('version', version), ('source_commit', commit), ('group', group), ('artifact', artifact))):
        raise ValueError('Candidate provenance identity mismatch')
    payloads = {}
    for name in names:
        data = read_limited(directory / 'unsigned' / name)
        if not data:
            raise ValueError('Empty payload: ' + name)
        if sha256(data) != hashes[name]:
            raise ValueError('Payload SHA-256 differs from unsigned receipt: ' + name)
        payloads[name] = data
    if sum(map(len, payloads.values())) > bundle.LIMIT:
        raise ValueError('Candidate payloads exceed size limit')
    project = ET.fromstring(payloads[names[0]])
    if any(project.findtext('p:' + key, namespaces=POM_NS) != value for key, value in
           (('groupId', group), ('artifactId', artifact), ('version', version))):
        raise ValueError('Candidate POM coordinate mismatch')
    return {'coordinate': f'{group}:{artifact}:{version}', 'group': group, 'artifact': artifact,
            'version': version, 'source_commit': commit, 'candidate_run': run,
            'unsigned_receipt_sha256': sha256(receipt_bytes),
            'provenance_workflow_head': provenance.get('workflow_head'),
            'payload_sha256': dict(hashes), 'payloads': payloads}


def sign_command(payload, signature, signer):
    # No passphrase, loopback pinentry or key import: the operator's agent decides.
    return ['gpg', '--no-options', '--batch', '--no-auto-key-retrieve', '--digest-algo', 'SHA512',
            '--local-user', signer, '--armor', '--output', str(signature), '--detach-sign', str(payload)]


def detach_sign(payload, signature, signer):
    result = subprocess.run(sign_command(payload, signature, signer), timeout=SIGN_TIMEOUT, check=False)
    if result.returncode or not signature.is_file():
        raise ValueError('gpg signing failed for ' + payload.name)


def tool_versions():
    gpg = subprocess.run(['gpg', '--version'], capture_output=True, text=True, timeout=15, check=True)
    return {'gpg': gpg.stdout.splitlines()[:2], 'python': platform.python_version(),
            'platform': platform.platform(),
            'tool_sha256': {name: sha256((TOOLS / name).read_bytes())
                            for name in ('sign-candidate.py', 'central-bundle.py', 'central-recovery.py')}}


def checkout_identity():
    try:
        head = subprocess.run(['git', '-C', str(CHECKOUT), 'rev-parse', 'HEAD'],
                              capture_output=True, text=True, timeout=15, check=True).stdout.strip()
        dirty = subprocess.run(['git', '-C', str(CHECKOUT), 'status', '--porcelain'],
                               capture_output=True, text=True, timeout=15, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return {'commit': None, 'clean': False}
    return {'commit': head, 'clean': not dirty}


def sign(directory, version, commit, run, signer, output, pom):
    check_inputs(version, commit, run, signer)
    directory, output = Path(directory).absolute(), Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('Output directory must be new')
    resolved_candidate, resolved_output = directory.resolve(), output.parent.resolve() / output.name
    for left, right in ((resolved_output, resolved_candidate), (resolved_candidate, resolved_output),
                        (resolved_candidate, CHECKOUT), (CHECKOUT, resolved_candidate),
                        (resolved_output, CHECKOUT)):
        if inside(left, right):
            raise ValueError('Candidate, output and trusted tool checkout must be separate directories')
    candidate = verify_candidate(directory, version, commit, run, pom)
    tools = tool_versions()
    output.mkdir(mode=0o700)
    signed = output / 'signed'
    signed.mkdir(mode=0o700)
    signatures = {}
    for name, data in candidate['payloads'].items():
        # Sign the verified in-memory bytes, not the downloaded file.
        target = signed / name
        with target.open('xb') as handle:
            handle.write(data)
        detach_sign(target, signed / (name + '.asc'), signer)
        signature = read_limited(signed / (name + '.asc'), 1024 * 1024)
        if sha256(target.read_bytes()) != candidate['payload_sha256'][name]:
            raise ValueError('Signed payload changed during signing: ' + name)
        bundle.verify_signature(data, signature, signer)
        signatures[name + '.asc'] = sha256(signature)
    archive, validation = output / 'candidate.zip', output / 'candidate.json'
    subprocess.run([sys.executable, '-I', str(TOOLS / 'central-bundle.py'), str(archive),
                    '--from-directory', str(signed), '--group', candidate['group'],
                    '--artifact', candidate['artifact'], '--version', version,
                    '--signer', signer, '--receipt', str(validation)],
                   cwd=output, timeout=SIGN_TIMEOUT, check=True)
    validated = json.loads(validation.read_text())
    root, entries = bundle.coordinate(candidate['group'], candidate['artifact'], version)
    expected = {**candidate['payload_sha256'], **signatures}
    if (validated['coordinate'] != candidate['coordinate'] or validated['signer_fingerprint'] != signer
            or validated['bundle_sha256'] != sha256(archive.read_bytes())
            or any(validated['entries'][f'{root}/{version}/{name}'] != value for name, value in expected.items())):
        raise ValueError('Assembled bundle differs from the verified payloads and signatures')
    for name in ('unsigned.json', 'provenance.json'):
        (output / name).write_bytes(read_limited(directory / name, 4 * 1024 * 1024))
    receipt = {'result': 'SIGNED_BUNDLE_VALIDATED_NOT_UPLOADED',
               'coordinate': candidate['coordinate'], 'version': version,
               'source_commit': commit, 'candidate_run': run,
               'provenance_workflow_head': candidate['provenance_workflow_head'],
               'unsigned_receipt_sha256': candidate['unsigned_receipt_sha256'],
               'signer_fingerprint': signer,
               'payload_sha256': candidate['payload_sha256'], 'signature_sha256': signatures,
               'bundle_sha256': validated['bundle_sha256'],
               'entries_sha256': validated['entries_sha256'],
               'validation_receipt_sha256': sha256(validation.read_bytes()),
               'tool_checkout': checkout_identity(), 'tools': tools,
               'signed_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat()}
    with (output / 'signing.json').open('x') as handle:
        json.dump(receipt, handle, indent=2)
        handle.write('\n')
    if not receipt['tool_checkout']['clean']:
        print('WARNING: trusted tool checkout is not a clean commit; do not upload this bundle.', file=sys.stderr)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('action', choices=['verify', 'sign'])
    parser.add_argument('--candidate', type=Path, required=True, help='Downloaded unsigned-central-candidate-* directory')
    parser.add_argument('--version', required=True)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--candidate-run', required=True)
    parser.add_argument('--signer', help='Full uppercase release-key fingerprint (sign only)')
    parser.add_argument('--output', type=Path, help='New directory for the signed bundle (sign only)')
    parser.add_argument('--pom', type=Path, default=CHECKOUT / 'pom.xml',
                        help='Trusted reviewed POM providing group/artifact (default: this checkout)')
    args = parser.parse_args()
    if args.action == 'verify':
        if args.signer or args.output:
            parser.error('verify neither signs nor writes output')
        result = verify_candidate(args.candidate, args.version, args.source_commit, args.candidate_run, args.pom)
        result.pop('payloads')
        result['result'] = 'UNSIGNED_CANDIDATE_MATCHES_RECEIPT'
    else:
        if not args.signer or not args.output:
            parser.error('sign requires --signer and --output')
        try:
            result = sign(args.candidate, args.version, args.source_commit, args.candidate_run,
                          args.signer, args.output, args.pom)
        except BaseException:
            if args.output.exists() and not (args.output / 'signing.json').exists():
                print(f'Signing incomplete: {args.output} has no signing.json; never upload from it.', file=sys.stderr)
            raise
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
