#!/usr/bin/env python3
"""Verify public release bytes against an existing reviewed candidate; never upload."""
import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
import urllib.request
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location('central_bundle', Path(__file__).with_name('central-bundle.py'))
bundle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bundle)


def verify(pom, version, signer, expected_receipt, output, unsigned_receipt=None, entries_sha256=None):
    """Compare public bytes with the offline validation receipt, or in CI with the
    unsigned candidate receipt plus the signed-entries digest from the signing receipt."""
    if bool(expected_receipt) == bool(unsigned_receipt) or bool(unsigned_receipt) != bool(entries_sha256):
        raise ValueError('Use either --expected-receipt or --unsigned-receipt with --entries-sha256')
    ns = {'p': 'http://maven.apache.org/POM/4.0.0'}
    project = ET.parse(pom).getroot()
    group = project.findtext('p:groupId', namespaces=ns)
    artifact = project.findtext('p:artifactId', namespaces=ns)
    source_version = project.findtext('p:version', namespaces=ns)
    if source_version not in (version, version + '-SNAPSHOT'):
        raise ValueError('Tag source version mismatch')
    _, payloads = bundle.coordinate(group, artifact, version)
    with tempfile.TemporaryDirectory(prefix='central-readback-') as directory:
        root = Path(directory)
        for name in payloads:
            for suffix in ('', '.asc', *('.' + algorithm for algorithm in bundle.ALGORITHMS)):
                with urllib.request.urlopen('https://repo.maven.apache.org/maven2/' + name + suffix, timeout=30) as response:
                    data = response.read(bundle.LIMIT + 1)
                if len(data) > bundle.LIMIT:
                    raise ValueError('Public artifact exceeds size limit')
                (root / (Path(name).name + suffix)).write_bytes(data)
        # Assemble validates the payload hashes while generating its own checksum
        # entries, so check downloaded checksum text separately before assembly.
        for name in payloads:
            data = (root / Path(name).name).read_bytes()
            for algorithm in bundle.ALGORITHMS:
                checksum = (root / (Path(name).name + '.' + algorithm)).read_text().strip()
                if checksum != bundle.digest(data, algorithm):
                    raise ValueError('Public checksum mismatch')
        archive = root / 'public.zip'
        bundle.assemble(root, archive, group, artifact, version)
        actual = bundle.validate(archive, group, artifact, version, signer)
    if expected_receipt:
        expected = json.loads(expected_receipt.read_text())
        matches = (actual['coordinate'] == expected['coordinate']
                   and actual['signer_fingerprint'] == expected['signer_fingerprint']
                   and actual['entries'] == expected['entries'])
    else:
        unsigned = json.loads(unsigned_receipt.read_text())
        matches = (unsigned.get('qualification') == 'UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE'
                   and unsigned.get('version') == version
                   and {Path(name).name: actual['entries'][name] for name in payloads} == unsigned.get('payload_sha256')
                   and actual['entries_sha256'] == entries_sha256)
    if not matches:
        raise ValueError('Public artifact bytes differ from the reviewed candidate; investigate without re-upload')
    actual['result'] = 'PUBLIC_BYTES_MATCH_REVIEWED_CANDIDATE'
    with output.open('x') as handle:
        json.dump(actual, handle, indent=2)
        handle.write('\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pom', type=Path, required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--signer', required=True)
    parser.add_argument('--expected-receipt', type=Path, help='Offline validation receipt (candidate.json)')
    parser.add_argument('--unsigned-receipt', type=Path, help='Unsigned candidate receipt (unsigned.json)')
    parser.add_argument('--entries-sha256', help='entries_sha256 from the offline signing receipt')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    verify(args.pom, args.version, args.signer, args.expected_receipt, args.output,
           args.unsigned_receipt, args.entries_sha256)


if __name__ == '__main__':
    main()
