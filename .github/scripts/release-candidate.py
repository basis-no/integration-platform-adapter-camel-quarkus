#!/usr/bin/env python3
"""Validate explicit candidate selection and reusable workflow artifacts."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET

NS = {'p': 'http://maven.apache.org/POM/4.0.0'}


def selection(mode, version, commit, signer, candidate_run='', state_run='', bundle_hash='', attempt='1'):
    if mode not in ('inspect', 'upload', 'resume', 'publish', 'readback'):
        raise ValueError('Unknown candidate action')
    if not re.fullmatch(r'\d+\.\d+\.\d+', version):
        raise ValueError('Explicit stable numeric candidate version required')
    if not re.fullmatch(r'[a-f0-9]{40}', commit):
        raise ValueError('Exact source commit required; branch names are not accepted')
    if not re.fullmatch(r'[A-F0-9]{40}', signer):
        raise ValueError('Pinned full signer fingerprint required')
    if mode == 'inspect':
        if candidate_run or state_run or bundle_hash:
            raise ValueError('Inspect builds new candidate evidence; recovery inputs must be empty')
    elif mode == 'readback':
        # Post-publication readback: unsigned candidate run plus offline signed entries digest.
        if (state_run or not re.fullmatch(r'[1-9][0-9]*', candidate_run)
                or not re.fullmatch(r'[a-f0-9]{64}', bundle_hash)):
            raise ValueError('Readback requires the candidate run and signed entries SHA-256 only')
    else:
        if not re.fullmatch(r'[1-9][0-9]*', candidate_run) or not re.fullmatch(r'[a-f0-9]{64}', bundle_hash):
            raise ValueError('Existing candidate run and reviewed bundle hash required')
        if mode in ('upload', 'publish') and attempt != '1':
            raise ValueError('Never rerun portal writes; inspect the latest saved state and use resume')
        if mode == 'upload' and state_run:
            raise ValueError('Upload cannot restore prior deployment state; use resume/publish')
        if mode in ('resume', 'publish') and not re.fullmatch(r'[1-9][0-9]*', state_run):
            raise ValueError('Existing state artifact run required; never start a fresh upload')


def source(path, version, commit):
    actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=path, text=True).strip()
    if actual != commit or subprocess.check_output(['git', 'status', '--porcelain'], cwd=path, text=True):
        raise ValueError('Candidate source must be the exact clean approved commit')
    pom = ET.parse(path / 'pom.xml').getroot()
    if pom.findtext('p:version', namespaces=NS) != version + '-SNAPSHOT':
        raise ValueError('Candidate must match the source snapshot; published stable sources are not rebuilt')
    if subprocess.check_output(['git', 'tag', '--list', 'v' + version], cwd=path, text=True).strip():
        raise ValueError('Candidate version already has a tag; reuse its artifacts, never rebuild')
    return pom


def provenance(path, version, commit, workflow_head, output, effective_pom):
    pom = ET.parse(path / 'pom.xml').getroot()
    if pom.findtext('p:version', namespaces=NS) != version:
        raise ValueError('Release version substitution did not produce the approved candidate')
    plugins = [{'group': element.findtext('p:groupId', default='org.apache.maven.plugins', namespaces=NS),
                'artifact': element.findtext('p:artifactId', namespaces=NS),
                'declared_version': element.findtext('p:version', namespaces=NS)}
               for element in pom.findall('.//p:plugin', NS)]
    effective = ET.parse(effective_pom).getroot()
    effective_plugins = [{'group': element.findtext('p:groupId', default='org.apache.maven.plugins', namespaces=NS),
                          'artifact': element.findtext('p:artifactId', namespaces=NS),
                          'version': element.findtext('p:version', namespaces=NS)}
                         for element in effective.findall('p:build/p:plugins/p:plugin', NS)]
    if any(not plugin['version'] or '${' in plugin['version'] for plugin in effective_plugins):
        raise ValueError('Effective plugin versions must be resolved')
    required = {('org.apache.maven.plugins', name) for name in
                ('maven-jar-plugin', 'maven-source-plugin', 'maven-javadoc-plugin', 'maven-gpg-plugin')}
    required.add(('org.sonatype.central', 'central-publishing-maven-plugin'))
    if not required <= {(plugin['group'], plugin['artifact']) for plugin in effective_plugins}:
        raise ValueError('Release toolchain/plugin inspection is incomplete')
    files = ['pom.xml', 'mvnw', '.mvn/wrapper/maven-wrapper.properties']
    record = {'source_commit': commit, 'workflow_head': workflow_head, 'version': version,
              'group': pom.findtext('p:groupId', namespaces=NS),
              'artifact': pom.findtext('p:artifactId', namespaces=NS),
              'input_sha256': {name: hashlib.sha256((path / name).read_bytes()).hexdigest() for name in files},
              'declared_plugins': plugins,
              'effective_plugins': effective_plugins,
              'effective_pom_sha256': hashlib.sha256(effective_pom.read_bytes()).hexdigest(),
              'maven_version': subprocess.check_output(['./mvnw', '--version'], cwd=path, text=True),
              'java_version': subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, text=True),
              'qualification': 'Candidate inspection only; no publication or runtime certification implied'}
    with output.open('x') as handle:
        json.dump(record, handle, indent=2)
        handle.write('\n')


def unsigned(path, version, commit, output, run):
    """Copy only opaque payloads; never sign or execute them in this collector."""
    if not re.fullmatch(r'[1-9][0-9]*', run):
        raise ValueError('Exact candidate workflow run ID required')
    record = json.loads((output / 'provenance.json').read_text())
    if record['source_commit'] != commit or record['version'] != version:
        raise ValueError('Unsigned candidate identity mismatch')
    group, artifact = record['group'], record['artifact']
    if (not re.fullmatch(r'[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+', group)
            or not re.fullmatch(r'[A-Za-z0-9_-]+', artifact)):
        raise ValueError('Invalid unsigned coordinate')
    names = [artifact + '-' + version + suffix
             for suffix in ('.pom', '.jar', '-sources.jar', '-javadoc.jar')]
    # GPG skip need not stage a target POM; collect the versioned source POM.
    sources = [path / 'pom.xml'] + [path / 'target' / name for name in names[1:]]
    if any(p.is_symlink() or not p.is_file() for p in sources):
        raise ValueError('Unsigned payload must be a regular file')
    if any(not p.stat().st_size for p in sources) or sum(p.stat().st_size for p in sources) > 64 * 1024 * 1024:
        raise ValueError('Unsigned payload size rejected')
    pom = ET.parse(sources[0]).getroot()
    if any(pom.findtext('p:' + key, namespaces=NS) != value
           for key, value in (('groupId', group), ('artifactId', artifact), ('version', version))):
        raise ValueError('Unsigned POM coordinate mismatch')
    destination = output / 'unsigned'
    destination.mkdir()
    hashes = {}
    for source, name in zip(sources, names):
        shutil.copyfile(source, destination / name)
        hashes[name] = hashlib.sha256(source.read_bytes()).hexdigest()
    receipt = {'source_commit': commit, 'version': version, 'candidate_run': run,
               'qualification': 'UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE', 'payload_sha256': hashes}
    (output / 'unsigned.json').write_text(json.dumps(receipt, indent=2) + '\n')


def check_artifact(directory, version, commit, signer, bundle_hash):
    receipt = json.loads((directory / 'candidate.json').read_text())
    record = json.loads((directory / 'provenance.json').read_text())
    actual = hashlib.sha256((directory / 'candidate.zip').read_bytes()).hexdigest()
    if record['version'] != version or record['source_commit'] != commit:
        raise ValueError('Downloaded candidate source/version mismatch')
    if receipt['bundle_sha256'] != actual or actual != bundle_hash or receipt['signer_fingerprint'] != signer:
        raise ValueError('Downloaded candidate differs from reviewed hash/signer')
    expected_coordinate = f"{record['group']}:{record['artifact']}:{version}"
    if receipt['coordinate'] != expected_coordinate:
        raise ValueError('Downloaded candidate coordinate mismatch')
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['selection', 'source', 'provenance', 'unsigned', 'artifact'])
    parser.add_argument('--mode', default='inspect')
    parser.add_argument('--version', required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--signer', required=True)
    parser.add_argument('--candidate-run', default='')
    parser.add_argument('--state-run', default='')
    parser.add_argument('--bundle-hash', default='')
    parser.add_argument('--attempt', default='1')
    parser.add_argument('--path', type=Path, default=Path('candidate'))
    parser.add_argument('--workflow-head', default='')
    parser.add_argument('--workflow-run', default='')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--effective-pom', type=Path)
    args = parser.parse_args()
    selection(args.mode, args.version, args.commit, args.signer, args.candidate_run,
              args.state_run, args.bundle_hash, args.attempt)
    if args.action == 'source':
        source(args.path, args.version, args.commit)
    elif args.action == 'provenance':
        if not args.output or not args.effective_pom or not re.fullmatch(r'[a-f0-9]{40}', args.workflow_head):
            parser.error('Provenance requires output, effective POM and exact workflow head')
        provenance(args.path, args.version, args.commit, args.workflow_head, args.output, args.effective_pom)
    elif args.action == 'unsigned':
        if not args.output:
            parser.error('Unsigned collection requires an output directory')
        unsigned(args.path, args.version, args.commit, args.output, args.workflow_run)
    elif args.action == 'artifact':
        print(json.dumps(check_artifact(args.path, args.version, args.commit, args.signer, args.bundle_hash)))


if __name__ == '__main__':
    main()
