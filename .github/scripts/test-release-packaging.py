#!/usr/bin/env python3
"""Bounded, offline fixtures for packaging and recovery guards."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile


def load(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bundle = load('central-bundle')
recovery = load('central-recovery')
evidence = load('release-evidence')
candidate = load('release-candidate')
public = load('verify-public-release')
GROUP, ARTIFACT, VERSION = 'org.example', 'adapter', '1.2.3'
SIGNER = 'A' * 40
ID = '12345678-1234-1234-1234-123456789abc'


class Fixtures(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.path = self.root / 'bundle.zip'
        self.receipt = self.root / 'receipt.json'
        self.state = self.root / 'state.json'
        _, payloads = bundle.coordinate(GROUP, ARTIFACT, VERSION)
        pom = (f'<project xmlns="http://maven.apache.org/POM/4.0.0"><groupId>{GROUP}</groupId>'
               f'<artifactId>{ARTIFACT}</artifactId><version>{VERSION}</version></project>').encode()
        self.blobs = {}
        for name in payloads:
            if name.endswith('.pom'):
                data = pom
            else:
                stream = io.BytesIO()
                with zipfile.ZipFile(stream, 'w') as jar:
                    jar.writestr('content', 'fixture')
                    jar.writestr(f'META-INF/maven/{GROUP}/{ARTIFACT}/pom.properties',
                                 f'groupId={GROUP}\nartifactId={ARTIFACT}\nversion={VERSION}\n')
                data = stream.getvalue()
            self.blobs[name] = data
            self.blobs[name + '.asc'] = b'fixture-signature'
            for algorithm in bundle.ALGORITHMS:
                self.blobs[name + '.' + algorithm] = bundle.digest(data, algorithm).encode()
        self.write()
        self.verify()

    def write(self, blobs=None):
        with zipfile.ZipFile(self.path, 'w') as archive:
            for name, data in (blobs or self.blobs).items():
                archive.writestr(name, data)

    def verify(self, path=None):
        result = bundle.validate(path or self.path, GROUP, ARTIFACT, VERSION, SIGNER,
                                 signature_verifier=lambda *args: None)
        self.receipt.write_text(json.dumps(result))
        return result

    def saved(self, **extra):
        recovery.save(self.state, {'bundle_sha256': hashlib.sha256(self.path.read_bytes()).hexdigest(),
                                  'receipt_sha256': hashlib.sha256(self.receipt.read_bytes()).hexdigest(),
                                  'coordinate': f'{GROUP}:{ARTIFACT}:{VERSION}', 'deployment_id': ID, **extra})

    def status(self, phase, identity=ID):
        return json.dumps({'deploymentId': identity, 'deploymentState': phase}).encode()

    def test_exact_inventory_rejects_unknown_and_missing(self):
        for blobs in ({**self.blobs, '../unexpected': b'x'}, dict(list(self.blobs.items())[1:])):
            self.write(blobs)
            with self.assertRaises(ValueError):
                self.verify()

    def test_duplicate_rejected(self):
        with zipfile.ZipFile(self.path, 'a') as archive:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                archive.writestr(next(iter(self.blobs)), b'duplicate')
        with self.assertRaises(ValueError):
            self.verify()

    def test_checksum_and_coordinate_rejected(self):
        key = next(name for name in self.blobs if name.endswith('.sha256'))
        self.write({**self.blobs, key: b'0' * 64})
        with self.assertRaises(ValueError):
            self.verify()
        self.write()
        with self.assertRaises(ValueError):
            bundle.validate(self.path, GROUP, ARTIFACT, '1.2.4', SIGNER)

    def test_clean_preserves_every_signed_byte(self):
        root, _ = bundle.coordinate(GROUP, ARTIFACT, VERSION)
        extras = {root + '/maven-metadata-local.xml': b'metadata',
                  root + f'/{VERSION}/_remote.repositories': b'repositories'}
        self.write({**self.blobs, **extras})
        output = self.root / 'clean.zip'
        bundle.clean(self.path, output, GROUP, ARTIFACT, VERSION)
        self.verify(output)
        with zipfile.ZipFile(output) as archive:
            self.assertEqual({name: archive.read(name) for name in archive.namelist()}, self.blobs)
        with self.assertRaises(ValueError):
            bundle.clean(self.path, output, GROUP, ARTIFACT, VERSION)

    def test_assemble_ignores_local_bookkeeping_and_preserves_payloads(self):
        for name, data in self.blobs.items():
            if not any(name.endswith('.' + algorithm) for algorithm in bundle.ALGORITHMS):
                (self.root / Path(name).name).write_bytes(data)
        (self.root / '_remote.repositories').write_text('local metadata')
        output = self.root / 'assembled.zip'
        bundle.assemble(self.root, output, GROUP, ARTIFACT, VERSION)
        self.verify(output)
        with zipfile.ZipFile(output) as archive:
            self.assertEqual({name: archive.read(name) for name in archive.namelist()}, self.blobs)

    def test_unknown_metadata_survives_clean_and_fails_validation(self):
        self.write({**self.blobs, 'unknown/maven-metadata.xml': b'x'})
        output = self.root / 'clean.zip'
        bundle.clean(self.path, output, GROUP, ARTIFACT, VERSION)
        with self.assertRaises(ValueError):
            self.verify(output)

    def test_signature_pin_and_revocation(self):
        for status, code, accepted in (
            (f'[GNUPG:] VALIDSIG {SIGNER} rest {SIGNER}\n', 0, True),
            (f'[GNUPG:] VALIDSIG {"B" * 40} rest {"B" * 40}\n', 0, False),
            (f'[GNUPG:] REVKEYSIG x\n[GNUPG:] VALIDSIG {SIGNER} rest {SIGNER}\n', 0, False),
            ('', 1, False)):
            with patch.object(bundle.subprocess, 'run', return_value=subprocess.CompletedProcess([], code, status, '')) as run:
                if accepted:
                    bundle.verify_signature(b'data', b'sig', SIGNER)
                else:
                    with self.assertRaises(ValueError):
                        bundle.verify_signature(b'data', b'sig', SIGNER)
                self.assertIn('--no-options', run.call_args.args[0])
                self.assertIn('--no-auto-key-retrieve', run.call_args.args[0])
                self.assertEqual(run.call_args.kwargs['timeout'], 30)

    def test_upload_intent_prepared_without_network_then_uploaded_once(self):
        result = recovery.recover(self.path, self.receipt, self.state, prepare_upload=True,
                                  call=lambda *args: self.fail('Portal accessed while preparing'))
        self.assertEqual(result['phase'], 'upload-prepared')
        self.assertIsNone(result['deployment_id'])
        with self.assertRaises(ValueError):
            recovery.recover(self.path, self.receipt, self.state, prepare_upload=True)
        calls = []
        def call(endpoint, *args):
            calls.append(endpoint)
            return ID.encode() if endpoint.startswith('/upload') else self.status('VALIDATED')
        recovery.recover(self.path, self.receipt, self.state, allow_upload=True, call=call)
        self.assertEqual(len([name for name in calls if name.startswith('/upload')]), 1)

    def test_unsigned_collection_cannot_be_reused_as_signed_candidate(self):
        output = self.root / 'evidence'
        output.mkdir()
        (output / 'provenance.json').write_text(json.dumps({
            'source_commit': 'a' * 40, 'version': VERSION, 'group': GROUP, 'artifact': ARTIFACT}))
        target = self.root / 'target'
        target.mkdir()
        for name, data in self.blobs.items():
            if name.endswith(('.pom', '.jar')):
                (target / Path(name).name).write_bytes(data)
        (self.root / 'pom.xml').write_bytes(self.blobs[next(n for n in self.blobs if n.endswith('.pom'))])
        (target / (ARTIFACT + '-' + VERSION + '.pom')).unlink()
        candidate.unsigned(self.root, VERSION, 'a' * 40, output)
        receipt = json.loads((output / 'unsigned.json').read_text())
        self.assertEqual(receipt['qualification'], 'UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE')
        self.assertEqual(len(list((output / 'unsigned').iterdir())), 4)
        with self.assertRaises(FileNotFoundError):
            candidate.check_artifact(output, VERSION, 'a' * 40, SIGNER, 'b' * 64)

    def test_unsigned_collection_refuses_symlinks_and_wrong_identity(self):
        output = self.root / 'evidence'
        output.mkdir()
        (output / 'provenance.json').write_text(json.dumps({
            'source_commit': 'a' * 40, 'version': VERSION, 'group': GROUP, 'artifact': ARTIFACT}))
        target = self.root / 'target'
        target.mkdir()
        for name, data in self.blobs.items():
            if name.endswith(('.pom', '.jar')):
                (target / Path(name).name).write_bytes(data)
        (self.root / 'pom.xml').write_bytes(self.blobs[next(n for n in self.blobs if n.endswith('.pom'))])
        with self.assertRaises(ValueError):
            candidate.unsigned(self.root, VERSION, 'b' * 40, output)
        payload = target / (ARTIFACT + '-' + VERSION + '.jar')
        payload.unlink()
        payload.symlink_to(self.path)
        with self.assertRaises(ValueError):
            candidate.unsigned(self.root, VERSION, 'a' * 40, output)
        self.assertFalse((output / 'unsigned').exists())

    def test_production_workflows_have_no_signing_or_portal_secret_references(self):
        workflows = Path(__file__).resolve().parents[1] / 'workflows'
        for name in ('release-candidate.yml', 'publish-snapshot.yml'):
            text = (workflows / name).read_text()
            self.assertNotIn('secrets.', text)
            self.assertNotIn('gpg-private-key:', text)
            self.assertNotIn('gpg-passphrase:', text)
        text = (workflows / 'release-candidate.yml').read_text()
        self.assertIn('test "$MODE" = inspect', text)
        self.assertIn('name: unsigned-central-candidate-', text)
        self.assertNotIn('name: central-candidate-', text)
        self.assertNotIn('central-recovery.py', text)
        self.assertIn('-Dgpg.skip=true', text)
        # Candidate code can write arbitrary Maven cache contents. Never share
        # its dependency cache with a later trusted signing/publishing runner.
        self.assertNotIn('cache:', text)
        self.assertNotIn('actions/cache', text)
        snapshot = (workflows / 'publish-snapshot.yml').read_text()
        self.assertIn('exit 1', snapshot)
        self.assertNotIn('checkout@', snapshot)
        self.assertNotIn('mvnw', snapshot)

    def test_candidate_selection_requires_exact_inputs_and_refuses_upload_rerun(self):
        candidate.selection('inspect', VERSION, 'a' * 40, SIGNER)
        candidate.selection('upload', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64)
        for args in [('inspect', VERSION, 'main', SIGNER),
                     ('inspect', VERSION + '-SNAPSHOT', 'a' * 40, SIGNER),
                     ('upload', VERSION, 'a' * 40, SIGNER),
                     ('upload', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64, '2'),
                     ('publish', VERSION, 'a' * 40, SIGNER, '123', '456', 'b' * 64, '2'),
                     ('resume', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64),
                     ('publish', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64)]:
            with self.assertRaises(ValueError):
                candidate.selection(*args)

    def test_candidate_source_refuses_published_or_dirty_source(self):
        pom = next(data for name, data in self.blobs.items() if name.endswith('.pom'))
        (self.root / 'pom.xml').write_bytes(pom)
        with patch.object(candidate.subprocess, 'check_output', side_effect=['a' * 40 + '\n', '']):
            with self.assertRaises(ValueError):
                candidate.source(self.root, VERSION, 'a' * 40)
        (self.root / 'pom.xml').write_bytes(pom.replace(VERSION.encode(), (VERSION + '-SNAPSHOT').encode()))
        with patch.object(candidate.subprocess, 'check_output', side_effect=['a' * 40 + '\n', '', '']):
            candidate.source(self.root, VERSION, 'a' * 40)
        with patch.object(candidate.subprocess, 'check_output', side_effect=['a' * 40 + '\n', '', 'v' + VERSION]):
            with self.assertRaises(ValueError):
                candidate.source(self.root, VERSION, 'a' * 40)
        with patch.object(candidate.subprocess, 'check_output', side_effect=['a' * 40 + '\n', ' M pom.xml']):
            with self.assertRaises(ValueError):
                candidate.source(self.root, VERSION, 'a' * 40)

    def test_release_notes_cli_uses_explicit_tagged_source_paths(self):
        pom = next(data for name, data in self.blobs.items() if name.endswith('.pom'))
        (self.root / 'tagged-pom.xml').write_bytes(pom)
        (self.root / 'tagged-changelog.md').write_text('## ' + VERSION + ' — 2026-10-12\n\nReviewed technical notes.\n')
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('release-notes.py')),
                                 '--tag', 'v' + VERSION, '--pom', str(self.root / 'tagged-pom.xml'),
                                 '--changelog', str(self.root / 'tagged-changelog.md'),
                                 '--output', str(self.root / 'notes.md')], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / 'notes.md').read_text(), 'Reviewed technical notes.\n')

    def test_provenance_requires_complete_resolved_release_plugin_versions(self):
        pom = next(data for name, data in self.blobs.items() if name.endswith('.pom'))
        (self.root / 'pom.xml').write_bytes(pom)
        (self.root / 'mvnw').write_text('fixture only')
        (self.root / '.mvn/wrapper').mkdir(parents=True)
        (self.root / '.mvn/wrapper/maven-wrapper.properties').write_text('fixture only')
        plugins = [('org.apache.maven.plugins', name) for name in
                   ('maven-jar-plugin', 'maven-source-plugin', 'maven-javadoc-plugin', 'maven-gpg-plugin')]
        plugins.append(('org.sonatype.central', 'central-publishing-maven-plugin'))
        text = '<project xmlns="http://maven.apache.org/POM/4.0.0"><build><plugins>' + ''.join(
            f'<plugin><groupId>{group}</groupId><artifactId>{name}</artifactId><version>1.0</version></plugin>'
            for group, name in plugins) + '</plugins></build></project>'
        effective = self.root / 'effective.xml'
        effective.write_text(text)
        with patch.object(candidate.subprocess, 'check_output', side_effect=['fixture Maven', 'fixture Java']):
            candidate.provenance(self.root, VERSION, 'a' * 40, 'b' * 40, self.root / 'provenance.json', effective)
        self.assertEqual(len(json.loads((self.root / 'provenance.json').read_text())['effective_plugins']), 5)
        effective.write_text(text.replace('<version>1.0</version>', '<version>${unresolved}</version>', 1))
        with self.assertRaises(ValueError):
            candidate.provenance(self.root, VERSION, 'a' * 40, 'b' * 40, self.root / 'failed.json', effective)

    def test_reused_candidate_must_match_approved_source_hash_and_signer(self):
        self.path.rename(self.root / 'candidate.zip')
        self.receipt.rename(self.root / 'candidate.json')
        record = {'version': VERSION, 'source_commit': 'a' * 40, 'group': GROUP, 'artifact': ARTIFACT}
        (self.root / 'provenance.json').write_text(json.dumps(record))
        digest = hashlib.sha256((self.root / 'candidate.zip').read_bytes()).hexdigest()
        candidate.check_artifact(self.root, VERSION, 'a' * 40, SIGNER, digest)
        for commit, signer, sha in [('b' * 40, SIGNER, digest), ('a' * 40, 'B' * 40, digest), ('a' * 40, SIGNER, '0' * 64)]:
            with self.assertRaises(ValueError):
                candidate.check_artifact(self.root, VERSION, commit, signer, sha)

    def test_public_readback_matches_reviewed_bytes_and_rejects_checksum_failure(self):
        pom = next(data for name, data in self.blobs.items() if name.endswith('.pom'))
        (self.root / 'pom.xml').write_bytes(pom)
        original = public.bundle.validate
        def download(url, **kwargs):
            return io.BytesIO(self.blobs[url.removeprefix('https://repo.maven.apache.org/maven2/')])
        def validate(*args):
            return original(*args, signature_verifier=lambda *unused: None)
        with patch.object(public.urllib.request, 'urlopen', side_effect=download), patch.object(public.bundle, 'validate', side_effect=validate):
            public.verify(self.root / 'pom.xml', VERSION, SIGNER, self.receipt, self.root / 'public.json')
            self.assertEqual(json.loads((self.root / 'public.json').read_text())['result'], 'PUBLIC_BYTES_MATCH_REVIEWED_CANDIDATE')
            expected = json.loads(self.receipt.read_text())
            expected['entries'][next(iter(expected['entries']))] = '0' * 64
            self.receipt.write_text(json.dumps(expected))
            with self.assertRaises(ValueError):
                public.verify(self.root / 'pom.xml', VERSION, SIGNER, self.receipt, self.root / 'mismatch.json')
            checksum = next(name for name in self.blobs if name.endswith('.sha1'))
            self.blobs[checksum] = b'0' * 40
            with self.assertRaises(ValueError):
                public.verify(self.root / 'pom.xml', VERSION, SIGNER, self.receipt, self.root / 'failed.json')

    def test_upload_intent_blocks_ambiguous_reupload(self):
        calls = []
        def failed(*args):
            calls.append(args[0])
            raise TimeoutError('ambiguous upload response')
        with self.assertRaises(TimeoutError):
            recovery.recover(self.path, self.receipt, self.state, allow_upload=True, call=failed)
        with self.assertRaises(ValueError):
            recovery.recover(self.path, self.receipt, self.state, allow_upload=True, call=failed)
        self.assertEqual(len(calls), 1)

    def test_deployment_id_saved_before_status_failure(self):
        def call(endpoint, *args):
            if endpoint.startswith('/upload'):
                return ID.encode()
            self.assertEqual(json.loads(self.state.read_text())['deployment_id'], ID)
            raise TimeoutError('status timeout')
        with self.assertRaises(TimeoutError):
            recovery.recover(self.path, self.receipt, self.state, allow_upload=True, call=call)

    def test_resume_validates_then_publishes_same_id_once(self):
        self.saved()
        calls = []
        statuses = iter(['VALIDATED', 'PUBLISHING', 'PUBLISHED'])
        def call(endpoint, *args):
            calls.append(endpoint)
            return b'' if endpoint.startswith('/deployment/') else self.status(next(statuses))
        result = recovery.recover(self.path, self.receipt, self.state, publish=True, call=call, sleep=lambda _: None)
        self.assertEqual(result['phase'], 'PUBLISHED')
        self.assertEqual(calls.count('/deployment/' + ID), 1)
        self.assertFalse(any('/upload' in item for item in calls))

    def test_wrong_status_id_and_failed_state_block_publish(self):
        for response in (self.status('VALIDATED', '87654321-1234-1234-1234-123456789abc'), self.status('FAILED')):
            self.saved()
            calls = []
            def call(endpoint, *args):
                calls.append(endpoint)
                return response
            with self.assertRaises(ValueError):
                recovery.recover(self.path, self.receipt, self.state, publish=True, call=call)
            self.assertEqual(calls, ['/status?id=' + ID])

    def test_ambiguous_publish_never_repeated(self):
        self.saved(publish_attempted=True)
        with self.assertRaises(ValueError):
            recovery.recover(self.path, self.receipt, self.state, publish=True,
                             call=lambda *args: self.status('VALIDATED'))

    def test_changed_bundle_or_receipt_blocks_recovery(self):
        self.saved()
        self.receipt.write_text(self.receipt.read_text() + '\n')
        with self.assertRaises(ValueError):
            recovery.recover(self.path, self.receipt, self.state, call=lambda *args: self.fail('Network called'))

    def test_evidence_invalidates_only_declared_inputs(self):
        manifests = {group: [] for group in evidence.GROUPS}
        (self.root / 'runtime.txt').write_text('before')
        (self.root / 'config.txt').write_text('before')
        manifests['runtime'] = ['runtime.txt']
        manifests['configuration'] = ['config.txt']
        previous = evidence.fingerprint(self.root, manifests)
        (self.root / 'config.txt').write_text('after')
        current = evidence.fingerprint(self.root, manifests)
        gates = {'unit': {'inputs': ['runtime'], 'result': 'PASS', 'evidence': 'receipt', 'kind': 'local_test'},
                 'acceptance': {'inputs': ['configuration'], 'result': 'PASS', 'evidence': 'receipt', 'kind': 'local_test'},
                 'required': {'inputs': ['runtime'], 'result': 'PASS', 'evidence': 'receipt', 'kind': 'required_ci'}}
        result = evidence.reuse(previous, current, gates)
        self.assertTrue(result['unit']['reusable'])
        self.assertFalse(result['acceptance']['reusable'])
        self.assertFalse(result['required']['reusable'])

    def test_evidence_rejects_missing_or_escaping_inputs(self):
        for name in ('missing', '../outside', '/etc/hosts'):
            manifest = {group: [] for group in evidence.GROUPS}
            manifest['runtime'] = [name]
            with self.assertRaises(ValueError):
                evidence.fingerprint(self.root, manifest)


if __name__ == '__main__':
    unittest.main()
