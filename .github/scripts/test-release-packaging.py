#!/usr/bin/env python3
"""Bounded, offline fixtures for packaging and recovery guards."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
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
signer = load('sign-candidate')
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
        candidate.unsigned(self.root, VERSION, 'a' * 40, output, '42')
        receipt = json.loads((output / 'unsigned.json').read_text())
        self.assertEqual(receipt['qualification'], 'UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE')
        self.assertEqual(receipt['candidate_run'], '42')
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
            candidate.unsigned(self.root, VERSION, 'b' * 40, output, '42')
        with self.assertRaises(ValueError):
            candidate.unsigned(self.root, VERSION, 'a' * 40, output, '')
        payload = target / (ARTIFACT + '-' + VERSION + '.jar')
        payload.unlink()
        payload.symlink_to(self.path)
        with self.assertRaises(ValueError):
            candidate.unsigned(self.root, VERSION, 'a' * 40, output, '42')
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
        self.assertIn('--workflow-run "$GITHUB_RUN_ID"', text)
        snapshot = (workflows / 'publish-snapshot.yml').read_text()
        self.assertIn('exit 1', snapshot)
        self.assertNotIn('checkout@', snapshot)
        self.assertNotIn('mvnw', snapshot)
        # Post-publication readback only: public key variable, never a signer or portal token.
        # Renamed so the publish-central.yml workflow, tag-triggered in older revisions, stays disabled.
        self.assertFalse((workflows / 'publish-central.yml').exists())
        readback = (workflows / 'release-page.yml').read_text()
        self.assertNotIn('push:', readback)
        self.assertNotIn('secrets.', readback)
        self.assertNotIn('central-recovery.py', readback)
        self.assertNotIn('mvnw', readback)
        self.assertNotIn('--detach-sign', readback)
        self.assertIn('vars.RELEASE_SIGNING_PUBLIC_KEY', readback)
        self.assertIn('name: unsigned-central-candidate-', readback)
        self.assertIn('sign-candidate.py verify', readback)
        self.assertNotIn('sign-candidate.py sign', readback)

    def test_release_workflow_keeps_project_code_away_from_credentials(self):
        workflows = Path(__file__).resolve().parents[1] / 'workflows'
        texts = {path.name: path.read_text() for path in sorted(workflows.glob('*.yml'))}
        for name, text in texts.items():
            for line in text.splitlines():
                if line.strip().startswith(('uses:', '- uses:')):
                    self.assertRegex(line, r'@[0-9a-f]{40} # v', name + ': pin every action by full SHA')
            for legacy in ('CENTRAL_TOKEN_USERNAME }}', 'CENTRAL_TOKEN_PASSWORD }}', 'GPG_PRIVATE_KEY', 'GPG_PASSPHRASE'):
                self.assertNotIn('secrets.' + legacy, text, name + ': repository-level release secret')
            for trigger in ('pull_request_target', 'workflow_run', 'issue_comment'):
                self.assertNotIn(trigger, text, name)
            if name != 'release.yml':
                self.assertNotIn('secrets.', text, name)
                self.assertNotRegex(text, r'(?m)^\s+tags:', name + ': only release.yml may start on a tag')
        text = texts['release.yml']
        head, body = text.split('\njobs:\n')
        self.assertIn("  push:\n    tags:\n      - 'v[0-9]+.[0-9]+.[0-9]+'\n", head)
        self.assertNotIn('workflow_dispatch', head)
        self.assertIn('\npermissions: {}\n', head)
        jobs = dict(re.findall(r'(?ms)^  ([a-z-]+):\n(.*?)(?=^  [a-z-]+:\n|\Z)', body))
        self.assertEqual(list(jobs), ['build', 'attest', 'sign-upload', 'publish'])
        for name, job in jobs.items():
            self.assertIn('    permissions:\n', job, name)
            self.assertNotIn('cache', job, name + ': no shared caches next to release jobs')
            # Values reach scripts only through env, never spliced into script text.
            scripts = re.findall(r'(?m)^( +)run: (.*\n(?:\1  .*\n|\s*\n)*)', job)
            self.assertTrue(scripts, name)
            for _, script in scripts:
                self.assertNotIn('${{', script, name)
        build, attest, sign, publish = jobs.values()
        self.assertIn('-Dgpg.skip=true', build)
        self.assertIn('--workflow-run "$GITHUB_RUN_ID"', build)
        self.assertIn('source --tagged', build)
        for forbidden in ('secrets.', 'id-token', 'environment:', 'write'):
            self.assertNotIn(forbidden, build)
        # Only attest may request an OIDC token, and it runs no project code.
        self.assertIn('id-token: write', attest)
        for forbidden in ('secrets.', 'checkout@', 'mvnw', '.github/scripts', 'environment:'):
            self.assertNotIn(forbidden, attest)
        self.assertIn('subject-checksums:', attest)
        for job, environment in ((sign, 'release'), (publish, 'release-publish')):
            self.assertIn(f'    environment: {environment}\n', job)
            self.assertIn(f'environments/{environment}', job)
            for forbidden in ('id-token', 'mvnw', 'contents: write', 'candidate/.github', 'python3 .github'):
                self.assertNotIn(forbidden, job)
            self.assertEqual(job.count('checkout@'), 1)
            self.assertIn('ref: ${{ vars.TRUSTED_TOOLING_SHA }}\n          path: tooling\n'
                          '          sparse-checkout: .github/scripts\n', job)
            self.assertEqual(set(re.findall(r'secrets\.([A-Z_]+)', job)) - {'RELEASE_SIGNING_SUBKEY'},
                             {'RELEASE_CENTRAL_TOKEN_USERNAME', 'RELEASE_CENTRAL_TOKEN_PASSWORD'})
            for script in re.findall(r'python3 (?:-I )?(\S+)', job):
                self.assertTrue(script.startswith('tooling/.github/scripts/'), script)
        self.assertIn('secrets.RELEASE_SIGNING_SUBKEY', sign)
        self.assertNotIn('secrets.RELEASE_SIGNING_SUBKEY', publish)
        self.assertIn('--prepare-upload', sign)
        self.assertNotIn('--publish', sign)
        self.assertNotIn('--allow-upload', publish)
        self.assertIn('/dev/shm', sign)

    def test_candidate_selection_requires_exact_inputs_and_refuses_upload_rerun(self):
        candidate.selection('inspect', VERSION, 'a' * 40, SIGNER)
        candidate.selection('upload', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64)
        candidate.selection('readback', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64)
        for args in [('inspect', VERSION, 'main', SIGNER),
                     ('inspect', VERSION + '-SNAPSHOT', 'a' * 40, SIGNER),
                     ('upload', VERSION, 'a' * 40, SIGNER),
                     ('upload', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64, '2'),
                     ('publish', VERSION, 'a' * 40, SIGNER, '123', '456', 'b' * 64, '2'),
                     ('resume', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64),
                     ('publish', VERSION, 'a' * 40, SIGNER, '123', '', 'b' * 64),
                     ('readback', VERSION, 'a' * 40, SIGNER, '', '', 'b' * 64),
                     ('readback', VERSION, 'a' * 40, SIGNER, '123', '456', 'b' * 64),
                     ('readback', VERSION, 'a' * 40, SIGNER, '123', '', '')]:
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

    def test_tagged_source_requires_the_tag_on_the_exact_commit(self):
        pom = next(data for name, data in self.blobs.items() if name.endswith('.pom'))
        (self.root / 'pom.xml').write_bytes(pom.replace(VERSION.encode(), (VERSION + '-SNAPSHOT').encode()))
        head = 'a' * 40 + '\n'
        with patch.object(candidate.subprocess, 'check_output', side_effect=[head, '', 'v' + VERSION, head]):
            candidate.source(self.root, VERSION, 'a' * 40, tagged=True)
        for outputs in ([head, '', ''], [head, '', 'v' + VERSION, 'b' * 40 + '\n']):
            with patch.object(candidate.subprocess, 'check_output', side_effect=outputs):
                with self.assertRaisesRegex(ValueError, 'tag'):
                    candidate.source(self.root, VERSION, 'a' * 40, tagged=True)

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


def jar(entries):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w') as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return stream.getvalue()


def gpg(home, *args):
    return subprocess.run(['gpg', '--no-options', '--batch', *args], env={**os.environ, 'GNUPGHOME': str(home)},
                          capture_output=True, check=True, timeout=60)


def synthetic_key(home, uid):
    """Throwaway unprotected key in a temporary keyring; never a real signer."""
    gpg(home, '--pinentry-mode', 'loopback', '--passphrase', '', '--quick-generate-key', uid, 'ed25519', 'sign', '1d')
    listing = gpg(home, '--with-colons', '--list-secret-keys', uid).stdout.decode()
    return next(line.split(':')[9] for line in listing.splitlines() if line.startswith('fpr:'))


class OfflineSigner(unittest.TestCase):
    """Real gpg signing with synthetic keys in temporary GNUPGHOME directories."""
    COMMIT, RUN = 'c' * 40, '4242'

    @classmethod
    def setUpClass(cls):
        cls.keys = tempfile.TemporaryDirectory(prefix='signer-keys-')
        cls.homes = [Path(cls.keys.name) / name for name in ('g', 'o')]
        for home in cls.homes:
            home.mkdir(mode=0o700)
        cls.home, other = cls.homes
        cls.previous = os.environ.get('GNUPGHOME')
        os.environ['GNUPGHOME'] = str(cls.home)
        cls.fingerprint = synthetic_key(cls.home, 'Synthetic Signer <signer@example.invalid>')
        cls.extra = synthetic_key(cls.home, 'Synthetic Extra <extra@example.invalid>')
        # Public key only: the workstation can verify but cannot sign as this key.
        cls.public_only = synthetic_key(other, 'Synthetic Public <public@example.invalid>')
        exported = gpg(other, '--armor', '--export', cls.public_only).stdout
        subprocess.run(['gpg', '--no-options', '--batch', '--import'], input=exported, capture_output=True,
                       env={**os.environ, 'GNUPGHOME': str(cls.home)}, check=True, timeout=60)

    @classmethod
    def tearDownClass(cls):
        for home in cls.homes:
            subprocess.run(['gpgconf', '--homedir', str(home), '--kill', 'all'], capture_output=True, timeout=15)
        if cls.previous is None:
            os.environ.pop('GNUPGHOME', None)
        else:
            os.environ['GNUPGHOME'] = cls.previous
        cls.keys.cleanup()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='signer-case-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        project = ('<project xmlns="http://maven.apache.org/POM/4.0.0"><groupId>{}</groupId>'
                   '<artifactId>{}</artifactId><version>{}</version></project>')
        self.pom = self.root / 'trusted-pom.xml'
        self.pom.write_text(project.format(GROUP, ARTIFACT, VERSION + '-SNAPSHOT'))
        self.candidate = self.root / 'unsigned-central-candidate'
        unsigned = self.candidate / 'unsigned'
        unsigned.mkdir(parents=True)
        properties = f'groupId={GROUP}\nartifactId={ARTIFACT}\nversion={VERSION}\n'
        self.payloads = {
            f'{ARTIFACT}-{VERSION}.pom': project.format(GROUP, ARTIFACT, VERSION).encode(),
            f'{ARTIFACT}-{VERSION}.jar': jar({'a/A.class': b'fixture',
                                              f'META-INF/maven/{GROUP}/{ARTIFACT}/pom.properties': properties}),
            f'{ARTIFACT}-{VERSION}-sources.jar': jar({'a/A.java': b'class A {}'}),
            f'{ARTIFACT}-{VERSION}-javadoc.jar': jar({'index.html': b'<html></html>'})}
        for name, data in self.payloads.items():
            (unsigned / name).write_bytes(data)
        self.write_receipt()
        (self.candidate / 'provenance.json').write_text(json.dumps({
            'source_commit': self.COMMIT, 'version': VERSION, 'group': GROUP, 'artifact': ARTIFACT,
            'workflow_head': 'd' * 40}))
        (self.candidate / 'effective-pom.xml').write_text('<project/>')
        (self.candidate / 'release-notes.md').write_text('Notes.\n')
        self.output = self.root / 'signed-output'

    def write_receipt(self, **changes):
        receipt = {'source_commit': self.COMMIT, 'version': VERSION, 'candidate_run': self.RUN,
                   'qualification': 'UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE',
                   'payload_sha256': {name: hashlib.sha256(data).hexdigest() for name, data in self.payloads.items()},
                   **changes}
        (self.candidate / 'unsigned.json').write_text(json.dumps(receipt))

    def sign(self, fingerprint=None, **changes):
        arguments = {'directory': self.candidate, 'version': VERSION, 'commit': self.COMMIT, 'run': self.RUN,
                     'signer': fingerprint or self.fingerprint, 'output': self.output, 'pom': self.pom, **changes}
        return signer.sign(**arguments)

    def refused(self, pattern=None, **changes):
        with self.assertRaisesRegex(ValueError, pattern or '.'):
            self.sign(**changes)
        self.assertFalse(self.output.exists(), 'Nothing may be signed or written before verification passes')

    def cli(self, action='sign', *extra):
        command = [sys.executable, '-I', str(Path(__file__).with_name('sign-candidate.py')), action,
                   '--candidate', str(self.candidate), '--version', VERSION, '--source-commit', self.COMMIT,
                   '--candidate-run', self.RUN, '--pom', str(self.pom), *extra]
        return subprocess.run(command, capture_output=True, text=True, timeout=120,
                              env={**os.environ, 'GNUPGHOME': str(self.home)})

    def test_happy_path_signs_exact_payloads_and_validates_bundle(self):
        result = self.cli('verify')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['result'], 'UNSIGNED_CANDIDATE_MATCHES_RECEIPT')
        result = self.cli('sign', '--signer', self.fingerprint, '--output', str(self.output))
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads((self.output / 'signing.json').read_text())
        validation = json.loads((self.output / 'candidate.json').read_text())
        archive = self.output / 'candidate.zip'
        self.assertEqual(receipt['result'], 'SIGNED_BUNDLE_VALIDATED_NOT_UPLOADED')
        self.assertEqual(receipt['signer_fingerprint'], self.fingerprint)
        self.assertEqual((receipt['version'], receipt['source_commit'], receipt['candidate_run']),
                         (VERSION, self.COMMIT, self.RUN))
        self.assertEqual(receipt['bundle_sha256'], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(receipt['entries_sha256'], validation['entries_sha256'])
        self.assertEqual(set(receipt['signature_sha256']), {name + '.asc' for name in self.payloads})
        self.assertIn('gpg', receipt['tools']['gpg'][0].lower())
        self.assertEqual(sorted(p.name for p in (self.output / 'signed').iterdir()),
                         sorted([*self.payloads, *(name + '.asc' for name in self.payloads)]))
        # Independent real-gpg validation and exact payload bytes inside the bundle.
        bundle.validate(archive, GROUP, ARTIFACT, VERSION, self.fingerprint)
        root, names = bundle.coordinate(GROUP, ARTIFACT, VERSION)
        with zipfile.ZipFile(archive) as content:
            for name in names:
                self.assertEqual(content.read(name), self.payloads[Path(name).name])
        with self.assertRaises(ValueError):
            bundle.validate(archive, GROUP, ARTIFACT, VERSION, self.extra)
        # The same bundle and receipt feed the workstation upload tool (no network here).
        state = recovery.recover(archive, self.output / 'candidate.json', self.root / 'deployment.json',
                                 prepare_upload=True, call=lambda *args: self.fail('Portal accessed'))
        self.assertEqual(state['bundle_sha256'], receipt['bundle_sha256'])
        # Post-publication CI readback: public bytes vs unsigned receipt plus signed entries digest.
        public_files = {}
        with zipfile.ZipFile(archive) as content:
            public_files = {name: content.read(name) for name in content.namelist()}
        def download(url, **kwargs):
            return io.BytesIO(public_files[url.removeprefix('https://repo.maven.apache.org/maven2/')])
        with patch.object(public.urllib.request, 'urlopen', side_effect=download):
            public.verify(self.pom, VERSION, self.fingerprint, None, self.root / 'public.json',
                          self.candidate / 'unsigned.json', receipt['entries_sha256'])
            self.assertEqual(json.loads((self.root / 'public.json').read_text())['entries_sha256'],
                             receipt['entries_sha256'])
            with self.assertRaises(ValueError):
                public.verify(self.pom, VERSION, self.fingerprint, None, self.root / 'mismatch.json',
                              self.candidate / 'unsigned.json', '0' * 64)

    def test_tampered_payload_rejected_before_signing(self):
        path = self.candidate / 'unsigned' / f'{ARTIFACT}-{VERSION}.jar'
        path.write_bytes(path.read_bytes() + b'tampered')
        self.refused('SHA-256')
        result = self.cli('verify')
        self.assertNotEqual(result.returncode, 0)

    def test_wrong_signer_rejected(self):
        self.refused('fingerprint', fingerprint=self.fingerprint.lower())
        self.refused('fingerprint', fingerprint=self.fingerprint[-16:])
        # Public key only: gpg cannot sign, and no completed receipt is written.
        result = self.cli('sign', '--signer', self.public_only, '--output', str(self.output))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.output / 'signing.json').exists())
        self.assertFalse((self.output / 'candidate.zip').exists())

    def test_signature_by_another_key_rejected_after_signing(self):
        original = signer.detach_sign
        def wrong_key(payload, signature, fingerprint):
            original(payload, signature, self.extra)
        with patch.object(signer, 'detach_sign', side_effect=wrong_key):
            with self.assertRaisesRegex(ValueError, 'expected signer'):
                self.sign()
        self.assertFalse((self.output / 'candidate.zip').exists())
        self.assertFalse((self.output / 'signing.json').exists())

    def test_extra_or_missing_files_rejected(self):
        for extra in ('extra.txt', 'unsigned/extra.jar', 'unsigned/.hidden'):
            (self.candidate / extra).write_bytes(b'x')
            self.refused('Unexpected')
            (self.candidate / extra).unlink()
        (self.candidate / 'nested').mkdir()
        self.refused('Unexpected')
        (self.candidate / 'nested').rmdir()
        for missing in (f'unsigned/{ARTIFACT}-{VERSION}-javadoc.jar', 'effective-pom.xml', 'unsigned.json'):
            data = (self.candidate / missing).read_bytes()
            (self.candidate / missing).unlink()
            self.refused('Missing')
            (self.candidate / missing).write_bytes(data)
        self.sign()

    def test_symlinks_rejected(self):
        outside = self.root / 'outside'
        outside.mkdir()
        for name in (f'unsigned/{ARTIFACT}-{VERSION}.jar', 'provenance.json'):
            path = self.candidate / name
            copy = outside / Path(name).name
            copy.write_bytes(path.read_bytes())
            path.unlink()
            path.symlink_to(copy)  # Identical bytes still fail: links are never followed.
            self.refused('Symbolic link')
            path.unlink()
            path.write_bytes(copy.read_bytes())
        link = self.root / 'linked-candidate'
        link.symlink_to(self.candidate)
        self.refused('real directory', directory=link)
        shutil.move(str(self.candidate / 'unsigned'), str(outside / 'unsigned'))
        (self.candidate / 'unsigned').symlink_to(outside / 'unsigned')
        self.refused('Symbolic link')

    def test_receipt_mismatch_rejected(self):
        self.refused('mismatch', commit='e' * 40)
        self.refused('mismatch', run='4243')
        self.refused('Missing|Unexpected', version='1.2.4')
        self.write_receipt(qualification='RELEASE_ELIGIBLE')
        self.refused('marker')
        self.write_receipt(extra='field')
        self.refused('fields')
        hashes = {name: hashlib.sha256(data).hexdigest() for name, data in self.payloads.items()}
        name = f'{ARTIFACT}-{VERSION}.jar'
        hashes['../' + name] = hashes.pop(name)
        self.write_receipt(payload_sha256=hashes)
        self.refused('payload names')
        self.write_receipt(payload_sha256={**hashes, name: '0' * 64})
        self.refused('payload names')
        self.write_receipt()
        (self.candidate / 'provenance.json').write_text(json.dumps({
            'source_commit': self.COMMIT, 'version': VERSION, 'group': 'org.other', 'artifact': ARTIFACT}))
        self.refused('provenance')

    def test_candidate_output_and_trusted_checkout_stay_separate(self):
        self.refused('separate', output=self.candidate / 'signed')
        with patch.object(signer, 'CHECKOUT', self.root):
            self.refused('separate')
        self.output.mkdir()
        with self.assertRaisesRegex(ValueError, 'new'):
            self.sign()

    def test_signing_subkey_only_keyring_signs_for_the_primary_fingerprint(self):
        """The CI design: certify-only primary stays offline; CI holds one signing subkey."""
        source, ci = (Path(self.keys.name) / name for name in ('primary-' + self.id()[-8:], 'ci-' + self.id()[-8:]))
        for home in (source, ci):
            home.mkdir(mode=0o700)
            self.addCleanup(subprocess.run, ['gpgconf', '--homedir', str(home), '--kill', 'all'],
                            capture_output=True, timeout=15)
        uid = 'Synthetic Release <release@example.invalid>'
        gpg(source, '--pinentry-mode', 'loopback', '--passphrase', '', '--quick-generate-key', uid, 'rsa3072', 'cert', '1d')
        primary = next(line.split(':')[9] for line in gpg(source, '--with-colons', '--list-keys', uid)
                       .stdout.decode().splitlines() if line.startswith('fpr:'))
        gpg(source, '--pinentry-mode', 'loopback', '--passphrase', '', '--quick-add-key', primary, 'rsa3072', 'sign', '1d')
        exported = gpg(source, '--armor', '--export-secret-subkeys', primary).stdout
        subprocess.run(['gpg', '--no-options', '--batch', '--import'], input=exported, capture_output=True,
                       env={**os.environ, 'GNUPGHOME': str(ci)}, check=True, timeout=60)
        listing = gpg(ci, '--with-colons', '--list-secret-keys').stdout.decode().splitlines()
        self.assertEqual([line.split(':')[14] for line in listing if line.startswith('sec:')], ['#'])
        command = [sys.executable, '-I', str(Path(__file__).with_name('sign-candidate.py')), 'sign',
                   '--candidate', str(self.candidate), '--version', VERSION, '--source-commit', self.COMMIT,
                   '--candidate-run', self.RUN, '--pom', str(self.pom), '--signer', primary, '--output', str(self.output)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=120, env={**os.environ, 'GNUPGHOME': str(ci)})
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads((self.output / 'signing.json').read_text())
        self.assertEqual(receipt['signer_fingerprint'], primary)
        self.assertEqual(receipt['result'], 'SIGNED_BUNDLE_VALIDATED_NOT_UPLOADED')

    def test_no_passphrase_input_and_no_candidate_execution(self):
        command = signer.sign_command(Path('payload'), Path('payload.asc'), self.fingerprint)
        self.assertEqual(command[:2], ['gpg', '--no-options'])
        self.assertIn('--detach-sign', command)
        self.assertEqual(command[command.index('--local-user') + 1], self.fingerprint)
        self.assertFalse(any('passphrase' in item or 'pinentry' in item or 'loopback' in item for item in command))
        result = self.cli('sign', '--signer', self.fingerprint, '--output', str(self.output), '--passphrase', 'x')
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.output.exists())
        source = Path(__file__).with_name('sign-candidate.py').read_text()
        self.assertNotIn('mvn', source)
        self.assertNotIn('PASSPHRASE', source)
        self.assertNotIn('environ', source)


if __name__ == '__main__':
    unittest.main()
