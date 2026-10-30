#!/usr/bin/env python3
"""Real synthetic Maven/GPG packaging qualification; never install or deploy."""
import argparse
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile

NS = 'http://maven.apache.org/POM/4.0.0'
GROUP, ARTIFACT, VERSION = 'org.example.packaging.fixture', 'signed-fixture', '1.2.3'
PLUGIN_IDS = {'maven-source-plugin', 'maven-javadoc-plugin', 'maven-gpg-plugin'}


def tag(name):
    return '{' + NS + '}' + name


def fixture_pom(source):
    """Use the source's exact signing/source/Javadoc plugins, never its publisher."""
    original = ET.parse(source).getroot()
    result = ET.Element(tag('project'))
    for key, value in [('modelVersion', '4.0.0'), ('groupId', GROUP),
                       ('artifactId', ARTIFACT), ('version', VERSION)]:
        ET.SubElement(result, tag(key)).text = value
    result.append(copy.deepcopy(original.find(tag('properties'))))
    plugins = ET.SubElement(ET.SubElement(result, tag('build')), tag('plugins'))
    compiler = next(p for p in original.findall('./' + tag('build') + '/' + tag('plugins') + '/' + tag('plugin'))
                    if p.findtext(tag('artifactId')) == 'maven-compiler-plugin')
    plugins.append(copy.deepcopy(compiler))
    base_javadoc = next(p for p in original.findall('./' + tag('build') + '/' + tag('plugins') + '/' + tag('plugin'))
                       if p.findtext(tag('artifactId')) == 'maven-javadoc-plugin')
    release = next(p for p in original.findall('./' + tag('profiles') + '/' + tag('profile'))
                   if p.findtext(tag('id')) == 'release')
    selected = [p for p in release.findall('./' + tag('build') + '/' + tag('plugins') + '/' + tag('plugin'))
                if p.findtext(tag('artifactId')) in PLUGIN_IDS]
    assert {p.findtext(tag('artifactId')) for p in selected} == PLUGIN_IDS
    for p in selected:
        child = copy.deepcopy(p)
        if p.findtext(tag('artifactId')) == 'maven-javadoc-plugin':
            child.insert(1, copy.deepcopy(base_javadoc.find(tag('version'))))
            child.append(copy.deepcopy(base_javadoc.find(tag('configuration'))))
        plugins.append(child)
    assert not result.findall('.//' + tag('extensions'))
    assert not any('deploy' in (x.text or '') or 'publish' in (x.text or '')
                   for x in result.findall('.//' + tag('goal')))
    ET.register_namespace('', NS)
    return ET.tostring(result, encoding='utf-8', xml_declaration=True)


def live(pid):
    p = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True, text=True, timeout=5)
    return p.returncode == 0 and p.stdout.strip() and not p.stdout.strip().startswith('Z')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--deadline-seconds', type=int, default=900)
    args = parser.parse_args()
    assert 30 <= args.deadline_seconds <= 1200
    source = Path(__file__).resolve().parents[2]
    assert not args.evidence.exists(), 'Evidence path must be new; preserve earlier results'
    args.evidence.mkdir(parents=True)
    start = time.monotonic()
    deadline = start + args.deadline_seconds
    receipt = {'coordinate': f'{GROUP}:{ARTIFACT}:{VERSION}', 'kind': 'synthetic packaging only',
               'source_sha256': {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in [source/'pom.xml', Path(__file__).resolve(),
                                           source/'.github/scripts/central-bundle.py',
                                           source/'.mvn/wrapper/maven-wrapper.properties', source/'mvnw',
                                           source/'.github/workflows/test.yml']},
               'commands': [], 'success': False, 'production_credentials_used': False,
               'portal_calls': False, 'installed_or_deployed': False}
    agent = None
    env = None
    work = None
    temporary = None
    groups = []
    def cancelled(signum, frame):
        raise KeyboardInterrupt('Owned qualification cancelled')
    signal.signal(signal.SIGTERM, cancelled)

    def run(command, name, limit=120):
        timeout = min(limit, deadline - time.monotonic())
        assert timeout > 0, 'Qualification deadline reached'
        log = args.evidence / (name + '.log')
        receipt['commands'].append({'name': name, 'argv': command, 'timeout_seconds': timeout})
        with log.open('w') as output:
            p = subprocess.Popen(command, cwd=work, env=env, stdout=output,
                                 stderr=subprocess.STDOUT, start_new_session=True)
            groups.append(p.pid)
            try:
                finish = time.monotonic() + timeout
                progress_at = time.monotonic()
                previous = -1
                while p.poll() is None:
                    size = log.stat().st_size
                    if size != previous:
                        progress_at = time.monotonic()
                        previous = size
                    if time.monotonic() >= finish or time.monotonic()-progress_at >= 180:
                        raise TimeoutError(name+' exceeded deadline or 180s output stall')
                    time.sleep(1)
                code = p.returncode
            finally:
                # Always stop the owned group, including children after parent exit.
                try:
                    os.killpg(p.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGKILL)
                    p.wait(timeout=5)
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        assert code == 0, f'{name} failed; preserved {log}'
        return log.read_text(errors='replace')

    try:
        temporary = tempfile.TemporaryDirectory(prefix='packaging-fixture-')
        work = Path(temporary.name)
        home = work/'home';home.mkdir(mode=0o700)
        keyring = work/'keyring';keyring.mkdir(mode=0o700)
        env = {key: os.environ[key] for key in ('PATH', 'JAVA_HOME', 'LANG', 'LC_ALL') if key in os.environ}
        env.update(HOME=str(home), GNUPGHOME=str(keyring), TMPDIR=str(work),
                   MAVEN_OPTS=f'-Xmx512m -XX:ActiveProcessorCount=2 -Duser.home={home}')
        settings = work/'settings.xml'
        settings.write_text('<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"/>\n')
        shutil.copy2(source/'mvnw', work/'mvnw')
        wrapper = work/'.mvn/wrapper';wrapper.mkdir(parents=True)
        for p in (source/'.mvn/wrapper').iterdir():
            assert p.is_file() and not p.is_symlink()
            shutil.copy2(p, wrapper/p.name)
        (work/'pom.xml').write_bytes(fixture_pom(source/'pom.xml'))
        java = work/'src/main/java/org/example/Fixture.java';java.parent.mkdir(parents=True)
        java.write_text('/** Synthetic packaging fixture. */\npackage org.example;\n'
                        '/** Public fixture API. */\npublic final class Fixture {\n'
                        '  /** Construct a fixture. */\n  public Fixture() {}\n}\n')
        java_version = run(['java', '-version'], 'java-version', 15)
        assert re.search(r'version "21(?:[."]|$)', java_version), 'Java21 required'
        run(['gpg', '--version'], 'gpg-version', 15)
        agent_text = run(['gpg-connect-agent', 'GETINFO pid', '/bye'], 'owned-agent-identity', 15)
        agent = int(re.search(r'^D (\d+)$', agent_text, re.M).group(1))
        run(['gpg', '--no-options', '--batch', '--pinentry-mode', 'loopback', '--passphrase', '',
             '--quick-generate-key', 'Packaging Fixture <fixture@example.invalid>', 'rsa2048', 'sign', '1d'],
            'ephemeral-key-generation', 60)
        listing = run(['gpg', '--no-options', '--batch', '--with-colons', '--list-secret-keys'], 'owned-key-identity', 15)
        keys = [line.split(':')[9] for line in listing.splitlines() if line.startswith('fpr:')]
        assert len(keys) == 1 and re.fullmatch('[A-F0-9]{40}', keys[0])
        signer = keys[0];receipt['signer_fingerprint'] = signer
        mvn = [str(work/'mvnw'), '--batch-mode', '--no-transfer-progress', '-s', str(settings), '-gs', str(settings)]
        run(mvn+['--version'], 'maven-wrapper-version', 120)
        run(mvn+['help:effective-pom', '-Doutput='+str(args.evidence.resolve()/'effective-pom.xml')], 'effective-pom', 180)
        # Generated POM has no publisher or deployment plugin. Only verify is used.
        run(mvn+['clean', 'verify', '-Dgpg.keyname='+signer], 'signed-maven-verify', 600)
        spec = importlib.util.spec_from_file_location('bundle', source/'.github/scripts/central-bundle.py')
        bundle = importlib.util.module_from_spec(spec);spec.loader.exec_module(bundle)
        archive = args.evidence.resolve()/'candidate.zip'
        bundle.assemble(work/'target', archive, GROUP, ARTIFACT, VERSION)
        old = os.environ.get('GNUPGHOME')
        os.environ['GNUPGHOME'] = str(keyring)
        try:
            def verify(data, signature, fingerprint):
                assert time.monotonic() < deadline, 'Signature phase deadline reached'
                return bundle.verify_signature(data, signature, fingerprint)
            valid = bundle.validate(archive, GROUP, ARTIFACT, VERSION, signer, signature_verifier=verify)
            _, payloads = bundle.coordinate(GROUP, ARTIFACT, VERSION)
            with zipfile.ZipFile(archive) as z:
                payload, signature = z.read(payloads[1]), z.read(payloads[1]+'.asc')
            for name, data, expected in [('corrupted-payload', payload+b'changed', signer),
                                         ('wrong-signer', payload, '0'*40)]:
                try:
                    verify(data, signature, expected)
                except ValueError:
                    receipt[name+'_rejected'] = True
                else:
                    raise AssertionError(name+' was accepted')
            receipt['bundle'] = valid
        finally:
            if old is None:
                os.environ.pop('GNUPGHOME', None)
            else:
                os.environ['GNUPGHOME'] = old
        receipt['success'] = True
    except BaseException as error:
        receipt['success'] = False
        receipt['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        # Keep the keyring until its dedicated agent has stopped; then remove it.
        try:
            if env is not None:
                stopped = subprocess.run(['gpgconf', '--homedir', env['GNUPGHOME'], '--kill', 'all'], env=env,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
                if stopped.returncode:
                    receipt['agent_shutdown_failed'] = True
            if agent is not None and live(agent):
                os.kill(agent, signal.SIGTERM)
                until = time.monotonic() + 5
                while live(agent) and time.monotonic() < until:
                    time.sleep(0.1)
                if live(agent):
                    os.kill(agent, signal.SIGKILL)
                    time.sleep(0.2)
            receipt['owned_agent_running'] = agent is not None and bool(live(agent))
        except Exception as error:
            receipt['cleanup_error'] = type(error).__name__ + ': ' + str(error)
            receipt['success'] = False
        finally:
            if temporary is not None:
                temporary.cleanup()
        receipt['ephemeral_keyring_removed'] = env is None or not Path(env['GNUPGHOME']).exists()
        receipt['temporary_workspace_removed'] = work is None or not work.exists()
        alive = []
        for group in groups:
            try:
                os.killpg(group, 0)
                alive.append(group)
            except ProcessLookupError:
                pass
        receipt['owned_process_groups_running'] = alive
        receipt['success'] = (receipt['success'] and receipt['ephemeral_keyring_removed']
                              and not receipt.get('owned_agent_running', True) and not alive
                              and not receipt.get('agent_shutdown_failed', False))
        receipt['wall_seconds'] = round(time.monotonic()-start, 2)
        (args.evidence/'result.json').write_text(json.dumps(receipt, indent=2)+'\n')
    assert receipt['success'], 'Cleanup or qualification failed; inspect result.json'


if __name__ == '__main__':
    main()
