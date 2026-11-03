#!/usr/bin/env python3
"""Fingerprint declared release inputs; conservatively assess passed-gate reuse."""
import argparse
import hashlib
import json
from pathlib import Path

GROUPS = {'runtime', 'dependencies', 'contract', 'fixtures', 'toolchain', 'configuration'}
PHASES = {'setup', 'implementation', 'build_test', 'resource_queue', 'runner_queue',
          'approval_access', 'recovery_rework'}


def fingerprint(root, manifest):
    if set(manifest) != GROUPS:
        raise ValueError('Declare every input group, including explicit empty groups')
    root = root.resolve()
    result = {}
    for group, names in sorted(manifest.items()):
        if not isinstance(names, list) or len(names) != len(set(names)):
            raise ValueError('Input paths must be a unique list')
        files = {}
        for name in sorted(names):
            path = root / name
            if Path(name).is_absolute() or path.is_symlink() or not path.resolve().is_relative_to(root):
                raise ValueError('Input path must remain inside the declared root')
            if not path.is_file():
                raise ValueError('Missing input; declare concrete files, not globs/directories')
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        result[group] = {'files': files, 'sha256': hashlib.sha256(
            json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
    return result


def reuse(previous, current, gates):
    if set(previous) != GROUPS or set(current) != GROUPS:
        raise ValueError('Incomplete input fingerprint')
    changed = {group for group in GROUPS if previous[group] != current[group]}
    result = {}
    for gate, definition in gates.items():
        kind = definition.get('kind')
        if kind not in ('local_test', 'documentation', 'required_ci', 'public_artifacts', 'environment_acceptance'):
            raise ValueError('Explicit supported gate kind required')
        dependencies = definition.get('inputs', [])
        if not dependencies or not set(dependencies) <= GROUPS:
            raise ValueError('Every gate must explicitly declare known relevant inputs')
        affected = sorted(changed & set(dependencies))
        passed = definition.get('result') == 'PASS' and bool(definition.get('evidence'))
        # Required CI, actual public artifacts and changed-environment acceptance
        # are independent gates, never inferred from unchanged runtime inputs.
        independent = kind in ('required_ci', 'public_artifacts', 'environment_acceptance')
        result[gate] = {'reusable': passed and not affected and not independent,
                        'changed_inputs': affected,
                        'reason': ('independent verification gate' if independent else
                                   'inputs changed' if affected else
                                   'passed evidence missing' if not passed else 'declared inputs unchanged'),
                        'evidence': definition.get('evidence')}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--previous', type=Path)
    parser.add_argument('--gates', type=Path)
    args = parser.parse_args()
    if bool(args.previous) != bool(args.gates):
        parser.error('--previous and --gates must be supplied together')
    result = {'inputs': fingerprint(args.root, json.loads(args.manifest.read_text())),
              'phases': [], 'phase_fields': ['pool', 'phase', 'start', 'end', 'reason'],
              'phase_names': sorted(PHASES), 'wall_seconds': None,
              'concurrent_job_seconds': None}
    if args.previous:
        result['gate_reuse'] = reuse(json.loads(args.previous.read_text())['inputs'],
                                     result['inputs'], json.loads(args.gates.read_text()))
    with args.output.open('x') as handle:
        json.dump(result, handle, indent=2)
        handle.write('\n')


if __name__ == '__main__':
    main()
