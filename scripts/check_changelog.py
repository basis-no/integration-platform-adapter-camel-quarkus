#!/usr/bin/env python3
"""Validate this source version's notes before candidate or stable publication."""
import argparse
from datetime import date
from pathlib import Path
import re
import xml.etree.ElementTree as ET

VERSION = r'[0-9]+\.[0-9]+\.[0-9]+'


def validate(pom, changelog, tag=None):
    root = ET.fromstring(pom)
    version = root.findtext('{*}version')
    if not version or not re.fullmatch(VERSION + r'(?:-SNAPSHOT)?', version):
        raise ValueError('POM must declare its own X.Y.Z or X.Y.Z-SNAPSHOT version')
    target = version.removesuffix('-SNAPSHOT')
    if tag is not None and tag != 'v' + target:
        raise ValueError('Stable tag must match the source POM version: v' + target)
    headings = list(re.finditer(r'^##[ \t]+([^\n]+)', changelog, re.MULTILINE))
    matches = []
    for index, heading in enumerate(headings):
        title = heading.group(1).strip()
        match = re.match(r'^\[?(' + VERSION + r')\]?(?=\s|$)', title)
        if match and match.group(1) == target:
            end = headings[index + 1].start() if index + 1 < len(headings) else len(changelog)
            matches.append((title, changelog[heading.end():end]))
    if len(matches) != 1:
        raise ValueError('Expected exactly one changelog section for ' + target)
    title, body = matches[0]
    prose = re.sub(r'<!--.*?-->', '', body, flags=re.DOTALL)
    prose = re.sub(r'^\s*#+[^\n]*$', '', prose, flags=re.MULTILINE)
    if not re.search(r'[A-Za-z0-9]', prose):
        raise ValueError('Matching changelog section must contain release notes')
    if tag is not None:
        pending_heading = re.search(r'\b(unreleased|pending|planned|candidate)\b', title, re.IGNORECASE)
        pending_status = re.search(
            r'^\s*(?:[-*]\s+)?(?:(?:release(?: status)?|publication|status)\s*(?::|—|-)?\s*'
            r'(?:is\s+)?(?:pending|unreleased|planned|candidate)\b|'
            r'planned\s+(?:maven\s+)?(?:artifact|release|publication)\b)',
            body, re.IGNORECASE | re.MULTILINE)
        if pending_heading or pending_status:
            raise ValueError('Stable release notes still contain pending wording')
        dated = re.fullmatch(r'\[?' + re.escape(target) + r'\]?\s+[—–-]\s+(\d{4}-\d{2}-\d{2})', title)
        if not dated:
            raise ValueError('Stable section heading must include its approved ISO release date')
        try:
            date.fromisoformat(dated.group(1))
        except ValueError as error:
            raise ValueError('Invalid stable release date') from error
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pom', type=Path, default=Path('pom.xml'))
    parser.add_argument('--changelog', type=Path, default=Path('CHANGELOG.md'))
    parser.add_argument('--tag', help='Stable release tag; omit for candidate validation')
    args = parser.parse_args()
    try:
        target = validate(args.pom.read_text(), args.changelog.read_text(), args.tag)
    except (ValueError, OSError, ET.ParseError) as error:
        parser.exit(1, str(error) + '\n')
    print('Changelog validated for ' + target + (' (stable)' if args.tag else ' (candidate)'))


if __name__ == '__main__':
    main()
