#!/usr/bin/env python3
"""Extract reviewed notes for the exact stable source version."""
import argparse
from datetime import date
from pathlib import Path
import re
import xml.etree.ElementTree as ET


def extract(pom, changelog, tag):
    version = ET.fromstring(pom).findtext('{*}version')
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', tag):
        raise ValueError('Expected a final vX.Y.Z tag')
    if version not in (tag[1:], tag[1:] + '-SNAPSHOT'):
        raise ValueError('Tag does not match the source POM')
    headings = list(re.finditer(r'^##[ \t]+([^\n]+)', changelog, re.MULTILINE))
    matches = []
    for index, heading in enumerate(headings):
        title = heading[1].strip()
        if re.match(re.escape(tag[1:]) + r'(?=\s|$)', title):
            match = re.fullmatch(re.escape(tag[1:]) + r' [—–-] (\d{4}-\d{2}-\d{2})', title)
            if not match:
                raise ValueError('Release heading must include its approved ISO date')
            date.fromisoformat(match[1])
            end = headings[index + 1].start() if index + 1 < len(headings) else len(changelog)
            matches.append(changelog[heading.end():end].strip())
    if len(matches) != 1:
        raise ValueError('Expected exactly one dated changelog section for ' + tag)
    body = matches[0]
    prose = re.sub(r'<!--.*?-->', '', body, flags=re.DOTALL)
    prose = re.sub(r'^\s*#+[^\n]*$', '', prose, flags=re.MULTILINE)
    if not re.search(r'[A-Za-z0-9]', prose):
        raise ValueError('Release notes must contain substantive text')
    # Release pages resolve relative links differently from tracked Markdown.
    def link(match):
        target = match[2]
        if re.match(r'[A-Za-z][A-Za-z0-9+.-]*:', target) or target.startswith('#'):
            return match[0]
        if target.startswith('/') or '..' in target.split('/'):
            raise ValueError('Release notes require repository-relative or absolute web links')
        return '[' + match[1] + '](https://github.com/basis-no/integration-platform-adapter-camel-quarkus/blob/' + tag + '/' + target + ')'
    return re.sub(r'\[([^\]]+)\]\(([^\s)]+)\)', link, body) + '\n'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(extract(Path('pom.xml').read_text(), Path('CHANGELOG.md').read_text(), args.tag))
