import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('release_notes', Path(__file__).resolve().parents[1] / '.github/scripts/release-notes.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
POM = '<project><version>0.3.5-SNAPSHOT</version></project>'


class ReleaseNotesTests(unittest.TestCase):
    def test_exact_section_and_tagged_links(self):
        notes = '## 0.3.5 — 2026-10-12\nFixed recovery. [Guide](docs/releasing.md).\n## 0.3.4\nOlder change.'
        body = module.extract(POM, notes, 'v0.3.5')
        self.assertNotIn('Older change', body)
        self.assertIn('/blob/v0.3.5/docs/releasing.md', body)

    def test_missing_empty_duplicate_and_invalid_date_rejected(self):
        for notes in ('## 0.3.50 — 2026-10-12\nWrong.', '## 0.3.5 — 2026-10-12\n### Added\n<!-- TODO -->', '## 0.3.5 — 2026-10-12\nFirst.\n## 0.3.5 — 2026-10-12\nSecond.', '## 0.3.5 — 2026-02-30\nInvalid.'):
            with self.subTest(notes=notes), self.assertRaises(ValueError):
                module.extract(POM, notes, 'v0.3.5')

    def test_tag_mismatch_and_candidate_rejected(self):
        for tag in ('v0.3.4', 'v0.3.5-rc1'):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                module.extract(POM, '## 0.3.5 — 2026-10-12\nChange.', tag)
