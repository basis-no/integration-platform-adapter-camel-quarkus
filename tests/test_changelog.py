import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from check_changelog import validate

POM = '<project xmlns="http://maven.apache.org/POM/4.0.0"><version>0.3.5-SNAPSHOT</version></project>'
CANDIDATE = '## 0.3.5 — unreleased maintenance candidate\n\nPlanned artifact. Added opt-in delivery recording.\n'
STABLE = '## 0.3.5 — 2026-10-12\n\nAdded opt-in delivery recording.\n'


class ChangelogTests(unittest.TestCase):
    def test_candidate_and_stable_with_snapshot_source(self):
        self.assertEqual(validate(POM, CANDIDATE), '0.3.5')
        self.assertEqual(validate(POM, STABLE, 'v0.3.5'), '0.3.5')

    def test_stable_source_version(self):
        self.assertEqual(validate(POM.replace('-SNAPSHOT', ''), STABLE, 'v0.3.5'), '0.3.5')

    def test_missing_exact_version(self):
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            validate(POM, CANDIDATE.replace('0.3.5', '0.3.50'))

    def test_duplicate_section(self):
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            validate(POM, CANDIDATE + STABLE)

    def test_empty_section_cannot_borrow_older_notes(self):
        with self.assertRaisesRegex(ValueError, 'contain release notes'):
            validate(POM, '## 0.3.5 — unreleased\n### Added\n<!-- TODO -->\n## 0.3.4\nOlder release notes.\n')

    def test_tag_must_match_pom_before_version_rewrite(self):
        for tag in ('v0.3.4', '0.3.5', 'v0.3.5-rc1', 'v0.3.50'):
            with self.subTest(tag=tag), self.assertRaisesRegex(ValueError, 'match the source POM'):
                validate(POM, STABLE, tag)

    def test_missing_own_pom_version(self):
        with self.assertRaisesRegex(ValueError, 'POM must declare'):
            validate('<project><parent><version>0.3.5</version></parent></project>', STABLE)

    def test_stable_rejects_pending_heading_and_status_declarations(self):
        for notes in (CANDIDATE, STABLE + '\nPublication pending.',
                      STABLE + '\nRelease status: pending', STABLE + '\nStatus: unreleased',
                      STABLE + '\nPlanned Maven artifact:', STABLE + '\n- Release is candidate'):
            with self.subTest(notes=notes), self.assertRaisesRegex(ValueError, 'pending wording'):
                validate(POM, notes, 'v0.3.5')

    def test_stable_accepts_technical_prose_about_candidates_and_pending_work(self):
        notes = STABLE + '\nFixed candidate CI validation.\nHandle pending deliveries correctly.\nAdded planned downtime handling.\n'
        self.assertEqual(validate(POM, notes, 'v0.3.5'), '0.3.5')

    def test_stable_needs_valid_date(self):
        for heading in ('0.3.5', '0.3.5 — 2026-02-30'):
            with self.subTest(heading=heading), self.assertRaisesRegex(ValueError, 'date'):
                validate(POM, '## ' + heading + '\nRelease notes.', 'v0.3.5')

    def test_historical_sections_do_not_require_invented_dates(self):
        historical = '\n## 0.3.4\nHistorical notes without a reconstructed release date.\n'
        self.assertEqual(validate(POM, STABLE + historical, 'v0.3.5'), '0.3.5')
        self.assertEqual(validate(POM, CANDIDATE + historical), '0.3.5')


if __name__ == '__main__':
    unittest.main()
