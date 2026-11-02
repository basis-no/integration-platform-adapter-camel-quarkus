import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / '.github/scripts/public-content-check.py'
spec = importlib.util.spec_from_file_location('public_content_check', SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class WordingRuleTests(unittest.TestCase):
    def test_product_wording_is_refused(self):
        for line in ('Built for the Integration' + ' Platform.', 'See the public hand' + 'book.',
                     'Called by the dashboard' + ' agent.', 'Run the certification' + ' suite.'):
            with self.subTest(line=line):
                self.assertTrue(module.wording_hits(line))

    def test_identifiers_and_generic_words_pass(self):
        for line in ('<groupId>no.basis.integrationplatform</groupId>',
                     'https://github.com/basis-no/integration-platform-adapter-dotnet',
                     'from("platform-http:/orders")', 'x-dashboard-user', 'A mounted CA certificate.'):
            with self.subTest(line=line):
                self.assertEqual([], module.wording_hits(line))

    def test_rules_do_not_match_their_own_source(self):
        for line in SCRIPT.read_text(encoding='utf-8').splitlines():
            with self.subTest(line=line):
                self.assertEqual([], module.wording_hits(line))


if __name__ == '__main__':
    unittest.main()
