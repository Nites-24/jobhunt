import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from job_ingestor.freehire import normalize, fetch_jobs
from tools.ops.skill_demand import analyze, extract, format_telegram


class TestFreeHire(unittest.TestCase):
    def test_normalize_us_ai(self):
        job = normalize({"title": "AI Engineer", "company": "Acme",
                         "url": "https://example.com/jobs/1", "countries": ["US"],
                         "public_slug": "ai-engineer-acme", "description": "Python"})
        self.assertEqual(job["source"], "freehire")
        self.assertEqual(job["job_id"], "freehire_ai-engineer-acme")

    def test_reject_non_us(self):
        self.assertIsNone(normalize({"title": "AI Engineer", "company": "Acme",
                                     "url": "https://example.com/1", "countries": ["DE"]}))

    def test_ignored_filters_fail_closed(self):
        response = Mock()
        response.read.return_value = b'{"data":[{"title":"AI Engineer"}],"meta":{"ignored_params":["countries"]}}'
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        self.assertEqual(fetch_jobs(opener=lambda *a, **k: response), [])


class TestSkillDemand(unittest.TestCase):
    def test_strength_and_dedup(self):
        text = ("Required:\nPython and Python and AWS\n"
                "Preferred:\nPyTorch and Docker")
        self.assertEqual(extract(text)["Python"], "required")
        self.assertEqual(extract(text)["PyTorch"], "preferred")
        today = datetime(2026, 10, 21, tzinfo=timezone.utc)
        item = {"job_id": "a", "description": text,
                "ingested_at": today.isoformat(), "company": "Acme"}
        result = analyze([item, dict(item)], now=today)
        self.assertEqual(result["sample_size"], 1)
        self.assertEqual(next(x for x in result["ranking"] if x["skill"] == "Python")["total"], 1)

    def test_unknown_strength_not_required(self):
        self.assertEqual(extract("Build RAG systems with LLMs")["RAG"], "unknown")

    def test_no_jobs(self):
        self.assertIn("Analyzed 0", format_telegram(analyze([])))


if __name__ == "__main__":
    unittest.main()
