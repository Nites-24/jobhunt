"""Contract tests for direct ATS adapters (Greenhouse, Lever, Ashby)."""

import json
import os
import unittest
from unittest.mock import patch

os.environ.update({
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_SESSION_TOKEN": "testing",
    "GREENHOUSE_BOARDS": '[{"slug":"acme","company":"Acme AI"}]',
    "LEVER_SITES": '[{"slug":"acme","company":"Acme AI"}]',
    "ASHBY_BOARDS": '[{"slug":"acme","company":"Acme AI"}]',
})

import job_ingestor.lambda_function as ingestor
ingestor.GREENHOUSE_BOARDS = ingestor._load_board_registry("GREENHOUSE_BOARDS")
ingestor.LEVER_SITES = ingestor._load_board_registry("LEVER_SITES")
ingestor.ASHBY_BOARDS = ingestor._load_board_registry("ASHBY_BOARDS")


class FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


class AdapterTests(unittest.TestCase):
    def fake_urlopen(self, request, timeout):
        url = request.full_url
        if "greenhouse" in url:
            return FakeResponse({"jobs": [{
                "id": 101,
                "title": "LLM Engineer",
                "content": "Build retrieval systems with Python and AWS.",
                "first_published": "2026-09-24T10:00:00Z",
                "updated_at": "2026-09-24T12:00:00Z",
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/101",
                "location": {"name": "Remote, US"},
            }]})
        if "lever" in url:
            return FakeResponse([{
                "id": "l-101",
                "text": "Applied AI Engineer",
                "descriptionPlain": "Build production ML systems.",
                "hostedUrl": "https://jobs.lever.co/acme/l-101",
                "applyUrl": "https://jobs.lever.co/acme/l-101/apply",
                "createdAt": 1727172000000,
                "categories": {"location": "Wichita, KS", "commitment": "Full-time"},
                "workplaceType": "hybrid",
            }])
        if "ashby" in url:
            return FakeResponse({"jobs": [{
                "title": "RAG Engineer",
                "location": "Remote",
                "descriptionPlain": "Own our retrieval augmented generation platform.",
                "publishedAt": "2026-09-24T09:00:00Z",
                "jobUrl": "https://jobs.ashbyhq.com/acme/r-101",
                "applyUrl": "https://jobs.ashbyhq.com/acme/r-101/application",
                "isRemote": True,
                "workplaceType": "Remote",
                "isListed": True,
                "employmentType": "FullTime",
            }]})
        raise AssertionError(f"unexpected URL: {url}")

    def test_fetches_and_normalizes_all_three_sources(self):
        with patch.object(ingestor.urllib.request, "urlopen", self.fake_urlopen):
            greenhouse = ingestor.fetch_greenhouse_jobs()
            lever = ingestor.fetch_lever_jobs()
            ashby = ingestor.fetch_ashby_jobs()

        self.assertEqual(len(greenhouse), 1)
        self.assertEqual(greenhouse[0]["source"], "greenhouse")
        self.assertEqual(greenhouse[0]["apply_url"], greenhouse[0]["url"])
        self.assertEqual(greenhouse[0]["company"], "Acme AI")
        self.assertEqual(greenhouse[0]["posted_at"], "2026-09-24T10:00:00Z")
        self.assertEqual(greenhouse[0]["updated_at"], "2026-09-24T12:00:00Z")

        self.assertEqual(len(lever), 1)
        self.assertEqual(lever[0]["source"], "lever")
        self.assertEqual(lever[0]["work_mode"], "hybrid")
        self.assertTrue(lever[0]["posted_at"].startswith("2024-09-24"))
        self.assertTrue(lever[0]["apply_url"].endswith("/apply"))

        self.assertEqual(len(ashby), 1)
        self.assertEqual(ashby[0]["source"], "ashby")
        self.assertEqual(ashby[0]["posted_at"], "2026-09-24T09:00:00Z")
        self.assertTrue(ashby[0]["is_remote"])

    def test_non_target_and_unlisted_jobs_are_dropped(self):
        greenhouse = ingestor._normalize_greenhouse_job({
            "id": 1,
            "title": "Accountant",
            "absolute_url": "https://boards.greenhouse.io/acme/jobs/1",
            "content": "Manage invoices.",
        }, {"slug": "acme", "company": "Acme AI"})
        ashby = ingestor._normalize_ashby_job({
            "title": "LLM Engineer",
            "jobUrl": "https://jobs.ashbyhq.com/acme/1",
            "descriptionPlain": "RAG systems.",
            "isListed": False,
        }, {"slug": "acme", "company": "Acme AI"})
        self.assertIsNone(greenhouse)
        self.assertIsNone(ashby)

    def test_greenhouse_updated_at_not_treated_as_publication_date(self):
        """Greenhouse jobs without first_published or created_at must not use updated_at as posted_at."""
        job = ingestor._normalize_greenhouse_job({
            "id": 102,
            "title": "AI Engineer",
            "content": "PyTorch LLM orchestration",
            "updated_at": "2026-09-27T10:00:00Z",
            "absolute_url": "https://boards.greenhouse.io/acme/jobs/102",
        }, {"slug": "acme", "company": "Acme AI"})
        self.assertIsNotNone(job)
        self.assertEqual(job["posted_at"], "")
        self.assertEqual(job["updated_at"], "2026-09-27T10:00:00Z")

    def test_lever_missing_created_at_handled_explicitly_without_inventing_freshness(self):
        """Lever jobs without createdAt must not fall back to updatedAt as publication date."""
        job = ingestor._normalize_lever_job({
            "id": "l-102",
            "text": "Machine Learning Engineer",
            "descriptionPlain": "PyTorch ML systems",
            "hostedUrl": "https://jobs.lever.co/acme/l-102",
            "updatedAt": 1727400000000,
        }, {"slug": "acme", "company": "Acme AI"})
        self.assertIsNotNone(job)
        self.assertEqual(job["posted_at"], "")
        self.assertEqual(job["updated_at"], "1727400000000")

    def test_ats_fetch_scans_beyond_index_100_for_target_roles(self):
        """Boards with >100 non-engineering listings must not drop target roles listed past index 100."""
        # 120 irrelevant jobs followed by 1 AI Engineer job
        jobs_payload = [
            {"id": i, "title": f"Sales Rep {i}", "absolute_url": f"https://boards.greenhouse.io/acme/jobs/{i}", "content": "Sales"}
            for i in range(120)
        ]
        jobs_payload.append({
            "id": 999,
            "title": "Lead AI Engineer",
            "first_published": "2026-09-27T08:00:00Z",
            "absolute_url": "https://boards.greenhouse.io/acme/jobs/999",
            "content": "Lead our RAG and LLM agent architecture.",
        })

        def fake_large_board(request, timeout):
            return FakeResponse({"jobs": jobs_payload})

        with patch.object(ingestor.urllib.request, "urlopen", fake_large_board):
            greenhouse = ingestor.fetch_greenhouse_jobs()

        self.assertEqual(len(greenhouse), 1)
        self.assertEqual(greenhouse[0]["title"], "Lead AI Engineer")
        self.assertEqual(greenhouse[0]["source_job_id"], "999")


if __name__ == "__main__":
    unittest.main()
