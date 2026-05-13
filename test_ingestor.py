import json
from unittest.mock import patch

import boto3
from moto import mock_aws

from conftest import MockContext


def _mock_response(payload):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(payload).encode("utf-8")

    return Response()


@mock_aws
class TestIngestor:
    def _setup_infra(self):
        import os
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        sqs = boto3.client("sqs", region_name="us-east-1")
        q = sqs.create_queue(QueueName="job-raw-queue")

        import job_ingestor.lambda_function as mod
        mod.sqs = sqs
        mod.QUEUE_URL = q["QueueUrl"]

        return mod, sqs, q["QueueUrl"]

    @patch("urllib.request.urlopen")
    def test_successful_hn_ingestion(self, mock_urlopen):
        mod, sqs, queue_url = self._setup_infra()

        mock_urlopen.side_effect = [
            _mock_response({"submitted": [1000]}),
            _mock_response({"id": 1000, "title": "Ask HN: Who is hiring? (May 2026)"}),
            _mock_response({"kids": [2001, 2002, 2003]}),
            _mock_response({
                "id": 2001,
                "time": 1778500000,
                "text": (
                    "Anthropic | Senior AI Engineer | San Francisco | Onsite<br>"
                    "Build LLM evaluation systems. Contact real@anthropic.com or jobs@anthropic.com"
                ),
            }),
            _mock_response({
                "id": 2002,
                "time": 1778500100,
                "text": "FrontendCo | React Developer | Remote<br>Build marketing pages.",
            }),
            _mock_response({
                "id": 2003,
                "time": 1778500200,
                "text": "DeepInfra | ML Engineer | Palo Alto<br>PyTorch and RAG platform work.",
            }),
        ]

        result = mod.lambda_handler({}, MockContext())

        assert result["status"] == "success"
        assert result["jobs_fetched"] == 2
        assert result["jobs_queued"] == 2

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        bodies = [json.loads(msg["Body"]) for msg in msgs.get("Messages", [])]

        assert len(bodies) == 2
        assert bodies[0]["title"] == "Senior AI Engineer"
        assert bodies[0]["company"] == "Anthropic"
        assert bodies[0]["location"] == "San Francisco"
        assert bodies[0]["source"] == "hackernews"
        assert bodies[0]["recruiter_email"] == "real@anthropic.com"
        assert bodies[1]["title"] == "ML Engineer"
        assert bodies[1]["company"] == "DeepInfra"

    def test_hn_parser_avoids_batch_as_role_title(self):
        mod, _, _ = self._setup_infra()

        parsed = mod._parse_hn_comment({
            "id": 47980487,
            "time": 1778500000,
            "text": (
                "Hyperspell (YC F25) | San Francisco, ONSITE<br>"
                "We are hiring an AI Engineer to build context and memory for AI agents. "
                "Contact founders@hyperspell.com"
            ),
        })

        assert parsed["company"] == "Hyperspell"
        assert parsed["title"] == "AI Engineer"
        assert parsed["location"] == "San Francisco, ONSITE"
        assert parsed["url"] == "https://news.ycombinator.com/item?id=47980487"
        assert parsed["source_provider"] == "Hacker News Firebase API"

    def test_hn_parser_compacts_prose_leaking_into_location(self):
        mod, _, _ = self._setup_infra()

        parsed = mod._parse_hn_comment({
            "id": 47982567,
            "time": 1778500000,
            "text": (
                "MintMCP | SF Bay Area (ONSITE) MintMCP helps enterprises safely roll out agents "
                "to the entire organization. We are hiring an AI Engineer for Python and RAG systems."
            ),
        })

        assert parsed["company"] == "MintMCP"
        assert parsed["title"] == "AI Engineer"
        assert parsed["location"] == "SF Bay Area (ONSITE)"

    def test_jsearch_normalization_preserves_source_evidence(self):
        mod, _, _ = self._setup_infra()

        job = mod._normalize_jsearch_job({
            "job_id": "abc123",
            "job_title": "AI Agent Engineer",
            "employer_name": "Source Verified AI",
            "job_city": "South San Francisco",
            "job_state": "CA",
            "job_description": "Build LLM agents and RAG workflows.",
            "job_apply_link": "https://company.example/jobs/ai-agent-engineer",
            "job_google_link": "https://www.google.com/search?q=abc123",
            "job_publisher": "Company Careers",
            "job_posted_at_datetime_utc": "2026-05-12T12:00:00Z",
            "job_is_remote": False,
            "job_employment_type": "FULLTIME",
        }, "AI Agent Engineer San Francisco")

        assert job["source"] == "jsearch"
        assert job["source_provider"] == "JSearch RapidAPI"
        assert job["source_origin"] == "Company Careers"
        assert job["url"] == "https://company.example/jobs/ai-agent-engineer"
        assert job["location"] == "South San Francisco, CA"

    def test_google_cse_normalization_preserves_source_evidence(self):
        mod, _, _ = self._setup_infra()

        job = mod._normalize_google_cse_item({
            "title": "AI Engineer - Example AI Careers",
            "link": "https://boards.greenhouse.io/exampleai/jobs/123",
            "snippet": "Build LLM agents and RAG systems in San Francisco.",
            "displayLink": "boards.greenhouse.io",
        }, "AI Engineer San Francisco site:greenhouse.io")

        assert job["source"] == "google_cse"
        assert job["source_provider"] == "Google Custom Search JSON API"
        assert job["source_origin"] == "boards.greenhouse.io"
        assert job["url"] == "https://boards.greenhouse.io/exampleai/jobs/123"
        assert job["title"] == "AI Engineer"

    def test_adzuna_normalization_preserves_source_evidence(self):
        mod, _, _ = self._setup_infra()

        job = mod._normalize_adzuna_job({
            "id": "adz-1",
            "title": "LLM Engineer",
            "company": {"display_name": "Adzuna AI Co"},
            "location": {"display_name": "San Francisco, CA"},
            "description": "Work on RAG and agent systems.",
            "redirect_url": "https://www.adzuna.com/details/adz-1",
            "created": "2026-05-12T12:00:00Z",
            "contract_time": "full_time",
        })

        assert job["source"] == "adzuna"
        assert job["source_provider"] == "Adzuna Jobs API"
        assert job["url"] == "https://www.adzuna.com/details/adz-1"
        assert job["title"] == "LLM Engineer"

    @patch("urllib.request.urlopen")
    def test_empty_when_no_thread_found(self, mock_urlopen):
        mod, _, _ = self._setup_infra()
        mock_urlopen.side_effect = [
            _mock_response({"submitted": [1000]}),
            _mock_response({"id": 1000, "title": "Ask HN: Freelancer? (May 2026)"}),
        ]

        result = mod.lambda_handler({}, MockContext())

        assert result["status"] == "success"
        assert result.get("jobs_ingested", 0) == 0

    def test_email_extraction_skips_generic_addresses(self):
        mod, _, _ = self._setup_infra()

        desc = "Apply at hr@company.com or noreply@test.com. Real contact: teja@deepmind.com"
        email = mod._extract_email_from_description(desc)
        assert email == "teja@deepmind.com"
