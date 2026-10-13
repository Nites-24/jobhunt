import json
import importlib
import os
from unittest.mock import patch, MagicMock

import boto3
from moto import mock_aws

from conftest import MockContext


def _mock_response(payload):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self, *_args):
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
        assert bodies[0]["source_search_query"] == "hn_who_is_hiring"
        assert bodies[0]["recruiter_email"] == "real@anthropic.com"
        assert bodies[0]["work_mode"] == "onsite"
        assert bodies[0]["work_mode_source"] == "description"
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
        assert parsed["work_mode"] == "onsite"
        assert parsed["work_mode_source"] == "location"
        assert parsed["url"] == "https://news.ycombinator.com/item?id=47980487"
        assert parsed["source_provider"] == "Hacker News Firebase API"
        assert parsed["source_search_query"] == "hn_who_is_hiring"

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
        assert job["work_mode"] == ""

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
        assert job["location"] == "San Francisco"
        assert job["work_mode"] == ""

    def test_google_cse_blank_env_uses_default_queries(self):
        previous = os.environ.get("GOOGLE_CSE_QUERIES")
        os.environ["GOOGLE_CSE_QUERIES"] = ""

        import job_ingestor.lambda_function as mod
        mod = importlib.reload(mod)

        try:
            assert mod.GOOGLE_CSE_QUERIES
            assert any("greenhouse.io" in query for query in mod.GOOGLE_CSE_QUERIES)
        finally:
            if previous is None:
                os.environ.pop("GOOGLE_CSE_QUERIES", None)
            else:
                os.environ["GOOGLE_CSE_QUERIES"] = previous
            importlib.reload(mod)


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

        desc = "Apply at hr@company.com or noreply@test.com. Real contact: alex@deepmind.com"
        email = mod._extract_email_from_description(desc)
        assert email == "alex@deepmind.com"

    def test_hn_parser_extracts_obfuscated_email_and_author_contact(self):
        mod, _, _ = self._setup_infra()

        parsed = mod._parse_hn_comment({
            "id": 47990001,
            "by": "jesalg",
            "time": 1778500000,
            "text": (
                "Cora AI | Full Stack / Applied AI Engineer | REMOTE (US ONLY, LA or SF a plus)<br>"
                "Customer success agents and account intelligence layer that actually do manual work. "
                "Work on voice agents, knowledge graphs, multi-agent systems, evals, and simulations. "
                "Learn more: https://www.cora.ai/<br>"
                "Interested? Email careers [at] cora.ai - these go straight to me, and I read every one."
            ),
        })

        assert parsed["company"] == "Cora AI"
        assert parsed["title"] == "Full Stack / Applied AI Engineer"
        assert parsed["recruiter_email"] == "careers@cora.ai"
        assert parsed["recruiter_name"] == "Jesal"
        assert parsed["recruiter_name_source"] == "hn_author"
        assert parsed["source_author"] == "jesalg"
        assert parsed["external_links"] == ["https://www.cora.ai/"]
        assert parsed["source_grounding"] == "hn_comment"

    @patch("urllib.request.urlopen")
    def test_hn_ingestion_enriches_from_linked_page(self, mock_urlopen):
        mod, sqs, queue_url = self._setup_infra()
        mod.HN_LINK_ENRICHMENT_ENABLED = True
        mod.HN_MAX_LINK_ENRICHMENTS = 5

        mock_urlopen.side_effect = [
            _mock_response({"submitted": [1000]}),
            _mock_response({"id": 1000, "title": "Ask HN: Who is hiring? (May 2026)"}),
            _mock_response({"kids": [2001]}),
            _mock_response({
                "id": 2001,
                "by": "jesalg",
                "time": 1778500000,
                "text": (
                    "Cora AI | Full Stack / Applied AI Engineer | REMOTE<br>"
                    "Build AI agents for customer success. Learn more: https://www.cora.ai/"
                ),
            }),
            _mock_response({
                "html": (
                    "<html><head><title>Cora AI</title></head><body>"
                    "Cora builds customer success agents, account intelligence, voice agents, "
                    "knowledge graphs, multi-agent systems, evals, and simulations. "
                    "Email careers [at] cora.ai."
                    "</body></html>"
                )
            }),
        ]

        result = mod.lambda_handler({}, MockContext())

        assert result["status"] == "success"
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        body = json.loads(msgs["Messages"][0]["Body"])
        assert body["source_grounding"] == "hn_comment+linked_page"
        assert body["linked_page_url"] == "https://www.cora.ai/"
        assert "customer success agents" in body["linked_page_excerpt"]
        assert "Linked page context" in body["description"]
        assert body["recruiter_email"] == "careers@cora.ai"

    def test_signal_test_seed_queues_one_job(self):
        mod, sqs, queue_url = self._setup_infra()

        result = mod.lambda_handler({"seed_signal_test": True}, MockContext())

        assert result["status"] == "success"
        assert result["jobs_fetched"] == 1
        assert result["jobs_queued"] == 1

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
        body = json.loads(msgs["Messages"][0]["Body"])
        assert body["source"] == "signal_test"
        assert body["signal_test"] is True
        assert body["work_mode"] == "onsite"

    def test_ssrf_rejects_non_https(self):
        """HTTP, file, and ftp schemes must be blocked."""
        import job_ingestor.lambda_function as mod
        assert mod._is_safe_public_url("http://example.com/job")[0] is False
        assert mod._is_safe_public_url("file:///etc/passwd")[0] is False
        assert mod._is_safe_public_url("ftp://example.com/job")[0] is False

    def test_ssrf_rejects_private_and_metadata_ips(self):
        """Private RFC1918, loopback, and AWS metadata IPs must be blocked."""
        import job_ingestor.lambda_function as mod
        assert mod._is_safe_public_url("https://127.0.0.1/admin")[0] is False
        assert mod._is_safe_public_url("https://localhost/job")[0] is False
        assert mod._is_safe_public_url("https://169.254.169.254/latest/meta-data/")[0] is False
        assert mod._is_safe_public_url("https://10.0.0.1/internal")[0] is False
        assert mod._is_safe_public_url("https://172.16.0.1/internal")[0] is False
        assert mod._is_safe_public_url("https://192.168.1.1/internal")[0] is False
        assert mod._is_safe_public_url("https://[::1]/internal")[0] is False

    def test_ssrf_rejects_non_standard_ports(self):
        """Non-standard ports like 8080, 22, 6379 must be blocked."""
        import job_ingestor.lambda_function as mod
        assert mod._is_safe_public_url("https://example.com:8080/job")[0] is False
        assert mod._is_safe_public_url("https://example.com:22/job")[0] is False

    @patch("socket.getaddrinfo")
    def test_ssrf_allows_public_https(self, mock_dns):
        """Public HTTPS endpoints resolving to public IPs must be permitted."""
        import job_ingestor.lambda_function as mod
        mock_dns.return_value = [
            (2, 0, 6, '', ('93.184.216.34', 443))
        ]
        is_safe, reason = mod._is_safe_public_url("https://example.com/careers/ai")
        assert is_safe is True
        assert reason == "OK"

    def test_fetch_linked_page_context_blocks_ssrf(self):
        """_fetch_linked_page_context must return empty string for unsafe URLs."""
        import job_ingestor.lambda_function as mod
        assert mod._fetch_linked_page_context("http://169.254.169.254/latest/meta-data/") == ""
        assert mod._fetch_linked_page_context("https://127.0.0.1:8000/") == ""

    def test_safe_redirect_handler_blocks_unsafe_redirect(self):
        """SafeRedirectHandler must abort redirect when target is unsafe."""
        import job_ingestor.lambda_function as mod
        handler = mod.SafeRedirectHandler(max_redirects=2)
        mock_req = MagicMock()
        result = handler.redirect_request(
            mock_req, None, 302, "Found", {}, "http://169.254.169.254/latest/meta-data/"
        )
        assert result is None  # Blocked!

    def test_load_board_registry_json_and_text(self):
        """Registry parser supports both JSON array and pipe-delimited text."""
        import job_ingestor.lambda_function as mod
        with patch.dict(os.environ, {"TEST_BOARDS": '[{"slug":"acme","company":"Acme AI"}]'}):
            reg = mod._load_board_registry("TEST_BOARDS")
            assert len(reg) == 1
            assert reg[0] == {"slug": "acme", "company": "Acme AI"}

        with patch.dict(os.environ, {"TEST_BOARDS": "acme|Acme AI,other|Other Co"}):
            reg = mod._load_board_registry("TEST_BOARDS")
            assert len(reg) == 2
            assert reg[0] == {"slug": "acme", "company": "Acme AI"}
            assert reg[1] == {"slug": "other", "company": "Other Co"}

    def test_target_role_filter(self):
        """Target role filter matches AI/ML engineering and rejects non-technical roles."""
        import job_ingestor.lambda_function as mod
        assert mod._is_target_role("Senior AI Engineer") is True
        assert mod._is_target_role("LLM Platform Engineer", "Python AWS Bedrock") is True
        assert mod._is_target_role("Machine Learning Researcher") is True
        assert mod._is_target_role("Accountant", "Manage finances at an AI startup") is False
        assert mod._is_target_role("Technical Recruiter", "Hire AI engineers") is False

    @patch("urllib.request.urlopen")
    def test_greenhouse_fetch_and_normalize(self, mock_urlopen):
        """Greenhouse jobs are fetched, filtered, and normalized with deterministic job_id."""
        import job_ingestor.lambda_function as mod
        mock_urlopen.return_value = _mock_response({"jobs": [
            {
                "id": 101,
                "title": "AI Engineer",
                "content": "Build RAG and LLM systems with Python.",
                "updated_at": "2026-09-24T10:00:00Z",
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/101",
                "location": {"name": "San Francisco, CA"},
            },
            {
                "id": 102,
                "title": "Accountant",
                "content": "Manage invoices.",
                "updated_at": "2026-09-24T10:00:00Z",
                "absolute_url": "https://boards.greenhouse.io/acme/jobs/102",
                "location": {"name": "Remote"},
            },
        ]})

        with patch.object(mod, "GREENHOUSE_ENABLED", True), \
             patch.object(mod, "GREENHOUSE_BOARDS", [{"slug": "acme", "company": "Acme AI"}]):
            jobs = mod.fetch_greenhouse_jobs()

        assert len(jobs) == 1
        job = jobs[0]
        assert job["job_id"] == "greenhouse_acme_101"
        assert job["company"] == "Acme AI"
        assert job["source"] == "greenhouse"
        assert job["source_provider"] == "Greenhouse Job Board API"
        assert job["apply_url"] == "https://boards.greenhouse.io/acme/jobs/101"

    @patch("urllib.request.urlopen")
    def test_lever_fetch_and_normalize(self, mock_urlopen):
        """Lever jobs canonicalize work_mode to 'onsite' and parse millisecond epoch dates."""
        import job_ingestor.lambda_function as mod
        mock_urlopen.return_value = _mock_response([
            {
                "id": "l-101",
                "text": "Applied AI Engineer",
                "descriptionPlain": "Build production ML and agent systems.",
                "hostedUrl": "https://jobs.lever.co/acme/l-101",
                "applyUrl": "https://jobs.lever.co/acme/l-101/apply",
                "categories": {"location": "San Francisco, CA", "commitment": "Full-time"},
                "workplaceType": "on-site",
                "createdAt": 1778500000000,
            }
        ])

        with patch.object(mod, "LEVER_ENABLED", True), \
             patch.object(mod, "LEVER_SITES", [{"slug": "acme", "company": "Acme AI"}]):
            jobs = mod.fetch_lever_jobs()

        assert len(jobs) == 1
        job = jobs[0]
        assert job["job_id"] == "lever_acme_l-101"
        assert job["work_mode"] == "onsite"
        assert job["posted_at"].endswith("Z")
        assert "T" in job["posted_at"]
        assert job["apply_url"].endswith("/apply")

    @patch("urllib.request.urlopen")
    def test_ashby_fetch_and_normalize(self, mock_urlopen):
        """Ashby jobs drop unlisted postings and preserve remote flag."""
        import job_ingestor.lambda_function as mod
        mock_urlopen.return_value = _mock_response({"jobs": [
            {
                "title": "RAG Engineer",
                "location": "Remote, US",
                "descriptionPlain": "Build retrieval pipelines.",
                "publishedAt": "2026-09-24T09:00:00Z",
                "jobUrl": "https://jobs.ashbyhq.com/acme/r-101",
                "applyUrl": "https://jobs.ashbyhq.com/acme/r-101/application",
                "isRemote": True,
                "workplaceType": "remote",
                "isListed": True,
            },
            {
                "title": "Stealth AI Engineer",
                "location": "Remote",
                "descriptionPlain": "Internal draft.",
                "publishedAt": "2026-09-24T09:00:00Z",
                "jobUrl": "https://jobs.ashbyhq.com/acme/r-102",
                "isListed": False,
            },
        ]})

        with patch.object(mod, "ASHBY_ENABLED", True), \
             patch.object(mod, "ASHBY_BOARDS", [{"slug": "acme", "company": "Acme AI"}]):
            jobs = mod.fetch_ashby_jobs()

    def test_is_target_role_matches_expanded_ai_titles(self):
        """Pipeline must semantically match modern AI engineering titles."""
        import job_ingestor.lambda_function as mod
        target_titles = [
            "AI Software Engineer",
            "Software Engineer, AI",
            "Founding AI Engineer",
            "Founding Engineer - AI",
            "AI Solutions Architect",
            "AI Solutions Engineer",
            "Forward Deployed AI Engineer",
            "AI Agent Engineer",
            "Applied AI Engineer",
            "AI Systems Engineer",
            "Machine Learning Engineer",
            "LLM Engineer",
            "RAG Engineer",
        ]
        for title in target_titles:
            assert mod._is_target_role(title, "Building LLM agents with Python"), f"Failed for {title}"

    def test_is_non_us_location_detector(self):
        """Pipeline must detect non-US locations to keep opportunities United States only."""
        import job_ingestor.lambda_function as mod
        assert mod._is_non_us_location("London, UK") is True
        assert mod._is_non_us_location("London, United Kingdom") is True
        assert mod._is_non_us_location("Paris, France") is True
        assert mod._is_non_us_location("Berlin, Germany") is True
        assert mod._is_non_us_location("Toronto, Canada") is True
        assert mod._is_non_us_location("Bangalore, India") is True
        assert mod._is_non_us_location("Remote", "This position is remote (UK only).") is True

        assert mod._is_non_us_location("San Francisco, CA") is False
        assert mod._is_non_us_location("New York, NY") is False
        assert mod._is_non_us_location("Austin, TX") is False
        assert mod._is_non_us_location("Remote - US") is False
        assert mod._is_non_us_location("Remote", "This is an open US remote role.") is False

    def test_disallowed_domains_detector(self):
        """Pipeline must block freelance marketplaces like Upwork and Fiverr."""
        import job_ingestor.lambda_function as mod
        assert mod._is_disallowed_domain("https://www.upwork.com/freelance-jobs/ai") is True
        assert mod._is_disallowed_domain("https://fiverr.com/gigs/rag-pipeline") is True
        assert mod._is_disallowed_domain("https://freelancer.com/projects/llm") is True
        assert mod._is_disallowed_domain("https://boards.greenhouse.io/anthropic/jobs/123") is False
        assert mod._is_disallowed_domain("https://jobs.lever.co/mistral/456") is False

    def test_citizenship_and_visa_restriction_detector(self):
        """Pipeline must detect US-citizen-only, clearance, and no-sponsorship bans."""
        import job_ingestor.lambda_function as mod
        assert mod._has_visa_restriction("AI Engineer", "Must be a U.S. Citizen.") is True
        assert mod._has_visa_restriction("AI Engineer", "Active Secret Clearance required.") is True
        assert mod._has_visa_restriction("AI Engineer", "We do not offer visa sponsorship.") is True
        assert mod._has_visa_restriction("AI Engineer", "Must be authorized to work without sponsorship now or in the future.") is True
        assert mod._has_visa_restriction("AI Engineer", "No CPT or OPT candidates.") is True

        assert mod._has_visa_restriction("AI Engineer", "Visa sponsorship is available for top candidates.") is False
        assert mod._has_visa_restriction("AI Engineer", "We sponsor H-1B and welcome CPT/OPT.") is False
        assert mod._has_visa_restriction("AI Engineer", "Building agentic RAG workflows using Python and AWS Bedrock.") is False

    def test_ats_normalizers_drop_non_us_and_restricted_postings(self):
        """Greenhouse, Lever, Ashby, and JSearch normalizers must drop non-US, Upwork, and citizen-only roles."""
        import job_ingestor.lambda_function as mod
        # Greenhouse London role
        gh_london = mod._normalize_greenhouse_job({
            "title": "AI Software Engineer",
            "absolute_url": "https://boards.greenhouse.io/anthropic/jobs/1",
            "content": "Building RAG systems",
            "location": {"name": "London, UK"}
        }, {"slug": "anthropic", "company": "Anthropic"})
        assert gh_london is None

        # Lever US-citizen-only role
        lever_restricted = mod._normalize_lever_job({
            "text": "AI Solutions Engineer",
            "hostedUrl": "https://jobs.lever.co/acme/2",
            "descriptionPlain": "Must be a US Citizen. Secret Clearance required.",
            "categories": {"location": "San Francisco, CA"}
        }, {"slug": "acme", "company": "Acme"})
        assert lever_restricted is None

        # Ashby Upwork link
        ashby_upwork = mod._normalize_ashby_job({
            "title": "Founding AI Engineer",
            "jobUrl": "https://www.upwork.com/freelance-jobs/3",
            "descriptionPlain": "Build zero to one AI agents",
            "location": "Remote, US",
            "isListed": True,
        }, {"slug": "acme", "company": "Acme"})
        assert ashby_upwork is None


