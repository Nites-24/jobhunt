"""
Tests for job-sender Lambda.

Tests:
  1. Successful email generation + send
  2. Idempotency: skip already-SENT jobs
  3. Batch processing with multiple records
  4. Bedrock tool-call response parsing
  5. Email sanitization (em dashes, exclamation points, etc.)
  6. Greeting logic (first name, company fallback, generic)
  7. Direct invoke (from job-router) vs SQS trigger
  8. SMTP failure handling
"""

import json
import pytest
import boto3
from decimal import Decimal
from moto import mock_aws
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from conftest import SAMPLE_JOB, MockContext, make_sqs_event


@mock_aws
class TestSender:
    """End-to-end tests for job-sender."""

    def _setup_infra(self):
        """Create DynamoDB, S3, and Secrets resources."""
        import os
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["RESUME_KEY"] = "data/resume/resume.pdf"  # Override conftest default

        dynamo = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamo.create_table(
            TableName="Jobsearch",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="Jobsearch")

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="job-search-pipeline-memory")
        s3.put_object(
            Bucket="job-search-pipeline-memory",
            Key="data/resume/resume.pdf",
            Body=b"%PDF-1.4 test resume content",
        )

        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(
            Name="jobserch_gmail",
            SecretString=json.dumps({"APP_PASSWORD": "test-app-pwd"}),
        )

        import job_sender.lambda_function as mod
        mod.dynamodb = dynamo
        mod.table = dynamo.Table("Jobsearch")
        mod.s3_client = s3
        mod.secrets_client = sm
        mod.BUCKET = "job-search-pipeline-memory"
        mod.RESUME_KEY = "data/resume/resume.pdf"
        mod.ALLOW_REAL_EMAIL_SEND = False
        mod._cached_resume = None
        mod._cached_creds = None

        # Pre-insert job
        table.put_item(Item={
            "job_id": SAMPLE_JOB["job_id"],
            "status": "APPROVED",
            "title": SAMPLE_JOB["title"],
        })

        return mod, table

    def _mock_bedrock_email_response(self, subject="AI Engineer at Anthropic", body=None):
        """Create mock Bedrock Converse response with email tool-call."""
        if body is None:
            body = (
                "Hi Sarah,\n\n"
                "Anthropic's role points at reliable RAG evaluation and agent workflows. "
                "My recent agent work is close to that: retrieve evidence, score results, "
                "and keep a human approval step before action.\n\n"
                "RetireIQ is a retirement "
                "planning and financial decision support app, and Rivulet "
                "is a personal life management app.\n\n"
                "I am currently based in San Francisco, CA, and I am open to remote, hybrid, "
                "or onsite roles in any location, including relocation.\n\n"
                "Would 20 minutes this week work to connect?\n\n"
                "Best,\nAlex Hunter\nAI Engineer"
            )
        return {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "test-id",
                                "name": "generate_email",
                                "input": {"subject": subject, "body": body},
                            }
                        }
                    ]
                }
            },
            "usage": {"inputTokens": 800, "outputTokens": 150},
        }

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_successful_email_send(self, mock_bedrock, mock_smtp):
        """Full flow: generate email → send → update DynamoDB to SENT."""
        mod, table = self._setup_infra()
        mod.ALLOW_REAL_EMAIL_SEND = True
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response()
        mock_smtp.return_value = [(SAMPLE_JOB["job_id"], True, None)]

        result = mod.lambda_handler(SAMPLE_JOB, MockContext())

        # SMTP was called
        mock_smtp.assert_called_once()

        # DynamoDB updated to SENT
        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["status"] == "SENT"

    @patch("job_sender.lambda_function._fetch_resume")
    @patch("job_sender.lambda_function._get_credentials")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_preview_only_generates_draft_without_send_resources(self, mock_bedrock, mock_creds, mock_resume):
        """Preview mode should generate a draft without fetching SMTP credentials or resume."""
        mod, table = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI outbound agent role",
            body="Hi Anthropic Team,\n\nPreview body.\n\nBest,\nAlex",
        )

        result = mod.lambda_handler({"preview_only": True, "job": SAMPLE_JOB}, MockContext())

        assert result["status"] == "preview"
        assert result["email"]["subject"] == "AI outbound agent role"
        mock_creds.assert_not_called()
        mock_resume.assert_not_called()

    @patch("job_sender.lambda_function.time.sleep")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_preview_only_falls_back_on_bedrock_throttle(self, mock_bedrock, mock_sleep):
        """Preview mode should return a deterministic fallback draft on Bedrock throttling."""
        mod, table = self._setup_infra()
        mock_bedrock.converse.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ThrottlingException",
                    "Message": "Too many requests",
                }
            },
            "Converse",
        )

        result = mod.lambda_handler({"preview_only": True, "job": SAMPLE_JOB}, MockContext())

        assert result["status"] == "preview"
        assert result["generated_with_fallback"] is True
        assert "role at Anthropic" in result["email"]["subject"]
        assert "RetireIQ" in result["email"]["body"]
        assert "Rivulet (https://rivulet.info)" not in result["email"]["body"]
        assert "If this looks relevant, would 20 minutes this week work to connect?" in result["email"]["body"]
        assert mock_sleep.call_count == mod.BEDROCK_THROTTLE_RETRIES

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function._get_credentials")
    @patch("job_sender.lambda_function._fetch_resume")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_real_send_disabled_by_default(self, mock_bedrock, mock_resume, mock_creds, mock_smtp):
        """Direct send requests should be blocked unless explicitly enabled."""
        mod, table = self._setup_infra()

        result = mod.lambda_handler(SAMPLE_JOB, MockContext())

        assert result["status"] == "send_disabled"
        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["status"] == "MANUAL_APPLY"
        assert "Real email sending is disabled" in item.get("failure_reason", "")
        mock_bedrock.converse.assert_not_called()
        mock_resume.assert_not_called()
        mock_creds.assert_not_called()
        mock_smtp.assert_not_called()

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_idempotency_skip_sent(self, mock_bedrock, mock_smtp):
        """Jobs already in SENT status should be skipped."""
        mod, table = self._setup_infra()

        # Mark as already sent
        table.update_item(
            Key={"job_id": SAMPLE_JOB["job_id"]},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "SENT"},
        )

        result = mod.lambda_handler(SAMPLE_JOB, MockContext())

        # Bedrock should NOT have been called
        mock_bedrock.converse.assert_not_called()
        mock_smtp.assert_not_called()

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_sqs_batch_processing(self, mock_bedrock, mock_smtp):
        """SQS batch with multiple records should all be processed."""
        mod, table = self._setup_infra()
        mod.ALLOW_REAL_EMAIL_SEND = True

        job2 = {**SAMPLE_JOB, "job_id": "test-job-002"}
        table.put_item(Item={"job_id": "test-job-002", "status": "APPROVED"})

        mock_bedrock.converse.return_value = self._mock_bedrock_email_response()
        mock_smtp.return_value = [
            (SAMPLE_JOB["job_id"], True, None),
            ("test-job-002", True, None),
        ]

        event = make_sqs_event([SAMPLE_JOB, job2])
        result = mod.lambda_handler(event, MockContext())

        assert result["batchItemFailures"] == []
        assert mock_bedrock.converse.call_count == 2

    def test_sanitize_body(self):
        """Sanitization should clean em dashes, exclamation points, smart quotes."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        dirty = "This is amazing! We\u2019re excited\u2014truly. Check it out..."
        clean = mod._sanitize_body(dirty)

        assert "!" not in clean  # No exclamation points
        assert "\u2014" not in clean  # No em dashes
        assert "\u2019" not in clean  # No smart quotes
        assert "..." not in clean  # No ellipsis

    def test_greeting_first_name(self):
        """Greeting should use recruiter's first name when available."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {**SAMPLE_JOB, "recruiter_name": "Sarah Chen"}
        msg = mod._build_user_message(job)
        assert "Hi Sarah," in msg
        assert "<candidate_graph>" in msg

    def test_greeting_company_fallback(self):
        """Greeting should fall back to company name when no recruiter name."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {**SAMPLE_JOB, "recruiter_name": "", "recruiter_email": ""}
        msg = mod._build_user_message(job)
        assert "Hi Anthropic Team," in msg

    def test_greeting_uses_recipient_email_name(self):
        """When recruiter_name is missing, infer a safe first name from the email."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {
            **SAMPLE_JOB,
            "company": "Engineering Square",
            "recruiter_name": "",
            "recruiter_email": "saikumar@engineeringsquare.us",
        }
        msg = mod._build_user_message(job)
        assert "GREETING TO USE: Hi Sai," in msg
        assert "Hi Engineering Square Team," not in msg

    def test_greeting_ignores_generic_recipient_email(self):
        """Careers inboxes should use a hiring-team greeting."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {
            **SAMPLE_JOB,
            "company": "Engineering Square",
            "recruiter_name": "",
            "recruiter_email": "careers@engineeringsquare.us",
        }
        msg = mod._build_user_message(job)
        assert "GREETING TO USE: Hi Engineering Square hiring team," in msg

    def test_greeting_treats_founders_inbox_as_generic(self):
        """founders@ inboxes without a named founder should not use 'Hi Founders'."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {
            **SAMPLE_JOB,
            "company": "Hyperspell",
            "recruiter_name": "",
            "recruiter_email": "founders@hyperspell.com",
            "description": "Hyperspell is building context and memory for AI agents.",
        }
        msg = mod._build_user_message(job)
        assert "GREETING TO USE: Hi Hyperspell Team," in msg
        assert "GREETING TO USE: Hi Founders," not in msg

    def test_founder_name_is_extracted_from_description(self):
        """Founder descriptions should produce a named greeting when possible."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {
            **SAMPLE_JOB,
            "company": "Hyperspell",
            "recruiter_name": "",
            "recruiter_email": "founders@hyperspell.com",
            "description": "I'm Manu, founder of Hyperspell. We are building context and memory for AI agents.",
        }
        msg = mod._build_user_message(job)
        assert "GREETING TO USE: Hi Manu," in msg

    def test_greeting_generic_fallback(self):
        """Greeting should use 'Hi there' when no name or company."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {**SAMPLE_JOB, "recruiter_name": "", "company": ""}
        msg = mod._build_user_message(job)
        assert "Hi there," in msg

    def test_greeting_filters_invalid_names(self):
        """Names like 'Hiring Manager' should be filtered out."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {**SAMPLE_JOB, "recruiter_name": "Hiring Manager"}
        msg = mod._build_user_message(job)
        # Should fall back to company, not use "Hi Hiring,"
        assert "Hi Anthropic hiring team," in msg

    @patch("job_sender.lambda_function.bedrock_client")
    def test_generated_body_greeting_is_enforced_from_email(self, mock_bedrock):
        """Even if the model returns a team greeting, code enforces the recipient name."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI Engineer role",
            body="Hi Engineering Square Team,\n\nI saw the role.\n\nBest,\nAlex",
        )
        job = {
            **SAMPLE_JOB,
            "company": "Engineering Square",
            "recruiter_name": "",
            "recruiter_email": "saikumar@engineeringsquare.us",
        }

        email = mod._generate_email(job)

        assert email["body"].startswith("Hi Sai,")
        assert "Hi Engineering Square Team," not in email["body"]

    @patch("job_sender.lambda_function.bedrock_client")
    def test_generated_body_rejects_robotic_or_unsupported_claims(self, mock_bedrock):
        """Robotic model output should be normalized into the deterministic final structure."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI Engineer for Production RAG Systems",
            body=(
                "Hi Engineering Square Team,\n\n"
                "I've been building production AI systems at Empower for the past few years, "
                "and your focus on shipping fast with ownership resonates with how I work.\n\n"
                "Best,\nAlex"
            ),
        )
        job = {
            **SAMPLE_JOB,
            "company": "Engineering Square",
            "recruiter_name": "",
            "recruiter_email": "saikumar@engineeringsquare.us",
        }

        email = mod._generate_email(job)

        assert email["body"].startswith("Hi Sai,")
        assert "at Empower" not in email["body"]
        assert "shipping fast with ownership" not in email["body"]
        assert "resonates" not in email["body"]
        assert "Engineering Square stood out to me because the role is centered on Python, PyTorch, and RAG systems" in email["body"]
        assert "RetireIQ" in email["body"]
        assert "Rivulet (https://rivulet.info)" not in email["body"]

    @patch("job_sender.lambda_function.bedrock_client")
    def test_generated_body_does_not_preserve_old_style_project_claims(self, mock_bedrock):
        """The final email should not keep stale project narration from the model body."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI Engineer for Hyperspell Agentic Systems",
            body=(
                "Hi Founders,\n\n"
                "Building context and memory for AI agents requires solid orchestration and evaluation infrastructure. "
                "I've spent the last months working on agentic systems that reduce operational overhead, and I'm interested in how Hyperspell is approaching this problem at scale.\n\n"
                "I've built RetireIQ, a financial decision support app that uses LLM orchestration to handle complex user queries, "
                "and Rivulet, a personal life management system that coordinates multiple agent workflows.\n\n"
                "Best,\nAlex"
            ),
        )
        job = {
            **SAMPLE_JOB,
            "company": "Hyperspell",
            "recruiter_name": "",
            "recruiter_email": "founders@hyperspell.com",
            "description": "Hyperspell is building context and memory for AI agents.",
        }

        email = mod._generate_email(job)

        assert email["body"].startswith("Hi Hyperspell Team,")
        assert "Hi Founders," not in email["body"]
        assert "I've spent the last months" not in email["body"]
        assert "I'm interested in how" not in email["body"]
        assert "at scale" not in email["body"]
        assert "personal life management system that coordinates multiple agent workflows" not in email["body"]
        assert "Hyperspell stood out to me because the role is centered on context and memory for AI agents" in email["body"]
        assert "private autonomous AI outreach agent" in email["body"]
        assert "RetireIQ" not in email["body"]
        assert "Rivulet (https://rivulet.info)" not in email["body"]

    @patch("job_sender.lambda_function.bedrock_client")
    def test_founder_mode_uses_named_greeting_and_founder_cta(self, mock_bedrock):
        """Founder contact should sound like a direct note, not a hiring-team template."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI Engineer at Hyperspell",
            body="Hi Manu,\n\nHyperspell is building context and memory for AI agents.\n\nBest,\nAlex",
        )
        job = {
            **SAMPLE_JOB,
            "company": "Hyperspell",
            "recruiter_name": "",
            "recruiter_email": "founders@hyperspell.com",
            "description": "I'm Manu, founder of Hyperspell. We are building context and memory for AI agents.",
        }

        email = mod._generate_email(job)

        assert email["body"].startswith("Hi Manu,")
        assert "If this seems relevant, would 20 minutes this week work to connect?" in email["body"]

    @patch("job_sender.lambda_function.bedrock_client")
    def test_careers_mode_uses_hiring_team_greeting_and_cta(self, mock_bedrock):
        """Careers route should sound role-focused rather than founder-directed."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI Engineer application",
            body="Hi Engineering Square hiring team,\n\nEngineering Square's AI Engineer role looks close to the practical AI product work I want to keep doing.\n\nBest,\nAlex",
        )
        job = {
            **SAMPLE_JOB,
            "company": "Engineering Square",
            "recruiter_name": "",
            "recruiter_email": "careers@engineeringsquare.us",
            "apply_url": "https://jobs.ashbyhq.com/engineeringsquare",
        }

        email = mod._generate_email(job)

        assert email["body"].startswith("Hi Engineering Square hiring team,")
        assert "If the role is still open, would 20 minutes this week work to connect?" in email["body"]

    def test_source_evidence_fragment_prefers_linked_page_excerpt(self):
        """Linked page content should drive the deterministic opener when available."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {
            **SAMPLE_JOB,
            "company": "Cora AI",
            "title": "Applied AI Engineer",
            "description": "Generic application text.",
            "linked_page_excerpt": (
                "Customer success agents and account intelligence layer that actually do the manual work "
                "so customers can focus on relationship-driven work."
            ),
        }

        opening = mod._deterministic_opening(job)

        assert opening == (
            "Cora AI stood out to me because the role is centered on customer success agents and account intelligence, "
            "which is close to the kind of applied AI work I want to keep doing."
        )

    def test_source_fragment_compression_shortens_long_hn_style_text(self):
        """Long source excerpts should compress to a short evidence phrase."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        fragment = mod._compress_source_fragment(
            "Customer success agents and account intelligence layer that actually do the manual work so customers can focus on relationship-driven work."
        )

        assert fragment == "customer success agents and account intelligence"

    def test_select_project_line_chooses_private_agent_for_agent_system_roles(self):
        """Agentic and eval-heavy roles should map to the private outreach system, not a two-project dump."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {
            **SAMPLE_JOB,
            "description": "Work on voice agents, knowledge graphs, multi-agent systems, evals, and automation.",
            "linked_page_excerpt": "Voice agents, knowledge graphs, and multi-agent systems.",
        }

        project_line = mod._select_project_line(job)

        assert "private autonomous AI outreach agent" in project_line
        assert "RetireIQ" not in project_line
        assert "Rivulet" not in project_line

    def test_decimal_deserialization(self):
        """DynamoDB Decimal values in job payload should not crash."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job_with_decimals = {
            **SAMPLE_JOB,
            "match_score": Decimal("95"),
            "final_score": Decimal("92.5"),
        }
        result = mod._deserialize_job(job_with_decimals)
        assert result["match_score"] == 95
        assert isinstance(result["match_score"], int)

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_smtp_failure_marks_failed(self, mock_bedrock, mock_smtp):
        """SMTP failure should mark the job as FAILED in DynamoDB."""
        mod, table = self._setup_infra()
        mod.ALLOW_REAL_EMAIL_SEND = True
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response()
        mock_smtp.return_value = [(SAMPLE_JOB["job_id"], False, "Connection refused")]

        result = mod.lambda_handler(SAMPLE_JOB, MockContext())

        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["status"] == "FAILED"
        assert "SMTP_ERROR" in item.get("failure_reason", "")

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_draft_approval_enforcement(self, mock_bedrock, mock_smtp):
        """When draft approval is required, unapproved, mismatched, or expired drafts are rejected."""
        import hashlib
        import time
        mod, table = self._setup_infra()
        mod.ALLOW_REAL_EMAIL_SEND = True
        mod.REQUIRE_DRAFT_APPROVAL = True

        recipient = SAMPLE_JOB["recruiter_email"]
        subject = "AI Engineer at Anthropic"
        body = "Hi Sarah,\n\nTest body.\n\nBest,\nAlex Hunter"
        valid_hash = hashlib.sha256(
            f"{SAMPLE_JOB['job_id']}:{recipient}:{subject}:{body}".encode("utf-8")
        ).hexdigest()

        # 1. Unapproved send fails
        result = mod.lambda_handler(SAMPLE_JOB, MockContext())
        mock_smtp.assert_not_called()
        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["status"] == "FAILED"
        assert "requires approved draft" in item["failure_reason"]

        # 2. Mismatched hash fails
        bad_event = {
            **SAMPLE_JOB,
            "approved_hash": "bad-hash",
            "approved_subject": subject,
            "approved_body": body,
        }
        result = mod.lambda_handler(bad_event, MockContext())
        mock_smtp.assert_not_called()
        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["status"] == "FAILED"
        assert "mismatch" in item["failure_reason"]

        # 3. Expired approval fails
        table.update_item(
            Key={"job_id": SAMPLE_JOB["job_id"]},
            UpdateExpression="SET draft_hash = :h, approval_expires_at = :exp",
            ExpressionAttributeValues={":h": valid_hash, ":exp": int(time.time()) - 3600},
        )
        expired_event = {
            **SAMPLE_JOB,
            "approved_hash": valid_hash,
            "approved_subject": subject,
            "approved_body": body,
            "approval_expires_at": int(time.time()) - 3600,
        }
        result = mod.lambda_handler(expired_event, MockContext())
        mock_smtp.assert_not_called()
        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["status"] == "FAILED"
        assert "expired" in item["failure_reason"]

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_atomic_approval_consumption_prevents_duplicate_send(self, mock_bedrock, mock_smtp):
        """Approval must be consumed atomically, preventing replay / duplicate sends."""
        import hashlib
        import time
        mod, table = self._setup_infra()
        mod.ALLOW_REAL_EMAIL_SEND = True
        mod.REQUIRE_DRAFT_APPROVAL = True

        recipient = SAMPLE_JOB["recruiter_email"]
        subject = "AI Engineer at Anthropic"
        body = "Hi Sarah,\n\nTest body.\n\nBest,\nAlex Hunter"
        valid_hash = hashlib.sha256(
            f"{SAMPLE_JOB['job_id']}:{recipient}:{subject}:{body}".encode("utf-8")
        ).hexdigest()

        table.update_item(
            Key={"job_id": SAMPLE_JOB["job_id"]},
            UpdateExpression="SET draft_hash = :h, approval_expires_at = :exp",
            ExpressionAttributeValues={":h": valid_hash, ":exp": int(time.time()) + 86400},
        )
        mock_smtp.return_value = [(SAMPLE_JOB["job_id"], True, None)]

        valid_event = {
            **SAMPLE_JOB,
            "approved_hash": valid_hash,
            "approved_subject": subject,
            "approved_body": body,
            "approval_expires_at": int(time.time()) + 86400,
        }

        # First send succeeds and atomically consumes approval
        res1 = mod.lambda_handler(valid_event, MockContext())
        assert res1["sent"] == 1
        assert mock_smtp.call_count == 1

        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["approval_consumed"] is True
        assert item["outreach_status"] == "SENT"

        # Attempting second send should be blocked (either idempotency check or conditional check failure)
        mock_smtp.reset_mock()
        res2 = mod.lambda_handler(valid_event, MockContext())
        assert mock_smtp.call_count == 0

    @patch("job_sender.lambda_function._send_via_smtp")
    @patch("job_sender.lambda_function.bedrock_client")
    def test_ambiguous_smtp_failure_marks_manual_reconciliation_and_skips_resend(self, mock_bedrock, mock_smtp):
        """Ambiguous SMTP connection failure transitions to MANUAL_RECONCILIATION_REQUIRED and prevents resend."""
        import hashlib
        import time
        mod, table = self._setup_infra()
        mod.ALLOW_REAL_EMAIL_SEND = True
        mod.REQUIRE_DRAFT_APPROVAL = True

        recipient = SAMPLE_JOB["recruiter_email"]
        subject = "AI Engineer at Anthropic"
        body = "Hi Sarah,\n\nTest body.\n\nBest,\nAlex Hunter"
        valid_hash = hashlib.sha256(
            f"{SAMPLE_JOB['job_id']}:{recipient}:{subject}:{body}".encode("utf-8")
        ).hexdigest()

        table.update_item(
            Key={"job_id": SAMPLE_JOB["job_id"]},
            UpdateExpression="SET draft_hash = :h, approval_expires_at = :exp",
            ExpressionAttributeValues={":h": valid_hash, ":exp": int(time.time()) + 86400},
        )
        mock_smtp.return_value = [(SAMPLE_JOB["job_id"], False, "Connection error: socket timeout")]

        event = {
            **SAMPLE_JOB,
            "approved_hash": valid_hash,
            "approved_subject": subject,
            "approved_body": body,
            "approval_expires_at": int(time.time()) + 86400,
        }

        # First attempt experiences ambiguous failure
        res = mod.lambda_handler(event, MockContext())
        assert res["failed"] == 1

        item = table.get_item(Key={"job_id": SAMPLE_JOB["job_id"]})["Item"]
        assert item["status"] == "MANUAL_RECONCILIATION_REQUIRED"
        assert item["outreach_status"] == "DELIVERY_AMBIGUOUS"

        # Subsequent attempt must skip instead of resending
        mock_smtp.reset_mock()
        res2 = mod.lambda_handler(event, MockContext())
        mock_smtp.assert_not_called()

