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
from conftest import SAMPLE_JOB, MockContext, make_sqs_event


@mock_aws
class TestSender:
    """End-to-end tests for job-sender."""

    def _setup_infra(self):
        """Create DynamoDB, S3, and Secrets resources."""
        import os
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["RESUME_KEY"] = "data/resume/Teja_Vaka.pdf"  # Override conftest default

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
            Key="data/resume/Teja_Vaka.pdf",
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
        mod.RESUME_KEY = "data/resume/Teja_Vaka.pdf"
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
                "RetireIQ (https://huggingface.co/spaces/vakaTeja/RetireIQ) handles "
                "multi-step financial reasoning, and Rivulet (https://rivulet.info) "
                "handles tool orchestration with human oversight.\n\n"
                "I am fully open to all locations and am ready to relocate or be onsite.\n\n"
                "Would 20 minutes this week work to connect?\n\n"
                "Best,\nTeja Gopal Reddy Vaka\nSenior AI Engineer"
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
            body="Hi Anthropic Team,\n\nPreview body.\n\nBest,\nTeja",
        )

        result = mod.lambda_handler({"preview_only": True, "job": SAMPLE_JOB}, MockContext())

        assert result["status"] == "preview"
        assert result["email"]["subject"] == "AI outbound agent role"
        mock_creds.assert_not_called()
        mock_resume.assert_not_called()

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
        """Generic inboxes should still fall back to company team."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()

        job = {
            **SAMPLE_JOB,
            "company": "Engineering Square",
            "recruiter_name": "",
            "recruiter_email": "careers@engineeringsquare.us",
        }
        msg = mod._build_user_message(job)
        assert "GREETING TO USE: Hi Engineering Square Team," in msg

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
        assert "Hi Anthropic Team," in msg

    @patch("job_sender.lambda_function.bedrock_client")
    def test_generated_body_greeting_is_enforced_from_email(self, mock_bedrock):
        """Even if the model returns a team greeting, code enforces the recipient name."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI Engineer role",
            body="Hi Engineering Square Team,\n\nI saw the role.\n\nBest,\nTeja",
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
        """Robotic cover-letter language should fail before SMTP can send it."""
        import job_sender.lambda_function as mod
        mod, _ = self._setup_infra()
        mock_bedrock.converse.return_value = self._mock_bedrock_email_response(
            subject="AI Engineer for Production RAG Systems",
            body=(
                "Hi Engineering Square Team,\n\n"
                "I've been building production AI systems at Empower for the past few years, "
                "and your focus on shipping fast with ownership resonates with how I work.\n\n"
                "Best,\nTeja"
            ),
        )
        job = {
            **SAMPLE_JOB,
            "company": "Engineering Square",
            "recruiter_name": "",
            "recruiter_email": "saikumar@engineeringsquare.us",
        }

        with pytest.raises(ValueError, match="quality gate"):
            mod._generate_email(job)

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
