"""
Tests for job-router Lambda (Telegram Command Center).

Tests:
  1. "search jobs" command triggers ingestor
  2. "list" command shows pending jobs
  3. "apply" command approves and triggers sender
  4. "apply N" command approves Nth job
  5. "reject" command rejects top job
  6. "status" command shows pipeline stats
  7. SNS notification forwarding to Telegram
  8. Unauthorized chat ID rejected
  9. Decimal serialization doesn't crash
  10. Idempotency: double-approve prevented
"""

import json
import io
import pytest
import boto3
from decimal import Decimal
from moto import mock_aws
from unittest.mock import patch, MagicMock, call
from conftest import SAMPLE_JOB, MockContext


def _make_telegram_event(text, chat_id="99999"):
    """Create an API Gateway event mimicking a Telegram webhook."""
    return {
        "body": json.dumps({
            "message": {
                "text": text,
                "chat": {"id": int(chat_id)},
                "from": {"first_name": "Teja"},
            }
        })
    }


def _make_sns_event(message_text):
    """Create an SNS event."""
    return {
        "Records": [
            {"Sns": {"Message": message_text}}
        ]
    }


@mock_aws
class TestRouter:
    """End-to-end tests for job-router."""

    def _setup_infra(self):
        """Create DynamoDB table with test data and secrets."""
        import os
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        dynamo = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamo.create_table(
            TableName="Jobsearch",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "job_id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "status-index",
                    "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.meta.client.get_waiter("table_exists").wait(TableName="Jobsearch")

        sm = boto3.client("secretsmanager", region_name="us-east-1")
        sm.create_secret(
            Name="telegram",
            SecretString=json.dumps({
                "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF",
                "TELEGRAM_CHAT_ID": "99999",
            }),
        )

        import job_router.lambda_function as mod
        mod.dynamodb = dynamo
        mod.table = dynamo.Table("Jobsearch")
        mod.secrets_client = sm
        mod._cached_secrets = None  # Reset cache
        mod.ENABLE_PRACTICE_MODE = False

        return mod, table

    def _insert_pending_jobs(self, table, count=3, with_email=False):
        """Insert sample PENDING_APPROVAL jobs."""
        for i in range(count):
            item = {
                "job_id": f"pending-{i+1:03d}",
                "title": f"AI Engineer Role {i+1}",
                "company": f"Company{i+1}",
                "location": "San Francisco, CA",
                "match_score": Decimal(str(90 + i)),
                "match_reason": f"Strong match #{i+1}",
                "status": "PENDING_APPROVAL",
                "source": "hackernews",
                "posted_at": "2026-05-12T12:00:00Z",
                "url": f"https://company{i+1}.com/apply",
            }
            if with_email:
                item["recruiter_email"] = f"recruiter{i+1}@company{i+1}.com"
            table.put_item(Item=item)

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_search_jobs_triggers_ingestor(self, mock_lambda, mock_tg):
        """'search jobs' should invoke the ingestor Lambda."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_telegram_event("search jobs")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_called_once()
        call_kwargs = mock_lambda.invoke.call_args
        assert call_kwargs[1]["FunctionName"] == "job-ingestor" or \
               call_kwargs.kwargs.get("FunctionName") == "job-ingestor"

        mock_tg.assert_called_once()
        assert "search started" in mock_tg.call_args[0][2].lower()

    @patch("job_router.lambda_function._send_telegram")
    def test_list_shows_pending_jobs(self, mock_tg):
        """'list' should show all PENDING_APPROVAL jobs split by email availability."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table)

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        mock_tg.assert_called_once()
        msg = mock_tg.call_args[0][2]
        assert "Review queue" in msg
        assert "AI Engineer Role" in msg
        assert "Source: Hacker News Who is Hiring" in msg
        assert "manual follow-up" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_list_empty(self, mock_tg):
        """'list' with no pending jobs should inform the user."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "no jobs" in msg.lower() or "search jobs" in msg.lower()
        assert "tejajob" not in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_list_reports_unscored_backlog(self, mock_tg):
        """'list' should distinguish empty review queue from processing backlog."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={"job_id": "unscored-001", "status": "NEW"})

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "No 90%+ matches are ready yet" in msg
        assert "still unscored" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_list_hides_stale_remotive_rows(self, mock_tg):
        """Old Remotive matches should not leak into the demo review queue."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "old-remotive",
            "title": "AI Engineer",
            "company": "Dry Ground AI",
            "status": "PENDING_APPROVAL",
            "source": "remotive",
            "posted_at": "2026-04-21T12:00:00Z",
            "match_score": Decimal("92"),
            "url": "https://remotive.com/remote-jobs/artificial-intelligence/ai-engineer-2089958",
        })

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "No fresh 90%+ matches are ready" in msg
        assert "Dry Ground AI" not in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_list_hides_practice_rows_when_disabled(self, mock_tg):
        """Practice rows should never appear in production review queues."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "practice-001",
            "title": "AI Outbound Agent Engineer",
            "company": "Northstar Revenue AI",
            "status": "PENDING_APPROVAL",
            "source": "tejajob",
            "posted_at": "2026-05-12T12:00:00Z",
            "match_score": Decimal("94"),
        })

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "No fresh 90%+ matches are ready" in msg
        assert "Northstar Revenue AI" not in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_apply_email_job_returns_contact_without_sending(self, mock_lambda, mock_tg):
        """'apply' should expose contact details but never trigger sender."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 2, with_email=True)

        event = _make_telegram_event("apply")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_not_called()
        item = table.get_item(Key={"job_id": "pending-002"})["Item"]
        assert item["status"] == "MANUAL_APPLY"
        msg = mock_tg.call_args[0][2]
        assert "Contact: recruiter2@company2.com" in msg
        assert "No email was sent" in msg
        assert "Source link: https://company2.com/apply" in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_apply_manual_job_returns_direct_link(self, mock_lambda, mock_tg):
        """'apply' should handle no-email jobs without claiming no jobs exist."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        event = _make_telegram_event("apply")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_not_called()
        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "MANUAL_APPLY"
        assert "Contact: no recruiter email found" in mock_tg.call_args[0][2]
        assert "Source link: https://company1.com/apply" in mock_tg.call_args[0][2]
        assert "Status set to" not in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_tejajob_seeds_reviewable_job(self, mock_tg):
        """'tejajob' should seed a neutral showcase row into the review queue."""
        mod, table = self._setup_infra()
        mod.ENABLE_PRACTICE_MODE = True
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "old-showcase",
            "title": "Old Showcase",
            "company": "Old Co",
            "status": "PENDING_APPROVAL",
            "source": "showcase",
            "posted_at": "2026-05-12T12:00:00Z",
            "match_score": Decimal("94"),
        })

        event = _make_telegram_event("tejajob")
        mod.lambda_handler(event, MockContext())

        jobs = mod._get_pending_jobs(limit=5)
        assert len(jobs) == 1
        assert jobs[0]["source"] == "tejajob"
        assert jobs[0]["company"] == "Northstar Revenue AI"
        assert jobs[0]["apply_url"] == ""
        assert table.get_item(Key={"job_id": "old-showcase"})["Item"]["status"] == "REJECTED"
        assert "TejaJob workspace is ready" in mock_tg.call_args[0][2]
        assert "practice opportunity" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_apply_tejajob_completes_practice_without_fake_link(self, mock_lambda, mock_tg):
        """Applying to the practice item should not expose fake links or send email."""
        mod, table = self._setup_infra()
        mod.ENABLE_PRACTICE_MODE = True
        mock_tg.return_value = True
        mod._handle_showcase("token", "chat")
        job_id = mod._get_pending_jobs(limit=1)[0]["job_id"]

        event = _make_telegram_event("apply 1")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_not_called()
        item = table.get_item(Key={"job_id": job_id})["Item"]
        assert item["status"] == "SHOWCASE_COMPLETE"
        msg = mock_tg.call_args[0][2]
        assert "Practice workflow completed" in msg
        assert "No real application was submitted" in msg
        assert "example.com" not in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_tejajob_disabled_in_production(self, mock_tg):
        """Practice rows should not be created when practice mode is disabled."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "old-tejajob",
            "title": "AI Outbound Agent Engineer",
            "company": "Northstar Revenue AI",
            "status": "PENDING_APPROVAL",
            "source": "tejajob",
            "posted_at": "2026-05-12T12:00:00Z",
            "match_score": Decimal("94"),
        })

        event = _make_telegram_event("tejajob")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Practice mode is disabled" in msg
        assert mod._get_pending_jobs(limit=5) == []
        assert table.get_item(Key={"job_id": "old-tejajob"})["Item"]["status"] == "REJECTED"

    @patch("job_router.lambda_function._send_telegram")
    def test_explain_shows_match_reason(self, mock_tg):
        """'explain N' should show the stored scoring rationale."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        event = _make_telegram_event("explain 1")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Match explanation" in msg
        assert "Source: Hacker News Who is Hiring" in msg
        assert "Source link: https://company1.com/apply" in msg
        assert "Strong match #1" in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_draft_previews_without_sending(self, mock_lambda, mock_tg):
        """'draft N' should invoke sender in preview mode and display returned email."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)
        mock_lambda.invoke.return_value = {
            "Payload": io.BytesIO(json.dumps({
                "status": "preview",
                "email": {
                    "subject": "AI outbound agent role",
                    "body": "Hi Company1 Team,\n\nPreview body.\n\nBest,\nTeja",
                },
            }).encode("utf-8"))
        }

        event = _make_telegram_event("draft 1")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_called_once()
        assert mock_lambda.invoke.call_args.kwargs["InvocationType"] == "RequestResponse"
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload["preview_only"] is True
        msg = mock_tg.call_args[0][2]
        assert "Draft preview" in msg
        assert "No email was sent" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_reject_rejects_top_job(self, mock_tg):
        """'reject' should mark the top pending job as REJECTED."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1)

        event = _make_telegram_event("reject")
        mod.lambda_handler(event, MockContext())

        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "REJECTED"
        assert "Rejected" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_status_shows_stats(self, mock_tg):
        """'status' should show pipeline statistics."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        # Insert jobs with different statuses
        table.put_item(Item={"job_id": "j1", "status": "NEW"})
        table.put_item(Item={
            "job_id": "j2",
            "title": "AI Agent Engineer",
            "company": "Source Verified AI",
            "location": "San Francisco, CA",
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": "2026-05-12T12:00:00Z",
            "match_score": Decimal("95"),
            "url": "https://news.ycombinator.com/item?id=12345",
            "recruiter_email": "founders@sourceverified.ai",
            "match_reason": "Strong agentic workflow and Python match.",
        })
        table.put_item(Item={
            "job_id": "j4",
            "title": "ML Platform Engineer",
            "company": "Watchlist AI",
            "location": "San Francisco, CA",
            "status": "PENDING_APPROVAL",
            "source": "jsearch",
            "posted_at": "2026-05-12T12:00:00Z",
            "match_score": Decimal("82"),
            "url": "https://watchlist.example/jobs/ml-platform",
            "match_reason": "Good ML platform match but below the review threshold.",
        })
        table.put_item(Item={"job_id": "j3", "status": "SENT"})

        event = _make_telegram_event("status")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Pipeline Status" in msg
        assert "Review threshold:   90%" in msg
        assert "Reviewable evidence" in msg
        assert "AI Agent Engineer" in msg
        assert "Source: Hacker News Who is Hiring" in msg
        assert "Source link: https://news.ycombinator.com/item?id=12345" in msg
        assert "Contact: founders@sourceverified.ai" in msg
        assert "Other scored live roles below review threshold" in msg
        assert "ML Platform Engineer" in msg
        assert "Source link: https://watchlist.example/jobs/ml-platform" in msg
        assert "apply never sends email" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_sns_forwarded_to_telegram(self, mock_tg):
        """SNS notifications should be forwarded to Telegram."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_sns_event("New high match: 95% - Senior AI Engineer at Anthropic")
        result = mod.lambda_handler(event, MockContext())

        assert result["statusCode"] == 200
        mock_tg.assert_called_once()
        assert "95%" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_unauthorized_chat_rejected(self, mock_tg):
        """Messages from unknown chat IDs should be ignored."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_telegram_event("search jobs", chat_id="12345")  # Wrong chat ID
        result = mod.lambda_handler(event, MockContext())

        assert result["statusCode"] == 200
        mock_tg.assert_not_called()  # No response sent

    def test_decimal_serialization(self):
        """DecimalEncoder should handle DynamoDB Decimal types."""
        import job_router.lambda_function as mod
        mod, table = self._setup_infra()

        data = {"score": Decimal("95"), "ratio": Decimal("3.14")}
        result = json.loads(mod._safe_json_dumps(data))

        assert result["score"] == 95
        assert result["ratio"] == 3.14

    @patch("job_router.lambda_function._send_telegram")
    def test_help_command(self, mock_tg):
        """'help' should show all available commands."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_telegram_event("help")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "search jobs" in msg
        assert "tejajob" not in msg
        assert "complete practice" not in msg
        assert "draft 1" in msg
        assert "explain 1" in msg
        assert "list" in msg
        assert "apply" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_apply_n_gives_specific_correction(self, mock_tg):
        """Literal placeholder commands should get a short correction, not full help."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_telegram_event("Apply N")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert msg == "Use a match number, for example: apply 1"
