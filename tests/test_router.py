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
import ast
import textwrap
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pytest
import boto3
from decimal import Decimal
from moto import mock_aws
from unittest.mock import patch, MagicMock, call
from conftest import SAMPLE_JOB, MockContext


def _make_telegram_event(text, chat_id="99999", headers=None):
    """Create an API Gateway event mimicking a Telegram webhook."""
    event = {
        "body": json.dumps({
            "message": {
                "text": text,
                "chat": {"id": int(chat_id)},
                "from": {"first_name": "Teja"},
            }
        })
    }
    if headers is not None:
        event["headers"] = headers
    return event


def _make_sns_event(message_text):
    """Create an SNS event."""
    return {
        "Records": [
            {"Sns": {"Message": message_text}}
        ]
    }


def _make_callback_event(data, chat_id="99999", message_id=321, callback_id=None):
    """Create an API Gateway event mimicking a Telegram callback query."""
    import uuid
    cb_id = callback_id or f"cb-{uuid.uuid4().hex[:8]}"
    return {
        "body": json.dumps({
            "callback_query": {
                "id": cb_id,
                "data": data,
                "message": {
                    "message_id": message_id,
                    "chat": {"id": int(chat_id)},
                },
            }
        })
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
        mod._PROCESSED_CALLBACK_IDS.clear()
        mod.ENABLE_PRACTICE_MODE = False
        mod.ALLOW_TELEGRAM_EMAIL_SEND = False

        return mod, table

    def _insert_pending_jobs(self, table, count=3, with_email=False):
        """Insert sample PENDING_APPROVAL jobs."""
        now = datetime.now(timezone.utc)
        for i in range(count):
            item = {
                "job_id": f"pending-{i+1:03d}",
                "title": f"AI Engineer Role {i+1}",
                "company": f"Company{i+1}",
                "location": "San Francisco, CA",
                "work_mode": "onsite",
                "work_mode_source": "location",
                "match_score": Decimal(str(90 + i)),
                "match_reason": f"Strong match #{i+1}",
                "status": "PENDING_APPROVAL",
                "source": "hackernews",
                "posted_at": (now - timedelta(hours=2 + i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
    @patch("job_router.lambda_function.lambda_client")
    def test_signal_test_triggers_ingestor_seed(self, mock_lambda, mock_tg):
        """'signal test' should invoke the ingestor with the synthetic notification seed."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_telegram_event("signal test")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_called_once()
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload["seed_signal_test"] is True
        msg = mock_tg.call_args[0][2]
        assert "Signal test started" in msg
        assert "synthetic AI Engineer match" in msg

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
        assert "Work mode: onsite" in msg
        assert "Work mode evidence: source location text" in msg
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
        assert "No 80%+ matches are ready yet" in msg
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
            "posted_at": (datetime.now(timezone.utc) - timedelta(days=25)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("92"),
            "url": "https://remotive.com/remote-jobs/artificial-intelligence/ai-engineer-2089958",
        })

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert "No fresh 80%+ matches are ready" in msg
        assert "Dry Ground AI" not in msg
        assert keyboard[0][0]["text"] == "Backlog"
        assert keyboard[0][1]["text"] == "Search"
        assert keyboard[1][0]["text"] == "List"
        assert keyboard[1][1]["text"] == "Next Match"

    @patch("job_router.lambda_function._send_telegram")
    def test_list_hides_hacker_news_rows_older_than_three_days(self, mock_tg):
        """HN posts older than three days should not remain reviewable."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "old-hn",
            "title": "Applied AI Engineer",
            "company": "Cora AI",
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("94"),
            "url": "https://news.ycombinator.com/item?id=123",
        })

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert "No fresh 80%+ matches are ready" in msg
        assert "Cora AI" not in msg
        assert keyboard[1][0]["text"] == "List"
        assert keyboard[1][1]["text"] == "Next Match"
        assert keyboard[2][0]["text"] == "Home"

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
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("94"),
        })

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Your review queue is empty." in msg
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
        assert "Work mode: onsite" in msg
        assert "Work mode evidence: source location text" in msg
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
        assert "reply applied" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_applied_records_real_world_outcome(self, mock_lambda, mock_tg):
        """After manual follow-up, 'applied' should record the real user outcome."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        mod.lambda_handler(_make_telegram_event("apply 1"), MockContext())
        mod.lambda_handler(_make_telegram_event("applied"), MockContext())

        mock_lambda.invoke.assert_not_called()
        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "APPLIED"
        assert item["application_outcome"] == "applied"
        assert item["user_feedback"] == "applied"
        assert item["feedback_source"] == "telegram"
        assert "applied_at" in item
        assert "Recorded: applied" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_not_applied_records_reason(self, mock_tg):
        """'not applied <reason>' should capture why the user skipped a role."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "manual-001",
            "title": "Applied AI Engineer",
            "company": "Cora AI",
            "location": "Remote",
            "status": "MANUAL_APPLY",
            "source": "hackernews",
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("91"),
            "url": "https://news.ycombinator.com/item?id=123",
            "status_updated_at": 1778600000,
        })

        mod.lambda_handler(_make_telegram_event("not applied too generic"), MockContext())

        item = table.get_item(Key={"job_id": "manual-001"})["Item"]
        assert item["status"] == "NOT_APPLIED"
        assert item["application_outcome"] == "not_applied"
        assert item["user_feedback"] == "not_applied"
        assert item["user_feedback_reason"] == "too generic"
        assert item["feedback_source"] == "telegram"
        assert "not_applied_at" in item
        assert "Recorded: not applied" in mock_tg.call_args[0][2]
        assert "too generic" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_followups_lists_manual_outcome_queue(self, mock_tg):
        """'followups' should show manual roles still waiting for applied/not applied."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "manual-001",
            "title": "Applied AI Engineer",
            "company": "Cora AI",
            "location": "Remote",
            "status": "MANUAL_APPLY",
            "source": "hackernews",
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("91"),
            "url": "https://news.ycombinator.com/item?id=123",
            "recruiter_email": "careers@cora.ai",
            "status_updated_at": 1778600000,
        })

        mod.lambda_handler(_make_telegram_event("followups"), MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Manual follow-ups waiting" in msg
        assert "Applied AI Engineer" in msg
        assert "Cora AI" in msg
        assert "applied 1" in msg
        assert "not applied 1" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_interested_records_preference_without_removing_job(self, mock_tg):
        """'interested N' should teach preference while keeping the role actionable."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        mod.lambda_handler(_make_telegram_event("interested 1"), MockContext())

        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "PENDING_APPROVAL"
        assert item["user_interest"] == "interested"
        assert item["user_feedback"] == "interested"
        assert item["feedback_source"] == "telegram"
        assert "Recorded: interested" in mock_tg.call_args[0][2]
        assert "use draft, apply, next, or skip <reason>" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_not_interested_records_reason_and_removes_job(self, mock_tg):
        """'not interested N reason' should remove the role and store why."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        mod.lambda_handler(_make_telegram_event("not interested 1 too remote"), MockContext())

        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "NOT_INTERESTED"
        assert item["user_interest"] == "not_interested"
        assert item["user_feedback"] == "not_interested"
        assert item["user_feedback_reason"] == "too remote"
        assert item["feedback_source"] == "telegram"
        assert mod._get_pending_jobs(limit=5) == []
        assert "Recorded: not interested" in mock_tg.call_args[0][2]
        assert "too remote" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_skip_requires_reason_before_removing_job(self, mock_tg):
        """Daily-driver 'skip' should ask for a reason before learning or removing a job."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        mod.lambda_handler(_make_telegram_event("skip"), MockContext())

        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "PENDING_APPROVAL"
        assert "user_feedback" not in item
        msg = mock_tg.call_args[0][2]
        assert "Tell me why" in msg
        assert "skip too senior" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_skip_with_reason_records_not_interested_feedback(self, mock_tg):
        """Daily-driver 'skip reason' should persist the reason as not-interested feedback."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        mod.lambda_handler(_make_telegram_event("skip too senior"), MockContext())

        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "NOT_INTERESTED"
        assert item["user_interest"] == "not_interested"
        assert item["user_feedback"] == "not_interested"
        assert item["user_feedback_reason"] == "too senior"
        assert "Recorded: not interested" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_meh_records_neutral_feedback(self, mock_tg):
        """'meh N reason' should save neutral feedback without hiding the job."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        mod.lambda_handler(_make_telegram_event("meh 1 draft generic"), MockContext())

        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "PENDING_APPROVAL"
        assert item["user_interest"] == "neutral"
        assert item["user_feedback"] == "neutral"
        assert item["user_feedback_reason"] == "draft generic"
        assert "Recorded: neutral" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    def test_next_and_why_show_top_match_explanation(self, mock_tg):
        """Daily-driver top-job commands should use the active review session."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 3, with_email=False)

        mod.lambda_handler(_make_telegram_event("next"), MockContext())
        assert "[92% match]" in mock_tg.call_args[0][2]
        markup = mock_tg.call_args.kwargs["reply_markup"]
        assert "keyboard" not in markup
        inline_kb = markup["inline_keyboard"]
        assert inline_kb[0][0]["text"] == "Why"
        assert inline_kb[0][2]["text"] == "✓ Apply"
        assert inline_kb[1][0]["text"] == "→ Next Match"

        mod.lambda_handler(_make_telegram_event("next"), MockContext())
        assert "AI Engineer Role 2" in mock_tg.call_args[0][2]

        mod.lambda_handler(_make_telegram_event("why"), MockContext())
        assert "Match #" in mock_tg.call_args[0][2]
        assert "AI Engineer Role 2" in mock_tg.call_args[0][2]

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_bare_draft_uses_active_review_job(self, mock_lambda, mock_tg):
        """Bare draft should preview the current session job rather than resetting to the top match."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 3, with_email=False)
        mock_lambda.invoke.return_value = {
            "Payload": io.BytesIO(json.dumps({
                "status": "preview",
                "email": {
                    "subject": "AI outbound agent role",
                    "body": "Hi Company2 Team,\n\nPreview body.\n\nBest,\nTeja",
                },
            }).encode("utf-8"))
        }

        mod.lambda_handler(_make_telegram_event("next"), MockContext())
        mod.lambda_handler(_make_telegram_event("next"), MockContext())
        mod.lambda_handler(_make_telegram_event("draft"), MockContext())

        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload["job"]["job_id"] == "pending-002"

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_bare_apply_uses_active_review_job(self, mock_lambda, mock_tg):
        """Bare apply should act on the current review-session job."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 3, with_email=False)

        mod.lambda_handler(_make_telegram_event("next"), MockContext())
        mod.lambda_handler(_make_telegram_event("next"), MockContext())
        mod.lambda_handler(_make_telegram_event("apply"), MockContext())

        mock_lambda.invoke.assert_not_called()
        assert table.get_item(Key={"job_id": "pending-002"})["Item"]["status"] == "MANUAL_APPLY"
        assert table.get_item(Key={"job_id": "pending-003"})["Item"]["status"] == "PENDING_APPROVAL"

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
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
        assert "Match #" in msg
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
        assert "Draft for match #1" in msg
        assert "No email was sent" in msg
        assert "skip <reason>" in msg
        assert "interested" in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_draft_shows_fallback_note_when_sender_degrades(self, mock_lambda, mock_tg):
        """Draft preview should clearly label fallback generation when sender was throttled."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)
        mock_lambda.invoke.return_value = {
            "Payload": io.BytesIO(json.dumps({
                "status": "preview",
                "generated_with_fallback": True,
                "email": {
                    "subject": "AI Engineer role at Company1",
                    "body": "Hi Company1 Team,\n\nFallback body.\n\nBest,\nTeja",
                },
            }).encode("utf-8"))
        }

        event = _make_telegram_event("draft 1")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "safe fallback template" in msg
        assert "No email was sent" in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_send_disabled_does_not_invoke_sender(self, mock_lambda, mock_tg):
        """'send N' should be blocked unless Telegram email send is enabled."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=True)

        event = _make_telegram_event("send 1")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_not_called()
        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "PENDING_APPROVAL"
        msg = mock_tg.call_args[0][2]
        assert "Email send is disabled" in msg
        assert "ALLOW_TELEGRAM_EMAIL_SEND=true" in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_send_enabled_requires_confirmation(self, mock_lambda, mock_tg):
        """'send N' should show a confirmation step before invoking sender."""
        mod, table = self._setup_infra()
        mod.ALLOW_TELEGRAM_EMAIL_SEND = True
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=True)

        event = _make_telegram_event("send 1")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_not_called()
        item = table.get_item(Key={"job_id": "pending-001"})["Item"]
        assert item["status"] == "PENDING_APPROVAL"
        msg = mock_tg.call_args[0][2]
        assert "Send confirmation required" in msg
        assert "Type exactly: confirm send 1" in msg
        assert "No email was sent" in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_confirm_send_invokes_sender(self, mock_lambda, mock_tg):
        """'confirm send N' should invoke sender with an explicit confirmed payload."""
        mod, table = self._setup_infra()
        mod.ALLOW_TELEGRAM_EMAIL_SEND = True
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=True)
        mock_lambda.invoke.return_value = {
            "Payload": io.BytesIO(json.dumps({"status": "sent", "sent": 1}).encode("utf-8"))
        }

        event = _make_telegram_event("confirm send 1")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_called_once()
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload["send_email"] is True
        assert payload["send_confirmed"] is True
        assert payload["job"]["job_id"] == "pending-001"
        msg = mock_tg.call_args[0][2]
        assert "Email sent" in msg
        assert "To: recruiter1@company1.com" in msg

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_send_requires_recruiter_email(self, mock_lambda, mock_tg):
        """'send N' should not invoke sender when no recruiter email exists."""
        mod, table = self._setup_infra()
        mod.ALLOW_TELEGRAM_EMAIL_SEND = True
        mock_tg.return_value = True
        self._insert_pending_jobs(table, 1, with_email=False)

        event = _make_telegram_event("send 1")
        mod.lambda_handler(event, MockContext())

        mock_lambda.invoke.assert_not_called()
        assert "No recruiter email" in mock_tg.call_args[0][2]

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
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("72"),
            "url": "https://watchlist.example/jobs/ml-platform",
            "match_reason": "Good ML platform match but below the review threshold.",
        })
        table.put_item(Item={"job_id": "j3", "status": "SENT"})

        event = _make_telegram_event("status")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Pipeline Status" in msg
        assert "Review threshold:   80%" in msg
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
    def test_status_points_to_backlog_when_only_hidden_pending_jobs_exist(self, mock_tg):
        """Status should give a clear next step when pending rows exist but nothing is reviewable now."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        table.put_item(Item={
            "job_id": "old-pending",
            "title": "Old AI Engineer",
            "company": "Archive Labs",
            "location": "Remote",
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("91"),
            "url": "https://archive.example/jobs/ai",
        })

        mod.lambda_handler(_make_telegram_event("status"), MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Reviewable evidence: none" in msg
        assert "processed job(s) sit behind the current rules" in msg
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert keyboard[1][0]["text"] == "List"
        assert keyboard[1][1]["text"] == "Next Match"

    @patch("job_router.lambda_function._send_telegram")
    def test_backlog_lists_hidden_pending_jobs(self, mock_tg):
        """Backlog should expose hidden pending jobs in a human-readable way."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        table.put_item(Item={
            "job_id": "old-pending",
            "title": "Old AI Engineer",
            "company": "Archive Labs",
            "location": "Remote",
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("91"),
            "url": "https://archive.example/jobs/ai",
        })

        mod.lambda_handler(_make_telegram_event("backlog"), MockContext())

        msg = mock_tg.call_args[0][2]
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert "Older jobs archive: 1 processed job(s)" in msg
        assert "Old AI Engineer" in msg
        assert "Hidden because: older than the 3-day freshness rule" in msg
        assert keyboard[0][1]["text"] == "Backlog"

    @patch("job_router.lambda_function._send_telegram")
    def test_backlog_hides_signal_test_rows(self, mock_tg):
        """Synthetic signal-test rows must not appear in the user-facing backlog."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        table.put_item(Item={
            "job_id": "signal-test-001",
            "title": "AI Engineer Notification Test",
            "company": "Signal Test AI",
            "location": "Wichita, Kansas",
            "status": "PENDING_APPROVAL",
            "source": "signal_test",
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("96"),
            "url": "https://example.com/signal-test-ai-engineer",
            "signal_test": True,
        })

        mod.lambda_handler(_make_telegram_event("backlog"), MockContext())

        msg = mock_tg.call_args[0][2]
        assert "No older processed jobs are sitting behind the review rules." in msg
        assert "Signal Test AI" not in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_next_empty_state_points_to_backlog(self, mock_tg):
        """Next should guide the user to backlog when hidden pending jobs exist but nothing is reviewable."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        table.put_item(Item={
            "job_id": "old-hn",
            "title": "Applied AI Engineer",
            "company": "Cora AI",
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("94"),
            "url": "https://news.ycombinator.com/item?id=123",
        })

        mod.lambda_handler(_make_telegram_event("next"), MockContext())

        msg = mock_tg.call_args[0][2]
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert "No fresh reviewable match is ready." in msg
        assert "Use backlog to inspect the older jobs archive" in msg
        assert keyboard[0][0]["text"] == "Backlog"
        assert keyboard[1][0]["text"] == "List"
        assert keyboard[1][1]["text"] == "Next Match"
        assert keyboard[2][0]["text"] == "Home"

    @patch("job_router.lambda_function._send_telegram")
    def test_list_empty_state_keeps_next_match_reachable(self, mock_tg):
        """After list shows no fresh jobs, the user should still be able to retry Next Match from the keyboard."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        table.put_item(Item={
            "job_id": "old-hn",
            "title": "Applied AI Engineer",
            "company": "Cora AI",
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("94"),
            "url": "https://news.ycombinator.com/item?id=123",
        })

        mod.lambda_handler(_make_telegram_event("list"), MockContext())

        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert keyboard[1][1]["text"] == "Next Match"

    @patch("job_router.lambda_function._send_telegram")
    def test_status_excludes_signal_test_rows_from_counts(self, mock_tg):
        """User-facing status counts should ignore synthetic signal-test rows."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        table.put_item(Item={
            "job_id": "signal-test-001",
            "title": "AI Engineer Notification Test",
            "company": "Signal Test AI",
            "location": "Wichita, Kansas",
            "status": "PENDING_APPROVAL",
            "source": "signal_test",
            "posted_at": (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "match_score": Decimal("96"),
            "url": "https://example.com/signal-test-ai-engineer",
            "signal_test": True,
        })

        mod.lambda_handler(_make_telegram_event("status"), MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Pending total:      0" in msg
        assert "Signal Test AI" not in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_sns_forwarded_to_telegram(self, mock_tg):
        """SNS notifications should be forwarded to Telegram."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_sns_event("New high match: 95% - AI Engineer at Anthropic")
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

    @patch("job_router.lambda_function._send_telegram")
    def test_webhook_secret_missing_rejected(self, mock_tg):
        """When TELEGRAM_WEBHOOK_SECRET is set, requests without the header must be rejected with 403."""
        mod, table = self._setup_infra()
        mod._cached_secrets = {
            "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF",
            "TELEGRAM_CHAT_ID": "99999",
            "TELEGRAM_WEBHOOK_SECRET": "correct-secret-token",
        }
        mock_tg.return_value = True

        event = _make_telegram_event("status")  # No headers
        result = mod.lambda_handler(event, MockContext())

        assert result["statusCode"] == 403
        assert "Forbidden" in result["body"]
        mock_tg.assert_not_called()

    @patch("job_router.lambda_function._send_telegram")
    def test_webhook_secret_invalid_rejected(self, mock_tg):
        """Requests with an incorrect secret token header must be rejected with 403."""
        mod, table = self._setup_infra()
        mod._cached_secrets = {
            "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF",
            "TELEGRAM_CHAT_ID": "99999",
            "TELEGRAM_WEBHOOK_SECRET": "correct-secret-token",
        }
        mock_tg.return_value = True

        event = _make_telegram_event("status", headers={"X-Telegram-Bot-Api-Secret-Token": "wrong-token"})
        result = mod.lambda_handler(event, MockContext())

        assert result["statusCode"] == 403
        assert "Forbidden" in result["body"]
        mock_tg.assert_not_called()

    @patch("job_router.lambda_function._send_telegram")
    def test_webhook_secret_valid_accepted(self, mock_tg):
        """Requests with the correct secret token header must be processed."""
        mod, table = self._setup_infra()
        mod._cached_secrets = {
            "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF",
            "TELEGRAM_CHAT_ID": "99999",
            "TELEGRAM_WEBHOOK_SECRET": "correct-secret-token",
        }
        mock_tg.return_value = True

        event = _make_telegram_event("status", headers={"X-Telegram-Bot-Api-Secret-Token": "correct-secret-token"})
        result = mod.lambda_handler(event, MockContext())

        assert result["statusCode"] == 200
        mock_tg.assert_called_once()

    @patch("job_router.lambda_function._send_telegram")
    def test_webhook_secret_case_insensitive_header(self, mock_tg):
        """Header comparison should be case-insensitive."""
        mod, table = self._setup_infra()
        mod._cached_secrets = {
            "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF",
            "TELEGRAM_CHAT_ID": "99999",
            "TELEGRAM_WEBHOOK_SECRET": "correct-secret-token",
        }
        mock_tg.return_value = True

        event = _make_telegram_event("status", headers={"x-telegram-bot-api-secret-token": "correct-secret-token"})
        result = mod.lambda_handler(event, MockContext())

        assert result["statusCode"] == 200
        mock_tg.assert_called_once()

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
        assert "Fresh AI-matched roles for Teja." in msg
        assert "tejajob" not in msg
        assert "complete practice" not in msg
        assert "Start with Next Match." in msg
        assert "Use Search for a new scan." in msg
        assert "Use Follow-up for jobs you acted on." in msg
        assert "Use More for Status, Backlog, and Signal Test." in msg
        assert "AI Job Copilot" in msg
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert keyboard[0][0]["text"] == "Next Match"
        assert keyboard[0][1]["text"] == "More"
        assert keyboard[1][0]["text"] == "Search"
        assert keyboard[1][1]["text"] == "List"
        assert len(keyboard) == 2
        assert keyboard[1][2]["text"] == "Follow-up"

    @patch("job_router.lambda_function._send_telegram")
    def test_help_command_with_send_enabled_shows_confirm_send(self, mock_tg):
        """Help should remain focused on the home-state keyboard even when send is enabled."""
        mod, table = self._setup_infra()
        mod.ALLOW_TELEGRAM_EMAIL_SEND = True
        mock_tg.return_value = True

        event = _make_telegram_event("help")
        mod.lambda_handler(event, MockContext())

        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert keyboard[0][1]["text"] == "More"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._edit_telegram_message")
    def test_help_callback_switches_to_review_page(self, mock_edit, mock_answer):
        """Breadcrumb buttons should edit the help message in place."""
        mod, table = self._setup_infra()
        mock_edit.return_value = True
        mock_answer.return_value = True

        event = _make_callback_event("help:review")
        mod.lambda_handler(event, MockContext())

        assert mock_edit.called
        text = mock_edit.call_args.args[3]
        markup = mock_edit.call_args.kwargs["reply_markup"]
        assert "Home > Review" in text
        assert "- why  Explain the current match" in text
        assert markup["inline_keyboard"][0][0]["text"] == "Home"
        assert markup["inline_keyboard"][0][1]["text"] == "Review"
        assert markup["inline_keyboard"][1][0]["text"] == "Next"
        assert markup["inline_keyboard"][1][1]["text"] == "Why"
        mock_answer.assert_called_once()

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._edit_telegram_message")
    def test_help_callback_switches_to_more_page(self, mock_edit, mock_answer):
        """Advanced actions should live under More, not on the main home surface."""
        mod, table = self._setup_infra()
        mock_edit.return_value = True
        mock_answer.return_value = True

        event = _make_callback_event("help:more")
        mod.lambda_handler(event, MockContext())

        text = mock_edit.call_args.args[3]
        markup = mock_edit.call_args.kwargs["reply_markup"]
        assert "Home > More" in text
        assert "- signal test" in text
        assert markup["inline_keyboard"][1][0]["text"] == "Status"
        mock_answer.assert_called_once()

    @patch("job_router.lambda_function._send_telegram")
    def test_more_command_shows_more_state_keyboard(self, mock_tg):
        """The More surface should use the advanced-state reply keyboard."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        mod.lambda_handler(_make_telegram_event("more"), MockContext())

        text = mock_tg.call_args[0][2]
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert "Advanced controls live here." in text
        assert keyboard[0][0]["text"] == "Status"
        assert keyboard[0][1]["text"] == "Backlog"
        assert keyboard[3][0]["text"] == "Home"

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function._answer_callback_query")
    def test_callback_confirm_send_is_blocked_until_typed(self, mock_answer, mock_tg):
        """Inline callbacks must not bypass the explicit typed send confirmation step."""
        mod, table = self._setup_infra()
        mock_answer.return_value = True
        mock_tg.return_value = True

        event = _make_callback_event("cmd:confirm send 1")
        mod.lambda_handler(event, MockContext())

        mock_answer.assert_called_once()
        assert "Type confirm send manually" in mock_answer.call_args.args[2]
        mock_tg.assert_called_once()
        assert "Type exactly: confirm send 1" in mock_tg.call_args.args[2]

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._handle_list")
    def test_callback_command_runs_list(self, mock_list, mock_answer):
        """Inline command shortcuts should route to normal command handlers."""
        mod, table = self._setup_infra()
        mock_answer.return_value = True

        event = _make_callback_event("cmd:list")
        mod.lambda_handler(event, MockContext())

        mock_list.assert_called_once()
        mock_answer.assert_called_once()

    @patch("job_router.lambda_function._send_telegram")
    def test_apply_n_gives_specific_correction(self, mock_tg):
        """Literal placeholder commands should get a short correction, not full help."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        event = _make_telegram_event("Apply N")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert msg == "Use a match number, for example: apply 1"

    @patch("job_router.lambda_function._send_telegram")
    def test_no_link_company_is_excluded_from_review_queue_and_list(self, mock_tg):
        """Jobs without a valid canonical source URL must not appear in review queue or list."""
        mod, table = self._setup_infra()
        now = datetime.now(timezone.utc)

        # Valid job
        table.put_item(Item={
            "job_id": "valid-job-1",
            "title": "AI Engineer",
            "company": "Valid Tech",
            "match_score": Decimal("90"),
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": "https://validtech.com/careers/ai",
        })

        # No Link Company with "Unavailable"
        table.put_item(Item={
            "job_id": "no-link-job-1",
            "title": "AI Engineer",
            "company": "No Link Company",
            "match_score": Decimal("95"),
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": "Unavailable",
        })

        # No Link Company with empty URL
        table.put_item(Item={
            "job_id": "no-link-job-2",
            "title": "ML Engineer",
            "company": "Empty Link Inc",
            "match_score": Decimal("92"),
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": "",
        })

        event = _make_telegram_event("list")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Valid Tech" in msg
        assert "No Link Company" not in msg
        assert "Empty Link Inc" not in msg
        assert "Review queue: 1 match(es) ready" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_no_link_company_appears_in_backlog_with_missing_source_link_reason(self, mock_tg):
        """Jobs with missing or invalid URLs are documented in the backlog archive."""
        mod, table = self._setup_infra()
        now = datetime.now(timezone.utc)

        table.put_item(Item={
            "job_id": "no-link-1",
            "title": "AI Engineer",
            "company": "No Link Company",
            "match_score": Decimal("88"),
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": "Unavailable",
        })

        event = _make_telegram_event("backlog")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "No Link Company" in msg
        assert "Hidden because: missing or invalid canonical source link" in msg

    @patch("job_router.lambda_function._send_telegram")
    def test_apply_and_draft_blocked_when_job_lacks_canonical_url(self, mock_tg):
        """Direct apply or draft calls must explicitly block opportunities without valid URLs."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        bad_job = {
            "job_id": "bad-url-job",
            "title": "AI Engineer",
            "company": "No Link Company",
            "url": "Unavailable",
            "source": "hackernews",
            "status": "PENDING_APPROVAL",
        }

        with patch.object(mod, "_get_pending_jobs", return_value=[bad_job]):
            mod._handle_draft("token", "99999", index=1)
            msg = mock_tg.call_args[0][2]
            assert "Blocked: match #1 (No Link Company) has no verifiable source link." in msg

            mod._handle_apply("token", "99999", index=1)
            msg = mock_tg.call_args[0][2]
            assert "Blocked: match #1 (No Link Company) has no verifiable source link." in msg


@mock_aws
class TestWorkModeUncertainty:
    """Fix #6: Explicit Work Mode Uncertainty.

    Verify that unknown work modes surface explicit uncertainty rather than
    silently displaying 'unknown'.
    """

    def _setup_infra(self):
        """Reuse the same DynamoDB + secrets setup as TestRouter."""
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
        mod._cached_secrets = None
        mod.ENABLE_PRACTICE_MODE = False
        mod.ALLOW_TELEGRAM_EMAIL_SEND = False

        return mod, table

    def test_work_mode_label_known_values(self):
        """Known work modes should pass through unchanged."""
        mod, _ = self._setup_infra()
        assert mod._work_mode_label({"work_mode": "remote"}) == "remote"
        assert mod._work_mode_label({"work_mode": "hybrid"}) == "hybrid"
        assert mod._work_mode_label({"work_mode": "onsite"}) == "onsite"
        assert mod._work_mode_label({"work_mode": " Remote "}) == "remote"

    def test_work_mode_label_unknown_values(self):
        """Missing, empty, or unrecognized work modes should return 'not stated'."""
        mod, _ = self._setup_infra()
        assert mod._work_mode_label({}) == "not stated"
        assert mod._work_mode_label({"work_mode": ""}) == "not stated"
        assert mod._work_mode_label({"work_mode": None}) == "not stated"
        assert mod._work_mode_label({"work_mode": "flexible"}) == "not stated"

    def test_is_work_mode_uncertain(self):
        """Uncertainty flag aligns with the label function."""
        mod, _ = self._setup_infra()
        assert mod._is_work_mode_uncertain({}) is True
        assert mod._is_work_mode_uncertain({"work_mode": ""}) is True
        assert mod._is_work_mode_uncertain({"work_mode": "remote"}) is False
        assert mod._is_work_mode_uncertain({"work_mode": "onsite"}) is False

    def test_work_mode_evidence_known_sources(self):
        """Each source field maps to a human-readable evidence label."""
        mod, _ = self._setup_infra()
        assert mod._work_mode_evidence({"work_mode_source": "job_is_remote"}) == "source API remote flag"
        assert mod._work_mode_evidence({"work_mode_source": "location"}) == "source location text"
        assert mod._work_mode_evidence({"work_mode_source": "description"}) == "source job description"

    def test_work_mode_evidence_missing_source(self):
        """Missing source evidence returns an explicit verify-before-acting message."""
        mod, _ = self._setup_infra()
        result = mod._work_mode_evidence({})
        assert "not stated in source" in result
        assert "verify" in result.lower()

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_review_card_shows_warning_for_uncertain_work_mode(self, mock_lambda, mock_tg):
        """Review card should show ⚠️ marker when work mode is uncertain."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True

        now = datetime.now(timezone.utc)
        table.put_item(Item={
            "job_id": "uncertain-wm-001",
            "title": "ML Engineer",
            "company": "TestCo",
            "location": "Remote",
            "work_mode": "",
            "work_mode_source": "",
            "match_score": Decimal("92"),
            "match_reason": "Strong ML match",
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": "https://testco.com/apply",
        })

        card = mod._format_next_review_card({
            "title": "ML Engineer",
            "company": "TestCo",
            "location": "Remote",
            "work_mode": "",
            "work_mode_source": "",
            "match_score": 92,
            "source": "hackernews",
        })
        assert "⚠️" in card
        assert "not stated" in card

    @patch("job_router.lambda_function._send_telegram")
    @patch("job_router.lambda_function.lambda_client")
    def test_review_card_no_warning_for_known_work_mode(self, mock_lambda, mock_tg):
        """Review card should NOT show ⚠️ when work mode is known."""
        mod, _ = self._setup_infra()

        card = mod._format_next_review_card({
            "title": "AI Engineer",
            "company": "GoodCo",
            "location": "SF",
            "work_mode": "remote",
            "work_mode_source": "job_is_remote",
            "match_score": 90,
            "source": "adzuna",
        })
        assert "⚠️" not in card
        assert "remote" in card


@mock_aws
class TestRouterRegressionEdgeCases:
    """Regression tests covering confirmed edge cases: repeated taps, old cards,
    delivery failure tracking, malformed webhooks, header redaction, and send gates.
    """

    def _setup_infra(self):
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
        mod._cached_secrets = None
        mod._PROCESSED_CALLBACK_IDS.clear()
        mod.ENABLE_PRACTICE_MODE = False
        mod.ALLOW_TELEGRAM_EMAIL_SEND = False
        mod.REQUIRE_TELEGRAM_WEBHOOK_SECRET = False

        return mod, table

    def _insert_pending_jobs(self, table, count=2):
        now = datetime.now(timezone.utc)
        for i in range(count):
            table.put_item(Item={
                "job_id": f"pending-{i+1:03d}",
                "title": f"AI Engineer Role {i+1}",
                "company": f"Company{i+1}",
                "location": "San Francisco, CA",
                "work_mode": "remote",
                "work_mode_source": "location",
                "match_score": Decimal(str(95 - i)),
                "match_reason": f"Strong match #{i+1}",
                "status": "PENDING_APPROVAL",
                "source": "hackernews",
                "posted_at": (now - timedelta(hours=1 + i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "url": f"https://company{i+1}.com/apply",
            })

    def test_malformed_webhook_inputs_return_400(self):
        """Malformed JSON or non-object bodies must return HTTP 400 without crashing."""
        mod, _ = self._setup_infra()

        # null body
        resp = mod.lambda_handler({"body": "null"}, MockContext())
        assert resp["statusCode"] == 400
        assert "Bad Request" in resp["body"]

        # empty array body
        resp = mod.lambda_handler({"body": "[]"}, MockContext())
        assert resp["statusCode"] == 400
        assert "Bad Request" in resp["body"]

        # non-dict message
        resp = mod.lambda_handler({"body": json.dumps({"message": "string not dict"})}, MockContext())
        assert resp["statusCode"] == 400

        # non-dict callback_query
        resp = mod.lambda_handler({"body": json.dumps({"callback_query": "string not dict"})}, MockContext())
        assert resp["statusCode"] == 400

    @patch("job_router.lambda_function._send_telegram")
    def test_numeric_message_text_does_not_crash(self, mock_tg):
        """Numeric message.text must be safely coerced to str without AttributeError."""
        mod, _ = self._setup_infra()
        mock_tg.return_value = True

        event = {"body": json.dumps({"message": {"text": 12345, "chat": {"id": 99999}}})}
        resp = mod.lambda_handler(event, MockContext())
        assert resp["statusCode"] == 200

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_duplicate_callback_replay_does_not_advance_to_second_job(self, mock_tg, mock_ack):
        """Replaying the exact same callback ID must be rejected as duplicate and not modify job 2."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 2)

        # First tap on apply
        event = _make_callback_event("cmd:apply", callback_id="same-cb-id-123")
        resp1 = mod.lambda_handler(event, MockContext())
        assert resp1["statusCode"] == 200

        item1 = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        item2 = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item1["status"] == "MANUAL_APPLY"
        assert item2["status"] == "PENDING_APPROVAL"

        # Replay EXACT same callback event
        resp2 = mod.lambda_handler(event, MockContext())
        assert resp2["statusCode"] == 200
        assert "Duplicate callback ignored" in resp2.get("body", "")

        # Verify job 2 is STILL PENDING_APPROVAL!
        item2_after = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item2_after["status"] == "PENDING_APPROVAL"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_apply_bound_to_job_id_is_idempotent_and_isolated(self, mock_tg, mock_ack):
        """Tapping apply bound to job 1 repeatedly re-delivers job 1 and never touches job 2."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 2)

        # First tap: cmd:apply:pending-001
        event1 = _make_callback_event("cmd:apply:pending-001", callback_id="cb-tap-1")
        mod.lambda_handler(event1, MockContext())

        item1 = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        item2 = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item1["status"] == "MANUAL_APPLY"
        assert item2["status"] == "PENDING_APPROVAL"

        # Second tap: cmd:apply:pending-001 with new callback ID (user tapped card 1 again)
        event2 = _make_callback_event("cmd:apply:pending-001", callback_id="cb-tap-2")
        mod.lambda_handler(event2, MockContext())

        # Follow-up info re-delivered
        msg = mock_tg.call_args[0][2]
        assert "Manual follow-up already selected (re-delivered)" in msg
        assert "Company1" in msg

        # Job 2 is COMPLETELY UNTOUCHED!
        item2_after = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item2_after["status"] == "PENDING_APPROVAL"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_old_card_actions_do_not_affect_pending_jobs(self, mock_tg, mock_ack):
        """Actions on stale cards (already REJECTED/APPLIED) must inform user and not touch pending jobs."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 1)

        # Insert an old rejected job
        table.put_item(Item={
            "job_id": "old-stale-001",
            "title": "Old AI Role",
            "company": "Old Corp",
            "status": "REJECTED",
            "source": "hackernews",
            "url": "https://oldcorp.com/apply",
        })

        # User taps Apply on the old card
        event = _make_callback_event("cmd:apply:old-stale-001", callback_id="old-card-tap")
        mod.lambda_handler(event, MockContext())

        msg = mock_tg.call_args[0][2]
        assert "Job already REJECTED" in msg
        assert "Old cards do not affect current matches" in msg

        # Pending job 1 is STILL PENDING_APPROVAL!
        item1 = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item1["status"] == "PENDING_APPROVAL"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_delivery_failure_tracks_delivery_status_and_allows_idempotent_retry(self, mock_tg, mock_ack):
        """When Telegram message fails to deliver, record DELIVERY_FAILED and allow idempotent retry."""
        mod, table = self._setup_infra()
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 2)

        # Delivery fails on first attempt
        mock_tg.return_value = False
        event1 = _make_callback_event("cmd:apply:pending-001", callback_id="fail-attempt-1")
        mod.lambda_handler(event1, MockContext())

        item1 = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item1["status"] == "MANUAL_APPLY"
        assert item1["delivery_status"] == "DELIVERY_FAILED"
        assert "telegram send failed" in item1["delivery_error"]

        # Now delivery succeeds on retry
        mock_tg.return_value = True
        event2 = _make_callback_event("cmd:apply:pending-001", callback_id="retry-attempt-2")
        mod.lambda_handler(event2, MockContext())

        item1_after = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item1_after["status"] == "MANUAL_APPLY"
        assert item1_after["delivery_status"] == "DELIVERED"
        assert item1_after["delivery_error"] == ""

        # Pending job 2 was not touched
        item2 = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item2["status"] == "PENDING_APPROVAL"

    def test_auth_headers_redacted_from_logging(self, caplog):
        """Authentication and secret tokens must be scrubbed from logs."""
        import logging
        mod, _ = self._setup_infra()

        event = {
            "headers": {
                "X-Telegram-Bot-Api-Secret-Token": "SUPER_SECRET_TOKEN_XYZ",
                "Authorization": "Bearer SECRET_AUTH_KEY",
                "Host": "api.telegram.org",
            },
            "body": json.dumps({"message": {"text": "list", "chat": {"id": 99999}}}),
        }

        with caplog.at_level(logging.INFO), patch.object(mod, "_send_telegram", return_value=True):
            mod.lambda_handler(event, MockContext())

        log_output = caplog.text
        assert "SUPER_SECRET_TOKEN_XYZ" not in log_output
        assert "SECRET_AUTH_KEY" not in log_output
        assert "[REDACTED]" in log_output

    def test_webhook_secret_enforcement(self):
        """When webhook secret is required, invalid or missing secrets must return 403."""
        mod, _ = self._setup_infra()
        mod.REQUIRE_TELEGRAM_WEBHOOK_SECRET = True
        mod._cached_secrets = {
            "TELEGRAM_BOT_TOKEN": "123456:ABC-DEF",
            "TELEGRAM_CHAT_ID": "99999",
            "TELEGRAM_WEBHOOK_SECRET": "test-webhook-secret-token",
        }

        # Missing secret header
        event_missing = {
            "headers": {"Host": "api.gateway.com"},
            "body": json.dumps({"message": {"text": "list", "chat": {"id": 99999}}}),
        }
        resp = mod.lambda_handler(event_missing, MockContext())
        assert resp["statusCode"] == 403

        # Invalid secret header
        event_invalid = {
            "headers": {"X-Telegram-Bot-Api-Secret-Token": "wrong-token"},
            "body": json.dumps({"message": {"text": "list", "chat": {"id": 99999}}}),
        }
        resp = mod.lambda_handler(event_invalid, MockContext())
        assert resp["statusCode"] == 403

        # Valid secret header
        event_valid = {
            "headers": {"X-Telegram-Bot-Api-Secret-Token": "test-webhook-secret-token"},
            "body": json.dumps({"message": {"text": "help", "chat": {"id": 99999}}}),
        }
        with patch.object(mod, "_send_telegram", return_value=True):
            resp = mod.lambda_handler(event_valid, MockContext())
        assert resp["statusCode"] == 200

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_two_apply_taps_with_different_callback_ids_do_not_move_two_different_jobs(self, mock_tg, mock_ack):
        """Two Apply taps with different callback IDs must NOT move two different jobs."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 2)

        # Set active review job to pending-001 as if user is viewing card 1
        mod._set_active_review_job("99999", "pending-001")

        # First tap with callback ID 1: moves pending-001 to MANUAL_APPLY
        event1 = _make_callback_event("cmd:apply", callback_id="cb-tap-first")
        mod.lambda_handler(event1, MockContext())

        item1 = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        item2 = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item1["status"] == "MANUAL_APPLY"
        assert item2["status"] == "PENDING_APPROVAL"

        # Second tap with completely DIFFERENT callback ID 2
        event2 = _make_callback_event("cmd:apply", callback_id="cb-tap-second")
        mod.lambda_handler(event2, MockContext())

        # Verify pending-002 was NOT moved!
        item2_after = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item2_after["status"] == "PENDING_APPROVAL"

        # Verify user was notified that the active match was re-delivered
        msg = mock_tg.call_args[0][2]
        assert "Manual follow-up already selected" in msg

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_replaying_unbound_callback_after_cache_cleared_does_not_move_another_job(self, mock_tg, mock_ack):
        """Replaying an unbound callback even after clearing the dedup cache must NOT move another job."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 2)

        # Set active review job to pending-001
        mod._set_active_review_job("99999", "pending-001")

        # Apply job 1
        event = _make_callback_event("cmd:apply", callback_id="unbound-apply-cb")
        mod.lambda_handler(event, MockContext())

        assert table.get_item(Key={"job_id": "pending-001"}).get("Item")["status"] == "MANUAL_APPLY"
        assert table.get_item(Key={"job_id": "pending-002"}).get("Item")["status"] == "PENDING_APPROVAL"

        # Simulate cache expiry or different container by clearing the cache
        mod._PROCESSED_CALLBACK_IDS.clear()
        mod._CALLBACK_STATES.clear()

        # Replay the same event
        mod.lambda_handler(event, MockContext())

        # Job 2 MUST NOT be touched!
        item2_after = table.get_item(Key={"job_id": "pending-002"}).get("Item")
        assert item2_after["status"] == "PENDING_APPROVAL"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_replaying_unbound_callback_after_navigation_and_cache_clear_does_not_move_job_2(self, mock_tg, mock_ack):
        """Replaying an unbound callback after navigating to job 2 and clearing memory cache must NOT move job 2."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 3)

        # Step 1: Submit unbound cmd:apply with callback ID old-unbound-event
        event_apply = _make_callback_event("cmd:apply", callback_id="old-unbound-event")
        mod.lambda_handler(event_apply, MockContext())

        assert table.get_item(Key={"job_id": "pending-001"}).get("Item")["status"] == "MANUAL_APPLY"
        assert table.get_item(Key={"job_id": "pending-002"}).get("Item")["status"] == "PENDING_APPROVAL"

        # Step 2: Submit Next - review session advances to pending-002
        event_next = _make_telegram_event("next")
        mod.lambda_handler(event_next, MockContext())
        assert mod._get_active_review_job_id("99999") == "pending-002"

        # Step 3: Clear the in-memory callback cache (simulating cold start / container recycle)
        mod._PROCESSED_CALLBACK_IDS.clear()
        mod._CALLBACK_STATES.clear()

        # Step 4: Replay the original callback old-unbound-event
        resp = mod.lambda_handler(event_apply, MockContext())
        assert resp["statusCode"] == 200

        # Verify final states: pending-001 is MANUAL_APPLY, pending-002 and pending-003 stay PENDING_APPROVAL
        assert table.get_item(Key={"job_id": "pending-001"}).get("Item")["status"] == "MANUAL_APPLY"
        assert table.get_item(Key={"job_id": "pending-002"}).get("Item")["status"] == "PENDING_APPROVAL"
        assert table.get_item(Key={"job_id": "pending-003"}).get("Item")["status"] == "PENDING_APPROVAL"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_unbound_apply_with_failed_delivery_recovers_on_retry(self, mock_tg, mock_ack):
        """An unbound Apply retry after delivery failure must re-deliver follow-up details."""
        mod, table = self._setup_infra()
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 1)

        # First attempt: Telegram delivery fails
        mock_tg.return_value = False
        event = _make_callback_event("cmd:apply", callback_id="unbound-retry-fail")
        mod.lambda_handler(event, MockContext())

        item = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item["status"] == "MANUAL_APPLY"
        assert item["delivery_status"] == "DELIVERY_FAILED"

        # Second attempt: retry the unbound callback now that delivery works
        mock_tg.return_value = True
        mod.lambda_handler(event, MockContext())

        # Verify delivery status recovered to DELIVERED
        item_after = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item_after["status"] == "MANUAL_APPLY"
        assert item_after["delivery_status"] == "DELIVERED"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_non_delivery_processing_exception_does_not_poison_callback_cache(self, mock_tg, mock_ack):
        """A transient exception during handler processing must NOT poison the callback dedup cache."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 1)

        event = _make_callback_event("cmd:apply:pending-001", callback_id="transient-fail-cb")

        # Inject a transient RuntimeError on the first attempt
        with patch.object(mod, "_update_job_status", side_effect=RuntimeError("transient db timeout")):
            with pytest.raises(RuntimeError):
                mod.lambda_handler(event, MockContext())

        # Job is still pending because the update failed
        assert table.get_item(Key={"job_id": "pending-001"}).get("Item")["status"] == "PENDING_APPROVAL"

        # Replaying the same callback must NOT be rejected as a duplicate; it should execute and succeed
        resp = mod.lambda_handler(event, MockContext())
        assert resp["statusCode"] == 200
        assert table.get_item(Key={"job_id": "pending-001"}).get("Item")["status"] == "MANUAL_APPLY"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_action_tokens_enforce_telegram_64_byte_limit_for_long_ats_ids(self, mock_tg, mock_ack):
        """Action tokens must strictly keep callback data <= 64 bytes even for long ATS job IDs."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True

        long_ats_id = "greenhouse_super-long-organization-name-enterprise-edition_a1b2c3d4-e5f6-7890-abcd-ef1234567890-extra-long-suffix"
        assert len(long_ats_id) > 100

        table.put_item(Item={
            "job_id": long_ats_id,
            "title": "Staff AI Engineer",
            "company": "Super Long Org",
            "match_score": Decimal("95"),
            "status": "PENDING_APPROVAL",
            "source": "greenhouse",
            "posted_at": datetime.now(timezone.utc).isoformat(),
            "url": "https://boards.greenhouse.io/superlong/jobs/1",
        })

        keyboard = mod._build_review_card_keyboard(long_ats_id)
        assert "inline_keyboard" in keyboard
        assert "keyboard" not in keyboard

        for row in keyboard["inline_keyboard"]:
            for button in row:
                cb_data = button["callback_data"]
                cb_bytes = cb_data.encode("utf-8")
                # Telegram limit: 1-64 bytes
                assert 1 <= len(cb_bytes) <= 64, f"Payload {cb_data} exceeded 64 bytes: {len(cb_bytes)}"

        # Verify clicking the tokenized button resolves the full job ID and mutates state
        token = mod._get_or_create_action_token(long_ats_id)
        event_apply = _make_callback_event(f"cmd:apply:{token}", callback_id="cb-long-token")
        resp = mod.lambda_handler(event_apply, MockContext())
        assert resp["statusCode"] == 200

        item = table.get_item(Key={"job_id": long_ats_id}).get("Item")
        assert item["status"] == "MANUAL_APPLY"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_excluded_or_low_score_job_cannot_be_acted_on_via_bound_apply(self, mock_tg, mock_ack):
        """A 2020 job with score 1, excluded from review, must be rejected when targeted by bound Apply."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True

        # Insert historical 2020 job with score 1
        table.put_item(Item={
            "job_id": "old-2020-job",
            "title": "Ancient AI Engineer",
            "company": "Past Corp",
            "match_score": Decimal("1"),
            "status": "PENDING_APPROVAL",
            "source": "hackernews",
            "posted_at": "2020-01-01T00:00:00Z",
            "url": "https://pastcorp.com/jobs/1",
        })

        # Attempt to apply via bound callback
        event = _make_callback_event("cmd:apply:old-2020-job", callback_id="old-apply-tap")
        mod.lambda_handler(event, MockContext())

        # Verify job was NOT updated to MANUAL_APPLY
        item = table.get_item(Key={"job_id": "old-2020-job"}).get("Item")
        assert item["status"] == "PENDING_APPROVAL"

        # Verify blocked message explaining why
        msg = mock_tg.call_args[0][2]
        assert "Blocked: this role is not reviewable" in msg

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_competing_apply_and_skip_sequence_does_not_overwrite_manual_apply(self, mock_tg, mock_ack):
        """A simulated competing Apply/Skip sequence must NOT overwrite MANUAL_APPLY with NOT_INTERESTED."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 1)

        # 1. Apply succeeds and transitions job to MANUAL_APPLY
        event_apply = _make_callback_event("cmd:apply:pending-001", callback_id="cb-compete-apply")
        mod.lambda_handler(event_apply, MockContext())

        item = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item["status"] == "MANUAL_APPLY"

        # 2. Competing Skip arrives for the same job
        event_skip = _make_callback_event("cmd:skip:pending-001", callback_id="cb-compete-skip")
        mod.lambda_handler(event_skip, MockContext())

        # Verify status was NOT overwritten!
        item_after = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item_after["status"] == "MANUAL_APPLY"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_replaying_same_callback_id_after_delivery_failure_retries_delivery(self, mock_tg, mock_ack):
        """Replaying the exact same callback ID after delivery failure must make a delivery retry attempt."""
        mod, table = self._setup_infra()
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 1)

        # First attempt: Telegram delivery fails
        mock_tg.return_value = False
        event = _make_callback_event("cmd:apply:pending-001", callback_id="retry-same-id-999")
        mod.lambda_handler(event, MockContext())

        assert mock_tg.call_count == 1
        item = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item["status"] == "MANUAL_APPLY"
        assert item["delivery_status"] == "DELIVERY_FAILED"

        # Replay the EXACT SAME callback event now that network is back
        mock_tg.return_value = True
        mod.lambda_handler(event, MockContext())

        # Delivery attempt MUST have been made again (call_count incremented to 2)
        assert mock_tg.call_count == 2
        item_after = table.get_item(Key={"job_id": "pending-001"}).get("Item")
        assert item_after["status"] == "MANUAL_APPLY"
        assert item_after["delivery_status"] == "DELIVERED"

    @patch("job_router.lambda_function._answer_callback_query")
    @patch("job_router.lambda_function._send_telegram")
    def test_displayed_review_cards_attach_bound_review_keyboard(self, mock_tg, mock_ack):
        """Displayed review cards in Next, Why, and Draft must attach bound inline keyboards with NO reply keyboard."""
        mod, table = self._setup_infra()
        mock_tg.return_value = True
        mock_ack.return_value = True
        self._insert_pending_jobs(table, 1)

        token = mod._get_or_create_action_token("pending-001")

        # 1. Test Next Match card has bound inline keyboard and NO reply keyboard
        event_next = _make_telegram_event("next")
        mod.lambda_handler(event_next, MockContext())

        last_call_kwargs = mock_tg.call_args[1]
        assert "reply_markup" in last_call_kwargs
        assert "keyboard" not in last_call_kwargs["reply_markup"]
        assert "resize_keyboard" not in last_call_kwargs["reply_markup"]
        inline_kb = last_call_kwargs["reply_markup"].get("inline_keyboard", [])
        assert len(inline_kb) > 0
        assert inline_kb[0][0]["callback_data"] == f"cmd:why:{token}"
        assert inline_kb[0][1]["callback_data"] == f"cmd:draft:{token}"
        assert inline_kb[0][2]["callback_data"] == f"cmd:apply:{token}"
        assert inline_kb[0][3]["callback_data"] == f"cmd:skip:{token}"

        # 2. Test Why (explain) card
        event_why = _make_telegram_event("why")
        mod.lambda_handler(event_why, MockContext())
        last_call_kwargs = mock_tg.call_args[1]
        assert "reply_markup" in last_call_kwargs
        assert "keyboard" not in last_call_kwargs["reply_markup"]
        inline_kb_why = last_call_kwargs["reply_markup"].get("inline_keyboard", [])
        assert inline_kb_why[0][0]["callback_data"] == f"cmd:why:{token}"

        # 3. Test Draft card
        with patch.object(mod.lambda_client, "invoke") as mock_invoke:
            mock_invoke.return_value = {
                "Payload": io.BytesIO(json.dumps({
                    "status": "preview",
                    "email": {"subject": "Test Subj", "body": "Test Body"}
                }).encode("utf-8"))
            }
            event_draft = _make_telegram_event("draft")
            mod.lambda_handler(event_draft, MockContext())
            last_call_kwargs = mock_tg.call_args[1]
            assert "reply_markup" in last_call_kwargs
            assert "keyboard" not in last_call_kwargs["reply_markup"]
            inline_kb_draft = last_call_kwargs["reply_markup"].get("inline_keyboard", [])
            assert inline_kb_draft[0][0]["callback_data"] == f"cmd:why:{token}"


@pytest.fixture
def durable_callback_runtime():
    """Real router handlers and Moto storage; only external delivery is mocked."""
    with mock_aws():
        helper = TestRouterRegressionEdgeCases()
        mod, table = helper._setup_infra()
        for cache in (mod._PROCESSED_CALLBACK_IDS, mod._CALLBACK_STATES,
                      mod._ACTION_TOKEN_CACHE, mod._JOB_ID_TO_TOKEN_CACHE):
            cache.clear()
        helper._insert_pending_jobs(table, 3)
        with patch.object(mod, "_send_telegram", return_value=True) as send, \
             patch.object(mod, "_answer_callback_query", return_value=True):
            yield mod, table, send


def _cold_callback_worker(mod):
    for cache in (mod._PROCESSED_CALLBACK_IDS, mod._CALLBACK_STATES,
                  mod._ACTION_TOKEN_CACHE, mod._JOB_ID_TO_TOKEN_CACHE):
        cache.clear()


def test_durable_failed_apply_next_cold_retry_keeps_original_target(durable_callback_runtime):
    mod, table, send = durable_callback_runtime
    event = _make_callback_event("cmd:apply", callback_id="durable-failed-next")
    send.return_value = False
    mod.lambda_handler(event, MockContext())
    send.return_value = True
    mod.lambda_handler(_make_telegram_event("next"), MockContext())
    _cold_callback_worker(mod)
    mod.lambda_handler(event, MockContext())
    first = table.get_item(Key={"job_id": "pending-001"})["Item"]
    second = table.get_item(Key={"job_id": "pending-002"})["Item"]
    state = table.get_item(Key={"job_id": "cb:durable-failed-next"})["Item"]
    assert first["delivery_status"] == "DELIVERED"
    assert second["status"] == "PENDING_APPROVAL"
    assert state["target_job_id"] == "pending-001"
    assert state["callback_status"] == "COMPLETED"


@pytest.mark.parametrize("operation", ["get_item", "put_item"])
def test_durable_storage_failure_stops_before_job_mutation(durable_callback_runtime, operation):
    mod, table, send = durable_callback_runtime
    original = getattr(mod.table, operation)

    def fail_callback_storage(**kwargs):
        key = kwargs.get("Item", kwargs.get("Key", {})).get("job_id", "")
        if key.startswith("cb:"):
            raise RuntimeError("injected callback database outage")
        return original(**kwargs)

    with patch.object(mod.table, operation, side_effect=fail_callback_storage):
        response = mod.lambda_handler(_make_callback_event("cmd:apply", callback_id="storage-outage"), MockContext())
    assert response["statusCode"] == 503
    assert table.get_item(Key={"job_id": "pending-001"})["Item"]["status"] == "PENDING_APPROVAL"
    send.assert_not_called()


def test_durable_overlapping_workers_claim_only_once(durable_callback_runtime):
    mod, _, _ = durable_callback_runtime
    event = _make_callback_event("cmd:next", callback_id="overlapping-workers")
    original_put = mod.table.put_item
    dispatched = False

    def second_worker_before_first_claim(**kwargs):
        nonlocal dispatched
        if kwargs.get("Item", {}).get("job_id") == "cb:overlapping-workers" and not dispatched:
            dispatched = True
            mod.lambda_handler(event, MockContext())
        return original_put(**kwargs)

    with patch.object(mod.table, "put_item", side_effect=second_worker_before_first_claim), \
         patch.object(mod, "_handle_callback_command") as handle:
        mod.lambda_handler(event, MockContext())
    assert handle.call_count == 1


@pytest.mark.parametrize("action", ["apply", "skip"])
def test_durable_actual_job_update_error_is_retryable(durable_callback_runtime, action):
    mod, table, _ = durable_callback_runtime
    event = _make_callback_event(f"cmd:{action}:pending-001", callback_id="job-write-error")
    original_update = mod.table.update_item

    def fail_job_write(**kwargs):
        if kwargs.get("Key", {}).get("job_id") == "pending-001":
            raise RuntimeError("injected actual job update timeout")
        return original_update(**kwargs)

    with patch.object(mod.table, "update_item", side_effect=fail_job_write):
        with pytest.raises(RuntimeError):
            mod.lambda_handler(event, MockContext())
    assert table.get_item(Key={"job_id": "cb:job-write-error"})["Item"]["callback_status"] == "FAILED"
    _cold_callback_worker(mod)
    mod.lambda_handler(event, MockContext())
    expected = "MANUAL_APPLY" if action == "apply" else "NOT_INTERESTED"
    assert table.get_item(Key={"job_id": "pending-001"})["Item"]["status"] == expected


def test_durable_failed_token_write_is_not_cached_or_advertised(durable_callback_runtime):
    mod, table, _ = durable_callback_runtime
    with patch.object(mod.table, "put_item", side_effect=RuntimeError("token write outage")):
        with pytest.raises(RuntimeError):
            mod._build_review_card_keyboard("pending-001")
    assert "pending-001" not in mod._JOB_ID_TO_TOKEN_CACHE
    token = mod._get_or_create_action_token("pending-001")
    assert table.get_item(Key={"job_id": f"tok:{token}"})["Item"]["target_job_id"] == "pending-001"
    _cold_callback_worker(mod)
    mod.lambda_handler(_make_callback_event(f"cmd:apply:{token}", callback_id="cold-token"), MockContext())
    assert table.get_item(Key={"job_id": "pending-001"})["Item"]["status"] == "MANUAL_APPLY"


def test_durable_token_read_outage_does_not_fall_back_to_job_id(durable_callback_runtime):
    mod, _, _ = durable_callback_runtime
    token = mod._get_or_create_action_token("pending-001")
    _cold_callback_worker(mod)
    with patch.object(mod.table, "get_item", side_effect=RuntimeError("token read outage")):
        with pytest.raises(RuntimeError):
            mod._resolve_action_token(token)


def test_durable_in_progress_retry_is_not_acknowledged_as_complete(durable_callback_runtime):
    mod, table, _ = durable_callback_runtime
    import time
    table.put_item(Item={"job_id": "cb:busy", "callback_status": "IN_PROGRESS",
                         "updated_at": int(time.time()), "lease_until": int(time.time()) + 900,
                         "claim_owner": "another-worker", "action_data": "cmd:next"})
    response = mod.lambda_handler(_make_callback_event("cmd:next", callback_id="busy"), MockContext())
    assert response["statusCode"] == 503


def test_durable_first_next_retry_does_not_skip_first_card(durable_callback_runtime):
    mod, table, _ = durable_callback_runtime
    event = _make_callback_event("cmd:next", callback_id="first-next-crash")
    original_record = mod._record_callback_state

    def fail_completion_once(callback_id, status, *args, **kwargs):
        if status == "COMPLETED":
            raise RuntimeError("injected completion failure after card delivery")
        return original_record(callback_id, status, *args, **kwargs)

    with patch.object(mod, "_record_callback_state", side_effect=fail_completion_once):
        with pytest.raises(RuntimeError):
            mod.lambda_handler(event, MockContext())
    assert mod._get_active_review_job_id("99999") == "pending-001"
    assert table.get_item(Key={"job_id": "cb:first-next-crash"})["Item"]["callback_status"] == "FAILED"
    _cold_callback_worker(mod)
    mod.lambda_handler(event, MockContext())
    assert mod._get_active_review_job_id("99999") == "pending-001"
    mod.lambda_handler(_make_callback_event("cmd:next", callback_id="second-next"), MockContext())
    assert mod._get_active_review_job_id("99999") == "pending-002"


def test_durable_legacy_failed_unbound_action_without_target_is_refused(durable_callback_runtime):
    mod, table, _ = durable_callback_runtime
    table.put_item(Item={"job_id": "cb:legacy-unsafe", "callback_status": "FAILED", "updated_at": 1})
    response = mod.lambda_handler(_make_callback_event("cmd:apply", callback_id="legacy-unsafe"), MockContext())
    assert response["statusCode"] == 400
    assert table.get_item(Key={"job_id": "pending-001"})["Item"]["status"] == "PENDING_APPROVAL"


def test_durable_review_journey_no_outreach_and_retry_after_navigation(durable_callback_runtime):
    mod, table, send = durable_callback_runtime
    mod.lambda_handler(_make_telegram_event("next"), MockContext())
    token = mod._get_or_create_action_token("pending-001")
    mod.lambda_handler(_make_callback_event(f"cmd:why:{token}", callback_id="journey-why"), MockContext())
    preview = {"status": "preview", "email": {"subject": "Preview", "body": "Grounded draft"}}
    with patch.object(mod.lambda_client, "invoke", return_value={"Payload": io.BytesIO(json.dumps(preview).encode())}) as invoke:
        mod.lambda_handler(_make_callback_event(f"cmd:draft:{token}", callback_id="journey-draft"), MockContext())
        assert json.loads(invoke.call_args.kwargs["Payload"])["preview_only"] is True
        assert invoke.call_args.kwargs["FunctionName"] == mod.SENDER_LAMBDA
    send.return_value = False
    apply = _make_callback_event(f"cmd:apply:{token}", callback_id="journey-apply")
    mod.lambda_handler(apply, MockContext())
    send.return_value = True
    mod.lambda_handler(_make_callback_event("cmd:next", callback_id="journey-next"), MockContext())
    _cold_callback_worker(mod)
    mod.lambda_handler(apply, MockContext())
    assert table.get_item(Key={"job_id": "pending-001"})["Item"]["delivery_status"] == "DELIVERED"
    assert table.get_item(Key={"job_id": "pending-002"})["Item"]["status"] == "PENDING_APPROVAL"


def test_monitor_detects_missing_three_hour_windows_and_empty_telemetry():
    source = (Path(__file__).resolve().parents[1] / ".github/workflows/health-monitor.yml").read_text(encoding="utf-8")
    script = textwrap.dedent(source.split("python - <<'HEALTH_SCRIPT'\n", 1)[1].split("HEALTH_SCRIPT", 1)[0])
    tree = ast.parse(script)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "missed_ingestor_windows")
    scope = {"datetime": datetime}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "health-monitor", "exec"), scope)
    check = scope["missed_ingestor_windows"]
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(hours=24)
    one_run = [{"Timestamp": end - timedelta(hours=1), "Sum": 1}]
    regular_runs = [{"Timestamp": start + timedelta(hours=2 + 3 * i), "Sum": 1} for i in range(8)]
    assert check(one_run, start, end) >= 6
    assert check(regular_runs, start, end) == 0
    assert check([], start, end) is None


def test_telegram_long_messages_are_chunked_and_keyboard_stays_on_last(monkeypatch):
    import job_router.lambda_function as mod
    sent = []
    monkeypatch.setattr(mod, "_telegram_request", lambda token, method, payload: sent.append(payload) or True)
    text = "\n".join(f"{i}. {'x' * 320}" for i in range(20))

    assert mod._send_telegram("token", "chat", text, reply_markup={"keyboard": [["Home"]]}) is True
    assert len(sent) > 1
    assert all(len(item["text"]) <= mod.TELEGRAM_SAFE_MESSAGE_CHARS for item in sent)
    assert "reply_markup" not in sent[0]
    assert sent[-1]["reply_markup"] == {"keyboard": [["Home"]]}
    assert "".join(item["text"] for item in sent).replace("\n", "")


def test_telegram_http_error_logs_reason_without_message_body(monkeypatch, caplog):
    import logging
    import urllib.error
    import job_router.lambda_function as mod

    def fail(*_args, **_kwargs):
        raise urllib.error.HTTPError(
            "https://api.telegram.org/botsecret/sendMessage", 400, "Bad Request",
            {}, io.BytesIO(b'{"ok":false,"description":"Bad Request: message is too long"}'),
        )

    monkeypatch.setattr(mod.urllib.request, "urlopen", fail)
    with caplog.at_level(logging.ERROR):
        assert mod._telegram_request("secret-token", "sendMessage", {"chat_id": "1", "text": "private"}) is False
    assert "message is too long" in caplog.text
    assert "secret-token" not in caplog.text
    assert "private" not in caplog.text
