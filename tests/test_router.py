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
from datetime import datetime, timezone, timedelta
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


def _make_callback_event(data, chat_id="99999", message_id=321):
    """Create an API Gateway event mimicking a Telegram callback query."""
    return {
        "body": json.dumps({
            "callback_query": {
                "id": "callback-001",
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
        assert "AI Engineer Role 3" in mock_tg.call_args[0][2]
        keyboard = mock_tg.call_args.kwargs["reply_markup"]["keyboard"]
        assert keyboard[0][0]["text"] == "Why"
        assert keyboard[0][2]["text"] == "Apply"
        assert keyboard[1][0]["text"] == "Next Match"

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
