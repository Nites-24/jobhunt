"""
Job Router Lambda - Telegram command center.

Handles:
  1. SNS notifications from job-processor and forwards them to Telegram.
  2. Telegram webhook commands for search, list, apply, reject, status, and help.

The review queue intentionally hides stale rows and old source experiments. This keeps
the demo aligned with the product promise: fresh, high-confidence AI roles only.
"""

import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key


logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

SECRET_NAME = os.environ.get("TELEGRAM_SECRET_NAME", "telegram")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "Jobsearch")
SENDER_LAMBDA = os.environ.get("SENDER_LAMBDA_NAME", "job-sender")
INGESTOR_LAMBDA = os.environ.get("INGESTOR_LAMBDA_NAME", "job-ingestor")
SCORE_THRESHOLD = int(os.environ.get("SCORE_THRESHOLD", "80"))
MAX_JOB_AGE_DAYS = int(os.environ.get("MAX_JOB_AGE_DAYS", "14"))
HN_MAX_JOB_AGE_DAYS = int(os.environ.get("HN_MAX_JOB_AGE_DAYS", "3"))
LIST_FETCH_LIMIT = int(os.environ.get("LIST_FETCH_LIMIT", "100"))
ENABLE_PRACTICE_MODE = os.environ.get("ENABLE_PRACTICE_MODE", "false").strip().lower() == "true"
ALLOW_TELEGRAM_EMAIL_SEND = os.environ.get("ALLOW_TELEGRAM_EMAIL_SEND", "false").strip().lower() == "true"
LIST_ALLOWED_SOURCES = {
    source.strip().lower()
    for source in os.environ.get("LIST_ALLOWED_SOURCES", "hackernews,hn,jsearch,google_cse,cse,adzuna,tejajob,signal_test").split(",")
    if source.strip()
}

table = dynamodb.Table(TABLE_NAME)
_cached_secrets = None


class DecimalEncoder(json.JSONEncoder):
    """Serialize DynamoDB Decimal values without crashing json.dumps."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


def _safe_json_dumps(obj) -> str:
    return json.dumps(obj, cls=DecimalEncoder)


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_posted_at(value) -> datetime | None:
    """Parse Unix timestamps and ISO timestamps into UTC datetimes."""
    if value in (None, ""):
        return None

    raw = str(value).strip()
    try:
        if raw.isdigit():
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)

        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _job_age_days(job: dict) -> int | None:
    posted = _parse_posted_at(job.get("posted_at"))
    if not posted:
        return None
    age_seconds = max(0, (datetime.now(timezone.utc) - posted).total_seconds())
    return int(age_seconds // 86400)


def _source_max_age_days(job: dict) -> int:
    source = str(job.get("source", "")).strip().lower()
    if source in {"hackernews", "hn"}:
        return HN_MAX_JOB_AGE_DAYS
    return MAX_JOB_AGE_DAYS


def _source_allowed(job: dict) -> bool:
    source = str(job.get("source", "")).strip().lower()
    if source in {"showcase", "tejajob"} and not ENABLE_PRACTICE_MODE:
        return False
    return bool(source and source in LIST_ALLOWED_SOURCES)


def _source_label(job: dict) -> str:
    source = str(job.get("source", "")).strip().lower()
    if source in {"hackernews", "hn"}:
        return "Hacker News Who is Hiring"
    if source in {"showcase", "tejajob"}:
        return "Practice item"
    if source == "signal_test":
        return "Notification test seed"
    if source in {"google_cse", "cse"}:
        return "Google Custom Search"
    if source == "jsearch":
        return "JSearch RapidAPI"
    if source == "adzuna":
        return "Adzuna Jobs API"
    return source or "unknown"


def _source_url(job: dict) -> str:
    return str(job.get("url") or job.get("apply_url") or "").strip()


def _work_mode_label(job: dict) -> str:
    label = str(job.get("work_mode") or "").strip().lower()
    if label in {"remote", "hybrid", "onsite"}:
        return label
    return "unknown"


def _work_mode_evidence(job: dict) -> str:
    source = str(job.get("work_mode_source") or "").strip().lower()
    if source == "job_is_remote":
        return "source API remote flag"
    if source == "location":
        return "source location text"
    if source == "description":
        return "source job description"
    if source == "title":
        return "source title text"
    if source == "snippet":
        return "source snippet text"
    if source == "employment_type":
        return "source employment text"
    return "not found in source"


def _short_text(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _is_reviewable_job(job: dict) -> bool:
    """Apply hard UX filters before a job can appear in the command queue."""
    if job.get("status") != "PENDING_APPROVAL":
        return False
    if _as_int(job.get("match_score")) < SCORE_THRESHOLD:
        return False
    if not _source_allowed(job):
        return False

    age = _job_age_days(job)
    if age is None or age > _source_max_age_days(job):
        return False
    return True


def _is_fresh_allowed_pending_job(job: dict) -> bool:
    if job.get("status") != "PENDING_APPROVAL":
        return False
    if not _source_allowed(job):
        return False
    age = _job_age_days(job)
    return age is not None and age <= _source_max_age_days(job)


def _sort_reviewable_jobs(jobs: list[dict]) -> list[dict]:
    return sorted(
        jobs,
        key=lambda job: (
            _as_int(job.get("match_score")),
            _as_int(job.get("evaluated_at")),
            _as_int(job.get("ingested_at")),
        ),
        reverse=True,
    )


def _get_secrets() -> dict:
    global _cached_secrets
    if not _cached_secrets:
        try:
            resp = secrets_client.get_secret_value(SecretId=SECRET_NAME)
            _cached_secrets = json.loads(resp["SecretString"])
        except Exception as e:
            logger.error("SECRET_ERROR: %s", str(e))
            return {}
    return _cached_secrets


def _telegram_request(bot_token: str, method: str, payload: dict) -> bool:
    """Send a Telegram Bot API request and return whether it succeeded."""
    url = f"https://api.telegram.org/bot{bot_token}/{method}"
    encoded_payload = {
        key: json.dumps(value) if isinstance(value, (dict, list)) else value
        for key, value in payload.items()
        if value is not None
    }
    data = urllib.parse.urlencode(encoded_payload).encode("utf-8")
    req = urllib.request.Request(url, data=data)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            return result.get("ok", False)
    except Exception as e:
        logger.error("TELEGRAM_%s_ERROR: %s", method.upper(), str(e))
        return False


def _send_telegram(
    bot_token: str,
    chat_id: str,
    text: str,
    parse_mode: str = None,
    reply_markup: dict | None = None,
) -> bool:
    """Send a Telegram message. Returns True on success."""
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return _telegram_request(bot_token, "sendMessage", payload)


def _edit_telegram_message(
    bot_token: str,
    chat_id: str,
    message_id: int,
    text: str,
    parse_mode: str = None,
    reply_markup: dict | None = None,
) -> bool:
    """Edit an existing Telegram message."""
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return _telegram_request(bot_token, "editMessageText", payload)


def _answer_callback_query(bot_token: str, callback_query_id: str, text: str | None = None) -> bool:
    """Acknowledge a Telegram callback query to clear button loading state."""
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    return _telegram_request(bot_token, "answerCallbackQuery", payload)


def _build_help_keyboard() -> dict:
    """Small reply keyboard for the most common navigation actions."""
    return {
        "keyboard": [
            [{"text": "search jobs"}, {"text": "list"}, {"text": "status"}],
            [{"text": "draft 1"}, {"text": "apply 1"}, {"text": "followups"}],
            [{"text": "signal test"}, {"text": "help"}],
        ],
        "resize_keyboard": True,
        "input_field_placeholder": "Type a command or tap an action",
    }


def _help_page(page: str) -> tuple[str, dict]:
    """Build a breadcrumb-style help page with inline navigation."""
    page_key = page if page in {"home", "start", "review", "followup", "send", "tools"} else "home"

    pages = {
        "home": [
            "⌂ JobAlert",
            "Home",
            "",
            "The quickest path:",
            "search jobs -> list -> explain 1 -> draft 1 -> interested 1 -> apply 1 -> applied",
            "",
            "Tap a section below or run a command directly.",
        ],
        "start": [
            "⌂ JobAlert",
            "Home › Start",
            "",
            "Core commands",
            "• search jobs  Scan live sources",
            f"• list  Review fresh {SCORE_THRESHOLD}%+ matches",
            "• status  Check queue counts and source evidence",
        ],
        "review": [
            "⌂ JobAlert",
            "Home › Review",
            "",
            "Review commands",
            "• explain 1  Why match #1 scored well",
            "• draft 1  Preview outreach",
            "• interested 1  Save positive feedback",
            "• meh 1 <reason>  Save neutral feedback",
            "• not interested 1 <reason>  Remove and teach why",
            "• reject 1  Remove match #1 from the queue",
        ],
        "followup": [
            "⌂ JobAlert",
            "Home › Follow-up",
            "",
            "Follow-up commands",
            "• apply 1  Show source and contact for manual follow-up",
            "• followups  Roles waiting for your real outcome",
            "• applied  Mark the latest manual follow-up as applied",
            "• not applied <reason>  Mark the latest manual follow-up as skipped",
        ],
        "send": [
            "⌂ JobAlert",
            "Home › Send",
            "",
            "Send commands",
            "• send 1  Prepare email send confirmation",
            "• confirm send 1  Send after review",
            "",
            "Real sending is shown only when Telegram send is enabled.",
        ],
        "tools": [
            "⌂ JobAlert",
            "Home › Tools",
            "",
            "Tool commands",
            "• signal test  Run one synthetic live alert",
            "• help  Return to this command center",
        ],
    }

    action_rows = {
        "home": [
            [{"text": "Search", "callback_data": "cmd:search jobs"}, {"text": "List", "callback_data": "cmd:list"}],
            [{"text": "Status", "callback_data": "cmd:status"}, {"text": "Signal Test", "callback_data": "cmd:signal test"}],
        ],
        "start": [
            [{"text": "Search Jobs", "callback_data": "cmd:search jobs"}],
            [{"text": "List", "callback_data": "cmd:list"}, {"text": "Status", "callback_data": "cmd:status"}],
        ],
        "review": [
            [{"text": "Explain 1", "callback_data": "cmd:explain 1"}, {"text": "Draft 1", "callback_data": "cmd:draft 1"}],
            [{"text": "Interested 1", "callback_data": "cmd:interested 1"}],
            [{"text": "Meh 1", "callback_data": "cmd:meh 1 no reason provided"}],
        ],
        "followup": [
            [{"text": "Apply 1", "callback_data": "cmd:apply 1"}, {"text": "Followups", "callback_data": "cmd:followups"}],
            [{"text": "Applied", "callback_data": "cmd:applied"}],
        ],
        "send": [
            [{"text": "Send 1", "callback_data": "cmd:send 1"}, {"text": "Confirm Send 1", "callback_data": "cmd:confirm send 1"}],
        ],
        "tools": [
            [{"text": "Signal Test", "callback_data": "cmd:signal test"}],
            [{"text": "Help Home", "callback_data": "help:home"}],
        ],
    }

    nav_rows = [
        [
            {"text": "Home", "callback_data": "help:home"},
            {"text": "Start", "callback_data": "help:start"},
            {"text": "Review", "callback_data": "help:review"},
        ],
        [
            {"text": "Follow-up", "callback_data": "help:followup"},
            {"text": "Send", "callback_data": "help:send"},
            {"text": "Tools", "callback_data": "help:tools"},
        ],
    ]

    if not ALLOW_TELEGRAM_EMAIL_SEND:
        nav_rows[1] = [button for button in nav_rows[1] if button["callback_data"] != "help:send"]
        if page_key == "send":
            page_key = "home"

    text = "\n".join(pages[page_key])
    keyboard = {"inline_keyboard": nav_rows + action_rows[page_key]}
    return text, keyboard


def _get_pending_jobs(limit: int = 10) -> list:
    """Fetch reviewable pending jobs, ranked for list/apply/reject commands."""
    try:
        items = []
        query_args = {
            "IndexName": "status-index",
            "KeyConditionExpression": Key("status").eq("PENDING_APPROVAL"),
            "Limit": LIST_FETCH_LIMIT,
        }

        while True:
            response = table.query(**query_args)
            items.extend(response.get("Items", []))
            if len(items) >= LIST_FETCH_LIMIT or "LastEvaluatedKey" not in response:
                break
            query_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]

        reviewable = [job for job in items if _is_reviewable_job(job)]
        return _sort_reviewable_jobs(reviewable)[:limit]
    except Exception as e:
        logger.error("QUERY_PENDING_ERROR: %s", str(e))
        return []


def _get_scored_pending_jobs(limit: int = 10) -> list:
    """Fetch fresh sourced pending jobs, including below-review-threshold watchlist rows."""
    try:
        items = []
        query_args = {
            "IndexName": "status-index",
            "KeyConditionExpression": Key("status").eq("PENDING_APPROVAL"),
            "Limit": LIST_FETCH_LIMIT,
        }

        while True:
            response = table.query(**query_args)
            items.extend(response.get("Items", []))
            if len(items) >= LIST_FETCH_LIMIT or "LastEvaluatedKey" not in response:
                break
            query_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]

        pending = [
            job for job in items
            if _is_fresh_allowed_pending_job(job) and _as_int(job.get("match_score")) > 0
        ]
        return _sort_reviewable_jobs(pending)[:limit]
    except Exception as e:
        logger.error("QUERY_SCORED_PENDING_ERROR: %s", str(e))
        return []


def _get_manual_followup_jobs(limit: int = 10) -> list:
    """Fetch jobs the user selected for manual follow-up but has not resolved."""
    try:
        items = []
        query_args = {
            "IndexName": "status-index",
            "KeyConditionExpression": Key("status").eq("MANUAL_APPLY"),
            "Limit": LIST_FETCH_LIMIT,
        }

        while True:
            response = table.query(**query_args)
            items.extend(response.get("Items", []))
            if len(items) >= LIST_FETCH_LIMIT or "LastEvaluatedKey" not in response:
                break
            query_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]

        return sorted(
            items,
            key=lambda job: (
                _as_int(job.get("status_updated_at")),
                _as_int(job.get("evaluated_at")),
                _as_int(job.get("ingested_at")),
            ),
            reverse=True,
        )[:limit]
    except Exception as e:
        logger.error("QUERY_MANUAL_FOLLOWUP_ERROR: %s", str(e))
        return []


def _get_pipeline_stats() -> dict:
    """Get counts of jobs by status using paginated scans."""
    stats = {
        "NEW": 0,
        "PENDING_APPROVAL": 0,
        "APPROVED": 0,
        "MANUAL_APPLY": 0,
        "APPLIED": 0,
        "NOT_APPLIED": 0,
        "NOT_INTERESTED": 0,
        "SHOWCASE_COMPLETE": 0,
        "SENT": 0,
        "REJECTED": 0,
        "FAILED": 0,
    }
    try:
        scan_args = {
            "ProjectionExpression": "#s",
            "ExpressionAttributeNames": {"#s": "status"},
        }
        while True:
            response = table.scan(**scan_args)
            for item in response.get("Items", []):
                status = item.get("status", "UNKNOWN")
                stats[status] = stats.get(status, 0) + 1
            if "LastEvaluatedKey" not in response:
                break
            scan_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        return stats
    except Exception as e:
        logger.error("SCAN_STATS_ERROR: %s", str(e))
        return stats


def _update_job_status(job_id: str, new_status: str) -> None:
    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET #s = :s, status_updated_at = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": new_status, ":t": int(time.time())},
    )


def _record_application_outcome(job_id: str, outcome: str, reason: str = "") -> str:
    """Persist the user's real-world application decision for future evals."""
    now = int(time.time())
    status = "APPLIED" if outcome == "applied" else "NOT_APPLIED"
    timestamp_field = "applied_at" if outcome == "applied" else "not_applied_at"
    update_expr = (
        "SET #s = :s, status_updated_at = :t, application_outcome = :outcome, "
        "user_feedback = :outcome, user_feedback_at = :t, feedback_source = :source, "
        f"{timestamp_field} = :t"
    )
    expr_vals = {
        ":s": status,
        ":t": now,
        ":outcome": outcome,
        ":source": "telegram",
    }
    if reason:
        update_expr += ", user_feedback_reason = :reason"
        expr_vals[":reason"] = reason[:500]

    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues=expr_vals,
    )
    return status


def _record_review_feedback(job_id: str, sentiment: str, reason: str = "") -> str:
    """Persist the user's review preference while preserving apply outcome semantics."""
    now = int(time.time())
    new_status = "NOT_INTERESTED" if sentiment == "not_interested" else "PENDING_APPROVAL"
    update_expr = (
        "SET #s = :s, status_updated_at = :t, user_interest = :sentiment, "
        "user_feedback = :sentiment, user_feedback_at = :t, feedback_source = :source"
    )
    expr_vals = {
        ":s": new_status,
        ":t": now,
        ":sentiment": sentiment,
        ":source": "telegram",
    }
    if reason:
        update_expr += ", user_feedback_reason = :reason"
        expr_vals[":reason"] = reason[:500]

    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues=expr_vals,
    )
    return new_status


def _format_posted(posted_at: str) -> str:
    dt = _parse_posted_at(posted_at)
    if not dt:
        return ""

    diff = datetime.now(timezone.utc) - dt
    if diff < timedelta(hours=1):
        return f"Posted: {max(0, int(diff.total_seconds() / 60))}m ago"
    if diff < timedelta(days=1):
        return f"Posted: {max(0, int(diff.total_seconds() / 3600))}h ago"
    return f"Posted: {max(0, diff.days)}d ago"


def _read_lambda_payload(response: dict[str, Any]) -> dict:
    payload = response.get("Payload")
    if hasattr(payload, "read"):
        raw = payload.read()
    else:
        raw = payload or b"{}"
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if not raw:
        return {}
    return json.loads(raw)


def _make_showcase_job() -> dict:
    """Create a neutral, interview-safe showcase opportunity."""
    now = datetime.now(timezone.utc)
    return {
        "job_id": f"tejajob_agent_{now.strftime('%Y%m%d')}",
        "title": "AI Outbound Agent Engineer",
        "company": "Northstar Revenue AI",
        "location": "San Francisco Bay Area",
        "description": (
            "Build production AI agents that automate outbound research, lead enrichment, "
            "personalized email drafting, LinkedIn follow-up sequencing, CRM hygiene, and "
            "human-in-the-loop approval workflows. Requires LLM orchestration, agentic RAG, "
            "Python, AWS, API integrations, evaluation, monitoring, and reliable execution "
            "over queue-based backend systems."
        ),
        "url": "",
        "apply_url": "",
        "source": "tejajob",
        "salary": "$130,000 - $180,000",
        "is_remote": False,
        "employment_type": "FULL_TIME",
        "posted_at": now.isoformat(),
        "recruiter_email": "",
        "recruiter_name": "",
        "match_score": 94,
        "match_reason": (
            "Strong fit for agentic outbound automation: LLM orchestration, RAG, Python, AWS, "
            "human approval loops, and production queue-based execution all match the candidate profile."
        ),
        "status": "PENDING_APPROVAL",
        "evaluated_at": int(time.time()),
        "ingested_at": now.isoformat(),
        "model_id": "showcase-seed",
    }


def _retire_old_showcase_rows(active_job_id: str) -> None:
    """Hide prior showcase/tejajob rows so the review queue stays clean."""
    try:
        scan_args = {
            "ProjectionExpression": "job_id, #s, #src",
            "ExpressionAttributeNames": {"#s": "status", "#src": "source"},
        }
        while True:
            response = table.scan(**scan_args)
            for item in response.get("Items", []):
                source = str(item.get("source", "")).lower()
                if (
                    item.get("job_id") != active_job_id
                    and item.get("status") == "PENDING_APPROVAL"
                    and source in {"showcase", "tejajob"}
                ):
                    _update_job_status(item["job_id"], "REJECTED")
            if "LastEvaluatedKey" not in response:
                break
            scan_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except Exception as e:
        logger.warning("SHOWCASE_CLEANUP_ERROR: %s", str(e))


def _format_job_explanation(job: dict, index: int) -> str:
    recruiter = (job.get("recruiter_email") or "").strip()
    if job.get("source") == "tejajob":
        action = "practice workflow: preview a draft or mark complete"
    elif recruiter:
        action = "email outreach available"
    else:
        action = "manual apply or draft preview available"
    posted = _format_posted(job.get("posted_at", "")) or "Posted date unavailable"
    reason = job.get("match_reason") or "No model reason stored."
    age = _job_age_days(job)
    freshness = "fresh" if age is not None and age <= _source_max_age_days(job) else "not fresh"

    return (
        f"Match explanation for #{index}\n\n"
        f"Role: {job.get('title', 'Unknown')}\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Score: {job.get('match_score', '?')}%\n"
        f"Location: {job.get('location', 'Unknown')}\n"
        f"Work mode: {_work_mode_label(job)}\n"
        f"Work mode evidence: {_work_mode_evidence(job)}\n"
        f"Source: {_source_label(job)}\n"
        f"Source link: {_source_url(job) or 'Unavailable'}\n"
        f"{posted}\n"
        f"Freshness gate: {freshness}\n"
        f"Action: {action}\n\n"
        f"Reason: {reason}"
    )


def _handle_search(bot_token: str, chat_id: str) -> None:
    """Trigger the job-ingestor pipeline asynchronously."""
    try:
        lambda_client.invoke(
            FunctionName=INGESTOR_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"source": "telegram", "command": "search"}),
        )
        _send_telegram(
            bot_token,
            chat_id,
            "Search started.\n\n"
            "I am scanning fresh AI roles, removing duplicates, and scoring each role against your profile.\n\n"
            f"Only strong {SCORE_THRESHOLD}%+ matches will appear in your review queue.\n"
            "Use list or status in a few minutes.",
        )
    except Exception as e:
        logger.error("INVOKE_INGESTOR_ERROR: %s", str(e))
        _send_telegram(bot_token, chat_id, f"Error triggering search: {str(e)}")


def _handle_signal_test(bot_token: str, chat_id: str) -> None:
    """Seed one deterministic high-match test through the live pipeline."""
    try:
        lambda_client.invoke(
            FunctionName=INGESTOR_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"source": "telegram", "command": "signal test", "seed_signal_test": True}),
        )
        _send_telegram(
            bot_token,
            chat_id,
            "Signal test started.\n\n"
            "I seeded one synthetic AI Engineer match into the live ingestion pipeline.\n"
            "If the pipeline is healthy, you should receive the normal high-match notification shortly.\n\n"
            "Then use list or status to verify the stored source, work mode, and evidence fields.",
        )
    except Exception as e:
        logger.error("INVOKE_SIGNAL_TEST_ERROR: %s", str(e))
        _send_telegram(bot_token, chat_id, f"Error triggering signal test: {str(e)}")


def _handle_showcase(bot_token: str, chat_id: str) -> None:
    """Seed one neutral showcase job into the review queue."""
    if not ENABLE_PRACTICE_MODE:
        _retire_old_showcase_rows("")
        _send_telegram(
            bot_token,
            chat_id,
            "Practice mode is disabled in production.\n\n"
            "Use search jobs to run the live pipeline, then list to review real matches.",
        )
        return

    job = _make_showcase_job()
    try:
        _retire_old_showcase_rows(job["job_id"])
        table.put_item(Item=job)
        _send_telegram(
            bot_token,
            chat_id,
            "TejaJob workspace is ready.\n\n"
            f"Added one practice opportunity to your review queue:\n"
            f"[{job['match_score']}%] {job['title']}\n"
            f"{job['company']} | {job['location']}\n\n"
            "Next steps:\n"
            "list - review the opportunity\n"
            "explain 1 - see the score rationale\n"
            "draft 1 - preview outreach without sending\n"
            "apply 1 - complete the practice workflow",
        )
    except Exception as e:
        logger.error("SHOWCASE_SEED_ERROR: %s", str(e))
        _send_telegram(bot_token, chat_id, f"Could not seed showcase agent: {str(e)}")


def _handle_list(bot_token: str, chat_id: str) -> None:
    """Show the ranked review queue."""
    jobs = _get_pending_jobs(limit=20)

    if not jobs:
        stats = _get_pipeline_stats()
        new_count = stats.get("NEW", 0)
        pending_total = stats.get("PENDING_APPROVAL", 0)
        if new_count:
            _send_telegram(
                bot_token,
                chat_id,
                f"No {SCORE_THRESHOLD}%+ matches are ready yet.\n\n"
                f"{new_count} job(s) are still unscored or waiting to be rescored. "
                "The processor is strict and only puts strong matches in this queue.\n\n"
                "Use status for pipeline counts, or check list again in a few minutes.",
            )
            return
        if pending_total:
            _send_telegram(
                bot_token,
                chat_id,
                f"No fresh {SCORE_THRESHOLD}%+ matches are ready.\n\n"
                f"{pending_total} older pending job(s) are hidden because they do not meet the current freshness/source rules.\n\n"
                "Use search jobs to scan live sources again.",
            )
            return
        empty_msg = (
            "Your review queue is empty.\n\n"
            "Use tejajob to practice the workflow, or search jobs to scan live sources."
            if ENABLE_PRACTICE_MODE
            else
            "Your review queue is empty.\n\n"
            "Use search jobs to scan live sources, then list to review real matches."
        )
        _send_telegram(
            bot_token,
            chat_id,
            empty_msg,
        )
        return

    lines = [f"Review queue: {len(jobs)} match(es) ready\n"]
    for i, job in enumerate(jobs, 1):
        score = job.get("match_score", "?")
        title = job.get("title", "Unknown Role")
        company = job.get("company", "Unknown")
        location = job.get("location", "Location not specified")
        posted = _format_posted(job.get("posted_at", ""))
        recruiter = (job.get("recruiter_email") or "").strip()
        source_url = _source_url(job)
        is_practice = job.get("source") == "tejajob"

        lines.append(f"{i}. {title}")
        lines.append(f"   {company} | {location}")
        lines.append(f"   Match: {score}%")
        lines.append(f"   Work mode: {_work_mode_label(job)}")
        lines.append(f"   Work mode evidence: {_work_mode_evidence(job)}")
        lines.append(f"   Source: {_source_label(job)}")
        if source_url:
            lines.append(f"   Source link: {source_url}")
        if posted:
            lines.append(f"   {posted}")
        if is_practice:
            lines.append(f"   Next: explain {i}, draft {i}, apply {i}")
        elif recruiter:
            lines.append(f"   Contact found: {recruiter}")
            if ALLOW_TELEGRAM_EMAIL_SEND:
                lines.append(f"   Next: draft {i} previews, send {i} sends email, apply {i} is manual")
            else:
                lines.append(f"   Next: draft {i} previews, apply {i} shows contact")
        else:
            lines.append(f"   Next: draft {i} to preview, apply {i} for manual follow-up")
        lines.append("")

    lines.append("Tip: use explain 1 or draft 1 before applying.")
    _send_telegram(bot_token, chat_id, "\n".join(lines))


def _handle_apply(bot_token: str, chat_id: str, index: int = 1) -> None:
    """Move a reviewable job to manual follow-up. Telegram never sends email here."""
    jobs = _get_pending_jobs(limit=20)

    if not jobs:
        empty_msg = (
            "Your review queue is empty.\n\nUse tejajob to practice the workflow, or search jobs to scan live sources."
            if ENABLE_PRACTICE_MODE
            else
            "Your review queue is empty.\n\nUse search jobs to scan live sources, then list to review real matches."
        )
        _send_telegram(
            bot_token,
            chat_id,
            empty_msg,
        )
        return

    if index < 1 or index > len(jobs):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    job = jobs[index - 1]
    job_id = job["job_id"]
    recruiter = (job.get("recruiter_email") or "").strip()
    source_url = _source_url(job)
    is_practice = job.get("source") == "tejajob"

    current = table.get_item(Key={"job_id": job_id}).get("Item", {})
    if current.get("status") != "PENDING_APPROVAL":
        _send_telegram(bot_token, chat_id, f"Job already {current.get('status', 'processed')}. Use 'list' to refresh.")
        return

    if is_practice:
        _update_job_status(job_id, "SHOWCASE_COMPLETE")
        _send_telegram(
            bot_token,
            chat_id,
            f"Practice workflow completed.\n\n"
            f"Role: {job.get('title', 'Unknown')}\n"
            f"Company: {job.get('company', 'Unknown')}\n"
            f"Match: {job.get('match_score', '?')}%\n\n"
            "No real application was submitted and no email was sent.\n"
            "Use tejajob anytime to reset the practice opportunity.",
        )
        return

    _update_job_status(job_id, "MANUAL_APPLY")
    contact_line = f"Contact: {recruiter}" if recruiter else "Contact: no recruiter email found"
    _send_telegram(
        bot_token,
        chat_id,
        f"Manual follow-up selected.\n\n"
        f"Role: {job.get('title', 'Unknown')}\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Match: {job.get('match_score', '?')}%\n"
        f"Work mode: {_work_mode_label(job)}\n"
        f"Work mode evidence: {_work_mode_evidence(job)}\n"
        f"Source: {_source_label(job)}\n"
        f"Source link: {source_url or 'No source link stored'}\n"
        f"{contact_line}\n\n"
        "No email was sent from Telegram. Review the source and send manually if it is a real fit.\n"
        "I moved it out of the active queue so it will not keep reappearing in list.\n\n"
        "After you act, reply applied. If you decide not to apply, reply not applied <reason>.",
    )


def _handle_followups(bot_token: str, chat_id: str) -> None:
    """Show manual follow-up jobs waiting for a real-world outcome."""
    jobs = _get_manual_followup_jobs(limit=10)
    if not jobs:
        _send_telegram(
            bot_token,
            chat_id,
            "No manual follow-ups are waiting for an outcome.\n\n"
            "Use apply 1 on a reviewable job first, then reply applied or not applied <reason>.",
        )
        return

    lines = [f"Manual follow-ups waiting: {len(jobs)}\n"]
    for i, job in enumerate(jobs, 1):
        source_url = _source_url(job)
        recruiter = (job.get("recruiter_email") or "").strip()
        lines.append(f"{i}. {job.get('title', 'Unknown Role')}")
        lines.append(f"   {job.get('company', 'Unknown')} | {job.get('location', 'Location not specified')}")
        lines.append(f"   Match: {job.get('match_score', '?')}%")
        lines.append(f"   Source: {_source_label(job)}")
        lines.append(f"   Source link: {source_url or 'No source link stored'}")
        lines.append(f"   Contact: {recruiter or 'none found'}")
        lines.append("")

    lines.append("Reply applied 1 after you apply, or not applied 1 <reason> if you skip it.")
    _send_telegram(bot_token, chat_id, "\n".join(lines))


def _handle_application_outcome(
    bot_token: str,
    chat_id: str,
    outcome: str,
    index: int = 1,
    reason: str = "",
) -> None:
    """Record whether the user really applied after manual follow-up."""
    jobs = _get_manual_followup_jobs(limit=10)
    if not jobs:
        _send_telegram(
            bot_token,
            chat_id,
            "I do not have any manual follow-ups waiting for an outcome.\n\n"
            "Use apply 1 first so I know which role you are acting on.",
        )
        return

    if index < 1 or index > len(jobs):
        _send_telegram(bot_token, chat_id, f"Invalid follow-up index. Use a number from 1 to {len(jobs)}.")
        return

    job = jobs[index - 1]
    status = _record_application_outcome(job["job_id"], outcome, reason)
    label = "applied" if outcome == "applied" else "not applied"
    reason_line = f"\nReason: {reason}" if reason else ""
    _send_telegram(
        bot_token,
        chat_id,
        f"Recorded: {label}.\n\n"
        f"Role: {job.get('title', 'Unknown')}\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Stored status: {status}{reason_line}\n\n"
        "This outcome is now stored on the job row for future feedback/eval analysis.",
    )


def _handle_review_feedback(
    bot_token: str,
    chat_id: str,
    sentiment: str,
    index: int = 1,
    reason: str = "",
) -> None:
    """Record whether the user is interested in a reviewable job."""
    jobs = _get_pending_jobs(limit=20)
    if not jobs:
        _send_telegram(bot_token, chat_id, "Your review queue is empty. Use search jobs to scan live sources.")
        return

    if index < 1 or index > len(jobs):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    job = jobs[index - 1]
    status = _record_review_feedback(job["job_id"], sentiment, reason)
    label = {
        "interested": "interested",
        "not_interested": "not interested",
        "neutral": "neutral",
    }.get(sentiment, sentiment)
    next_step = (
        "Next: use draft {0} or apply {0} if you want to act.".format(index)
        if sentiment != "not_interested"
        else "I removed it from the active review queue."
    )
    reason_line = f"\nReason: {reason}" if reason else ""
    _send_telegram(
        bot_token,
        chat_id,
        f"Recorded: {label}.\n\n"
        f"Role: {job.get('title', 'Unknown')}\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Stored status: {status}{reason_line}\n\n"
        f"{next_step}",
    )


def _handle_explain(bot_token: str, chat_id: str, index: int = 1) -> None:
    """Explain why a job is in the review queue."""
    jobs = _get_pending_jobs(limit=20)
    if not jobs:
        msg = (
            "Your review queue is empty. Use tejajob or search jobs to add matches."
            if ENABLE_PRACTICE_MODE
            else
            "Your review queue is empty. Use search jobs to scan live sources."
        )
        _send_telegram(bot_token, chat_id, msg)
        return
    if index < 1 or index > len(jobs):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    _send_telegram(bot_token, chat_id, _format_job_explanation(jobs[index - 1], index))


def _handle_draft(bot_token: str, chat_id: str, index: int = 1) -> None:
    """Generate a preview-only outreach draft for a reviewable job."""
    jobs = _get_pending_jobs(limit=20)
    if not jobs:
        msg = (
            "Your review queue is empty. Use tejajob or search jobs to add matches."
            if ENABLE_PRACTICE_MODE
            else
            "Your review queue is empty. Use search jobs to scan live sources."
        )
        _send_telegram(bot_token, chat_id, msg)
        return
    if index < 1 or index > len(jobs):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    job = jobs[index - 1]
    try:
        response = lambda_client.invoke(
            FunctionName=SENDER_LAMBDA,
            InvocationType="RequestResponse",
            Payload=_safe_json_dumps({"preview_only": True, "job": job}),
        )
        payload = _read_lambda_payload(response)
        if payload.get("status") != "preview":
            raise ValueError(payload.get("error") or "sender did not return a preview")

        email = payload["email"]
        body = str(email.get("body", "")).strip()
        if len(body) > 2500:
            body = body[:2500].rstrip() + "\n[truncated]"
        fallback_note = ""
        if payload.get("generated_with_fallback"):
            fallback_note = (
                "\n\nNote: this preview used the safe fallback template because Bedrock was temporarily throttled."
            )

        _send_telegram(
            bot_token,
            chat_id,
            f"Draft preview for #{index}\n\n"
            f"Subject: {email.get('subject', '')}\n\n"
            f"{body}{fallback_note}\n\n"
            "No email was sent.\n\n"
            f"Next: interested {index}, meh {index} <reason>, not interested {index} <reason>, or apply {index}.",
        )
    except Exception as e:
        logger.error("DRAFT_PREVIEW_ERROR: %s", str(e))
        _send_telegram(bot_token, chat_id, f"Could not generate draft preview: {str(e)}")


def _handle_send(bot_token: str, chat_id: str, index: int = 1, confirmed: bool = False) -> None:
    """Send outreach for a reviewable job after explicit Telegram command."""
    jobs = _get_pending_jobs(limit=20)
    if not jobs:
        _send_telegram(bot_token, chat_id, "Your review queue is empty. Use search jobs to scan live sources.")
        return
    if index < 1 or index > len(jobs):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    job = jobs[index - 1]
    recruiter = (job.get("recruiter_email") or "").strip()
    source_url = _source_url(job)
    if job.get("source") == "tejajob":
        _send_telegram(bot_token, chat_id, "Practice jobs cannot send real email.")
        return
    if not recruiter:
        _send_telegram(
            bot_token,
            chat_id,
            "No recruiter email is stored for this role.\n\n"
            f"Use apply {index} for the source link and manual follow-up.",
        )
        return
    if not source_url.startswith(("http://", "https://")):
        _send_telegram(bot_token, chat_id, "Blocked: this role has no verifiable source link.")
        return
    if not ALLOW_TELEGRAM_EMAIL_SEND:
        _send_telegram(
            bot_token,
            chat_id,
            "Email send is disabled in Telegram config.\n\n"
            "To enable it, set ALLOW_TELEGRAM_EMAIL_SEND=true on job-router and "
            "ALLOW_REAL_EMAIL_SEND=true on job-sender.\n\n"
            f"For now, use draft {index} then send manually.",
        )
        return
    if not confirmed:
        _send_telegram(
            bot_token,
            chat_id,
            f"Send confirmation required.\n\n"
            f"Role: {job.get('title', 'Unknown')}\n"
            f"Company: {job.get('company', 'Unknown')}\n"
            f"To: {recruiter}\n"
            f"Source link: {source_url}\n\n"
            f"Type exactly: confirm send {index}\n\n"
            "No email was sent.",
        )
        return

    try:
        response = lambda_client.invoke(
            FunctionName=SENDER_LAMBDA,
            InvocationType="RequestResponse",
            Payload=_safe_json_dumps({"send_email": True, "send_confirmed": True, "job": job}),
        )
        payload = _read_lambda_payload(response)
        status = payload.get("status", "")
        if status == "send_disabled":
            _send_telegram(
                bot_token,
                chat_id,
                "Sender Lambda refused to send because ALLOW_REAL_EMAIL_SEND is false.\n\n"
                f"Use draft {index} then send manually, or enable the sender kill switch.",
            )
            return
        if status == "sent" or payload.get("sent", 0) > 0:
            _send_telegram(
                bot_token,
                chat_id,
                f"Email sent.\n\n"
                f"Role: {job.get('title', 'Unknown')}\n"
                f"Company: {job.get('company', 'Unknown')}\n"
                f"To: {recruiter}\n"
                f"Source: {_source_label(job)}\n"
                f"Source link: {source_url}",
            )
            return
        if status == "no_recruiter_email":
            _send_telegram(bot_token, chat_id, f"No recruiter email is stored. Use apply {index}.")
            return
        raise ValueError(payload.get("reason") or payload.get("error") or f"unexpected sender status: {status}")
    except Exception as e:
        logger.error("SEND_COMMAND_ERROR: %s", str(e))
        _send_telegram(bot_token, chat_id, f"Could not send email: {str(e)}")


def _handle_reject(bot_token: str, chat_id: str, index: int = 1) -> None:
    """Reject a specific reviewable job."""
    jobs = _get_pending_jobs(limit=20)

    if not jobs:
        _send_telegram(bot_token, chat_id, "Your review queue is empty.")
        return

    if index < 1 or index > len(jobs):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    job = jobs[index - 1]
    _update_job_status(job["job_id"], "REJECTED")
    _send_telegram(bot_token, chat_id, f"Rejected: {job.get('title', 'Unknown')} at {job.get('company', 'Unknown')}")


def _handle_status(bot_token: str, chat_id: str) -> None:
    """Show pipeline statistics plus source evidence for reviewable jobs."""
    stats = _get_pipeline_stats()
    jobs = _get_pending_jobs(limit=20)
    scored_pending = _get_scored_pending_jobs(limit=20)
    reviewable_ids = {job.get("job_id") for job in jobs}
    watchlist = [job for job in scored_pending if job.get("job_id") not in reviewable_ids][:5]
    lines = [
        "Pipeline Status\n"
        "------------------",
        "",
        f"New, unscored:      {stats.get('NEW', 0)}",
        f"Pending total:      {stats.get('PENDING_APPROVAL', 0)}",
        f"Review threshold:   {SCORE_THRESHOLD}%",
        f"Reviewable now:     {len(jobs)}",
        f"Scored live rows:   {len(scored_pending)}",
        f"Manual follow-up:   {stats.get('MANUAL_APPLY', 0)}",
        f"Applied:            {stats.get('APPLIED', 0)}",
        f"Not applied:        {stats.get('NOT_APPLIED', 0)}",
        f"Not interested:     {stats.get('NOT_INTERESTED', 0)}",
        f"Sent before safety: {stats.get('SENT', 0)}",
        f"Rejected:           {stats.get('REJECTED', 0)}",
        f"Failed:             {stats.get('FAILED', 0)}",
        "",
    ]

    if not jobs and not watchlist:
        lines.extend([
            "Reviewable evidence: none",
            "",
            "Use search jobs to scan live sources, then status to inspect source links before acting.",
        ])
        _send_telegram(bot_token, chat_id, "\n".join(lines))
        return

    lines.append("Reviewable evidence:" if jobs else f"Reviewable evidence: none at {SCORE_THRESHOLD}%+")
    for i, job in enumerate(jobs, 1):
        recruiter = (job.get("recruiter_email") or "").strip()
        source_url = _source_url(job)
        reason = _short_text(job.get("match_reason"), 180)

        lines.append("")
        lines.append(f"{i}. {job.get('title', 'Unknown Role')}")
        lines.append(f"   Company: {job.get('company', 'Unknown')}")
        lines.append(f"   Location: {job.get('location', 'Location not specified')}")
        lines.append(f"   Work mode: {_work_mode_label(job)}")
        lines.append(f"   Work mode evidence: {_work_mode_evidence(job)}")
        lines.append(f"   Match: {job.get('match_score', '?')}%")
        lines.append(f"   Source: {_source_label(job)}")
        lines.append(f"   Source link: {source_url or 'MISSING - verify before acting'}")
        lines.append(f"   {_format_posted(job.get('posted_at', '')) or 'Posted date unavailable'}")
        lines.append(f"   Contact: {recruiter or 'none found'}")
        if reason:
            lines.append(f"   Model reason: {reason}")
        lines.append(f"   Safe actions: explain {i}, draft {i}, apply {i}")

    if watchlist:
        lines.append("")
        lines.append("Other scored live roles below review threshold:")
        for i, job in enumerate(watchlist, 1):
            source_url = _source_url(job)
            lines.append("")
            lines.append(f"{i}. {job.get('title', 'Unknown Role')}")
            lines.append(f"   Company: {job.get('company', 'Unknown')}")
            lines.append(f"   Work mode: {_work_mode_label(job)}")
            lines.append(f"   Work mode evidence: {_work_mode_evidence(job)}")
            lines.append(f"   Match: {job.get('match_score', '?')}%")
            lines.append(f"   Source: {_source_label(job)}")
            lines.append(f"   Source link: {source_url or 'MISSING - verify before acting'}")
            lines.append(f"   Why hidden: below {SCORE_THRESHOLD}% review threshold")

    lines.extend([
        "",
        "Rule: if a role has no source link, do not act on it.",
        "apply never sends email. send requires explicit enablement and a recruiter email.",
    ])
    _send_telegram(bot_token, chat_id, "\n".join(lines))


def _handle_help(bot_token: str, chat_id: str) -> None:
    """Show available commands."""
    text, inline_keyboard = _help_page("home")
    _send_telegram(
        bot_token,
        chat_id,
        text,
        reply_markup=inline_keyboard,
    )


def _handle_help_callback(bot_token: str, chat_id: str, message_id: int, page: str) -> None:
    """Edit the help message to the selected breadcrumb page."""
    text, keyboard = _help_page(page)
    _edit_telegram_message(bot_token, chat_id, message_id, text, reply_markup=keyboard)


APPLY_PATTERN = re.compile(r"^apply\s*(\d+)?$", re.IGNORECASE)
REVIEW_FEEDBACK_PATTERN = re.compile(
    r"^(interested|like|liked|yes|good|not\s+interested|not\s+like|dislike|bad|no|meh|neutral|maybe)"
    r"(?:\s+(\d+))?(?:\s+(.+))?$",
    re.IGNORECASE,
)
APPLICATION_OUTCOME_PATTERN = re.compile(
    r"^(applied|not\s+applied|not\s+apply|did\s+not\s+apply|skipped|skip|pass|passed)"
    r"(?:\s+(\d+))?(?:\s+(.+))?$",
    re.IGNORECASE,
)
REJECT_PATTERN = re.compile(r"^reject\s*(\d+)?$", re.IGNORECASE)
EXPLAIN_PATTERN = re.compile(r"^explain\s*(\d+)?$", re.IGNORECASE)
DRAFT_PATTERN = re.compile(r"^draft\s*(\d+)?$", re.IGNORECASE)
SEND_PATTERN = re.compile(r"^send\s*(\d+)?$", re.IGNORECASE)
CONFIRM_SEND_PATTERN = re.compile(r"^confirm\s+send\s+(\d+)$", re.IGNORECASE)


def _route_command(command: str, bot_token: str, chat_id: str) -> None:
    """Parse user input and route to a command handler."""
    raw = command.strip()
    cmd = raw.lower()

    if cmd in (
        "search jobs",
        "search",
        "find jobs",
        "serch jobs",
        "search job",
        "find",
        "new jobs",
        "new",
        "fetch",
        "fetch jobs",
    ):
        _handle_search(bot_token, chat_id)
        return

    if cmd in ("signal test", "test signal", "notify test", "notification test"):
        _handle_signal_test(bot_token, chat_id)
        return

    if cmd in ("tejajob", "teja job", "showcase", "showcase agent", "agent showcase", "seed showcase", "showcase mode"):
        _handle_showcase(bot_token, chat_id)
        return

    if cmd in ("list", "jobs", "pending", "show"):
        _handle_list(bot_token, chat_id)
        return

    if cmd in ("followups", "follow ups", "manual followups", "manual follow-ups", "manual"):
        _handle_followups(bot_token, chat_id)
        return

    if cmd in ("status", "stats"):
        _handle_status(bot_token, chat_id)
        return

    if cmd in ("help", "start", "/start", "/help"):
        _handle_help(bot_token, chat_id)
        return

    match = APPLICATION_OUTCOME_PATTERN.match(raw)
    if match:
        command_word = re.sub(r"\s+", " ", match.group(1).strip().lower())
        outcome = "applied" if command_word == "applied" else "not_applied"
        index = int(match.group(2)) if match.group(2) else 1
        reason = (match.group(3) or "").strip()
        if outcome == "not_applied" and not reason:
            reason = "No reason provided"
        _handle_application_outcome(bot_token, chat_id, outcome, index, reason)
        return

    match = REVIEW_FEEDBACK_PATTERN.match(raw)
    if match:
        command_word = re.sub(r"\s+", " ", match.group(1).strip().lower())
        if command_word in {"interested", "like", "liked", "yes", "good"}:
            sentiment = "interested"
        elif command_word in {"meh", "neutral", "maybe"}:
            sentiment = "neutral"
        else:
            sentiment = "not_interested"
        index = int(match.group(2)) if match.group(2) else 1
        reason = (match.group(3) or "").strip()
        if sentiment == "not_interested" and not reason:
            reason = "No reason provided"
        _handle_review_feedback(bot_token, chat_id, sentiment, index, reason)
        return

    match = APPLY_PATTERN.match(cmd)
    if match:
        index = int(match.group(1)) if match.group(1) else 1
        _handle_apply(bot_token, chat_id, index)
        return
    if cmd.startswith("apply"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: apply 1")
        return

    match = EXPLAIN_PATTERN.match(cmd)
    if match:
        index = int(match.group(1)) if match.group(1) else 1
        _handle_explain(bot_token, chat_id, index)
        return
    if cmd.startswith("explain"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: explain 1")
        return

    match = DRAFT_PATTERN.match(cmd)
    if match:
        index = int(match.group(1)) if match.group(1) else 1
        _handle_draft(bot_token, chat_id, index)
        return
    if cmd.startswith("draft"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: draft 1")
        return

    match = CONFIRM_SEND_PATTERN.match(cmd)
    if match:
        _handle_send(bot_token, chat_id, int(match.group(1)), confirmed=True)
        return

    match = SEND_PATTERN.match(cmd)
    if match:
        index = int(match.group(1)) if match.group(1) else 1
        _handle_send(bot_token, chat_id, index, confirmed=False)
        return
    if cmd.startswith("send"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: send 1")
        return

    match = REJECT_PATTERN.match(cmd)
    if match:
        index = int(match.group(1)) if match.group(1) else 1
        _handle_reject(bot_token, chat_id, index)
        return
    if cmd.startswith("reject"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: reject 1")
        return

    _handle_help(bot_token, chat_id)


def _handle_callback_command(bot_token: str, chat_id: str, callback_query_id: str, data: str, message_id: int) -> None:
    """Handle Telegram inline button callbacks for help navigation and command shortcuts."""
    if data.startswith("help:"):
        page = data.split(":", 1)[1]
        _handle_help_callback(bot_token, chat_id, message_id, page)
        _answer_callback_query(bot_token, callback_query_id)
        return

    if data.startswith("cmd:"):
        command = data.split(":", 1)[1]
        _answer_callback_query(bot_token, callback_query_id, f"Running {command}")
        _route_command(command, bot_token, chat_id)
        return

    _answer_callback_query(bot_token, callback_query_id, "Unknown action")


def lambda_handler(event, context):
    """
    Entry point for both SNS notifications and Telegram webhook commands.
    """
    logger.info("ROUTER_EVENT: %s", json.dumps(event, default=str)[:500])

    secrets = _get_secrets()
    bot_token = secrets.get("TELEGRAM_BOT_TOKEN")
    chat_id = secrets.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        logger.error("MISSING_TELEGRAM_CREDENTIALS")
        return {"statusCode": 500, "body": "Missing Telegram credentials"}

    if "Records" in event:
        for record in event["Records"]:
            try:
                message_text = record["Sns"]["Message"]
                _send_telegram(bot_token, chat_id, message_text)
                logger.info("SNS_FORWARDED: %s", message_text[:100])
            except (KeyError, TypeError) as e:
                logger.error("SNS_PARSE_ERROR: %s", str(e))
        return {"statusCode": 200, "body": "Alerts processed"}

    if "body" in event:
        try:
            body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
            callback_query = body.get("callback_query")
            if callback_query:
                data = callback_query.get("data", "")
                callback_query_id = callback_query.get("id", "")
                message = callback_query.get("message", {})
                incoming_chat_id = str(message.get("chat", {}).get("id", ""))
                message_id = _as_int(message.get("message_id"))

                if incoming_chat_id != str(chat_id):
                    logger.warning("UNAUTHORIZED_CALLBACK_CHAT: %s (expected %s)", incoming_chat_id, chat_id)
                    return {"statusCode": 200, "body": "Unauthorized"}

                if data and callback_query_id and message_id:
                    logger.info("TELEGRAM_CALLBACK: '%s' from chat %s", data, incoming_chat_id)
                    _handle_callback_command(bot_token, chat_id, callback_query_id, data, message_id)
                return {"statusCode": 200}

            message = body.get("message", {})
            user_text = message.get("text", "").strip()
            incoming_chat_id = str(message.get("chat", {}).get("id", ""))

            if incoming_chat_id != str(chat_id):
                logger.warning("UNAUTHORIZED_CHAT: %s (expected %s)", incoming_chat_id, chat_id)
                return {"statusCode": 200, "body": "Unauthorized"}

            if user_text:
                logger.info("TELEGRAM_CMD: '%s' from chat %s", user_text, incoming_chat_id)
                _route_command(user_text, bot_token, chat_id)

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error("WEBHOOK_PARSE_ERROR: %s", str(e))

    return {"statusCode": 200}
