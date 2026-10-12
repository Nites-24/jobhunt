"""
Job Router Lambda — Telegram Control Plane & Human-in-the-Loop Gateway

=============================================================================
PIPELINE ROLE & ARCHITECTURE
=============================================================================
Trigger 1:  API Gateway HTTP POST from Telegram Webhook (user commands & callbacks)
Trigger 2:  Amazon SNS topic 'jobsearch-approvals' (high-match alerts from job-processor)
Invocation: Invokes job-ingestor (async for 'search jobs') and job-sender (sync for 'draft'/'send')
Datastore:  Reads/updates opportunity state in DynamoDB ('Jobsearch' table)

=============================================================================
SUPPORTED TELEGRAM COMMANDS & CONTROLS
=============================================================================
- Discovery:
  * 'search jobs'    → Invokes job-ingestor to scan all configured sources asynchronously.
  * 'list'           → Displays top reviewable pending jobs ranked by match score.
  * 'next'           → Presents the single freshest, highest-match card with action buttons.
  * 'why [N]'        → Explains scoring rationale, strengths, and gaps from stored eval data.
- Outreach & Decisions:
  * 'draft [N]'      → Invokes job-sender in preview mode to generate a concise outreach draft.
  * 'apply [N]'      → Displays direct apply link & contact for manual follow-up (AIR-GAPPED).
  * 'skip [reason]'  → Rejects the current card and stores the skip reason for eval modeling.
  * 'send 1'         → Triggers real email delivery ONLY if both security gates are enabled.
- Feedback & Learning Loop:
  * 'interested N' / 'meh N <reason>' / 'not interested N <reason>' → Persists labeled preferences.
  * 'followups' / 'applied' / 'not applied <reason>' → Tracks real-world application outcomes.
- Diagnostics & Onboarding:
  * 'status'         → Shows queue counts, stage distributions, and source breakdown.
  * 'practice'       → Seeds an interview-safe practice card to explore the bot workflow.

=============================================================================
SECURITY & OPERATIONAL INVARIANTS
=============================================================================
- Air-Gapped Outreach (Gate 1): 'apply' NEVER sends email. Real email dispatch via
  'send 1' requires ALLOW_TELEGRAM_EMAIL_SEND=true AND Gate 2 on job-sender.
- Review Queue Freshness: Enforces strict freshness (1 day generic, 3 days HN) so the
  candidate never sees stale or expired postings.
- Source Link Integrity: Unverifiable or missing source links are filtered out of review.
- Webhook Secret Validation: Optional X-Telegram-Bot-Api-Secret-Token header check.

=============================================================================
ENVIRONMENT VARIABLES & SECRETS
=============================================================================
- TELEGRAM_SECRET_NAME:            Secrets Manager secret ID for bot token & chat ID
- DYNAMODB_TABLE:                  DynamoDB table name (default: 'Jobsearch')
- SENDER_LAMBDA_NAME:              Name of job-sender Lambda function (default: 'job-sender')
- INGESTOR_LAMBDA_NAME:            Name of job-ingestor Lambda function (default: 'job-ingestor')
- SCORE_THRESHOLD:                 Review threshold score (default: 80)
- MAX_JOB_AGE_DAYS:                Review queue freshness window in days (default: 1)
- HN_MAX_JOB_AGE_DAYS:             Hacker News freshness window in days (default: 3)
- ALLOW_TELEGRAM_EMAIL_SEND:       Gate 1 flag to allow 'send' command (default: false)
- REQUIRE_TELEGRAM_WEBHOOK_SECRET: Whether to enforce webhook token header (default: false)

=============================================================================
DEVELOPER & MAINTENANCE GUIDE
=============================================================================
- Modifying Telegram keyboards: Update `_build_help_navigation()` and `_make_review_keyboard()`.
- Adding new commands: Register pattern in `_route_command()` and implement handler.
- Testing: Thoroughly covered by 60+ mocked test cases in tests/test_router.py.
"""

import json
import logging
import os
import re
import time
import hmac
import hashlib
import uuid
import urllib.error
import urllib.parse
import urllib.request
from contextvars import ContextVar
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

from shared.url_validation import get_canonical_job_url, is_valid_canonical_url


logger = logging.getLogger()
logger.setLevel(logging.INFO)

TELEGRAM_MAX_MESSAGE_CHARS = 4096
TELEGRAM_SAFE_MESSAGE_CHARS = 3800

AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

SECRET_NAME = os.environ.get("TELEGRAM_SECRET_NAME", "telegram")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "Jobsearch")
SENDER_LAMBDA = os.environ.get("SENDER_LAMBDA_NAME", "job-sender")
INGESTOR_LAMBDA = os.environ.get("INGESTOR_LAMBDA_NAME", "job-ingestor")
SCORE_THRESHOLD = int(os.environ.get("SCORE_THRESHOLD", "80"))
MAX_JOB_AGE_DAYS = int(os.environ.get("MAX_JOB_AGE_DAYS", "1"))
HN_MAX_JOB_AGE_DAYS = int(os.environ.get("HN_MAX_JOB_AGE_DAYS", "3"))
LIST_FETCH_LIMIT = int(os.environ.get("LIST_FETCH_LIMIT", "100"))
ENABLE_PRACTICE_MODE = os.environ.get("ENABLE_PRACTICE_MODE", "false").strip().lower() == "true"
ALLOW_TELEGRAM_EMAIL_SEND = os.environ.get("ALLOW_TELEGRAM_EMAIL_SEND", "false").strip().lower() == "true"
REQUIRE_TELEGRAM_WEBHOOK_SECRET = os.environ.get("REQUIRE_TELEGRAM_WEBHOOK_SECRET", "false").strip().lower() == "true"
REQUIRE_DRAFT_APPROVAL = os.environ.get("REQUIRE_DRAFT_APPROVAL", "true").strip().lower() == "true"
LIST_ALLOWED_SOURCES = {
    source.strip().lower()
    for source in os.environ.get(
        "LIST_ALLOWED_SOURCES",
        "hackernews,hn,jsearch,google_cse,cse,greenhouse,lever,ashby,showcase,practice,signal_test",
    ).split(",")
    if source.strip()
}

table = dynamodb.Table(TABLE_NAME)
_cached_secrets = None
REVIEW_SESSION_PREFIX = "telegram_session_"
_PROCESSED_CALLBACK_IDS: dict[str, float] = {}
_CALLBACK_STATES: dict[str, dict] = {}
_CALLBACK_DEDUP_TTL = 300
_ACTION_TOKEN_CACHE: dict[str, str] = {}
_JOB_ID_TO_TOKEN_CACHE: dict[str, str] = {}
_ACTIVE_CALLBACK_CLAIM: ContextVar[tuple[str, str] | None] = ContextVar("active_callback_claim", default=None)
_CALLBACK_DELIVERY_FAILED: ContextVar[bool] = ContextVar("callback_delivery_failed", default=False)


def _get_or_create_action_token(job_id: str | None) -> str:
    """Generate a compact 12-char hex token for a job_id and persist mapping in DynamoDB."""
    if not job_id:
        return ""
    job_id_str = str(job_id).strip()
    if not job_id_str:
        return ""
    if job_id_str in _JOB_ID_TO_TOKEN_CACHE:
        return _JOB_ID_TO_TOKEN_CACHE[job_id_str]

    token = hashlib.sha256(job_id_str.encode("utf-8")).hexdigest()[:12]
    try:
        table.put_item(
            Item={
                "job_id": f"tok:{token}",
                "entity_type": "telegram_action_token",
                "target_job_id": job_id_str,
                "created_at": int(time.time()),
            },
            ConditionExpression="attribute_not_exists(job_id) OR target_job_id = :target",
            ExpressionAttributeValues={":target": job_id_str},
        )
    except Exception as e:
        logger.error("PUT_ACTION_TOKEN_ERROR: %s", str(e))
        raise

    _ACTION_TOKEN_CACHE[token] = job_id_str
    _JOB_ID_TO_TOKEN_CACHE[job_id_str] = token
    return token


def _resolve_action_token(token_or_id: str | None) -> str | None:
    """Resolve a compact token or direct job_id to the canonical job_id."""
    if not token_or_id:
        return None
    val = str(token_or_id).strip()
    if not val:
        return None

    if val in _ACTION_TOKEN_CACHE:
        return _ACTION_TOKEN_CACHE[val]

    try:
        item = table.get_item(Key={"job_id": f"tok:{val}"}, ConsistentRead=True).get("Item")
        if item and item.get("target_job_id"):
            resolved = str(item["target_job_id"])
            _ACTION_TOKEN_CACHE[val] = resolved
            _JOB_ID_TO_TOKEN_CACHE[resolved] = val
            return resolved
    except Exception as e:
        logger.error("GET_ACTION_TOKEN_ERROR: %s", str(e))
        raise

    if re.fullmatch(r"[0-9a-f]{12}", val):
        if not table.get_item(Key={"job_id": val}, ConsistentRead=True).get("Item"):
            raise ValueError("Review button expired. Open a fresh job card.")
    return val


def _get_callback_state(callback_query_id: str) -> dict | None:
    """Check both in-memory cache and DynamoDB for callback processing state."""
    if not callback_query_id:
        return None
    try:
        item = table.get_item(Key={"job_id": f"cb:{callback_query_id}"}, ConsistentRead=True).get("Item")
        if item:
            rec = {
                "status": item.get("callback_status", "COMPLETED"),
                "ts": int(item.get("updated_at", 0)),
                "target_job_id": item.get("target_job_id"),
                "telegram_chat_id": item.get("telegram_chat_id"),
                "claim_owner": item.get("claim_owner"),
                "lease_until": int(item.get("lease_until", 0)),
                "action_data": item.get("action_data"),
                "initial_review_unset": bool(item.get("initial_review_unset", False)),
            }
            _CALLBACK_STATES[callback_query_id] = rec
            _PROCESSED_CALLBACK_IDS[callback_query_id] = rec["ts"]
            return rec
    except Exception as e:
        logger.error("GET_CALLBACK_STATE_ERROR: %s", str(e))
        raise
    return None


def _claim_callback(callback_query_id: str, chat_id: str, action_data: str, context: Any) -> tuple[str, str | None]:
    """Atomically claim a callback and pin its review target before any action."""
    previous = _get_callback_state(callback_query_id)
    now = int(time.time())
    if previous:
        if previous["status"] == "COMPLETED":
            return "duplicate", None
        if previous["status"] == "IN_PROGRESS" and previous["lease_until"] > now:
            return "busy", None
        if previous.get("action_data") and previous["action_data"] != action_data:
            return "invalid", None

    parts = action_data.split(":")
    action = parts[1].strip().lower() if len(parts) > 1 and parts[0] == "cmd" else ""
    raw_target = parts[2].strip() if len(parts) > 2 else ""
    target = previous.get("target_job_id") if previous else None
    initial_review_unset = previous.get("initial_review_unset", False) if previous else False
    if previous and not target and action in {"apply", "skip"}:
        # Legacy FAILED callbacks without identity must not select a new active job.
        return "invalid", None
    if not target and raw_target and action in {"apply", "skip", "why", "draft", "next"}:
        target = _resolve_action_token(raw_target)
    if not target and action in {"apply", "skip", "next"}:
        target = _get_active_review_job_id(chat_id)
        if not target:
            initial_review_unset = action == "next"
            jobs = _get_pending_jobs(limit=1)
            target = jobs[0]["job_id"] if jobs else None
        if not target and action in {"apply", "skip"}:
            return "invalid", None

    owner = uuid.uuid4().hex
    remaining = getattr(context, "get_remaining_time_in_millis", lambda: 900000)()
    lease_seconds = max(120, int(remaining / 1000) + 30)
    item = {
        "job_id": f"cb:{callback_query_id}", "entity_type": "telegram_callback",
        "callback_status": "IN_PROGRESS", "telegram_chat_id": str(chat_id),
        "updated_at": now, "lease_until": now + lease_seconds,
        "claim_owner": owner, "action_data": action_data,
    }
    if target:
        item["target_job_id"] = target
    if initial_review_unset:
        item["initial_review_unset"] = True
    if previous:
        old_owner = previous.get("claim_owner")
        condition = "#state = :old_state AND #updated = :old_updated"
        values = {":old_state": previous["status"], ":old_updated": previous["ts"]}
        if old_owner:
            condition += " AND claim_owner = :old_owner"
            values[":old_owner"] = old_owner
        else:
            condition += " AND attribute_not_exists(claim_owner)"
        names = {"#state": "callback_status", "#updated": "updated_at"}
    else:
        condition, values, names = "attribute_not_exists(job_id)", None, None
    try:
        kwargs = {"Item": item, "ConditionExpression": condition}
        if values:
            kwargs["ExpressionAttributeValues"] = values
        if names:
            kwargs["ExpressionAttributeNames"] = names
        table.put_item(**kwargs)
    except Exception as e:
        if "ConditionalCheckFailed" in type(e).__name__ or "ConditionalCheckFailed" in str(e):
            current = _get_callback_state(callback_query_id)
            return ("duplicate" if current and current["status"] == "COMPLETED" else "busy"), None
        raise
    _CALLBACK_STATES[callback_query_id] = {"status": "IN_PROGRESS", "ts": now,
        "target_job_id": target, "claim_owner": owner, "lease_until": now + lease_seconds}
    _PROCESSED_CALLBACK_IDS[callback_query_id] = now
    return "claimed", owner


def _record_callback_state(
    callback_query_id: str,
    status: str,
    chat_id: str = "",
    target_job_id: str | None = None,
    action: str = "",
) -> None:
    """Finish the owned callback without replacing its pinned target."""
    if not callback_query_id:
        return
    active = _ACTIVE_CALLBACK_CLAIM.get()
    if not active or active[0] != callback_query_id:
        raise RuntimeError("Callback completion attempted without ownership")
    table.update_item(
        Key={"job_id": f"cb:{callback_query_id}"},
        UpdateExpression="SET #state = :state, updated_at = :updated",
        ConditionExpression="claim_owner = :owner AND #state = :in_progress",
        ExpressionAttributeNames={"#state": "callback_status"},
        ExpressionAttributeValues={":state": status, ":updated": int(time.time()),
                                   ":owner": active[1], ":in_progress": "IN_PROGRESS"},
    )
    _CALLBACK_STATES.pop(callback_query_id, None)
    _PROCESSED_CALLBACK_IDS.pop(callback_query_id, None)


def _is_duplicate_callback(callback_query_id: str) -> bool:
    """Check if callback query was already completed or currently in-progress."""
    if not callback_query_id:
        return False
    rec = _get_callback_state(callback_query_id)
    if not rec:
        return False
    status = rec.get("status")
    if status == "COMPLETED":
        return True
    if status == "IN_PROGRESS":
        now = time.time()
        if rec.get("lease_until", 0) > now:
            return True
    return False


def _clear_processed_callback(callback_query_id: str) -> None:
    """Clear only local hints; never delete durable callback ownership."""
    if not callback_query_id:
        return
    _CALLBACK_STATES.pop(callback_query_id, None)
    _PROCESSED_CALLBACK_IDS.pop(callback_query_id, None)


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


def _is_synthetic_job(job: dict) -> bool:
    """Return True for non-user-facing practice, test, or internal session rows."""
    source = str(job.get("source", "")).strip().lower()
    url = _source_url(job).lower()
    model_id = str(job.get("model_id", "")).strip().lower()
    entity_type = str(job.get("entity_type", "")).strip().lower()
    if source in {"showcase", "practice"} and ENABLE_PRACTICE_MODE:
        return False
    return bool(
        entity_type in {"telegram_review_session", "telegram_callback", "telegram_action_token"}
        or str(job.get("job_id", "")).startswith(("telegram_session_", "cb:", "tok:"))
        or source in {"signal_test", "showcase", "practice"}
        or job.get("signal_test")
        or model_id == "showcase-seed"
        or url.startswith("https://example.com/")
    )


def _source_allowed(job: dict) -> bool:
    source = str(job.get("source", "")).strip().lower()
    if source in {"showcase", "practice"} and not ENABLE_PRACTICE_MODE:
        return False
    return bool(source and source in LIST_ALLOWED_SOURCES)


def _source_label(job: dict) -> str:
    source = str(job.get("source", "")).strip().lower()
    if source in {"hackernews", "hn"}:
        return "Hacker News Who is Hiring"
    if source in {"showcase", "practice"}:
        return "Practice item"
    if source == "signal_test":
        return "Notification test seed"
    if source in {"google_cse", "cse"}:
        return "Google Custom Search"
    if source == "jsearch":
        return "JSearch RapidAPI"
    if source == "greenhouse":
        return "Greenhouse Job Board"
    if source == "lever":
        return "Lever Postings"
    if source == "ashby":
        return "Ashby Job Board"
    return source or "unknown"


def _is_practice_job(job: dict) -> bool:
    return str(job.get("source", "")).strip().lower() in {"showcase", "practice"}


def _source_url(job: dict) -> str:
    url = get_canonical_job_url(job)
    if url:
        return url
    if _is_practice_job(job):
        return str(job.get("url") or job.get("apply_url") or "").strip()
    return ""


def _work_mode_label(job: dict) -> str:
    """Human-readable work mode.  Returns explicit uncertainty when source
    didn't state a mode so the reviewer knows to verify."""
    label = str(job.get("work_mode") or "").strip().lower()
    if label in {"remote", "hybrid", "onsite"}:
        return label
    return "not stated"


def _is_work_mode_uncertain(job: dict) -> bool:
    """True when work mode was not found in the source."""
    return _work_mode_label(job) == "not stated"


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
    return "not stated in source — verify before acting"


def _short_text(value: Any, limit: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _is_reviewable_job(job: dict) -> bool:
    """Apply hard UX filters before a job can appear in the command queue."""
    if _is_synthetic_job(job):
        return False
    if job.get("status") != "PENDING_APPROVAL":
        return False
    if _as_int(job.get("match_score")) < SCORE_THRESHOLD:
        return False
    if not _source_allowed(job):
        return False
    if not _is_practice_job(job) and not _source_url(job):
        return False

    age = _job_age_days(job)
    if age is None or age > _source_max_age_days(job):
        return False
    return True


def _is_fresh_allowed_pending_job(job: dict) -> bool:
    if _is_synthetic_job(job):
        return False
    if job.get("status") != "PENDING_APPROVAL":
        return False
    if not _source_allowed(job):
        return False
    if not _is_practice_job(job) and not _source_url(job):
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
    except urllib.error.HTTPError as e:
        # Telegram's response body contains the actionable reason (for example,
        # message-too-long), but never log credentials or the full message.
        try:
            reason = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            reason = "unavailable"
        if method == "editMessageText" and e.code == 400:
            try:
                description = str(json.loads(reason).get("description", "")).lower()
            except (ValueError, AttributeError):
                description = ""
            if "message is not modified" in description:
                return True
        logger.error(
            "TELEGRAM_%s_ERROR: HTTP %s reason=%s payload_chars=%s",
            method.upper(), e.code, reason, len(str(payload.get("text", ""))),
        )
        return False
    except Exception as e:
        logger.error("TELEGRAM_%s_ERROR: %s", method.upper(), str(e))
        return False


def _split_telegram_text(text: str, limit: int = TELEGRAM_SAFE_MESSAGE_CHARS) -> list[str]:
    """Split long Telegram messages without breaking line-oriented job records."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        if current and len(current) + len(line) > limit:
            chunks.append(current.rstrip())
            current = ""
        while len(line) > limit:
            if current:
                chunks.append(current.rstrip())
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        current += line
    if current:
        chunks.append(current.rstrip())
    return chunks or [text[:limit]]


def _send_telegram(
    bot_token: str,
    chat_id: str,
    text: str,
    parse_mode: str = None,
    reply_markup: dict | None = None,
) -> bool:
    """Send a Telegram message. Returns True on success."""
    chunks = _split_telegram_text(text)
    for index, chunk in enumerate(chunks):
        payload = {"chat_id": chat_id, "text": chunk}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        # Keep the keyboard on the final message so the user has one clear
        # action surface after a multi-part List or Status response.
        if reply_markup and index == len(chunks) - 1:
            payload["reply_markup"] = reply_markup
        if not _telegram_request(bot_token, "sendMessage", payload):
            if _ACTIVE_CALLBACK_CLAIM.get():
                _CALLBACK_DELIVERY_FAILED.set(True)
            logger.error(
                "TELEGRAM_MESSAGE_DELIVERY_FAILED part=%s/%s chars=%s",
                index + 1, len(chunks), len(chunk),
            )
            return False
    return True


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
    delivered = _telegram_request(bot_token, "editMessageText", payload)
    if not delivered and _ACTIVE_CALLBACK_CLAIM.get():
        _CALLBACK_DELIVERY_FAILED.set(True)
    return delivered


def _answer_callback_query(bot_token: str, callback_query_id: str, text: str | None = None) -> bool:
    """Acknowledge a Telegram callback query to clear button loading state."""
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    return _telegram_request(bot_token, "answerCallbackQuery", payload)


def _build_state_keyboard(state: str) -> dict:
    """Reply keyboard tuned to the current Telegram state."""
    state_key = state if state in {"home", "review", "followup", "more", "empty_pending"} else "home"
    layouts = {
        "home": [
            [{"text": "Next Match"}, {"text": "More"}],
            [{"text": "Search"}, {"text": "List"}, {"text": "Follow-up"}],
        ],
        "review": [
            [{"text": "Why"}, {"text": "Draft"}, {"text": "Apply"}, {"text": "Skip"}],
            [{"text": "Next Match"}],
            [{"text": "List"}, {"text": "More"}, {"text": "Home"}],
        ],
        "followup": [
            [{"text": "Followups"}, {"text": "Applied"}, {"text": "Not Applied"}],
            [{"text": "List"}, {"text": "Next Match"}, {"text": "More"}],
            [{"text": "Home"}],
        ],
        "more": [
            [{"text": "Status"}, {"text": "Backlog"}, {"text": "Followups"}],
            [{"text": "Signal Test"}],
            [{"text": "Search"}, {"text": "List"}, {"text": "Next Match"}],
            [{"text": "Home"}],
        ],
        "empty_pending": [
            [{"text": "Backlog"}, {"text": "Search"}],
            [{"text": "List"}, {"text": "Next Match"}],
            [{"text": "Home"}],
        ],
    }
    return {
        "keyboard": layouts[state_key],
        "resize_keyboard": True,
        "input_field_placeholder": "Choose an action or type a command",
    }


def _help_text(page: str) -> str:
    """Telegram-friendly product copy for the main state pages."""
    page_key = page if page in {"home", "review", "followup", "more"} else "home"
    pages = {
        "home": [
            "JobAlert",
            "AI Job Copilot",
            "",
            "Fresh AI-matched roles ready for review.",
            "",
            "Start with Next Match.",
            "Use Search for a new scan.",
            "Use List to view ready matches.",
            "Use Follow-up for jobs you acted on.",
            "Use More for Status, Backlog, and Signal Test.",
        ],
        "review": [
            "Review",
            "",
            "Use one match at a time.",
            "",
            "Next Match  Move forward",
            "Why  Full fit explanation",
            "Draft  Outreach preview",
            "Apply  Manual follow-up",
            "Skip  Remove it and tell me why",
        ],
        "followup": [
            "Follow-up",
            "",
            "Use this area for jobs you decided to act on in the real world.",
            "",
            "Followups  See waiting outcomes",
            "Applied  Record success",
            "Not Applied  Record why you skipped it",
        ],
        "more": [
            "More",
            "",
            "Advanced controls live here.",
            "",
            "Status  Queue counts and evidence",
            "Backlog  Older processed jobs behind the freshness rules",
            "Signal Test  Synthetic live alert",
            "Followups  Open follow-up tracking",
        ],
    }
    return "\n".join(pages[page_key])


def _help_page(page: str) -> tuple[str, dict]:
    """Build a concise help page with inline navigation."""
    page_key = page if page in {"home", "review", "followup", "more"} else "home"

    pages = {
        "home": [
            "JobAlert",
            "Home",
            "",
            "Best path:",
            "search jobs -> list -> next -> why -> draft -> skip <reason> or apply",
            "",
            "Use next to review one fresh match at a time.",
        ],
        "review": [
            "JobAlert",
            "Home > Review",
            "",
            "Review commands",
            "- next  Move through fresh review items",
            "- why  Explain the current match",
            "- draft  Preview outreach for the current match",
            "- skip <reason>  Remove the current match and teach why",
            "- apply  Show source and contact for manual follow-up",
            "- interested 1 / meh 1 <reason> / not interested 1 <reason>  Numbered feedback stays available",
        ],
        "followup": [
            "JobAlert",
            "Home > Follow-up",
            "",
            "Follow-up commands",
            "- apply  Show source and contact for the current review item",
            "- followups  Roles waiting for your real outcome",
            "- applied  Mark the latest manual follow-up as applied",
            "- not applied <reason>  Mark the latest manual follow-up as skipped",
        ],
        "more": [
            "JobAlert",
            "Home > More",
            "",
            "Advanced commands",
            f"- status  Check queue counts and source evidence at {SCORE_THRESHOLD}%+",
            "- followups  See roles waiting on your real outcome",
            "- send 1  Prepare email send confirmation",
            "- confirm send 1  Type this manually after review",
            "- signal test  Run one synthetic live alert",
            "",
            "Use these only when you need deeper control.",
        ],
    }

    action_rows = {
        "home": [
            [{"text": "Search", "callback_data": "cmd:search jobs"}, {"text": "List", "callback_data": "cmd:list"}],
            [{"text": "Next", "callback_data": "cmd:next"}, {"text": "Why", "callback_data": "cmd:why"}],
            [{"text": "Draft", "callback_data": "cmd:draft"}, {"text": "Apply", "callback_data": "cmd:apply"}],
        ],
        "review": [
            [{"text": "Next", "callback_data": "cmd:next"}, {"text": "Why", "callback_data": "cmd:why"}],
            [{"text": "Draft", "callback_data": "cmd:draft"}, {"text": "Apply", "callback_data": "cmd:apply"}],
            [{"text": "Interested 1", "callback_data": "cmd:interested 1"}],
        ],
        "followup": [
            [{"text": "Apply", "callback_data": "cmd:apply"}, {"text": "Followups", "callback_data": "cmd:followups"}],
            [{"text": "Applied", "callback_data": "cmd:applied"}],
        ],
        "more": [
            [{"text": "Status", "callback_data": "cmd:status"}, {"text": "Followups", "callback_data": "cmd:followups"}],
            [{"text": "Signal Test", "callback_data": "cmd:signal test"}],
        ],
    }

    if ALLOW_TELEGRAM_EMAIL_SEND:
        action_rows["more"].insert(1, [{"text": "Send 1", "callback_data": "cmd:send 1"}])

    if page_key == "home":
        nav_rows = [
            [
                {"text": "Review", "callback_data": "help:review"},
                {"text": "Follow-up", "callback_data": "help:followup"},
                {"text": "More", "callback_data": "help:more"},
            ]
        ]
    else:
        nav_rows = [
            [
                {"text": "Home", "callback_data": "help:home"},
                {"text": "Review", "callback_data": "help:review"},
                {"text": "Follow-up", "callback_data": "help:followup"},
                {"text": "More", "callback_data": "help:more"},
            ]
        ]

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


def _hidden_pending_reason(job: dict) -> str:
    """Human-readable reason a pending job is hidden from the active review queue."""
    if _is_synthetic_job(job):
        return "internal test or practice row"
    if job.get("status") != "PENDING_APPROVAL":
        return "not pending review"
    if not _source_allowed(job):
        return "source is not allowed in the review queue"
    if not _is_practice_job(job) and not _source_url(job):
        return "missing or invalid canonical source link"
    age = _job_age_days(job)
    if age is None:
        return "missing posted date"
    max_age = _source_max_age_days(job)
    if age > max_age:
        return f"older than the {max_age}-day freshness rule"
    if _as_int(job.get("match_score")) < SCORE_THRESHOLD:
        return f"below the {SCORE_THRESHOLD}% review threshold"
    return "hidden by review rules"


def _get_hidden_pending_jobs(limit: int = 10) -> list:
    """Fetch pending jobs that exist in the pipeline but are not reviewable right now."""
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

        hidden = [job for job in items if not _is_synthetic_job(job) and not _is_reviewable_job(job)]
        return _sort_reviewable_jobs(hidden)[:limit]
    except Exception as e:
        logger.error("QUERY_HIDDEN_PENDING_ERROR: %s", str(e))
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

        visible = [job for job in items if not _is_synthetic_job(job)]
        return sorted(
            visible,
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


def _review_session_key(chat_id: str) -> str:
    return f"{REVIEW_SESSION_PREFIX}{chat_id}"


def _get_active_review_job_id(chat_id: str) -> str:
    try:
        item = table.get_item(Key={"job_id": _review_session_key(chat_id)}).get("Item", {})
        return str(item.get("active_review_job_id") or "").strip()
    except Exception as e:
        logger.warning("GET_REVIEW_SESSION_ERROR: %s", str(e))
        return ""


def _set_active_review_job(chat_id: str, job_id: str) -> None:
    table.put_item(Item={
        "job_id": _review_session_key(chat_id),
        "entity_type": "telegram_review_session",
        "telegram_chat_id": str(chat_id),
        "active_review_job_id": job_id,
        "updated_at": int(time.time()),
    })


def _clear_active_review_job(chat_id: str) -> None:
    try:
        table.delete_item(Key={"job_id": _review_session_key(chat_id)})
    except Exception as e:
        logger.warning("CLEAR_REVIEW_SESSION_ERROR: %s", str(e))


def _job_index_by_id(jobs: list[dict], job_id: str) -> int | None:
    for idx, job in enumerate(jobs):
        if job.get("job_id") == job_id:
            return idx
    return None


def _sanitize_event_for_logging(event: Any) -> Any:
    """Recursively scrub secrets and sensitive authentication headers from event dict before logging."""
    if not isinstance(event, dict):
        return event
    sanitized = {}
    sensitive_substrings = ("token", "secret", "authorization", "auth", "key", "password", "signature")
    for k, v in event.items():
        k_lower = str(k).lower()
        if any(sens in k_lower for sens in sensitive_substrings):
            sanitized[k] = "[REDACTED]"
        elif isinstance(v, dict):
            sanitized[k] = _sanitize_event_for_logging(v)
        elif isinstance(v, list):
            sanitized[k] = [
                _sanitize_event_for_logging(item) if isinstance(item, dict) else item
                for item in v
            ]
        else:
            sanitized[k] = v
    return sanitized


def _resolve_review_job(chat_id: str, jobs: list[dict], index: int | None = None) -> tuple[dict | None, int | None]:
    """Resolve an explicit index or the chat's active review job."""
    if not jobs:
        return None, None

    if index is not None:
        if index < 1 or index > len(jobs):
            return None, None
        job = jobs[index - 1]
        _set_active_review_job(chat_id, job["job_id"])
        return job, index

    active_job_id = _get_active_review_job_id(chat_id)
    if active_job_id:
        active_idx = _job_index_by_id(jobs, active_job_id)
        if active_idx is not None:
            return jobs[active_idx], active_idx + 1
        # Active job was set, but it is no longer pending in jobs (already applied/rejected).
        # Fetch it from table so caller can see its processed status, but DO NOT advance to jobs[0]!
        processed_job = table.get_item(Key={"job_id": active_job_id}).get("Item")
        if processed_job:
            return processed_job, None
        return None, None

    # Initial session: no active job has ever been viewed or acted on; initialize to jobs[0]
    job = jobs[0]
    _set_active_review_job(chat_id, job["job_id"])
    return job, 1


def _advance_review_job(chat_id: str, jobs: list[dict]) -> tuple[dict | None, int | None, bool]:
    """Advance to the next reviewable job for this chat."""
    if not jobs:
        _clear_active_review_job(chat_id)
        return None, None, False

    active_job_id = _get_active_review_job_id(chat_id)
    active_idx = _job_index_by_id(jobs, active_job_id) if active_job_id else None
    if active_idx is None:
        job = jobs[0]
        _set_active_review_job(chat_id, job["job_id"])
        return job, 1, True
    if active_idx + 1 >= len(jobs):
        return jobs[active_idx], active_idx + 1, False

    next_idx = active_idx + 1
    job = jobs[next_idx]
    _set_active_review_job(chat_id, job["job_id"])
    return job, next_idx + 1, True


def _refresh_active_review_job(chat_id: str) -> None:
    jobs = _get_pending_jobs(limit=20)
    if jobs:
        _set_active_review_job(chat_id, jobs[0]["job_id"])
    else:
        _clear_active_review_job(chat_id)


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
            "ProjectionExpression": "job_id, #s, entity_type, #src, signal_test, model_id",
            "ExpressionAttributeNames": {"#s": "status", "#src": "source"},
        }
        while True:
            response = table.scan(**scan_args)
            for item in response.get("Items", []):
                if _is_synthetic_job(item):
                    continue
                status = item.get("status", "UNKNOWN")
                stats[status] = stats.get(status, 0) + 1
            if "LastEvaluatedKey" not in response:
                break
            scan_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        return stats
    except Exception as e:
        logger.error("SCAN_STATS_ERROR: %s", str(e))
        return stats


def _update_job_status(job_id: str, new_status: str, expected_status: str | None = None) -> bool:
    try:
        kwargs = {
            "Key": {"job_id": job_id},
            "UpdateExpression": "SET #s = :s, status_updated_at = :t",
            "ExpressionAttributeNames": {"#s": "status"},
            "ExpressionAttributeValues": {":s": new_status, ":t": int(time.time())},
        }
        if expected_status:
            kwargs["ConditionExpression"] = "attribute_exists(job_id) AND #s = :expected"
            kwargs["ExpressionAttributeValues"][":expected"] = expected_status
        table.update_item(**kwargs)
        return True
    except Exception as e:
        if "ConditionalCheckFailed" in type(e).__name__ or "ConditionalCheckFailed" in str(e):
            logger.warning("CONDITIONAL_CHECK_FAILED | job_id=%s | expected=%s", job_id, expected_status)
            return False
        logger.error("UPDATE_STATUS_ERROR | job_id=%s | error=%s", job_id, str(e))
        raise


def _record_delivery_status(job_id: str, success: bool, error: str = "") -> None:
    """Track Telegram delivery outcome separately on the DynamoDB job row."""
    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET delivery_status = :ds, delivery_attempted_at = :dt, delivery_error = :de",
            ExpressionAttributeValues={
                ":ds": "DELIVERED" if success else "DELIVERY_FAILED",
                ":dt": int(time.time()),
                ":de": str(error)[:200] if not success else "",
            },
        )
    except Exception as e:
        logger.warning("FAILED_TO_RECORD_DELIVERY_STATUS | job_id=%s | %s", job_id, str(e))


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


def _record_review_feedback(job_id: str, sentiment: str, reason: str = "") -> str | None:
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
        ":expected": "PENDING_APPROVAL",
    }
    if reason:
        update_expr += ", user_feedback_reason = :reason"
        expr_vals[":reason"] = reason[:500]

    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=expr_vals,
            ConditionExpression="attribute_exists(job_id) AND #s = :expected",
        )
        return new_status
    except Exception as e:
        if "ConditionalCheckFailed" in type(e).__name__ or "ConditionalCheckFailed" in str(e):
            logger.warning("CONDITIONAL_CHECK_FAILED | job_id=%s | review_feedback", job_id)
            return None
        logger.error("UPDATE_REVIEW_FEEDBACK_ERROR | job_id=%s | error=%s", job_id, str(e))
        raise


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
        "job_id": f"practice_agent_{now.strftime('%Y%m%d')}",
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
        "source": "showcase",
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
    """Hide prior showcase rows so the review queue stays clean."""
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
                    and source in {"showcase", "practice"}
                ):
                    _update_job_status(item["job_id"], "REJECTED")
            if "LastEvaluatedKey" not in response:
                break
            scan_args["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except Exception as e:
        logger.warning("SHOWCASE_CLEANUP_ERROR: %s", str(e))


def _format_job_explanation(job: dict, index: int) -> str:
    posted = _format_posted(job.get("posted_at", "")) or "Posted date unavailable"

    match_reasons = job.get("match_reasons") or []
    missing_requirements = job.get("missing_requirements") or []
    stored_reason = (job.get("match_reason") or "").strip()

    explanation_parts = []
    if match_reasons:
        for r in match_reasons[:4]:
            explanation_parts.append(f"• {r}")
        if missing_requirements:
            for m in missing_requirements[:3]:
                explanation_parts.append(f"• Note: {m}")
    elif stored_reason:
        bullets = _reason_bullets(stored_reason)
        for b in bullets:
            explanation_parts.append(f"• {b}")
    else:
        title = job.get("title", "AI role")
        company = job.get("company", "the company")
        score = job.get("match_score", SCORE_THRESHOLD)
        explanation_parts.append(
            f"• Scored {score}% against candidate profile (RAG, LLMs, Python, AWS) based on requirements for {title} at {company}."
        )

    reason_text = "\n".join(explanation_parts)

    return (
        f"Match #{index}\n\n"
        f"{job.get('title', 'Unknown')} at {job.get('company', 'Unknown')}\n"
        f"Score: {job.get('match_score', '?')}%\n"
        f"Location: {job.get('location', 'Unknown')}\n"
        f"Work mode: {'⚠️ ' if _is_work_mode_uncertain(job) else ''}{_work_mode_label(job)} ({_work_mode_evidence(job)})\n"
        f"Source: {_source_label(job)}\n"
        f"Source link: {_source_url(job) or 'Unavailable'}\n"
        f"{posted}\n\n"
        f"Why it matched:\n{reason_text}\n\n"
        "Next: draft, apply, skip <reason>, or next"
    )


def _reason_bullets(reason: str, limit: int = 3) -> list[str]:
    """Turn a stored match reason into short Telegram-friendly bullets."""
    cleaned = (reason or "").strip()
    if not cleaned:
        return ["Matched core AI engineering and LLM orchestration profile."]

    parts = [
        part.strip(" -\t\r\n.")
        for part in re.split(r"(?:\s*[•;\n]\s*|\.\s+)", cleaned)
        if part and part.strip(" -\t\r\n.")
    ]
    if not parts:
        return [cleaned]
    return parts[:limit]


def _build_review_card_keyboard(job_id: str | None = None) -> dict:
    """Focused inline keyboard for reviewing one match at a time, bound to immutable compact token."""
    token = _get_or_create_action_token(job_id) if job_id else ""
    suffix = f":{token}" if token else ""
    return {
        "inline_keyboard": [
            [
                {"text": "Why", "callback_data": f"cmd:why{suffix}"},
                {"text": "Draft", "callback_data": f"cmd:draft{suffix}"},
                {"text": "✓ Apply", "callback_data": f"cmd:apply{suffix}"},
                {"text": "Skip", "callback_data": f"cmd:skip{suffix}"},
            ],
            [
                {"text": "→ Next Match", "callback_data": f"cmd:next{suffix}"},
            ],
        ],
    }


def _format_next_review_card(job: dict) -> str:
    """Compact card-style job view for the daily review flow."""
    title = job.get("title", "Unknown role")
    company = job.get("company", "Unknown company")
    location = job.get("location", "Location not specified")
    score = job.get("match_score", "?")
    posted = _format_posted(job.get("posted_at", "")) or "Posted date unavailable"
    source = _source_label(job)
    work_mode = _work_mode_label(job)

    match_reasons = job.get("match_reasons") or []
    if match_reasons:
        bullets = [str(r).strip() for r in match_reasons[:3] if str(r).strip()]
    else:
        bullets = _reason_bullets(job.get("match_reason", ""))
    if not bullets:
        bullets = ["Matched core AI engineering and LLM orchestration profile."]

    work_mode_display = f"⚠️ {work_mode}" if _is_work_mode_uncertain(job) else work_mode
    lines = [
        f"{title}  [{score}% match]",
        f"{company} · {location}",
        f"{work_mode_display} · {source}",
        "",
        "Why you fit",
    ]
    lines.extend(f"• {bullet}" for bullet in bullets)
    lines.extend([
        "",
        f"{posted} · {source}",
    ])
    return "\n".join(lines)


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
            "Practice workspace is ready.\n\n"
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
                "Use backlog to inspect the older jobs archive, or search jobs to scan live sources again.",
                reply_markup=_build_state_keyboard("empty_pending"),
            )
            return
        empty_msg = (
            "Your review queue is empty.\n\n"
            "Use practice to explore the workflow, or search jobs to scan live sources."
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
        is_practice = job.get("source") in {"showcase", "practice"}

        lines.append(f"{i}. {title}")
        lines.append(f"   {company} | {location}")
        lines.append(f"   Match: {score}%")
        wm = _work_mode_label(job)
        lines.append(f"   Work mode: {'⚠️ ' if _is_work_mode_uncertain(job) else ''}{wm}")
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


def _handle_backlog(bot_token: str, chat_id: str) -> None:
    """Show hidden pending jobs when the user wants to inspect the broader pipeline backlog."""
    jobs = _get_hidden_pending_jobs(limit=10)
    if not jobs:
        _send_telegram(
            bot_token,
            chat_id,
            "No older processed jobs are sitting behind the review rules.\n\n"
            "Use list for ready matches or search jobs for a fresh scan.",
            reply_markup=_build_state_keyboard("more"),
        )
        return

    lines = [f"Older jobs archive: {len(jobs)} processed job(s)\n"]
    for i, job in enumerate(jobs, 1):
        source_url = _source_url(job)
        lines.append(f"{i}. {job.get('title', 'Unknown Role')}")
        lines.append(f"   {job.get('company', 'Unknown')} | {job.get('location', 'Location not specified')}")
        lines.append(f"   Match: {job.get('match_score', '?')}%")
        lines.append(f"   Hidden because: {_hidden_pending_reason(job)}")
        lines.append(f"   Source: {_source_label(job)}")
        if source_url:
            lines.append(f"   Source link: {source_url}")
        posted = _format_posted(job.get("posted_at", ""))
        if posted:
            lines.append(f"   {posted}")
        lines.append("")

    lines.append("These jobs are already processed, but they stay out of Next Match because they do not pass the current review rules.")
    _send_telegram(bot_token, chat_id, "\n".join(lines), reply_markup=_build_state_keyboard("more"))


def _handle_apply(
    bot_token: str,
    chat_id: str,
    index: int | None = None,
    target_job_id: str | None = None,
    callback_query_id: str | None = None,
) -> None:
    """Move a reviewable job to manual follow-up. Telegram never sends email here."""
    if target_job_id:
        job = table.get_item(Key={"job_id": target_job_id}).get("Item")
        if not job:
            _send_telegram(bot_token, chat_id, "Job not found. Use 'list' to view pending matches.")
            return
        resolved_index = None
    elif index is not None:
        jobs = _get_pending_jobs(limit=20)
        if not jobs:
            empty_msg = (
                "Your review queue is empty.\n\nUse practice to explore the workflow, or search jobs to scan live sources."
                if ENABLE_PRACTICE_MODE
                else
                "Your review queue is empty.\n\nUse search jobs to scan live sources, then list to review real matches."
            )
            _send_telegram(
                bot_token,
                chat_id,
                empty_msg,
                reply_markup=_build_state_keyboard("home"),
            )
            return

        if index < 1 or index > len(jobs):
            _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
            return

        job = jobs[index - 1]
        resolved_index = index
        _set_active_review_job(chat_id, job["job_id"])
    else:
        active_job_id = _get_active_review_job_id(chat_id)
        if active_job_id:
            active_job = table.get_item(Key={"job_id": active_job_id}).get("Item")
            if active_job:
                status = active_job.get("status")
                if status == "MANUAL_APPLY":
                    job = active_job
                    resolved_index = None
                elif status == "PENDING_APPROVAL":
                    job = active_job
                    jobs = _get_pending_jobs(limit=20)
                    idx = _job_index_by_id(jobs, active_job_id)
                    resolved_index = (idx + 1) if idx is not None else 1
                else:
                    _send_telegram(
                        bot_token,
                        chat_id,
                        "The active match was already processed. Use 'next' to review the next match, or 'list' to view pending matches.",
                    )
                    return
            else:
                jobs = _get_pending_jobs(limit=20)
                if not jobs:
                    empty_msg = (
                        "Your review queue is empty.\n\nUse practice to explore the workflow, or search jobs to scan live sources."
                        if ENABLE_PRACTICE_MODE
                        else
                        "Your review queue is empty.\n\nUse search jobs to scan live sources, then list to review real matches."
                    )
                    _send_telegram(
                        bot_token,
                        chat_id,
                        empty_msg,
                        reply_markup=_build_state_keyboard("home"),
                    )
                    return
                job = jobs[0]
                resolved_index = 1
                _set_active_review_job(chat_id, job["job_id"])
        else:
            jobs = _get_pending_jobs(limit=20)
            if not jobs:
                empty_msg = (
                    "Your review queue is empty.\n\nUse practice to explore the workflow, or search jobs to scan live sources."
                    if ENABLE_PRACTICE_MODE
                    else
                    "Your review queue is empty.\n\nUse search jobs to scan live sources, then list to review real matches."
                )
                _send_telegram(
                    bot_token,
                    chat_id,
                    empty_msg,
                    reply_markup=_build_state_keyboard("home"),
                )
                return
            job = jobs[0]
            resolved_index = 1
            _set_active_review_job(chat_id, job["job_id"])

    job_id = job["job_id"]
    recruiter = (job.get("recruiter_email") or "").strip()
    source_url = _source_url(job)
    is_practice = job.get("source") in {"showcase", "practice"}

    if not is_practice and not source_url:
        item_ref = f"match #{resolved_index}" if resolved_index else job.get("company", "this role")
        _send_telegram(
            bot_token,
            chat_id,
            f"Blocked: {item_ref} ({job.get('company', 'Unknown')}) has no verifiable source link.",
        )
        return

    current = table.get_item(Key={"job_id": job_id}).get("Item", {})
    status = current.get("status")

    if status == "MANUAL_APPLY":
        # Idempotent re-delivery: re-send follow-up details without altering state or affecting other jobs
        contact_line = f"Contact: {recruiter}" if recruiter else "Contact: no recruiter email found"
        msg = (
            f"Manual follow-up already selected (re-delivered):\n\n"
            f"Role: {job.get('title', 'Unknown')}\n"
            f"Company: {job.get('company', 'Unknown')}\n"
            f"Match: {job.get('match_score', '?')}%\n"
            f"Work mode: {'⚠️ ' if _is_work_mode_uncertain(job) else ''}{_work_mode_label(job)}\n"
            f"Work mode evidence: {_work_mode_evidence(job)}\n"
            f"Source: {_source_label(job)}\n"
            f"Source link: {source_url or 'No source link stored'}\n"
            f"{contact_line}\n\n"
            "No email was sent from Telegram. Review the source and send manually if it is a real fit.\n\n"
            "After you act, reply applied. If you decide not to apply, reply not applied <reason>."
        )
        delivery_ok = _send_telegram(bot_token, chat_id, msg)
        _record_delivery_status(job_id, delivery_ok, "" if delivery_ok else "telegram send failed")
        if callback_query_id:
            if delivery_ok:
                _record_callback_state(callback_query_id, "COMPLETED", chat_id=chat_id, target_job_id=job_id)
            else:
                _record_callback_state(callback_query_id, "FAILED", chat_id=chat_id, target_job_id=job_id)
        return

    if status != "PENDING_APPROVAL":
        _send_telegram(
            bot_token,
            chat_id,
            f"Job already {status or 'processed'}. Old cards do not affect current matches. Use 'list' to refresh.",
        )
        return

    if not is_practice and not _is_reviewable_job(job):
        reason_msg = _hidden_pending_reason(job)
        _send_telegram(
            bot_token,
            chat_id,
            f"Blocked: this role is not reviewable ({reason_msg}).",
        )
        return

    if is_practice:
        updated = _update_job_status(job_id, "SHOWCASE_COMPLETE", expected_status="PENDING_APPROVAL")
        if not updated:
            _send_telegram(bot_token, chat_id, "Practice job was already updated.")
            return
        if _get_active_review_job_id(chat_id) == job_id:
            _clear_active_review_job(chat_id)
        delivery_ok = _send_telegram(
            bot_token,
            chat_id,
            f"Practice workflow completed.\n\n"
            f"Role: {job.get('title', 'Unknown')}\n"
            f"Company: {job.get('company', 'Unknown')}\n"
            f"Match: {job.get('match_score', '?')}%\n\n"
            "No real application was submitted and no email was sent.\n"
            "Use practice anytime to reset the practice opportunity.",
        )
        if callback_query_id:
            _record_callback_state(callback_query_id, "COMPLETED" if delivery_ok else "FAILED", chat_id=chat_id, target_job_id=job_id)
        return

    updated = _update_job_status(job_id, "MANUAL_APPLY", expected_status="PENDING_APPROVAL")
    if not updated:
        _send_telegram(
            bot_token,
            chat_id,
            "Job was updated concurrently or already processed. Use 'list' to refresh.",
        )
        return

    contact_line = f"Contact: {recruiter}" if recruiter else "Contact: no recruiter email found"
    msg = (
        f"Manual follow-up selected.\n\n"
        f"Role: {job.get('title', 'Unknown')}\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Match: {job.get('match_score', '?')}%\n"
        f"Work mode: {'⚠️ ' if _is_work_mode_uncertain(job) else ''}{_work_mode_label(job)}\n"
        f"Work mode evidence: {_work_mode_evidence(job)}\n"
        f"Source: {_source_label(job)}\n"
        f"Source link: {source_url or 'No source link stored'}\n"
        f"{contact_line}\n\n"
        "No email was sent from Telegram. Review the source and send manually if it is a real fit.\n"
        "I moved it out of the active queue so it will not keep reappearing in list.\n\n"
        "After you act, reply applied. If you decide not to apply, reply not applied <reason>."
    )
    delivery_ok = _send_telegram(bot_token, chat_id, msg)
    _record_delivery_status(job_id, delivery_ok, "" if delivery_ok else "telegram send failed")
    if callback_query_id:
        if delivery_ok:
            _record_callback_state(callback_query_id, "COMPLETED", chat_id=chat_id, target_job_id=job_id)
        else:
            logger.error("TELEGRAM_DELIVERY_FAILED | job_id=%s", job_id)
            _record_callback_state(callback_query_id, "FAILED", chat_id=chat_id, target_job_id=job_id)


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
    index: int | None = None,
    reason: str = "",
) -> None:
    """Record whether the user is interested in a reviewable job."""
    jobs = _get_pending_jobs(limit=20)
    if not jobs:
        _send_telegram(bot_token, chat_id, "Your review queue is empty. Use search jobs to scan live sources.")
        return

    if index is not None and (index < 1 or index > len(jobs)):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    job, resolved_index = _resolve_review_job(chat_id, jobs, index)
    if not job or resolved_index is None:
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return
    status = _record_review_feedback(job["job_id"], sentiment, reason)
    if not status:
        _send_telegram(
            bot_token,
            chat_id,
            "Job was updated concurrently or already processed. Use 'list' to refresh.",
        )
        return
    if sentiment == "not_interested":
        _set_active_review_job(chat_id, job["job_id"])
    label = {
        "interested": "interested",
        "not_interested": "not interested",
        "neutral": "neutral",
    }.get(sentiment, sentiment)
    next_step = (
        "Next: use draft, apply, next, or skip <reason>."
        if sentiment != "not_interested"
        else "I removed it from the active review queue. Use next to continue."
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


def _handle_explain(
    bot_token: str,
    chat_id: str,
    index: int | None = None,
    target_job_id: str | None = None,
) -> None:
    """Explain why a job is in the review queue."""
    if target_job_id:
        job = table.get_item(Key={"job_id": target_job_id}).get("Item")
        if not job:
            _send_telegram(bot_token, chat_id, "Job not found. Use 'list' to view pending matches.")
            return
        if not _is_practice_job(job) and not _is_reviewable_job(job):
            reason_msg = _hidden_pending_reason(job)
            _send_telegram(bot_token, chat_id, f"Blocked: this role is not reviewable ({reason_msg}).")
            return
        resolved_index = 1
    else:
        jobs = _get_pending_jobs(limit=20)
        if not jobs:
            msg = (
                "Your review queue is empty. Use practice or search jobs to add matches."
                if ENABLE_PRACTICE_MODE
                else
                "Your review queue is empty. Use search jobs to scan live sources."
            )
            _send_telegram(bot_token, chat_id, msg)
            return
        if index is not None and (index < 1 or index > len(jobs)):
            _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
            return

        job, resolved_index = _resolve_review_job(chat_id, jobs, index)
        if not job or resolved_index is None:
            _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
            return

    source_url = _source_url(job)
    if not _is_practice_job(job) and not source_url:
        item_ref = f"match #{resolved_index}" if resolved_index else job.get("company", "this role")
        _send_telegram(
            bot_token,
            chat_id,
            f"Blocked: {item_ref} ({job.get('company', 'Unknown')}) has no verifiable source link.",
        )
        return

    _send_telegram(
        bot_token,
        chat_id,
        _format_job_explanation(job, resolved_index),
        reply_markup=_build_review_card_keyboard(job.get("job_id")),
    )


def _handle_skip(
    bot_token: str,
    chat_id: str,
    target_job_id: str | None = None,
    reason: str = "Skipped from review button",
    callback_query_id: str | None = None,
) -> None:
    """Skip a reviewable job and record feedback."""
    if target_job_id:
        job = table.get_item(Key={"job_id": target_job_id}).get("Item")
        if not job:
            _send_telegram(bot_token, chat_id, "Job not found. Use 'list' to refresh.")
            return
        if job.get("status") != "PENDING_APPROVAL":
            _send_telegram(
                bot_token,
                chat_id,
                f"Job already {job.get('status', 'processed')}. Old cards do not affect current matches. Use 'list' to refresh.",
            )
            return
        if not _is_practice_job(job) and not _is_reviewable_job(job):
            reason_msg = _hidden_pending_reason(job)
            _send_telegram(bot_token, chat_id, f"Blocked: this role is not reviewable ({reason_msg}).")
            return
        status = _record_review_feedback(target_job_id, "not_interested", reason)
        if not status:
            _send_telegram(
                bot_token,
                chat_id,
                "Job was updated concurrently or already processed. Use 'list' to refresh.",
            )
            return

        _send_telegram(
            bot_token,
            chat_id,
            f"Recorded: not interested.\n\n"
            f"Role: {job.get('title', 'Unknown')}\n"
            f"Company: {job.get('company', 'Unknown')}\n"
            f"Stored status: {status}\nReason: {reason}\n\n"
            "I removed it from the active review queue. Use next to continue.",
        )
        if callback_query_id:
            _record_callback_state(callback_query_id, "COMPLETED", chat_id=chat_id, target_job_id=target_job_id)
        return
    _handle_review_feedback(bot_token, chat_id, "not_interested", None, reason)
    if callback_query_id:
        active_id = _get_active_review_job_id(chat_id)
        _record_callback_state(callback_query_id, "COMPLETED", chat_id=chat_id, target_job_id=active_id)


def _handle_next(bot_token: str, chat_id: str) -> None:
    """Advance the review session to the next reviewable job."""
    jobs = _get_pending_jobs(limit=20)
    if not jobs:
        msg = (
            "Your review queue is empty. Use practice or search jobs to add matches."
            if ENABLE_PRACTICE_MODE
            else
            "No fresh reviewable match is ready.\n\nUse backlog to inspect the older jobs archive, or search jobs for a fresh scan."
        )
        _send_telegram(bot_token, chat_id, msg, reply_markup=_build_state_keyboard("empty_pending"))
        return

    job, resolved_index, advanced = _advance_review_job(chat_id, jobs)
    if not job or resolved_index is None:
        _send_telegram(
            bot_token,
            chat_id,
            "No fresh reviewable match is ready.\n\nUse backlog to inspect the older jobs archive, or search jobs for a fresh scan.",
            reply_markup=_build_state_keyboard("empty_pending"),
        )
        return
    if not advanced:
        _send_telegram(
            bot_token,
            chat_id,
            _format_next_review_card(job) + "\n\nYou are already on the last reviewable match.",
            reply_markup=_build_review_card_keyboard(job.get("job_id")),
        )
        return

    _send_telegram(
        bot_token,
        chat_id,
        _format_next_review_card(job),
        reply_markup=_build_review_card_keyboard(job.get("job_id")),
    )


def _handle_draft(
    bot_token: str,
    chat_id: str,
    index: int | None = None,
    target_job_id: str | None = None,
) -> None:
    """Generate a preview-only outreach draft for a reviewable job."""
    if target_job_id:
        job = table.get_item(Key={"job_id": target_job_id}).get("Item")
        if not job:
            _send_telegram(bot_token, chat_id, "Job not found. Use 'list' to view pending matches.")
            return
        if not _is_practice_job(job) and not _is_reviewable_job(job):
            reason_msg = _hidden_pending_reason(job)
            _send_telegram(bot_token, chat_id, f"Blocked: this role is not reviewable ({reason_msg}).")
            return
        resolved_index = 1
    else:
        jobs = _get_pending_jobs(limit=20)
        if not jobs:
            msg = (
                "Your review queue is empty. Use practice or search jobs to add matches."
                if ENABLE_PRACTICE_MODE
                else
                "Your review queue is empty. Use search jobs to scan live sources."
            )
            _send_telegram(bot_token, chat_id, msg)
            return
        if index is not None and (index < 1 or index > len(jobs)):
            _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
            return

        job, resolved_index = _resolve_review_job(chat_id, jobs, index)
        if not job or resolved_index is None:
            _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
            return

    source_url = _source_url(job)
    if not _is_practice_job(job) and not source_url:
        item_ref = f"match #{resolved_index}" if resolved_index else job.get("company", "this role")
        _send_telegram(
            bot_token,
            chat_id,
            f"Blocked: {item_ref} ({job.get('company', 'Unknown')}) has no verifiable source link.",
        )
        return
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
        subject = str(email.get("subject", "")).strip()
        body = str(email.get("body", "")).strip()
        recipient = (job.get("recruiter_email") or "").strip()
        now = int(time.time())
        expiry = now + 86400  # 24h approval validity
        approval_hash = hashlib.sha256(
            f"{job['job_id']}:{recipient}:{subject}:{body}".encode("utf-8")
        ).hexdigest()

        try:
            table.update_item(
                Key={"job_id": job["job_id"]},
                UpdateExpression=(
                    "SET draft_subject = :s, draft_body = :b, draft_hash = :h, "
                    "draft_recipient = :r, draft_created_at = :t, approval_expires_at = :exp, "
                    "approval_consumed = :f"
                ),
                ExpressionAttributeValues={
                    ":s": subject,
                    ":b": body,
                    ":h": approval_hash,
                    ":r": recipient,
                    ":t": now,
                    ":exp": expiry,
                    ":f": False,
                },
            )
        except Exception as exc:
            logger.warning("FAILED_TO_PERSIST_DRAFT_APPROVAL | job_id=%s | %s", job["job_id"], str(exc))

        display_body = body
        if len(display_body) > 2500:
            display_body = display_body[:2500].rstrip() + "\n[truncated]"
        fallback_note = ""
        if payload.get("generated_with_fallback"):
            fallback_note = (
                "\n\nNote: this preview used the safe fallback template because Bedrock was temporarily throttled."
            )

        _send_telegram(
            bot_token,
            chat_id,
            f"Draft for match #{resolved_index}\n\n"
            f"Subject: {subject}\n\n"
            f"{display_body}{fallback_note}\n\n"
            f"Draft approval: {approval_hash[:8]} (valid 24h).\n\n"
            "No email was sent.\n\n"
            "Next: interested, meh <reason>, apply, skip <reason>, next, or confirm send "
            f"{resolved_index}.",
            reply_markup=_build_review_card_keyboard(job.get("job_id")),
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
    if job.get("source") in {"showcase", "practice"}:
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
    if not source_url or not is_valid_canonical_url(source_url):
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

    current_item = {}
    try:
        current_item = table.get_item(Key={"job_id": job["job_id"]}).get("Item", {})
    except Exception as e:
        logger.warning("FETCH_JOB_FOR_SEND_ERROR | %s", str(e))

    draft_hash = current_item.get("draft_hash") or job.get("draft_hash")
    draft_subject = current_item.get("draft_subject") or job.get("draft_subject")
    draft_body = current_item.get("draft_body") or job.get("draft_body")
    draft_recipient = current_item.get("draft_recipient") or job.get("draft_recipient")
    approval_expires_at = int(current_item.get("approval_expires_at") or job.get("approval_expires_at") or 0)
    approval_consumed = bool(current_item.get("approval_consumed", job.get("approval_consumed", False)))
    now = int(time.time())

    if REQUIRE_DRAFT_APPROVAL:
        if not draft_hash or not draft_body:
            _send_telegram(
                bot_token,
                chat_id,
                f"No draft approved for match #{index}.\n\n"
                f"Use draft {index} first to review and approve the exact outreach before sending.",
            )
            return
        if approval_consumed:
            _send_telegram(
                bot_token,
                chat_id,
                f"The approval for match #{index} was already consumed.\n\n"
                f"Use draft {index} to review and approve a fresh draft.",
            )
            return
        if approval_expires_at and now > approval_expires_at:
            _send_telegram(
                bot_token,
                chat_id,
                f"Draft approval for match #{index} has expired (24h limit).\n\n"
                f"Use draft {index} to generate and approve a fresh draft.",
            )
            return
        if draft_recipient and draft_recipient != recruiter:
            _send_telegram(
                bot_token,
                chat_id,
                f"Recipient email ({recruiter}) changed since draft was approved ({draft_recipient}).\n\n"
                f"Use draft {index} to generate a fresh draft.",
            )
            return

    if not confirmed:
        hash_note = f"\nDraft approval: {draft_hash[:8]}" if draft_hash else ""
        _send_telegram(
            bot_token,
            chat_id,
            f"Send confirmation required.\n\n"
            f"Role: {job.get('title', 'Unknown')}\n"
            f"Company: {job.get('company', 'Unknown')}\n"
            f"To: {recruiter}\n"
            f"Source link: {source_url}{hash_note}\n\n"
            f"Type exactly: confirm send {index}\n\n"
            "No email was sent.",
        )
        return

    try:
        send_payload = {
            "send_email": True,
            "send_confirmed": True,
            "job": job,
            "approved_hash": draft_hash,
            "approved_subject": draft_subject,
            "approved_body": draft_body,
            "approval_expires_at": approval_expires_at,
        }
        response = lambda_client.invoke(
            FunctionName=SENDER_LAMBDA,
            InvocationType="RequestResponse",
            Payload=_safe_json_dumps(send_payload),
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


def _handle_reject(bot_token: str, chat_id: str, index: int | None = None) -> None:
    """Reject a specific reviewable job."""
    jobs = _get_pending_jobs(limit=20)

    if not jobs:
        _send_telegram(bot_token, chat_id, "Your review queue is empty.")
        return

    if index is not None and (index < 1 or index > len(jobs)):
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return

    job, _resolved_index = _resolve_review_job(chat_id, jobs, index)
    if not job:
        _send_telegram(bot_token, chat_id, f"Invalid index. Use a number from 1 to {len(jobs)}.")
        return
    _update_job_status(job["job_id"], "REJECTED")
    _set_active_review_job(chat_id, job["job_id"])
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
        hidden_total = stats.get("PENDING_APPROVAL", 0)
        lines.extend([
            "Reviewable evidence: none",
            "",
            (
                f"{hidden_total} processed job(s) sit behind the current rules. "
                "Use backlog to inspect them."
                if hidden_total
                else
                "Use search jobs to scan live sources, then status to inspect source links before acting."
            ),
        ])
        _send_telegram(
            bot_token,
            chat_id,
            "\n".join(lines),
            reply_markup=_build_state_keyboard("empty_pending") if hidden_total else _build_state_keyboard("more"),
        )
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
        lines.append(f"   Work mode: {'⚠️ ' if _is_work_mode_uncertain(job) else ''}{_work_mode_label(job)}")
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
            lines.append(f"   Work mode: {'⚠️ ' if _is_work_mode_uncertain(job) else ''}{_work_mode_label(job)}")
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
    text = _help_text("home")
    _send_telegram(
        bot_token,
        chat_id,
        text,
        reply_markup=_build_state_keyboard("home"),
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
    daily_skip = re.match(r"^skip(?:\s+(.+))?$", raw, re.IGNORECASE)

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

    if cmd in ("practice", "practice job", "practicejob", "showcase", "showcase agent", "agent showcase", "seed showcase", "showcase mode"):
        _handle_showcase(bot_token, chat_id)
        return

    if cmd in ("list", "jobs", "pending", "show"):
        _handle_list(bot_token, chat_id)
        return

    if cmd in ("followups", "follow ups", "manual followups", "manual follow-ups", "manual"):
        _handle_followups(bot_token, chat_id)
        return

    if cmd in ("backlog", "hidden", "hidden jobs", "pending jobs", "pending backlog"):
        _handle_backlog(bot_token, chat_id)
        return

    if cmd in ("follow-up", "follow up"):
        _send_telegram(bot_token, chat_id, _help_text("followup"), reply_markup=_build_state_keyboard("followup"))
        return

    if cmd in ("status", "stats"):
        _handle_status(bot_token, chat_id)
        return

    if cmd in ("more", "advanced"):
        _send_telegram(bot_token, chat_id, _help_text("more"), reply_markup=_build_state_keyboard("more"))
        return

    if cmd in ("help", "start", "/start", "/help"):
        _handle_help(bot_token, chat_id)
        return

    if cmd in ("home", "main menu"):
        _handle_help(bot_token, chat_id)
        return

    if cmd in ("next", "next match", "next job", "show next", "review next"):
        _handle_next(bot_token, chat_id)
        return

    if cmd in ("why", "why this", "why this job"):
        _handle_explain(bot_token, chat_id, None)
        return

    if daily_skip:
        reason = (daily_skip.group(1) or "").strip()
        if not reason:
            _send_telegram(
                bot_token,
                chat_id,
                "Tell me why you want to skip it so I can learn.\n\nExample: skip too senior",
            )
            return
        _handle_review_feedback(bot_token, chat_id, "not_interested", None, reason)
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
        index = int(match.group(1)) if match.group(1) else None
        _handle_apply(bot_token, chat_id, index)
        return
    if cmd.startswith("apply"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: apply 1")
        return

    match = EXPLAIN_PATTERN.match(cmd)
    if match:
        index = int(match.group(1)) if match.group(1) else None
        _handle_explain(bot_token, chat_id, index)
        return
    if cmd.startswith("explain"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: explain 1")
        return

    match = DRAFT_PATTERN.match(cmd)
    if match:
        index = int(match.group(1)) if match.group(1) else None
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
        index = int(match.group(1)) if match.group(1) else None
        _handle_reject(bot_token, chat_id, index)
        return
    if cmd.startswith("reject"):
        _send_telegram(bot_token, chat_id, "Use a match number, for example: reject 1")
        return

    _handle_help(bot_token, chat_id)


def _handle_callback_command(bot_token: str, chat_id: str, callback_query_id: str, data: str,
                             message_id: int, pinned_target: str | None = None,
                             retrying: bool = False) -> None:
    """Handle Telegram inline button callbacks for help navigation and command shortcuts."""
    if data.startswith("help:"):
        page = data.split(":", 1)[1]
        _handle_help_callback(bot_token, chat_id, message_id, page)
        _answer_callback_query(bot_token, callback_query_id)
        return

    if data.startswith("cmd:"):
        parts = data.split(":")
        action = parts[1].strip().lower() if len(parts) > 1 else ""
        raw_target = parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
        target_job_id = pinned_target or (_resolve_action_token(raw_target) if raw_target else None)

        if CONFIRM_SEND_PATTERN.match(action):
            _answer_callback_query(bot_token, callback_query_id, "Type confirm send manually")
            _send_telegram(
                bot_token,
                chat_id,
                "For safety, confirm send must be typed manually.\n\nType exactly: confirm send 1",
            )
            return

        _answer_callback_query(bot_token, callback_query_id, f"Running {action}")

        if action == "apply":
            _handle_apply(
                bot_token,
                chat_id,
                target_job_id=target_job_id,
                callback_query_id=callback_query_id,
            )
            return
        if action == "why":
            _handle_explain(bot_token, chat_id, target_job_id=target_job_id)
            return
        if action == "draft":
            _handle_draft(bot_token, chat_id, target_job_id=target_job_id)
            return
        if action == "skip":
            _handle_skip(
                bot_token,
                chat_id,
                target_job_id=target_job_id,
                callback_query_id=callback_query_id,
            )
            return
        if action == "next":
            if retrying and target_job_id:
                current = _get_active_review_job_id(chat_id)
                state = _get_callback_state(callback_query_id)
                if current and (current != target_job_id or (state and state.get("initial_review_unset"))):
                    # The previous attempt changed the session, but its reply may
                    # have failed. Re-show that card instead of silently skipping it.
                    jobs = _get_pending_jobs(limit=20)
                    current_index = _job_index_by_id(jobs, current)
                    if current_index is not None:
                        job = jobs[current_index]
                        _send_telegram(
                            bot_token, chat_id, _format_next_review_card(job),
                            reply_markup=_build_review_card_keyboard(job.get("job_id")),
                        )
                    else:
                        _send_telegram(bot_token, chat_id, "Review queue changed. Use list to refresh.")
                    return
            _handle_next(bot_token, chat_id)
            return

        command = data.split(":", 1)[1]
        _route_command(command, bot_token, chat_id)
        return

    _answer_callback_query(bot_token, callback_query_id, "Unknown action")


def _get_header(event: dict, header_name: str) -> str:
    """Extract a header from an API Gateway event case-insensitively."""
    headers = event.get("headers") or {}
    target = header_name.lower()
    for k, v in headers.items():
        if k.lower() == target:
            return str(v).strip()
    return ""


def _validate_webhook_secret(event: dict, expected_secret: str) -> bool:
    """Validate Telegram secret token header using constant-time comparison."""
    if not expected_secret:
        return True
    provided_token = _get_header(event, "X-Telegram-Bot-Api-Secret-Token")
    if not provided_token:
        return False
    return hmac.compare_digest(provided_token, expected_secret)


def _claim_update_id(update_id: int | str | None) -> bool:
    """Atomically claim a Telegram update_id to prevent duplicate command execution."""
    if not update_id:
        return True
    now = int(time.time())
    try:
        table.put_item(
            Item={
                "job_id": f"upd:{update_id}",
                "entity_type": "telegram_update",
                "created_at": now,
                "ttl": now + 86400,
            },
            ConditionExpression="attribute_not_exists(job_id)",
        )
        return True
    except Exception as e:
        if "ConditionalCheckFailed" in type(e).__name__ or "ConditionalCheckFailed" in str(e):
            logger.info("DUPLICATE_TELEGRAM_UPDATE_IGNORED | update_id=%s", update_id)
            return False
        logger.warning("UPDATE_CLAIM_ERROR: %s", str(e))
        return True


def lambda_handler(event, context):
    """
    Entry point for both SNS notifications and Telegram webhook commands.
    """
    logger.info("ROUTER_EVENT: %s", json.dumps(_sanitize_event_for_logging(event), default=str)[:500])

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
        expected_secret = (
            secrets.get("TELEGRAM_WEBHOOK_SECRET")
            or os.environ.get("TELEGRAM_WEBHOOK_SECRET", "")
        )
        if REQUIRE_TELEGRAM_WEBHOOK_SECRET and not expected_secret:
            logger.error("WEBHOOK_SECRET_REQUIRED_BUT_NOT_CONFIGURED")
            return {"statusCode": 500, "body": "Webhook secret required but not configured"}

        if expected_secret:
            if not _validate_webhook_secret(event, expected_secret):
                logger.warning("UNAUTHORIZED_WEBHOOK_SECRET: missing or invalid secret token header")
                return {"statusCode": 403, "body": "Forbidden: invalid secret token"}
        elif REQUIRE_TELEGRAM_WEBHOOK_SECRET:
            logger.warning("UNAUTHORIZED_WEBHOOK_SECRET: missing secret token header")
            return {"statusCode": 403, "body": "Forbidden: webhook secret required"}

        raw_body = event["body"]
        if raw_body is None:
            logger.warning("MALFORMED_WEBHOOK: body is null")
            return {"statusCode": 400, "body": "Bad Request: body cannot be null"}

        try:
            body = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("MALFORMED_WEBHOOK: invalid JSON: %s", str(e))
            return {"statusCode": 400, "body": "Bad Request: invalid JSON"}

        if not isinstance(body, dict):
            logger.warning("MALFORMED_WEBHOOK: body is not a JSON object (type=%s)", type(body).__name__)
            return {"statusCode": 400, "body": "Bad Request: body must be a JSON object"}

        try:
            callback_query = body.get("callback_query")
            if callback_query is not None:
                if not isinstance(callback_query, dict):
                    logger.warning("MALFORMED_WEBHOOK: callback_query is not a JSON object")
                    return {"statusCode": 400, "body": "Bad Request: callback_query must be a JSON object"}

                data = str(callback_query.get("data") or "")
                callback_query_id = str(callback_query.get("id") or "")
                message = callback_query.get("message")
                if not isinstance(message, dict):
                    message = {}
                chat = message.get("chat")
                incoming_chat_id = str(chat.get("id", "")) if isinstance(chat, dict) else ""
                message_id = _as_int(message.get("message_id"))

                if incoming_chat_id != str(chat_id):
                    logger.warning("UNAUTHORIZED_CALLBACK_CHAT: %s (expected %s)", incoming_chat_id, chat_id)
                    return {"statusCode": 200, "body": "Unauthorized"}

                if data and callback_query_id and message_id:
                    logger.info("TELEGRAM_CALLBACK: '%s' from chat %s", data, incoming_chat_id)
                    try:
                        previous = _get_callback_state(callback_query_id)
                        claim_status, owner = _claim_callback(callback_query_id, incoming_chat_id, data, context)
                    except ValueError as exc:
                        _answer_callback_query(bot_token, callback_query_id, str(exc))
                        return {"statusCode": 400, "body": str(exc)}
                    except Exception:
                        logger.exception("CALLBACK_CLAIM_UNAVAILABLE")
                        return {"statusCode": 503, "body": "Callback storage unavailable"}
                    if claim_status == "duplicate":
                        _answer_callback_query(bot_token, callback_query_id, "Action already processed")
                        return {"statusCode": 200, "body": "Duplicate callback ignored"}
                    if claim_status == "busy":
                        return {"statusCode": 503, "body": "Callback already in progress; retry later"}
                    if claim_status == "invalid":
                        _answer_callback_query(bot_token, callback_query_id, "Open a fresh review card")
                        return {"statusCode": 400, "body": "Callback target unavailable; open a fresh review card"}
                    claim_token = _ACTIVE_CALLBACK_CLAIM.set((callback_query_id, owner))
                    delivery_token = _CALLBACK_DELIVERY_FAILED.set(False)
                    try:
                        claimed = _get_callback_state(callback_query_id)
                        _handle_callback_command(bot_token, chat_id, callback_query_id, data, message_id,
                                                 pinned_target=claimed.get("target_job_id") if claimed else None,
                                                 retrying=previous is not None)
                        if _CALLBACK_DELIVERY_FAILED.get():
                            action = data.split(":")[1].strip().lower() if data.startswith("cmd:") else ""
                            retryable = data.startswith("help:") or action in {
                                "list", "status", "backlog", "followups", "why", "draft", "apply", "skip", "next",
                            }
                            if retryable:
                                raise RuntimeError("Telegram callback reply delivery failed")
                            logger.error("TELEGRAM_CALLBACK_REPLY_FAILED action=%s; action will not repeat", action)
                        state_now = _get_callback_state(callback_query_id)
                        if state_now and state_now.get("status") == "IN_PROGRESS":
                            _record_callback_state(callback_query_id, "COMPLETED", chat_id=incoming_chat_id, action=data)
                    except Exception as exc:
                        try:
                            state_now = _get_callback_state(callback_query_id)
                            if state_now and state_now.get("status") == "IN_PROGRESS":
                                _record_callback_state(callback_query_id, "FAILED", chat_id=incoming_chat_id, action=data)
                        except Exception:
                            logger.exception("CALLBACK_FAILURE_RECORD_UNAVAILABLE")
                        logger.error("CALLBACK_PROCESSING_ERROR: %s", str(exc))
                        raise
                    finally:
                        _CALLBACK_DELIVERY_FAILED.reset(delivery_token)
                        _ACTIVE_CALLBACK_CLAIM.reset(claim_token)
                return {"statusCode": 200}

            message = body.get("message")
            if message is not None:
                if not isinstance(message, dict):
                    logger.warning("MALFORMED_WEBHOOK: message is not a JSON object")
                    return {"statusCode": 400, "body": "Bad Request: message must be a JSON object"}

                update_id = body.get("update_id")
                if update_id and not _claim_update_id(update_id):
                    return {"statusCode": 200, "body": "Duplicate update ignored"}

                raw_text = message.get("text")
                user_text = str(raw_text).strip() if raw_text is not None else ""
                chat = message.get("chat")
                incoming_chat_id = str(chat.get("id", "")) if isinstance(chat, dict) else ""

                if incoming_chat_id != str(chat_id):
                    logger.warning("UNAUTHORIZED_CHAT: %s (expected %s)", incoming_chat_id, chat_id)
                    return {"statusCode": 200, "body": "Unauthorized"}

                if user_text:
                    logger.info("TELEGRAM_CMD: '%s' from chat %s", user_text, incoming_chat_id)
                    _route_command(user_text, bot_token, chat_id)

        except (KeyError, TypeError, AttributeError) as e:
            logger.error("WEBHOOK_PARSE_ERROR: %s", str(e))
            return {"statusCode": 400, "body": f"Bad Request: {str(e)}"}

    return {"statusCode": 200}
