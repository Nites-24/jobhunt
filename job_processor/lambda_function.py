"""
Job Processor Lambda — Unified Scorer (v2.0)
Merges job-processor + job-evaluator into a single scoring brain.

Pipeline: SQS (filtered-jobs) → Score via Bedrock → DynamoDB update → SNS alert (90%+)

Uses:
  - Bedrock Converse API with forced tool-calling for deterministic JSON output
  - S3 memory context (resume, config, projects) with warm-start caching
  - SQS partial batch failure handling for reliable retries
"""

import json
import os
import logging
import time
import re
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import Tuple, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# 1. OBSERVABILITY
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# 2. AWS CLIENTS — Global for warm-start reuse, retries for resilience
# ---------------------------------------------------------------------------
aws_config = Config(retries={"max_attempts": 3, "mode": "standard"})

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1", config=aws_config)
dynamodb = boto3.resource("dynamodb", region_name="us-east-1", config=aws_config)
sns = boto3.client("sns", region_name="us-east-1", config=aws_config)
s3 = boto3.client("s3", region_name="us-east-1", config=aws_config)

# ---------------------------------------------------------------------------
# 3. CONFIGURATION — All from environment variables with sane defaults
# ---------------------------------------------------------------------------
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "Jobsearch")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "job-search-pipeline-memory")
RESUME_KEY = os.environ.get("RESUME_KEY", "data/resume/Teja_Vaka.txt")
SCORE_THRESHOLD = int(os.environ.get("SCORE_THRESHOLD", "90"))
MAX_JOB_AGE_DAYS = int(os.environ.get("MAX_JOB_AGE_DAYS", "14"))
STALE_SCORE_CAP = int(os.environ.get("STALE_SCORE_CAP", "55"))
MISSING_DATE_SCORE_CAP = int(os.environ.get("MISSING_DATE_SCORE_CAP", "70"))
REMOTE_SCORE_CAP = int(os.environ.get("REMOTE_SCORE_CAP", "80"))
LOW_AI_SIGNAL_SCORE_CAP = int(os.environ.get("LOW_AI_SIGNAL_SCORE_CAP", "50"))

# Context folders in S3 to assemble the full candidate profile
CONTEXT_FOLDERS = os.environ.get("CONTEXT_FOLDERS", "config/,data/,prompts/").split(",")
ALLOWED_EXTENSIONS = (".txt", ".json", ".md", ".csv")

table = dynamodb.Table(TABLE_NAME)

AI_SIGNAL_PATTERN = re.compile(
    r"\b(ai|ml|llm|rag|nlp|machine learning|deep learning|generative ai|"
    r"transformer|pytorch|tensorflow|hugging face|vector database|agentic|"
    r"autonomous agent|model evaluation|language model)\b",
    re.IGNORECASE,
)

BAY_AREA_PATTERN = re.compile(
    r"\b(san francisco|sf bay|bay area|south san francisco|ssf|san mateo|"
    r"mountain view|palo alto|menlo park|redwood city|san jose|oakland|"
    r"berkeley|fremont|sunnyvale|cupertino|santa clara)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 4. WARM-START GLOBAL CACHE — Survives across invocations on the same container
# ---------------------------------------------------------------------------
_cached_context: Optional[str] = None
_context_loaded_at: float = 0
CONTEXT_TTL_SECONDS = 300  # Refresh S3 context every 5 minutes


def _load_s3_file(key: str) -> str:
    """Read a single S3 text file, return empty string on failure."""
    try:
        res = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return res["Body"].read().decode("utf-8", errors="ignore")
    except ClientError as e:
        logger.warning("S3_READ_SKIP: %s — %s", key, e.response["Error"]["Code"])
        return ""


def _build_context_from_s3() -> str:
    """
    Assembles all memory files from S3 into an XML structure for the LLM.
    Merges: resume text + config files + project descriptions + prompt rules.
    Cached globally with TTL for warm-start efficiency.
    """
    global _cached_context, _context_loaded_at

    now = time.time()
    if _cached_context and (now - _context_loaded_at) < CONTEXT_TTL_SECONDS:
        logger.info("CONTEXT_CACHE_HIT: Using cached context (age=%.0fs)", now - _context_loaded_at)
        return _cached_context

    logger.info("CONTEXT_CACHE_MISS: Building context from S3...")
    parts = []

    # Load resume first
    resume_text = _load_s3_file(RESUME_KEY)
    if resume_text:
        parts.append(f"<candidate_resume>\n{resume_text}\n</candidate_resume>")
    else:
        logger.warning("RESUME_MISSING: %s not found, using fallback", RESUME_KEY)
        parts.append(
            "<candidate_resume>\n"
            "Teja Gopal Reddy Vaka — Senior AI Engineer\n"
            "Specializing in RAG pipelines, LLM orchestration, agentic systems.\n"
            "Python, PyTorch, AWS, LangChain, vector databases.\n"
            "Achievements: 33% GPU cost reduction, 87% autonomous ticket resolution.\n"
            "Projects: RetireIQ (HuggingFace), Rivulet (rivulet.info)\n"
            "</candidate_resume>"
        )

    # Load context folders
    for folder in CONTEXT_FOLDERS:
        folder = folder.strip()
        if not folder:
            continue
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=folder)
            if "Contents" not in response:
                continue

            folder_tag = folder.strip("/").replace(" ", "_")
            folder_parts = [f"<{folder_tag}>"]

            for obj in response["Contents"]:
                key = obj["Key"]
                if key.endswith("/") or not key.lower().endswith(ALLOWED_EXTENSIONS):
                    continue
                content = _load_s3_file(key)
                if content:
                    tag = key.split("/")[-1].replace(".", "_").replace(" ", "_")
                    folder_parts.append(f"  <{tag}>\n{content}\n  </{tag}>")

            folder_parts.append(f"</{folder_tag}>")
            parts.append("\n".join(folder_parts))
        except Exception as e:
            logger.error("CONTEXT_FOLDER_ERROR: %s — %s", folder, str(e))

    _cached_context = "\n\n".join(parts)
    _context_loaded_at = now
    logger.info("CONTEXT_BUILT: %d chars from %d sections", len(_cached_context), len(parts))
    return _cached_context


# ---------------------------------------------------------------------------
# 5. BEDROCK SCORING — Deterministic via forced tool-calling
# ---------------------------------------------------------------------------
SYSTEM_INSTRUCTION = (
    "You are an elite technical recruiter evaluating job-candidate fit.\n"
    "The candidate is a Senior AI Engineer specializing in RAG, LLMs, agentic systems, "
    "Python, PyTorch, and AWS.\n\n"
    "SCORING CRITERIA (weight each equally):\n"
    "1. SKILL OVERLAP: How many required skills match the candidate's profile?\n"
    "2. SENIORITY FIT: Does the role level match Senior/Staff/Lead AI Engineer?\n"
    "3. DOMAIN RELEVANCE: Is the domain (AI/ML/NLP/Data) aligned?\n"
    "4. TECH STACK: Does the tech stack overlap (Python, AWS, PyTorch, LLMs)?\n"
    "5. ROLE TYPE: Is this an IC engineering role (not pure management/sales)?\n\n"
    "CRITICAL GUARDRAILS:\n"
    "- FRESHNESS: If the job was posted more than 14 days ago, or if the date is missing/stale, "
    "reduce the final score by 30 points. We prioritize FRESH leads.\n"
    "- LOCATION: The candidate prefers Onsite/Hybrid roles in South San Francisco or general SF Bay Area. "
    "If the job is purely remote or in another country (e.g., Brazil, Colombia), reduce the score by 20 points "
    "unless it is an exceptional match. Pure remote jobs should not score above 80.\n"
    "- RELEVANCE: If the job is purely Frontend (React), Backend (Java/Kotlin), "
    "or generic Full Stack without heavy AI/LLM/ML focus, the score MUST be below 50.\n\n"
    "Score 0-100. Be extremely strict. Only true, fresh, local AI/ML/LLM roles should score 85+.\n\n"
    "Use the submit_evaluation tool to return your score and a 1-sentence justification."
)

TOOL_CONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "submit_evaluation",
                "description": "Submit the final match score and justification for this job.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "match_score": {
                                "type": "integer",
                                "description": "Score from 0 to 100 based on skill overlap, seniority, domain, tech stack, and role type.",
                            },
                            "match_reason": {
                                "type": "string",
                                "description": "A concise 1-sentence justification for the score.",
                            },
                        },
                        "required": ["match_score", "match_reason"],
                    }
                },
            }
        }
    ],
    # Force Claude to use this exact tool — no free-text escape
    "toolChoice": {"tool": {"name": "submit_evaluation"}},
}


def evaluate_job(job: Dict[str, Any], context: str) -> Tuple[int, str]:
    """
    Score a job against the candidate profile using Bedrock Converse API
    with forced tool-calling for deterministic structured output.
    """
    description = job.get("description", "")
    # Truncate to avoid token limits while keeping enough signal
    if len(description) > 6000:
        description = description[:6000] + "\n[TRUNCATED]"

    prompt_text = (
        f"{context}\n\n"
        f"<job_to_evaluate>\n"
        f"Title: {job.get('title', 'Unknown')}\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Location: {job.get('location', 'Not specified')}\n"
        f"Employment Type: {job.get('employment_type', 'Not specified')}\n"
        f"Remote: {job.get('is_remote', 'Unknown')}\n"
        f"Description:\n{description}\n"
        f"</job_to_evaluate>"
    )

    try:
        response = bedrock.converse(
            modelId=MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt_text}]}],
            system=[{"text": SYSTEM_INSTRUCTION}],
            inferenceConfig={"maxTokens": 400, "temperature": 0.1},
            toolConfig=TOOL_CONFIG,
        )

        # Log token usage for cost tracking
        usage = response.get("usage", {})
        logger.info(
            "LLM_TELEMETRY | job_id=%s | input_tokens=%s | output_tokens=%s",
            job.get("job_id", "?"),
            usage.get("inputTokens", "?"),
            usage.get("outputTokens", "?"),
        )

        # Extract the forced tool-call response
        output_content = response["output"]["message"]["content"]
        for block in output_content:
            if "toolUse" in block:
                result = block["toolUse"]["input"]
                score = int(result["match_score"])
                reason = str(result["match_reason"])
                # Clamp score to valid range
                score = max(0, min(100, score))
                return score, reason

        logger.error("TOOL_CALL_MISSING: Model did not invoke submit_evaluation")
        return 0, "Model failed to invoke evaluation tool."

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.error("BEDROCK_CLIENT_ERROR: %s — %s", error_code, str(e))
        if error_code == "ThrottlingException":
            raise  # Let SQS retry
        return 0, f"Bedrock error: {error_code}"
    except Exception as e:
        logger.error("BEDROCK_EVAL_ERROR: %s", str(e))
        return 0, f"Evaluation error: {str(e)}"


def _parse_posted_at(value: Any) -> Optional[datetime]:
    """Parse common job posting timestamp formats into UTC datetimes."""
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


def _job_age_days(job: Dict[str, Any], now: Optional[datetime] = None) -> Optional[int]:
    posted_at = _parse_posted_at(job.get("posted_at"))
    if not posted_at:
        return None

    current = now or datetime.now(timezone.utc)
    age_seconds = max(0, (current - posted_at).total_seconds())
    return int(age_seconds // 86400)


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, Decimal)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _is_remote_job(job: Dict[str, Any]) -> bool:
    location = str(job.get("location", ""))
    return _to_bool(job.get("is_remote")) or "remote" in location.lower()


def _is_bay_area_location(job: Dict[str, Any]) -> bool:
    location = str(job.get("location", ""))
    return bool(BAY_AREA_PATTERN.search(location))


def _has_ai_signal(job: Dict[str, Any]) -> bool:
    text = " ".join(
        str(job.get(field, ""))
        for field in ("title", "description", "keywords", "employment_type")
    )
    return bool(AI_SIGNAL_PATTERN.search(text))


def _blocking_score_cap(cap: int) -> int:
    """Keep hard guardrail caps below the configured approval threshold."""
    return min(cap, max(0, SCORE_THRESHOLD - 1))


def _apply_deterministic_guardrails(job: Dict[str, Any], score: int, reason: str) -> Tuple[int, str]:
    """
    Enforce hard product rules after model scoring.

    The model may still over-score stale or remote keyword matches. These caps keep
    the approval queue aligned with the demo promise: fresh, high-signal AI roles.
    """
    caps: list[tuple[int, str]] = []

    age_days = _job_age_days(job)
    if age_days is None:
        caps.append((_blocking_score_cap(MISSING_DATE_SCORE_CAP), "missing posting date"))
    elif age_days > MAX_JOB_AGE_DAYS:
        caps.append((_blocking_score_cap(STALE_SCORE_CAP), f"posted {age_days} days ago"))

    if _is_remote_job(job) and not _is_bay_area_location(job):
        caps.append((_blocking_score_cap(REMOTE_SCORE_CAP), "remote-only or non-local role"))

    if not _has_ai_signal(job):
        caps.append((_blocking_score_cap(LOW_AI_SIGNAL_SCORE_CAP), "insufficient AI/ML signal"))

    if not caps:
        return score, reason

    capped_score = min([score] + [cap for cap, _ in caps])
    if capped_score < score:
        guardrail_text = ", ".join(detail for _, detail in caps)
        reason = f"{reason} Guardrail applied: {guardrail_text}."
        logger.info(
            "SCORE_GUARDRAIL | job_id=%s | original=%d | capped=%d | reasons=%s",
            job.get("job_id", "?"),
            score,
            capped_score,
            guardrail_text,
        )
    return capped_score, reason


def _pre_llm_rejection(job: Dict[str, Any]) -> Optional[Tuple[int, str]]:
    """
    Reject jobs before Bedrock when hard guardrails make approval impossible.

    This protects the live demo from burst throttling: a HN search can return 100+
    keyword hits, but remote/stale/low-signal jobs can never reach the 90% queue.
    """
    rejection_caps: list[tuple[int, str]] = []

    age_days = _job_age_days(job)
    if age_days is None:
        rejection_caps.append((_blocking_score_cap(MISSING_DATE_SCORE_CAP), "missing posting date"))
    elif age_days > MAX_JOB_AGE_DAYS:
        rejection_caps.append((_blocking_score_cap(STALE_SCORE_CAP), f"posted {age_days} days ago"))

    if _is_remote_job(job) and not _is_bay_area_location(job):
        rejection_caps.append((_blocking_score_cap(REMOTE_SCORE_CAP), "remote-only or non-local role"))

    if not _has_ai_signal(job):
        rejection_caps.append((_blocking_score_cap(LOW_AI_SIGNAL_SCORE_CAP), "insufficient AI/ML signal"))

    if not rejection_caps:
        return None

    score = min(cap for cap, _ in rejection_caps)
    reasons = ", ".join(reason for _, reason in rejection_caps)
    return score, f"Rejected before LLM scoring: {reasons}."


# ---------------------------------------------------------------------------
# 6. DYNAMO UPDATE + SNS NOTIFICATION
# ---------------------------------------------------------------------------
def _update_job_status(job_id: str, score: int, reason: str, status: str, email: str = "") -> None:
    """Update DynamoDB with evaluation results."""
    try:
        update_expr = (
            "SET match_score = :ms, match_reason = :mr, #s = :st, "
            "evaluated_at = :ea, model_id = :mid"
        )
        expr_vals = {
            ":ms": score,
            ":mr": reason,
            ":st": status,
            ":ea": int(time.time()),
            ":mid": MODEL_ID,
        }
        
        if email:
            update_expr += ", recruiter_email = :re"
            expr_vals[":re"] = email

        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=expr_vals,
        )
        logger.info("DB_UPDATE | job_id=%s | score=%d | status=%s", job_id, score, status)
    except ClientError as e:
        logger.error("DB_UPDATE_ERROR | job_id=%s | %s", job_id, str(e))
        raise


def _send_match_notification(job: Dict[str, Any], score: int, reason: str) -> None:
    """Send SNS notification for high-scoring jobs (routed to Telegram via job-router)."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_SKIP: No SNS_TOPIC_ARN configured")
        return

    msg = (
        f"NEW HIGH MATCH: {score}%\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"Company: {job.get('company', 'Unknown')}\n"
        f"Role: {job.get('title', 'Unknown')}\n"
        f"Location: {job.get('location', 'Not specified')}\n\n"
        f"AI Evaluation:\n{reason}\n\n"
        f"Apply: {job.get('url', 'N/A')}\n\n"
        f"Reply 'status' to audit source links, or 'list' to review matches."
    )

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"Match {score}%: {job.get('title', 'Role')}",
            Message=msg,
        )
        logger.info("SNS_SENT | job_id=%s | score=%d", job.get("job_id"), score)
    except ClientError as e:
        logger.error("SNS_ERROR | %s", str(e))
        # Non-fatal — don't crash the batch for a notification failure


# ---------------------------------------------------------------------------
# 7. HUNTER.IO ENRICHMENT
# ---------------------------------------------------------------------------
import urllib.parse
import urllib.request
_cached_hunter_key = None

def _get_hunter_key() -> str:
    global _cached_hunter_key
    if not _cached_hunter_key:
        try:
            sm = boto3.client("secretsmanager", region_name="us-east-1", config=aws_config)
            resp = sm.get_secret_value(SecretId="jobsearch/hunterio")
            _cached_hunter_key = json.loads(resp["SecretString"]).get("api_key", "")
        except Exception as e:
            logger.error("HUNTER_SECRET_ERROR: %s", str(e))
            _cached_hunter_key = ""
    return _cached_hunter_key

def _find_hunter_email(company: str) -> str:
    key = _get_hunter_key()
    if not key or not company:
        return ""
    try:
        url = f"https://api.hunter.io/v2/domain-search?company={urllib.parse.quote(company)}&api_key={key}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read().decode())
            emails = data.get("data", {}).get("emails", [])
            if emails:
                return emails[0].get("value", "")
    except Exception as e:
        logger.warning("HUNTER_API_ERROR | company=%s | error=%s", company, str(e))
    return ""


# ---------------------------------------------------------------------------
# 8. LAMBDA HANDLER — SQS Partial Batch Failure pattern
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Main entry point. Processes SQS batch with partial failure handling.
    Each record is evaluated independently — failing records are retried via SQS.
    """
    records = event.get("Records", [])
    logger.info("BATCH_START | records=%d | function=%s", len(records), context.function_name)

    # Build context once per invocation (cached across warm starts)
    profile_context = _build_context_from_s3()

    batch_item_failures = []
    stats = {"evaluated": 0, "matched": 0, "rejected": 0, "failed": 0}

    for record in records:
        message_id = record.get("messageId", "unknown")
        job_id = "unknown"

        try:
            job = json.loads(record["body"])
            job_id = job.get("job_id", "unknown")

            logger.info(
                "EVAL_START | job_id=%s | title=%s | company=%s",
                job_id, job.get("title"), job.get("company"),
            )

            # Reject impossible matches before Bedrock to avoid burst throttling.
            prefilter_result = _pre_llm_rejection(job)
            if prefilter_result:
                raw_score, raw_reason = prefilter_result
                match_score, match_reason = raw_score, raw_reason
                logger.info(
                    "PREFILTER_REJECT | job_id=%s | score=%d | reason=%s",
                    job_id, match_score, match_reason,
                )
            else:
                raw_score, raw_reason = evaluate_job(job, profile_context)
                match_score, match_reason = _apply_deterministic_guardrails(job, raw_score, raw_reason)
            stats["evaluated"] += 1

            logger.info(
                "EVAL_RESULT | job_id=%s | raw_score=%d | final_score=%d | reason=%s",
                job_id, raw_score, match_score, match_reason,
            )

            # Route based on threshold
            if match_score >= SCORE_THRESHOLD:
                status = "PENDING_APPROVAL"
                stats["matched"] += 1
                
                email = job.get("recruiter_email", "")
                if not email:
                    found_email = _find_hunter_email(job.get("company", ""))
                    if found_email:
                        email = found_email
                        logger.info("HUNTER_SUCCESS | job_id=%s | email=%s", job_id, email)
                
                _update_job_status(job_id, match_score, match_reason, status, email)
                # Update the job object with the email so SNS uses it
                job["recruiter_email"] = email
                _send_match_notification(job, match_score, match_reason)
            else:
                status = "REJECTED"
                stats["rejected"] += 1
                _update_job_status(job_id, match_score, match_reason, status)

        except Exception as e:
            logger.error("RECORD_FAILED | job_id=%s | msg_id=%s | error=%s", job_id, message_id, str(e))
            stats["failed"] += 1
            batch_item_failures.append({"itemIdentifier": message_id})

    logger.info("BATCH_COMPLETE | %s", json.dumps(stats))

    # SQS partial batch failures — only retry the specific records that failed
    return {"batchItemFailures": batch_item_failures}
