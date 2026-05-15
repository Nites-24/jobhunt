"""
Job Processor Lambda — Unified Scorer (v2.0)
Merges job-processor + job-evaluator into a single scoring brain.

Pipeline: SQS (filtered-jobs) → Score via Bedrock → DynamoDB update → SNS alert (80%+)

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
from shared.knowledge_graph import build_candidate_graph, render_candidate_graph

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
SCORE_THRESHOLD = int(os.environ.get("SCORE_THRESHOLD", "80"))
DEFAULT_EVAL_PROFILE_VERSION = f"2026-05-13-th{SCORE_THRESHOLD}"
JOB_EVAL_PROFILE_VERSION = os.environ.get("JOB_EVAL_PROFILE_VERSION", DEFAULT_EVAL_PROFILE_VERSION)
MAX_JOB_AGE_DAYS = int(os.environ.get("MAX_JOB_AGE_DAYS", "14"))
HN_MAX_JOB_AGE_DAYS = int(os.environ.get("HN_MAX_JOB_AGE_DAYS", "3"))
STALE_SCORE_CAP = int(os.environ.get("STALE_SCORE_CAP", "55"))
MISSING_DATE_SCORE_CAP = int(os.environ.get("MISSING_DATE_SCORE_CAP", "70"))
LOW_AI_SIGNAL_SCORE_CAP = int(os.environ.get("LOW_AI_SIGNAL_SCORE_CAP", "50"))
MISSING_SOURCE_SCORE_CAP = int(os.environ.get("MISSING_SOURCE_SCORE_CAP", "40"))

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

NEGATED_AI_SIGNAL_PATTERN = re.compile(
    r"\b(no|not|without)\s+(?:\w+\s+){0,3}"
    r"(ai|ml|llm|rag|nlp|machine learning|deep learning|generative ai|"
    r"machine-learning|language model|modeling)\b|"
    r"\bnon[-\s](ai|ml|llm|rag|nlp|machine learning|language model)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# 4. WARM-START GLOBAL CACHE — Survives across invocations on the same container
# ---------------------------------------------------------------------------
_cached_context: Optional[str] = None
_context_loaded_at: float = 0
CONTEXT_TTL_SECONDS = 300  # Refresh S3 context every 5 minutes


def _source_key(job: Dict[str, Any]) -> tuple[str, str]:
    source = str(job.get("source") or "unknown").strip().lower() or "unknown"
    query = str(job.get("source_search_query") or "").strip() or "-"
    return source, query


def _summarize_source_query_outcomes(results: list[dict]) -> list[dict]:
    summary: dict[tuple[str, str], dict[str, int | str]] = {}
    for result in results:
        source = str(result.get("source") or "unknown")
        query = str(result.get("query") or "-")
        key = (source, query)
        if key not in summary:
            summary[key] = {
                "source": source,
                "query": query,
                "evaluated": 0,
                "matched": 0,
                "rejected": 0,
                "failed": 0,
            }
        bucket = summary[key]
        for field in ("evaluated", "matched", "rejected", "failed"):
            bucket[field] += int(result.get(field, 0) or 0)
    return [summary[key] for key in sorted(summary)]


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
            "Teja Gopal Reddy Vaka — AI Engineer\n"
            "Specializing in RAG pipelines, LLM orchestration, agentic systems.\n"
            "Python, PyTorch, AWS, LangChain, vector databases.\n"
            "Achievements: 33% GPU cost reduction, 87% autonomous ticket resolution.\n"
            "Projects: RetireIQ (retirement planning and financial decision support), "
            "Rivulet (personal life management)\n"
            "Location: currently Wichita, Kansas. Open to relocate and open to remote, hybrid, or onsite roles in any location.\n"
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


def _build_candidate_graph_context() -> str:
    """Build a compact candidate graph from S3 context files."""
    global _cached_context, _context_loaded_at

    now = time.time()
    if _cached_context and (now - _context_loaded_at) < CONTEXT_TTL_SECONDS:
        logger.info("CONTEXT_CACHE_HIT: Using cached graph context (age=%.0fs)", now - _context_loaded_at)
        return _cached_context

    logger.info("CONTEXT_CACHE_MISS: Building compact candidate graph from S3...")
    raw_text_parts = []
    raw_char_count = 0

    resume_text = _load_s3_file(RESUME_KEY)
    if resume_text:
        raw_text_parts.append(resume_text)
        raw_char_count += len(resume_text)
    else:
        logger.warning("RESUME_MISSING: %s not found, using graph fallback", RESUME_KEY)
        raw_text_parts.append(
            "Teja Gopal Reddy Vaka AI Engineer specializing in RAG pipelines, "
            "LLM orchestration, agentic systems, Python, PyTorch, AWS, LangChain, "
            "vector databases. Achievements include 33% GPU cost reduction and 87% "
            "autonomous ticket resolution. Projects include RetireIQ, a retirement planning "
            "and financial decision support app, and Rivulet, a personal life management app. "
            "The candidate is currently based in Wichita, Kansas, is open to relocate, "
            "is open to remote, hybrid, or onsite roles in any location, and is a CPT student."
        )

    for folder in CONTEXT_FOLDERS:
        folder = folder.strip()
        if not folder:
            continue
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=folder)
            if "Contents" not in response:
                continue

            for obj in response["Contents"]:
                key = obj["Key"]
                if key.endswith("/") or not key.lower().endswith(ALLOWED_EXTENSIONS):
                    continue
                content = _load_s3_file(key)
                if content:
                    raw_text_parts.append(content)
                    raw_char_count += len(content)
        except Exception as e:
            logger.error("CONTEXT_FOLDER_ERROR: %s - %s", folder, str(e))

    graph = build_candidate_graph(*raw_text_parts)
    _cached_context = render_candidate_graph(graph)
    _context_loaded_at = now
    logger.info(
        "CONTEXT_GRAPH_BUILT: raw_chars=%d | compact_chars=%d | focus=%s | skills=%s",
        raw_char_count,
        len(_cached_context),
        ",".join(graph.get("focus_areas", [])[:4]),
        ",".join(graph.get("skills", [])[:6]),
    )
    return _cached_context


# ---------------------------------------------------------------------------
# 5. BEDROCK SCORING — Deterministic via forced tool-calling
# ---------------------------------------------------------------------------
SYSTEM_INSTRUCTION = (
    "You are an elite technical recruiter evaluating job-candidate fit.\n"
    "The candidate is an AI Engineer specializing in RAG, LLMs, agentic systems, "
    "Python, PyTorch, and AWS.\n\n"
    "SCORING CRITERIA (weight each equally):\n"
    "1. SKILL OVERLAP: How many required skills match the candidate's profile?\n"
    "2. SENIORITY FIT: Does the role level match an individual-contributor AI Engineer path, including AI Engineer, Applied AI Engineer, or ML Engineer?\n"
    "3. DOMAIN RELEVANCE: Is the domain (AI/ML/NLP/Data) aligned?\n"
    "4. TECH STACK: Does the tech stack overlap (Python, AWS, PyTorch, LLMs)?\n"
    "5. ROLE TYPE: Is this an IC engineering role (not pure management/sales)?\n\n"
    "CRITICAL GUARDRAILS:\n"
    "- FRESHNESS: If the job was posted more than 14 days ago, or if the date is missing/stale, "
    "reduce the final score by 30 points. We prioritize FRESH leads.\n"
    "- LOCATION: The candidate is currently based in Wichita, Kansas and is open to relocate. "
    "Strong remote, hybrid, and onsite roles in any location should still score well. "
    "Do not hard-penalize a strong AI role only because it is outside Kansas or requires relocation.\n"
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


def _source_max_age_days(job: Dict[str, Any]) -> int:
    source = str(job.get("source", "")).strip().lower()
    if source in {"hackernews", "hn"}:
        return HN_MAX_JOB_AGE_DAYS
    return MAX_JOB_AGE_DAYS


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, Decimal)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _is_remote_job(job: Dict[str, Any]) -> bool:
    location = str(job.get("location", ""))
    return _to_bool(job.get("is_remote")) or "remote" in location.lower()


def _has_ai_signal(job: Dict[str, Any]) -> bool:
    text = " ".join(
        str(job.get(field, ""))
        for field in ("title", "description", "keywords", "employment_type")
    )
    if NEGATED_AI_SIGNAL_PATTERN.search(text):
        return False
    return bool(AI_SIGNAL_PATTERN.search(text))


def _has_source_link(job: Dict[str, Any]) -> bool:
    source_url = str(job.get("url") or job.get("apply_url") or "").strip()
    return source_url.startswith("http://") or source_url.startswith("https://")


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
    max_age_days = _source_max_age_days(job)
    if age_days is None:
        caps.append((_blocking_score_cap(MISSING_DATE_SCORE_CAP), "missing posting date"))
    elif age_days > max_age_days:
        caps.append((_blocking_score_cap(STALE_SCORE_CAP), f"posted {age_days} days ago, source limit {max_age_days} days"))

    if not _has_ai_signal(job):
        caps.append((_blocking_score_cap(LOW_AI_SIGNAL_SCORE_CAP), "insufficient AI/ML signal"))

    if not _has_source_link(job):
        caps.append((_blocking_score_cap(MISSING_SOURCE_SCORE_CAP), "missing verifiable source link"))

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
    keyword hits, but remote/stale/low-signal jobs can never reach the 80% queue.
    """
    rejection_caps: list[tuple[int, str]] = []

    age_days = _job_age_days(job)
    max_age_days = _source_max_age_days(job)
    if age_days is None:
        rejection_caps.append((_blocking_score_cap(MISSING_DATE_SCORE_CAP), "missing posting date"))
    elif age_days > max_age_days:
        rejection_caps.append((_blocking_score_cap(STALE_SCORE_CAP), f"posted {age_days} days ago, source limit {max_age_days} days"))

    if not _has_ai_signal(job):
        rejection_caps.append((_blocking_score_cap(LOW_AI_SIGNAL_SCORE_CAP), "insufficient AI/ML signal"))

    if not _has_source_link(job):
        rejection_caps.append((_blocking_score_cap(MISSING_SOURCE_SCORE_CAP), "missing verifiable source link"))

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
            "evaluated_at = :ea, model_id = :mid, evaluation_profile_version = :epv"
        )
        expr_vals = {
            ":ms": score,
            ":mr": reason,
            ":st": status,
            ":ea": int(time.time()),
            ":mid": MODEL_ID,
            ":epv": JOB_EVAL_PROFILE_VERSION,
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
        f"Location: {job.get('location', 'Not specified')}\n"
        f"Work mode: {job.get('work_mode', 'unknown') or 'unknown'}\n"
        f"Work mode evidence: {job.get('work_mode_source', 'not found in source') or 'not found in source'}\n\n"
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

    # Build compact profile context once per invocation (cached across warm starts)
    profile_context = _build_candidate_graph_context()

    batch_item_failures = []
    stats = {"evaluated": 0, "matched": 0, "rejected": 0, "failed": 0}
    source_query_results = []

    for record in records:
        message_id = record.get("messageId", "unknown")
        job_id = "unknown"
        job: Dict[str, Any] = {}

        try:
            job = json.loads(record["body"])
            job_id = job.get("job_id", "unknown")
            source, query = _source_key(job)

            logger.info(
                "EVAL_START | job_id=%s | title=%s | company=%s",
                job_id, job.get("title"), job.get("company"),
            )

            if job.get("signal_test"):
                raw_score = 96
                raw_reason = "Synthetic signal test seed routed through the live pipeline."
                match_score, match_reason = raw_score, raw_reason
                logger.info("SIGNAL_TEST_MATCH | job_id=%s | score=%d", job_id, match_score)
            else:
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
            source_query_results.append({
                "source": source,
                "query": query,
                "evaluated": 1,
                "matched": 1 if status == "PENDING_APPROVAL" else 0,
                "rejected": 1 if status == "REJECTED" else 0,
                "failed": 0,
            })

        except Exception as e:
            logger.error("RECORD_FAILED | job_id=%s | msg_id=%s | error=%s", job_id, message_id, str(e))
            stats["failed"] += 1
            batch_item_failures.append({"itemIdentifier": message_id})
            source, query = _source_key(job)
            source_query_results.append({
                "source": source,
                "query": query,
                "evaluated": 0,
                "matched": 0,
                "rejected": 0,
                "failed": 1,
            })

    logger.info("BATCH_COMPLETE | %s", json.dumps(stats))
    logger.info("PROCESSOR_YIELD_BY_SOURCE_QUERY | %s", json.dumps(_summarize_source_query_outcomes(source_query_results)))

    # SQS partial batch failures — only retry the specific records that failed
    return {"batchItemFailures": batch_item_failures}
