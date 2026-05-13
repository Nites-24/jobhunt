"""
Job Sender Lambda — Email Outreach Engine (v2.0)

Pipeline: job-router (approve) → Generate personalized email via Bedrock → SMTP send → DynamoDB update

Improvements over v1:
  - Batch SQS processing with partial failure handling
  - Resume + secrets warm-start caching
  - Idempotency guard (skip already-SENT jobs)
  - SMTP connection reuse across batch
  - Migrated to Bedrock Converse API with forced tool-calling
  - Enhanced sanitization (exclamation points, em dashes, etc.)
  - Real-send kill switch for production demos
"""

import json
import re
import os
import logging
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from shared.knowledge_graph import build_candidate_graph, render_candidate_graph

# ---------------------------------------------------------------------------
# 1. OBSERVABILITY
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# 2. AWS CLIENTS — Global for warm-start reuse
# ---------------------------------------------------------------------------
aws_config = Config(retries={"max_attempts": 3, "mode": "standard"})

s3_client = boto3.client("s3", config=aws_config)
secrets_client = boto3.client("secretsmanager", config=aws_config)
bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1", config=aws_config)
dynamodb = boto3.resource("dynamodb", config=aws_config)

# ---------------------------------------------------------------------------
# 3. CONFIGURATION
# ---------------------------------------------------------------------------
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "vakateja24@gmail.com")
SECRET_ID = os.environ.get("SECRETS_NAME", "jobserch_gmail")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "Jobsearch")
BUCKET = os.environ.get("RESUME_BUCKET", "job-search-pipeline-memory")
RESUME_KEY = os.environ.get("RESUME_KEY", "data/resume/Teja_Vaka.pdf")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
ALLOW_REAL_EMAIL_SEND = os.environ.get("ALLOW_REAL_EMAIL_SEND", "false").strip().lower() == "true"

table = dynamodb.Table(TABLE_NAME)

CANDIDATE_GRAPH_CONTEXT = render_candidate_graph(
    build_candidate_graph(
        "Teja Gopal Reddy Vaka Senior AI Engineer focused on RAG pipelines, "
        "LLM orchestration, agentic systems, Python, PyTorch, AWS, LangChain, "
        "evaluation pipelines, RetireIQ, and Rivulet. Achievements include 33% "
        "GPU cost reduction and 87% autonomous ticket resolution. Open to relocate "
        "and work onsite in the San Francisco Bay Area."
    )
)

# ---------------------------------------------------------------------------
# 4. SYSTEM PROMPT — The "Brain"
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are Teja Gopal Reddy Vaka, a Senior AI Engineer. You're writing a short, warm "
    "cold email to a hiring manager or recruiter. This person skims 50 emails a day. "
    "Your goal is to sound like a real human who read the role and wrote a useful note, not an AI.\n\n"

    "GREETING:\n"
    "The very first line of your email MUST be EXACTLY the text provided in the GREETING TO USE field. "
    "Do not add anything before it. Never change it.\n\n"

    "TONE & STYLE:\n"
    "Write like a direct note to a colleague you respect, not a cover letter. "
    "Short sentences. Plain English. No hype. No fake warmth. No corporate buzzwords. "
    "The line after the greeting should immediately mention the role, company, or specific work they need. "
    "A recruiter should read this and think this person actually looked at the job.\n\n"

    "BODY (2 short paragraphs + ask):\n"
    "Paragraph 1: Read the provided job description carefully. Identify the single biggest core challenge, specific product, or main technology stack they are hiring for, for example LLM evaluation, agentic workflows, or data pipelines. Write one specific sentence mapping that need to Teja's AI-agent project work. Do not say 'I saw your posting', 'hope you're well', or use generic filler. Do not claim employers, dates, customer outcomes, or metrics unless they are explicitly present in the job payload.\n\n"

    "Paragraph 2: Mention your two live projects naturally, explicitly connecting them to the challenge you identified in Paragraph 1. "
    "You MUST include the raw URLs in parentheses exactly as provided: "
    "RetireIQ (https://huggingface.co/spaces/vakaTeja/RetireIQ) handles multi-step financial "
    "reasoning, and Rivulet (https://rivulet.info) does tool orchestration with human oversight. "
    "Weave them into a sentence, don't just drop links.\n\n"

    "Then add: I am fully open to all locations and am ready to relocate or be onsite as "
    "required for this role.\n\n"

    "End with: Would 20 minutes this week work to connect?\n\n"

    "Sign off: Best,\\nTeja Gopal Reddy Vaka\\nSenior AI Engineer\\nhttps://linkedin.com/in/tejagopalreddyvaka/\n\n"

    "HARD RULES:\n"
    "- Under 130 words total\n"
    "- No exclamation points, no em dashes, no ellipsis\n"
    "- No words: leverage, passionate, cutting-edge, synergy, seamlessly, excited, thrilled, "
    "innovative, delighted, keen, resonate, resonated\n"
    "- Do not write cover-letter phrases like 'for the past few years', 'from the ground up', "
    "'exactly what I have been doing', or 'shipping fast with ownership'.\n"
    "- No bullet points or markdown\n"
    "- Do not invent numbers, employer names, customer claims, or production metrics.\n"
    "- Commas and periods only\n"
    "- Sound like a real person who actually read their job description, not ChatGPT\n\n"

    "OUTPUT: You MUST use the generate_email tool. No free text."
)

# Tool config for deterministic structured output
EMAIL_TOOL_CONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "generate_email",
                "description": "Submit the generated email subject and body.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "subject": {
                                "type": "string",
                                "description": "5-7 word subject line referencing the role or company.",
                            },
                            "body": {
                                "type": "string",
                                "description": "Full email body starting with greeting and ending with signature.",
                            },
                        },
                        "required": ["subject", "body"],
                    }
                },
            }
        }
    ],
    "toolChoice": {"tool": {"name": "generate_email"}},
}

INVALID_FIRST_NAMES = {"hiring", "manager", "recruiter", "team", "hr", "talent", "hello", "dear", ""}
GENERIC_EMAIL_NAMES = {
    "admin", "apply", "careers", "contact", "hello", "hi", "hiring", "hr", "info", "jobs",
    "no-reply", "noreply", "recruiting", "support", "talent", "team",
}
ROBOTIC_OR_UNSUPPORTED_PHRASES = {
    "at empower",
    "exactly what i've been doing",
    "exactly what i have been doing",
    "for the past few years",
    "from ground up",
    "from the ground up",
    "i care about both",
    "resonate",
    "resonates",
    "shipping fast with ownership",
}

# ---------------------------------------------------------------------------
# 5. WARM-START CACHES
# ---------------------------------------------------------------------------
_cached_resume: bytes = None
_cached_creds: dict = None


def _fetch_resume() -> bytes:
    """Fetch resume PDF from S3, cached across warm starts."""
    global _cached_resume
    if _cached_resume:
        logger.info("RESUME_CACHE_HIT")
        return _cached_resume
    try:
        _cached_resume = s3_client.get_object(Bucket=BUCKET, Key=RESUME_KEY)["Body"].read()
        logger.info("RESUME_LOADED: %d bytes from s3://%s/%s", len(_cached_resume), BUCKET, RESUME_KEY)
        return _cached_resume
    except ClientError as exc:
        logger.error("S3_FETCH_FAILED: s3://%s/%s — %s", BUCKET, RESUME_KEY, exc)
        raise


def _get_credentials() -> dict:
    """Fetch Gmail credentials from Secrets Manager, cached across warm starts."""
    global _cached_creds
    if _cached_creds:
        return _cached_creds
    try:
        raw = secrets_client.get_secret_value(SecretId=SECRET_ID)["SecretString"]
        _cached_creds = json.loads(raw)
        return _cached_creds
    except ClientError as exc:
        logger.error("SECRETS_FETCH_FAILED: %s — %s", SECRET_ID, exc)
        raise


# ---------------------------------------------------------------------------
# 6. EMAIL GENERATION
# ---------------------------------------------------------------------------
def _format_first_name(value: str) -> str:
    value = re.sub(r"[^A-Za-z]", "", value or "").strip()
    if not value:
        return ""
    return value[:1].upper() + value[1:].lower()


def _first_name_from_email(email: str) -> str:
    local = str(email or "").split("@", 1)[0].split("+", 1)[0].strip().lower()
    if not local or local in GENERIC_EMAIL_NAMES:
        return ""

    tokens = [token for token in re.split(r"[._\-\d]+", local) if token]
    token = tokens[0] if tokens else local
    if token in GENERIC_EMAIL_NAMES or len(token) < 2:
        return ""
    if token.startswith("sai") and len(token) > 3:
        return "Sai"
    return _format_first_name(token)


def _resolve_greeting(job: dict) -> str:
    full_name = str(job.get("recruiter_name", "")).strip()
    first_name = _format_first_name(full_name.split()[0] if full_name else "")
    if first_name.lower() in INVALID_FIRST_NAMES:
        first_name = ""

    if not first_name:
        first_name = _first_name_from_email(str(job.get("recruiter_email", "")).strip())

    company = str(job.get("company", "")).strip()

    if first_name:
        return f"Hi {first_name},"
    if company:
        return f"Hi {company} Team,"
    return "Hi there,"


def _enforce_greeting(body: str, greeting: str) -> str:
    lines = (body or "").strip().splitlines()
    if not lines:
        return greeting
    if lines[0].strip() == greeting:
        return "\n".join(lines).strip()
    if re.match(r"^\s*hi\b.*,\s*$", lines[0], re.IGNORECASE):
        lines[0] = greeting
        return "\n".join(lines).strip()
    return f"{greeting}\n\n" + "\n".join(lines).strip()


def _validate_email_quality(subject: str, body: str) -> None:
    text = f"{subject}\n{body}".lower()
    for phrase in ROBOTIC_OR_UNSUPPORTED_PHRASES:
        if phrase in text:
            raise ValueError(f"Generated email failed quality gate: blocked phrase '{phrase}'")
    if not re.match(r"^\s*hi\b.+,\s*", body, re.IGNORECASE):
        raise ValueError("Generated email failed quality gate: missing greeting")
    word_count = len(re.findall(r"\b[\w']+\b", body))
    if word_count > 160:
        raise ValueError(f"Generated email failed quality gate: too long ({word_count} words)")


def _build_user_message(job: dict) -> str:
    """Construct the user message for Bedrock with greeting logic."""
    greeting = _resolve_greeting(job)
    company = str(job.get("company", "")).strip()
    title = job.get("title", "the role")
    description = job.get("description", "Not provided")
    keywords = job.get("keywords", [])

    logger.info("GREETING: %s | company: %s", greeting, company)

    lines = [
        f"GREETING TO USE: {greeting}",
        f"COMPANY NAME (mention by name in paragraph 1): {company or 'Not provided'}",
        f"ROLE: {title}",
        "CANDIDATE GRAPH:",
        CANDIDATE_GRAPH_CONTEXT,
    ]
    if keywords and isinstance(keywords, list):
        lines.append(f"KEY SKILLS TO REFERENCE: {', '.join(keywords)}")
    lines.append("JOB DESCRIPTION:")
    # Truncate description to avoid token waste
    lines.append(description[:2500] if description else "Not provided")

    return "\n".join(lines)


def _sanitize_body(body: str) -> str:
    """Clean up the email body to match strict formatting rules."""
    # Em dashes → commas
    body = body.replace("\u2014", ",").replace("\u2013", ",").replace("--", ",")
    # Ellipsis → period
    body = body.replace("\u2026", ".").replace("...", ".")
    # Remove exclamation points (per strict rules)
    body = body.replace("!", ".")
    # Smart quotes → straight quotes
    body = body.replace("\u201c", '"').replace("\u201d", '"')
    body = body.replace("\u2018", "'").replace("\u2019", "'")
    # Collapse multiple spaces
    body = re.sub(r" {2,}", " ", body)
    # Collapse multiple newlines (keep max 2)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def _generate_email(job: dict) -> dict:
    """Generate personalized email using Bedrock Converse API with tool-calling."""
    user_message = _build_user_message(job)

    logger.info("BEDROCK_INVOKE | job_id=%s | company=%s | model=%s",
                job.get("job_id"), job.get("company"), MODEL_ID)

    try:
        response = bedrock_client.converse(
            modelId=MODEL_ID,
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            system=[{"text": SYSTEM_PROMPT}],
            inferenceConfig={"maxTokens": 1000, "temperature": 0.3},
            toolConfig=EMAIL_TOOL_CONFIG,
        )

        # Log token usage
        usage = response.get("usage", {})
        logger.info("LLM_TELEMETRY | input=%s | output=%s",
                     usage.get("inputTokens", "?"), usage.get("outputTokens", "?"))

        # Extract tool-call result
        output_content = response["output"]["message"]["content"]
        for block in output_content:
            if "toolUse" in block:
                result = block["toolUse"]["input"]
                subject = str(result.get("subject", "")).strip()
                body = _enforce_greeting(_sanitize_body(str(result.get("body", ""))), _resolve_greeting(job))

                if not subject:
                    raise ValueError("Model returned empty subject")
                if not body:
                    raise ValueError("Model returned empty body")
                _validate_email_quality(subject, body)

                return {"subject": subject, "body": body}

        raise ValueError("Model did not invoke generate_email tool")

    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error("BEDROCK_ERROR: %s — %s", error_code, str(exc))
        raise
    except Exception as exc:
        logger.error("EMAIL_GEN_ERROR: %s", str(exc))
        raise


# ---------------------------------------------------------------------------
# 7. EMAIL SENDING
# ---------------------------------------------------------------------------
def _build_mime_message(sender: str, recipient: str, subject: str, body: str, resume_bytes: bytes):
    """Construct MIME message with resume attachment."""
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(body, "plain", "utf-8"))

    attachment = MIMEApplication(resume_bytes, Name="Teja_Vaka_Resume.pdf")
    attachment["Content-Disposition"] = 'attachment; filename="Teja_Vaka_Resume.pdf"'
    msg.attach(attachment)
    return msg


def _send_via_smtp(sender: str, app_password: str, messages: list) -> list:
    """
    Send multiple emails over a single SMTP connection.
    Returns list of (job_id, success, error) tuples.
    """
    results = []
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(sender, app_password)
            for job_id, msg in messages:
                try:
                    server.send_message(msg)
                    logger.info("EMAIL_SENT | job_id=%s | to=%s | subject=%s",
                                job_id, msg["To"], msg["Subject"])
                    results.append((job_id, True, None))
                except smtplib.SMTPException as e:
                    logger.error("SMTP_SEND_ERROR | job_id=%s | %s", job_id, str(e))
                    results.append((job_id, False, str(e)))
    except smtplib.SMTPException as e:
        logger.error("SMTP_CONNECTION_ERROR: %s", str(e))
        # All unsent messages fail
        sent_ids = {r[0] for r in results}
        for job_id, msg in messages:
            if job_id not in sent_ids:
                results.append((job_id, False, f"Connection error: {str(e)}"))
    return results


# ---------------------------------------------------------------------------
# 8. DYNAMO STATUS UPDATE
# ---------------------------------------------------------------------------
def _mark_status(job_id: str, status: str, reason: str = None) -> None:
    """Update job status in DynamoDB."""
    expr = "SET #s = :s, sent_at = :t"
    attr_names = {"#s": "status"}
    attr_values = {":s": status, ":t": int(time.time())}
    if reason:
        expr += ", failure_reason = :r"
        attr_values[":r"] = reason

    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=expr,
            ExpressionAttributeNames=attr_names,
            ExpressionAttributeValues=attr_values,
        )
    except Exception as e:
        logger.error("DB_UPDATE_ERROR | job_id=%s | %s", job_id, str(e))


# ---------------------------------------------------------------------------
# 9. DECIMAL-SAFE JSON PARSING
# ---------------------------------------------------------------------------
def _deserialize_job(raw) -> dict:
    """Parse job from event, handling DynamoDB Decimal types."""
    if isinstance(raw, str):
        raw = json.loads(raw)
    # Convert any Decimal values to int/float
    if isinstance(raw, dict):
        return {k: (int(v) if isinstance(v, Decimal) else v) for k, v in raw.items()}
    return raw


# ---------------------------------------------------------------------------
# 10. LAMBDA HANDLER — Batch processing with partial failure support
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Process job send requests. Supports:
      - SQS batch records (from queue trigger)
      - Direct invoke (from job-router approval)
      - Preview-only direct invoke for draft generation
    """
    logger.info("SENDER_START | event_keys=%s", list(event.keys()) if isinstance(event, dict) else "?")

    if isinstance(event, str):
        event = json.loads(event)

    if isinstance(event, dict) and event.get("preview_only"):
        try:
            job = _deserialize_job(event.get("job", {}))
            email_data = _generate_email(job)
            return {
                "status": "preview",
                "job_id": job.get("job_id", "unknown"),
                "email": email_data,
            }
        except Exception as e:
            logger.error("PREVIEW_FAILED: %s", str(e))
            return {"status": "failed", "error": str(e)}

    # --- Determine input source ---
    jobs_to_process = []

    if "Records" in event:
        # SQS batch trigger
        for record in event["Records"]:
            try:
                job = _deserialize_job(record["body"])
                jobs_to_process.append((record.get("messageId", "unknown"), job))
            except Exception as e:
                logger.error("RECORD_PARSE_ERROR | %s", str(e))
    elif isinstance(event, dict) and event.get("send_email") and "job" in event:
        job = _deserialize_job(event.get("job", {}))
        jobs_to_process.append(("direct", job))
    else:
        # Direct invoke from job-router
        job = _deserialize_job(event)
        jobs_to_process.append(("direct", job))

    if not jobs_to_process:
        logger.warning("NO_JOBS_TO_PROCESS")
        return {"batchItemFailures": []}

    logger.info("PROCESSING_BATCH | count=%d", len(jobs_to_process))

    if not ALLOW_REAL_EMAIL_SEND:
        logger.warning("REAL_EMAIL_SEND_BLOCKED | count=%d", len(jobs_to_process))
        for _msg_id, job in jobs_to_process:
            job_id = job.get("job_id", "unknown")
            if job_id == "unknown":
                continue
            try:
                current = table.get_item(
                    Key={"job_id": job_id},
                    ProjectionExpression="#s",
                    ExpressionAttributeNames={"#s": "status"},
                ).get("Item", {})
                if current.get("status") == "SENT":
                    logger.info("SEND_DISABLED_SKIP | job_id=%s | already SENT", job_id)
                    continue
            except Exception:
                pass
            _mark_status(
                job_id,
                "MANUAL_APPLY",
                "Real email sending is disabled. Use draft preview and send manually.",
            )
        return {
            "status": "send_disabled",
            "reason": "ALLOW_REAL_EMAIL_SEND is false",
            "batchItemFailures": [],
        }

    # --- Pre-fetch shared resources ---
    try:
        resume_bytes = _fetch_resume()
        creds = _get_credentials()
        app_password = creds["APP_PASSWORD"]
    except Exception as e:
        logger.error("RESOURCE_FETCH_FAILED: %s", str(e))
        # All records fail
        return {
            "batchItemFailures": [
                {"itemIdentifier": msg_id} for msg_id, _ in jobs_to_process if msg_id != "direct"
            ]
        }

    # --- Generate emails ---
    messages_to_send = []  # [(job_id, MIMEMessage)]
    batch_item_failures = []
    no_recruiter_count = 0

    for msg_id, job in jobs_to_process:
        job_id = job.get("job_id", "unknown")

        # Idempotency check — skip if already sent
        try:
            current = table.get_item(Key={"job_id": job_id}, ProjectionExpression="#s",
                                     ExpressionAttributeNames={"#s": "status"}).get("Item", {})
            if current.get("status") == "SENT":
                logger.info("IDEMPOTENCY_SKIP | job_id=%s | already SENT", job_id)
                continue
        except Exception:
            pass  # Non-fatal — proceed with send

        # Generate email
        try:
            recipient = (job.get("recruiter_email") or "").strip()

            # No recruiter email? Skip sending, mark for manual apply
            if not recipient:
                apply_url = job.get("apply_url") or job.get("url") or ""
                logger.info("NO_RECRUITER_EMAIL | job_id=%s | company=%s | apply_url=%s",
                            job_id, job.get("company"), apply_url)
                _mark_status(job_id, "NO_RECRUITER_EMAIL",
                             f"Apply directly: {apply_url}")
                no_recruiter_count += 1
                continue

            email_data = _generate_email(job)
            msg = _build_mime_message(
                sender=SENDER_EMAIL,
                recipient=recipient,
                subject=email_data["subject"],
                body=email_data["body"],
                resume_bytes=resume_bytes,
            )
            messages_to_send.append((job_id, msg))
            logger.info("EMAIL_GENERATED | job_id=%s | to=%s | subject=%s",
                        job_id, recipient, email_data["subject"])
        except Exception as e:
            logger.error("EMAIL_GEN_FAILED | job_id=%s | %s", job_id, str(e))
            _mark_status(job_id, "FAILED", f"EMAIL_GEN_ERROR: {str(e)}")
            if msg_id != "direct":
                batch_item_failures.append({"itemIdentifier": msg_id})

    # --- Send all emails over a single SMTP connection ---
    sent_count = 0
    failed_count = 0
    if messages_to_send:
        send_results = _send_via_smtp(SENDER_EMAIL, app_password, messages_to_send)

        for job_id, success, error in send_results:
            if success:
                _mark_status(job_id, "SENT")
                sent_count += 1
            else:
                _mark_status(job_id, "FAILED", f"SMTP_ERROR: {error}")
                failed_count += 1
                # Find the message_id for this job to add to failures
                for m_id, job in jobs_to_process:
                    if job.get("job_id") == job_id and m_id != "direct":
                        batch_item_failures.append({"itemIdentifier": m_id})
                        break

    logger.info("SENDER_COMPLETE | generated=%d | failures=%d",
                len(messages_to_send), len(batch_item_failures))

    if no_recruiter_count and not sent_count and not failed_count:
        status = "no_recruiter_email"
    elif failed_count:
        status = "failed"
    elif sent_count:
        status = "sent"
    else:
        status = "skipped"

    return {
        "status": status,
        "generated": len(messages_to_send),
        "sent": sent_count,
        "failed": failed_count,
        "no_recruiter_email": no_recruiter_count,
        "batchItemFailures": batch_item_failures,
    }
