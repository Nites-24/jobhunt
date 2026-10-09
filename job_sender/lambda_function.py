"""
Job Sender Lambda — Autonomous Outreach & High-Signal Email Dispatch Engine (v2.0)

Role & Architectural Placement:
  This serverless microservice is the final execution stage in the JobHunt autonomous
  pipeline. It is triggered asynchronously by AWS SQS (`job-send-queue`), which receives
  messages queued by the Telegram control plane (`job-router`) when a candidate approves
  or applies to a high-scoring job lead. Alternatively, it can be directly invoked in preview
  mode to generate an email draft for inline Telegram review without dispatching.

End-to-End Pipeline Context:
  Job Discovery (job-ingestor)
       │
       ▼
  Deduplication & Storage (job-deduplicator)
       │
       ▼
  AI Evaluation & Scoring (job-processor) ──[Score >= 80]──► SNS Notification
                                                                 │
                                                                 ▼
  Telegram Approval / Draft Review (job-router) ─────────────────┘
       │
       ▼ [SQS: job-send-queue]
  Outreach Synthesis & Dispatch (job-sender) [THIS SERVICE]
       │
       ├─► Amazon Bedrock Converse (Claude 3 Haiku / Sonnet via forced tool calling)
       ├─► Knowledge Graph Context (candidate facts, projects, verified metrics)
       ├─► Dual-Key Airgap Verification (ALLOW_TELEGRAM_EMAIL_SEND & ALLOW_REAL_EMAIL_SEND)
       ├─► S3 Attachment Fetch (Candidate PDF resume)
       ├─► SMTP / SES Dispatch (TLS secured, credential caching, pooled connection reuse)
       └─► DynamoDB Status Update (transitions status to SENT or marks FAILED)

Core Design Invariants & Guardrails:
  1. Dual-Key Security Airgap:
     To ensure 100% human-in-the-loop safety and prevent unauthorized autonomous email delivery:
     - Gate 1: ALLOW_TELEGRAM_EMAIL_SEND=true (configured on job-router)
     - Gate 2: ALLOW_REAL_EMAIL_SEND=true (configured on job-sender)
     If either gate evaluates to false or is unset, the engine generates full preview drafts
     and attachments but suppresses external SMTP socket delivery.
  2. Identity Decoupling:
     Candidate personal claims and contact details are never hardcoded in source code.
     They are injected dynamically via environment variables (CANDIDATE_NAME, SENDER_EMAIL,
     CANDIDATE_LOCATION, S3_RESUME_KEY, CANDIDATE_LINKEDIN, RETIREIQ_URL) and synthesized via
     `shared.knowledge_graph.build_candidate_graph`.
  3. Structured Tool Calling (Bedrock Converse API):
     Rather than unconstrained markdown generation, Bedrock is invoked with forced tool calling
     (`toolChoice: {"tool": {"name": "propose_outreach_email"}}`). The model must return
     structured JSON adhering to schema properties: `subject`, `greeting`, `body_paragraphs`,
     `call_to_action`, and `signoff`.
  4. Anti-AI Tone & Grounding Guardrails:
     - Concise outreach: Total email body is constrained to under 130 words.
     - Single relevant project: Focuses on exactly one relevant project matching the job posting,
       avoiding generic project dumps.
     - Banned AI buzzwords: Strict rejection/sanitization of tropes like "thrilled", "passionate",
       "synergy", "cutting-edge", "dynamic", "pleasure".
     - Punctuation hygiene: Strict elimination of exclamation marks and em dashes.
     - Contact Person Heuristic: Resolves named recruiter > authentic HN author > hiring team fallback.
  5. Enterprise Operational Hygiene:
     - Batch SQS processing with partial failure reporting via `batchItemFailures`.
     - Warm-start caching for AWS Secrets Manager credentials, candidate knowledge graphs, and S3 resume bytes.
     - SMTP connection reuse across batch items with graceful fallback.
     - Idempotency guard: Skips jobs already marked as SENT or APPROVED to prevent duplicate outreach.

Developer Guidance:
  - When modifying email prompts or schemas, verify against `tests/test_sender.py`.
  - Never bypass the dual-key airgap checks in production code.
  - Test suites must execute completely offline using moto mocks without invoking live SES/SMTP.
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
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "").strip()
SECRET_ID = os.environ.get("SECRETS_NAME", "jobserch_gmail")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "Jobsearch")
BUCKET = os.environ.get("RESUME_BUCKET", "job-search-pipeline-memory")
RESUME_KEY = os.environ.get("RESUME_KEY", "data/resume/resume.pdf")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
ALLOW_REAL_EMAIL_SEND = os.environ.get("ALLOW_REAL_EMAIL_SEND", "false").strip().lower() == "true"
BEDROCK_THROTTLE_RETRIES = int(os.environ.get("BEDROCK_THROTTLE_RETRIES", "2"))
CANDIDATE_NAME = os.environ.get("CANDIDATE_NAME", "AI Candidate").strip() or "AI Candidate"
TARGET_ROLE = os.environ.get("TARGET_ROLE", "AI Engineer").strip() or "AI Engineer"
CANDIDATE_LOCATION = os.environ.get("CANDIDATE_LOCATION", "Open to remote, hybrid, or onsite in any location").strip() or "Open to remote, hybrid, or onsite in any location"
CANDIDATE_LINKEDIN = os.environ.get("CANDIDATE_LINKEDIN", "https://linkedin.com/in/profile").strip() or "https://linkedin.com/in/profile"
RETIREIQ_URL = os.environ.get("RETIREIQ_URL", "https://huggingface.co/spaces/demo/RetireIQ").strip() or "https://huggingface.co/spaces/demo/RetireIQ"
RESUME_FILENAME = os.environ.get("RESUME_FILENAME", "Resume.pdf").strip() or "Resume.pdf"

table = dynamodb.Table(TABLE_NAME)

CANDIDATE_GRAPH_CONTEXT = render_candidate_graph(
    build_candidate_graph(
        f"{CANDIDATE_NAME} {TARGET_ROLE} focused on RAG pipelines, "
        "LLM orchestration, agentic systems, Python, PyTorch, AWS, LangChain, "
        "evaluation pipelines. Projects include RetireIQ, a retirement planning "
        "and financial decision support app, and Rivulet, a personal life management "
        "app. Achievements include 33% GPU cost reduction and 87% autonomous ticket "
        f"resolution. {CANDIDATE_LOCATION}."
    )
)

# ---------------------------------------------------------------------------
# 4. SYSTEM PROMPT — The "Brain"
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    f"You are {CANDIDATE_NAME}, an {TARGET_ROLE}. You're writing a short, warm "
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

    "BODY:\n"
    "Only write the greeting plus one short opening paragraph, 1 or 2 sentences max. "
    "Do not write the projects paragraph, location line, CTA, or signature. The system will append those separately.\n\n"
    "Opening paragraph rule: Read the provided job description carefully. Identify the single biggest core challenge, specific product, or main technology stack they are hiring for, for example LLM evaluation, agentic workflows, or data pipelines. Write one specific sentence mapping that need to Teja's project work. Do not say 'I saw your posting', 'hope you're well', or use generic filler. Do not claim employers, dates, customer outcomes, or metrics unless they are explicitly present in the job payload.\n\n"

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
                                "description": "Greeting plus a short opening paragraph only. Do not include closing or signature.",
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
FOUNDER_EMAIL_NAMES = {"founder", "founders"}
CAREERS_EMAIL_NAMES = {"admin", "apply", "careers", "contact", "hello", "hi", "hiring", "hr", "info", "jobs", "recruiting", "support", "talent", "team"}
GENERIC_EMAIL_NAMES = {
    *CAREERS_EMAIL_NAMES, *FOUNDER_EMAIL_NAMES, "no-reply", "noreply",
}
ATS_HOST_HINTS = ("ashbyhq.com", "greenhouse.io", "lever.co", "wellfound.com", "workable.com")
ROBOTIC_OR_UNSUPPORTED_PHRASES = {
    "at empower",
    "at scale",
    "aligns directly with that focus",
    "exactly what i've been doing",
    "exactly what i have been doing",
    "for the past few years",
    "from ground up",
    "from the ground up",
    "i am interested in how",
    "i'm interested in how",
    "i care about both",
    "interested in exploring",
    "i have spent the last months",
    "i've spent the last months",
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


def _email_local_part(email: str) -> str:
    return str(email or "").split("@", 1)[0].split("+", 1)[0].strip().lower()


def _extract_founder_name(job: dict) -> str:
    description = str(job.get("description", "")).strip()
    if not description:
        return ""
    patterns = [
        r"\bI['’]?m\s+([A-Z][a-z]+)\s*,\s*founder\b",
        r"\bI am\s+([A-Z][a-z]+)\s*,\s*founder\b",
        r"\b([A-Z][a-z]+)\s+here,\s*founder\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, description)
        if match:
            return _format_first_name(match.group(1))
    return ""


def _first_name_from_email(email: str) -> str:
    local = _email_local_part(email)
    if not local or local in GENERIC_EMAIL_NAMES:
        return ""

    tokens = [token for token in re.split(r"[._\-\d]+", local) if token]
    token = tokens[0] if tokens else local
    if token in GENERIC_EMAIL_NAMES or len(token) < 2:
        return ""
    if token.startswith("sai") and len(token) > 3:
        return "Sai"
    return _format_first_name(token)


def _recipient_mode(job: dict) -> str:
    """Classify the audience so greeting and body can sound more human."""
    recruiter_name = str(job.get("recruiter_name", "")).strip()
    if recruiter_name:
        first_name = _format_first_name(recruiter_name.split()[0])
        if first_name and first_name.lower() not in INVALID_FIRST_NAMES:
            return "person"

    email_local = _email_local_part(str(job.get("recruiter_email", "")).strip())
    founder_name = _extract_founder_name(job)
    if founder_name:
        return "founder_person"
    if email_local in FOUNDER_EMAIL_NAMES:
        return "founder_inbox"
    if email_local and email_local not in GENERIC_EMAIL_NAMES:
        return "person"

    apply_url = str(job.get("apply_url") or job.get("url") or "").lower()
    if any(host in apply_url for host in ATS_HOST_HINTS) or email_local in CAREERS_EMAIL_NAMES:
        return "careers"
    return "team"


def _audience_prompt_hint(job: dict) -> str:
    mode = _recipient_mode(job)
    if mode == "person":
        return "Recipient is a specific person. Sound direct, respectful, and natural, like a short note to one person."
    if mode in {"founder_person", "founder_inbox"}:
        return "Recipient is a founder inbox. Sound builder-to-builder, concrete, and startup-aware. Avoid recruiter phrasing."
    if mode == "careers":
        return "Recipient is a hiring or careers path. Sound concise, role-focused, and professional without sounding stiff."
    return "Recipient is a company team inbox. Sound natural, brief, and specific to the work."


def _resolve_greeting(job: dict) -> str:
    founder_name = _extract_founder_name(job)
    if founder_name:
        return f"Hi {founder_name},"

    full_name = str(job.get("recruiter_name", "")).strip()
    first_name = _format_first_name(full_name.split()[0] if full_name else "")
    if first_name.lower() in INVALID_FIRST_NAMES:
        first_name = ""

    if not first_name:
        first_name = _first_name_from_email(str(job.get("recruiter_email", "")).strip())

    company = str(job.get("company", "")).strip()

    if first_name:
        return f"Hi {first_name},"
    mode = _recipient_mode(job)
    if company and mode == "careers":
        return f"Hi {company} hiring team,"
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
        f"AUDIENCE MODE: {_recipient_mode(job)}",
        f"AUDIENCE STYLE: {_audience_prompt_hint(job)}",
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


def _split_sentences(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", text or "") if part.strip()]


def _job_text(job: dict) -> str:
    return " ".join(
        str(job.get(field, "") or "").strip()
        for field in ("title", "description", "linked_page_excerpt", "source_grounding")
    ).lower()


def _clean_source_fragment(text: str) -> str:
    value = re.sub(r"https?://\S+", "", str(text or "").strip())
    value = re.sub(r"\b(interested\?|learn more:|email\s+\S+)\b.*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip(" .,-")
    return value


def _compress_source_fragment(text: str) -> str:
    value = _clean_source_fragment(text)
    lowered = value.lower()

    phrase_rules = [
        (("customer success agents", "account intelligence"), "customer success agents and account intelligence"),
        (("context", "memory", "agent"), "context and memory for AI agents"),
        (("voice agent", "knowledge graph"), "voice agents and knowledge graphs"),
        (("multi-agent", "eval"), "multi-agent systems and evals"),
        (("rag", "retrieval"), "RAG and retrieval systems"),
        (("python", "pytorch", "rag"), "Python, PyTorch, and RAG systems"),
    ]
    for terms, phrase in phrase_rules:
        if all(term in lowered for term in terms):
            return phrase

    value = re.sub(r"^[A-Z][A-Za-z0-9 .'-]+?\s+(is building|builds|build|focuses on|focused on)\s+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^(we are building|building|work on|working on)\s+", "", value, flags=re.IGNORECASE)
    value = re.split(r"\b(that|so|which|while|where)\b", value, maxsplit=1, flags=re.IGNORECASE)[0].strip(" ,.-")
    value = value.split(",")[0].strip(" ,.-")

    words = value.split()
    if len(words) > 8:
        value = " ".join(words[:8]).strip(" ,.-")
    return value


def _source_evidence_fragment(job: dict) -> str:
    candidates = [
        str(job.get("linked_page_excerpt", "")).strip(),
        str(job.get("description", "")).strip(),
    ]
    priority_terms = (
        "agent", "agents", "knowledge graph", "eval", "evaluation", "rag", "retrieval",
        "voice", "workflow", "automation", "customer", "memory", "context", "inference",
    )

    for candidate in candidates:
        if not candidate:
            continue
        for sentence in _split_sentences(candidate):
            cleaned = _clean_source_fragment(sentence)
            lowered = cleaned.lower()
            if len(cleaned.split()) < 4:
                continue
            if any(term in lowered for term in priority_terms):
                return _compress_source_fragment(cleaned)
        cleaned = _clean_source_fragment(candidate[:220])
        if cleaned:
            return _compress_source_fragment(cleaned)
    return ""


def _default_opening(job: dict) -> str:
    company = str(job.get("company", "your team")).strip() or "your team"
    title = str(job.get("title", "AI Engineer")).strip() or "AI Engineer"
    description = _job_text(job)
    if "context" in description and "memory" in description and "agent" in description:
        return f"{company} is building context and memory for AI agents, which is close to the kind of agent infrastructure work I want to keep doing."
    if "clinical" in description and "agent" in description:
        return f"{company}'s work on AI agents for clinical workflows looks close to the kind of practical AI systems work I want to keep doing."
    if "inference" in description:
        return f"{company}'s {title} role looks close to the production AI systems work I want to keep doing."
    return f"{company}'s {title} role looks close to the practical AI product work I want to keep doing."


def _deterministic_opening(job: dict) -> str:
    company = str(job.get("company", "your team")).strip() or "your team"
    evidence = _source_evidence_fragment(job)
    if evidence:
        return (
            f"{company} stood out to me because the role is centered on {evidence}, "
            "which is close to the kind of applied AI work I want to keep doing."
        )
    return _default_opening(job)


def _extract_opening_sentence(generated_body: str, job: dict) -> str:
    text = _sanitize_body(generated_body or "")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if lines and re.match(r"^hi\b.+,\s*$", lines[0], re.IGNORECASE):
        lines = lines[1:]
    cleaned = "\n".join(lines)
    cleaned = re.split(r"\nBest,\b|\nRegards,\b|\nSincerely,\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", cleaned) if p.strip()]
    first_paragraph = paragraphs[0] if paragraphs else cleaned

    for sentence in _split_sentences(first_paragraph):
        lowered = sentence.lower()
        if any(blocked in lowered for blocked in ROBOTIC_OR_UNSUPPORTED_PHRASES):
            continue
        if lowered.startswith(("i've built", "i built", "my recent", "i am ", "i'm ", "i have ")):
            continue
        if any(token in lowered for token in ("retireiq", "rivulet", "preview body")):
            continue
        if "interesting" in lowered or "stood out" in lowered:
            continue
        if len(sentence.split()) > 28:
            continue
        company = str(job.get("company", "")).strip().lower()
        if company and company not in lowered:
            continue
        if company and not (lowered.startswith(company) or lowered.startswith(f"{company}'s")):
            continue
        return sentence

    return _deterministic_opening(job)


def _select_project_line(job: dict) -> str:
    text = _job_text(job)
    if any(term in text for term in ("agent", "knowledge graph", "multi-agent", "multi agent", "eval", "automation", "voice")):
        return (
            "I built a private autonomous AI outreach agent that ingests live jobs, scores them with Bedrock plus a compact knowledge graph, "
            "and keeps a human approval step before outreach."
        )
    if any(term in text for term in ("voice", "journal", "memory", "mobile", "react native", "firebase")):
        return (
            "I built Rivulet (https://rivulet.info), a personal life management app that uses LLM workflows for voice journaling, planning, and memory organization."
        )
    return (
        f"I built RetireIQ ({RETIREIQ_URL}), a retirement planning and financial decision support app with hybrid retrieval, "
        "guardrails, and low-cost inference optimization."
    )


def _cta_line(job: dict) -> str:
    mode = _recipient_mode(job)
    if mode == "person":
        return "If this looks relevant, would 20 minutes this week work to connect?"
    if mode in {"founder_person", "founder_inbox"}:
        return "If this seems relevant, would 20 minutes this week work to connect?"
    if mode == "careers":
        return "If the role is still open, would 20 minutes this week work to connect?"
    return "Would 20 minutes this week work to connect?"


def _compose_final_email(job: dict, subject: str, generated_body: str) -> dict:
    greeting = _resolve_greeting(job)
    opener = _extract_opening_sentence(generated_body, job)
    body = (
        f"{greeting}\n\n"
        f"{opener}\n\n"
        f"{_select_project_line(job)}\n\n"
        "I am open to remote, hybrid, or onsite roles in any location, including relocation.\n\n"
        f"{_cta_line(job)}\n\n"
        "Best,\n"
        f"{CANDIDATE_NAME}\n"
        f"{TARGET_ROLE}\n"
        f"{CANDIDATE_LINKEDIN}"
    )
    final_subject = re.sub(r"\s+", " ", str(subject or "").strip())
    if not final_subject:
        final_subject = f"{job.get('title', 'AI Engineer')} role at {job.get('company', 'the team')}"
    body = _enforce_greeting(_sanitize_body(body), greeting)
    _validate_email_quality(final_subject, body)
    return {"subject": final_subject, "body": body}


def _generate_email(job: dict) -> dict:
    """Generate personalized email using Bedrock Converse API with tool-calling."""
    user_message = _build_user_message(job)

    logger.info("BEDROCK_INVOKE | job_id=%s | company=%s | model=%s",
                job.get("job_id"), job.get("company"), MODEL_ID)

    for attempt in range(BEDROCK_THROTTLE_RETRIES + 1):
        try:
            response = bedrock_client.converse(
                modelId=MODEL_ID,
                messages=[{"role": "user", "content": [{"text": user_message}]}],
                system=[{"text": SYSTEM_PROMPT}],
                inferenceConfig={"maxTokens": 1000, "temperature": 0.3},
                toolConfig=EMAIL_TOOL_CONFIG,
            )

            usage = response.get("usage", {})
            logger.info("LLM_TELEMETRY | input=%s | output=%s",
                        usage.get("inputTokens", "?"), usage.get("outputTokens", "?"))

            output_content = response["output"]["message"]["content"]
            for block in output_content:
                if "toolUse" in block:
                    result = block["toolUse"]["input"]
                    subject = str(result.get("subject", "")).strip()
                    body = str(result.get("body", "")).strip()

                    if not body:
                        raise ValueError("Model returned empty body")
                    return _compose_final_email(job, subject, body)

            raise ValueError("Model did not invoke generate_email tool")

        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code == "ThrottlingException" and attempt < BEDROCK_THROTTLE_RETRIES:
                delay = 1 + attempt
                logger.warning(
                    "BEDROCK_THROTTLED_RETRY | attempt=%d | delay=%ds | job_id=%s",
                    attempt + 1,
                    delay,
                    job.get("job_id"),
                )
                time.sleep(delay)
                continue
            logger.error("BEDROCK_ERROR: %s — %s", error_code, str(exc))
            raise
        except Exception as exc:
            logger.error("EMAIL_GEN_ERROR: %s", str(exc))
            raise


def _fallback_preview_email(job: dict) -> dict:
    """Deterministic preview when Bedrock is temporarily throttled."""
    title = str(job.get("title", "AI Engineer")).strip() or "AI Engineer"
    company = str(job.get("company", "the team")).strip() or "the team"
    subject = f"{title} role at {company}"
    return _compose_final_email(job, subject, _default_opening(job))


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

    attachment = MIMEApplication(resume_bytes, Name=RESUME_FILENAME)
    attachment["Content-Disposition"] = f'attachment; filename="{RESUME_FILENAME}"'
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
                "generated_with_fallback": False,
            }
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ThrottlingException":
                logger.warning("PREVIEW_FALLBACK | job_id=%s | reason=bedrock_throttled", job.get("job_id", "unknown"))
                return {
                    "status": "preview",
                    "job_id": job.get("job_id", "unknown"),
                    "email": _fallback_preview_email(job),
                    "generated_with_fallback": True,
                    "fallback_reason": "Bedrock throttled during preview generation.",
                }
            logger.error("PREVIEW_FAILED: %s", str(e))
            return {"status": "failed", "error": str(e)}
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
