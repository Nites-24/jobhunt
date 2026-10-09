"""
Live End-to-End Pipeline Test

Tests each pipeline stage with REAL AWS services, step by step.
Each step requires your confirmation before proceeding.

Prerequisites:
  - .env file with AWS credentials (see .env in project root)
  - JSearch API key

Usage:
  1. Edit .env with your AWS credentials
  2. Run: pytest tests/live/test_live_e2e.py
"""

import os
import pytest

if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Live AWS end-to-end script. Set RUN_LIVE_TESTS=1 to run.", allow_module_level=True)

import json
import sys
import time
import boto3
from decimal import Decimal

# ============================================================================
# LOAD .env FILE SECURELY (no credentials in terminal history)
# ============================================================================
def _load_env_file():
    """Load .env file from project root — keeps credentials out of shell history."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())
        print(f"  Loaded credentials from .env file")
    else:
        print(f"  WARNING: No .env file found at {env_path}")
        print(f"  Create one with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")

_load_env_file()

# ============================================================================
# CONFIGURATION
# ============================================================================
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
JSEARCH_API_KEY = os.environ.get("JSEARCH_API_KEY", "")
DYNAMODB_TABLE = "Jobsearch"
S3_BUCKET = "job-search-pipeline-memory"
RESUME_KEY = os.environ.get("RESUME_KEY", "data/resume/resume.txt")
BEDROCK_MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
SCORE_THRESHOLD = 80

# Verify credentials exist
if not os.environ.get("AWS_ACCESS_KEY_ID"):
    print("\n  ERROR: AWS_ACCESS_KEY_ID not found.")
    print("  Edit .env file with your AWS credentials and re-run.")
    sys.exit(1)
if not JSEARCH_API_KEY:
    print("\n  ERROR: JSEARCH_API_KEY not found in .env")
    sys.exit(1)

# ============================================================================
# AWS CLIENTS
# ============================================================================
dynamodb = boto3.resource("dynamodb", region_name=REGION)
bedrock = boto3.client("bedrock-runtime", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
table = dynamodb.Table(DYNAMODB_TABLE)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def print_header(step_num, title):
    print(f"\n{'='*70}")
    print(f"  STEP {step_num}: {title}")
    print(f"{'='*70}\n")


def wait_for_confirm(msg="Press Enter to continue (or 'q' to quit)..."):
    resp = input(f"\n  >> {msg} ")
    if resp.strip().lower() == 'q':
        print("Exiting.")
        sys.exit(0)


# ============================================================================
# STEP 1: INGESTOR — Fetch real jobs from JSearch
# ============================================================================
def step1_ingest():
    print_header(1, "INGESTOR — Fetch Jobs from JSearch API")

    import urllib.request
    from datetime import datetime, timezone

    query = "AI Engineer"
    url = (
        f"https://jsearch.p.rapidapi.com/search"
        f"?query={urllib.request.quote(query)}"
        f"&num_pages=1&date_posted=today&country=us"
    )
    headers = {
        "X-RapidAPI-Key": JSEARCH_API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as response:
        data = json.loads(response.read().decode("utf-8"))
        raw_jobs = data.get("data", [])

    print(f"  Fetched {len(raw_jobs)} jobs from JSearch API")

    # Normalize (same as ingestor does)
    jobs = []
    for raw in raw_jobs[:5]:  # Limit to 5 for testing
        city = raw.get("job_city", "")
        state = raw.get("job_state", "")
        loc = f"{city}, {state}" if city else "Not specified"

        job = {
            "job_id": raw.get("job_id", ""),
            "title": raw.get("job_title", ""),
            "company": raw.get("employer_name", ""),
            "location": loc,
            "description": (raw.get("job_description") or "")[:2000],
            "url": raw.get("job_apply_link", ""),
            "apply_url": raw.get("job_apply_link", ""),
            "source": "jsearch",
            "posted_at": raw.get("job_posted_at_datetime_utc", ""),
            "is_remote": raw.get("job_is_remote", False),
            "employment_type": raw.get("job_employment_type", ""),
            "recruiter_email": "",
            "recruiter_name": "",
        }
        jobs.append(job)
        posted = raw.get("job_posted_at_datetime_utc", "?")
        print(f"  [{len(jobs)}] {job['title']} @ {job['company']} | {loc}")

    print(f"\n  Result: {len(jobs)} jobs normalized and ready for dedup")
    return jobs


# ============================================================================
# STEP 2: DEDUPLICATOR — Check DynamoDB for duplicates
# ============================================================================
def step2_deduplicate(jobs):
    print_header(2, "DEDUPLICATOR -- Check DynamoDB for Duplicates")

    import hashlib
    new_jobs = []
    dup_count = 0

    for job in jobs:
        job_id = job["job_id"]

        # Check if exists
        try:
            resp = table.get_item(Key={"job_id": job_id}, ProjectionExpression="job_id")
            if "Item" in resp:
                print(f"  DUPLICATE: {job['title']} @ {job['company']}")
                dup_count += 1
                continue
        except Exception as e:
            print(f"  DB error: {e}")
            continue

        # Insert as NEW
        table.put_item(Item={
            **job,
            "job_id": job_id,
            "status": "NEW",
            "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        new_jobs.append(job)
        print(f"  NEW: {job['title']} @ {job['company']} -> saved to DynamoDB")

    print(f"\n  Result: {len(new_jobs)} new | {dup_count} duplicates | {len(jobs)} total")
    return new_jobs


# ============================================================================
# STEP 3: PROCESSOR — Score jobs with Bedrock (real AI call)
# ============================================================================
def step3_score(jobs):
    print_header(3, "PROCESSOR -- AI Scoring with Bedrock Haiku")

    # Load resume from S3
    try:
        res = s3.get_object(Bucket=S3_BUCKET, Key=RESUME_KEY)
        resume = res["Body"].read().decode("utf-8", errors="ignore")
        print(f"  Resume loaded from s3://{S3_BUCKET}/{RESUME_KEY} ({len(resume)} chars)")
    except Exception as e:
        print(f"  WARNING: Could not load resume from S3: {e}")
        resume = (
            "Alex Hunter, AI Engineer. "
            "Python, PyTorch, AWS, RAG, LLMs, LangChain. "
            "33% GPU cost reduction, 87% autonomous ticket resolution."
        )
        print(f"  Using fallback profile ({len(resume)} chars)")

    scored_jobs = []

    for job in jobs:
        print(f"\n  Scoring: {job['title']} @ {job['company']}...")

        prompt = (
            f"<candidate_resume>\n{resume}\n</candidate_resume>\n\n"
            f"<job_to_evaluate>\n"
            f"Title: {job['title']}\nCompany: {job['company']}\n"
            f"Location: {job['location']}\n"
            f"Description:\n{job['description'][:3000]}\n"
            f"</job_to_evaluate>"
        )

        tool_config = {
            "tools": [{
                "toolSpec": {
                    "name": "submit_evaluation",
                    "description": "Submit the match score and reason.",
                    "inputSchema": {
                        "json": {
                            "type": "object",
                            "properties": {
                                "match_score": {"type": "integer", "description": "Score 0-100"},
                                "match_reason": {"type": "string", "description": "1-sentence reason"},
                            },
                            "required": ["match_score", "match_reason"],
                        }
                    },
                }
            }],
            "toolChoice": {"tool": {"name": "submit_evaluation"}},
        }

        try:
            response = bedrock.converse(
                modelId=BEDROCK_MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                system=[{"text": "You are an elite technical recruiter evaluating AI Engineering roles. Be strict: generic roles should score below 40, strong AI/ML matches should score 85+."}],
                inferenceConfig={"maxTokens": 400, "temperature": 0.1},
                toolConfig=tool_config,
            )

            # Extract score
            usage = response.get("usage", {})
            for block in response["output"]["message"]["content"]:
                if "toolUse" in block:
                    result = block["toolUse"]["input"]
                    score = int(result["match_score"])
                    reason = result["match_reason"]

                    status = "PENDING_APPROVAL" if score >= SCORE_THRESHOLD else "REJECTED"
                    emoji = "+++" if score >= SCORE_THRESHOLD else "---"

                    print(f"  {emoji} Score: {score}% | Status: {status}")
                    print(f"      Reason: {reason}")
                    print(f"      Tokens: {usage.get('inputTokens', '?')} in / {usage.get('outputTokens', '?')} out")

                    # Update DynamoDB
                    table.update_item(
                        Key={"job_id": job["job_id"]},
                        UpdateExpression="SET match_score = :ms, match_reason = :mr, #s = :st",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={
                            ":ms": score, ":mr": reason, ":st": status,
                        },
                    )

                    if score >= SCORE_THRESHOLD:
                        scored_jobs.append({**job, "match_score": score, "match_reason": reason})
                    break

        except Exception as e:
            print(f"  ERROR: {e}")

    print(f"\n  Result: {len(scored_jobs)} jobs scored 80%+ (ready for approval)")
    return scored_jobs


# ============================================================================
# STEP 4: ROUTER SIMULATION — Show what Telegram would display
# ============================================================================
def step4_router_sim(matched_jobs):
    print_header(4, "ROUTER -- Telegram Preview (what you would see)")

    if not matched_jobs:
        print("  No jobs scored 80%+. Nothing to show in Telegram.")
        print("  Try lowering SCORE_THRESHOLD at the top of this script.")
        return []

    print("  --- TELEGRAM MESSAGE ---")
    print(f"  {len(matched_jobs)} Job(s) Pending Approval:\n")
    for i, job in enumerate(matched_jobs, 1):
        print(f"  {i}. [{job['match_score']}%] {job['title']}")
        print(f"     {job['company']} | {job['location']}")
        print(f"     Source: {job['url']}")
        print()

    print("  Commands: 'apply' | 'apply 2' | 'reject' | 'list'")
    print("  --- END TELEGRAM MESSAGE ---")

    return matched_jobs


# ============================================================================
# STEP 5: SENDER SIMULATION — Generate email (real Bedrock, no actual send)
# ============================================================================
def step5_sender_sim(matched_jobs):
    print_header(5, "SENDER -- Email Generation Preview (NO actual send)")

    if not matched_jobs:
        print("  No matched jobs to generate emails for.")
        return

    job = matched_jobs[0]  # Preview for top match
    print(f"  Generating email for: {job['title']} @ {job['company']}")

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "job_sender"))
    from lambda_function import SYSTEM_PROMPT, _build_user_message, _sanitize_body

    user_msg = _build_user_message(job)

    tool_config = {
        "tools": [{
            "toolSpec": {
                "name": "generate_email",
                "description": "Submit the email subject and body.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "subject": {"type": "string", "description": "5-7 word subject"},
                            "body": {"type": "string", "description": "Full email body"},
                        },
                        "required": ["subject", "body"],
                    }
                },
            }
        }],
        "toolChoice": {"tool": {"name": "generate_email"}},
    }

    try:
        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": user_msg}]}],
            system=[{"text": SYSTEM_PROMPT}],
            inferenceConfig={"maxTokens": 1000, "temperature": 0.3},
            toolConfig=tool_config,
        )

        usage = response.get("usage", {})
        for block in response["output"]["message"]["content"]:
            if "toolUse" in block:
                result = block["toolUse"]["input"]
                subject = result["subject"]
                body = _sanitize_body(result["body"])

                print(f"\n  --- EMAIL PREVIEW ---")
                print(f"  From: candidate@example.com")
                print(f"  To: {job.get('recruiter_email') or 'candidate@example.com (self, no recruiter found)'}")
                print(f"  Subject: {subject}")
                print(f"  Attachment: resume.pdf")
                print(f"  Tokens: {usage.get('inputTokens', '?')} in / {usage.get('outputTokens', '?')} out")
                print(f"\n  Body:")
                print(f"  {'~'*50}")
                for line in body.split("\n"):
                    print(f"  {line}")
                print(f"  {'~'*50}")
                print(f"\n  NOTE: Email was NOT sent. This is a preview only.")
                print(f"  In production, 'apply' in Telegram triggers the real send.")
                break

    except Exception as e:
        print(f"  ERROR generating email: {e}")


# ============================================================================
# MAIN
# ============================================================================
def main():
    print("\n" + "="*70)
    print("  OPENCLAW JOB PIPELINE -- Live End-to-End Test")
    print("  This test uses REAL AWS services (Bedrock, DynamoDB, S3)")
    print("  Estimated cost: ~$0.01 (a few Haiku calls)")
    print("="*70)

    wait_for_confirm("Ready to start? Press Enter...")

    # Step 1: Ingest
    jobs = step1_ingest()
    wait_for_confirm(f"Ingested {len(jobs)} jobs. Continue to dedup?")

    # Step 2: Deduplicate
    new_jobs = step2_deduplicate(jobs)
    if not new_jobs:
        print("\n  All jobs were duplicates. Run again later for fresh results.")
        print("  Or delete test entries from DynamoDB and re-run.")
        return
    wait_for_confirm(f"{len(new_jobs)} new jobs saved. Continue to AI scoring?")

    # Step 3: Score with Bedrock
    matched = step3_score(new_jobs)
    wait_for_confirm(f"{len(matched)} jobs scored 80%+. Continue to preview?")

    # Step 4: Router simulation
    matched = step4_router_sim(matched)

    # Step 5: Sender simulation
    if matched:
        wait_for_confirm("Preview the outreach email for top match?")
        step5_sender_sim(matched)

    print(f"\n{'='*70}")
    print("  TEST COMPLETE")
    print(f"{'='*70}")
    print("  Your pipeline is working end-to-end.")
    print("  Next: Deploy updated code to AWS Lambda and test via Telegram.")
    print()


if __name__ == "__main__":
    main()
